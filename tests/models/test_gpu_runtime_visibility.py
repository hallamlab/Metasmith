"""What a protocol can see about GPUs while it runs.

Two questions a transform needs answered, and they are different questions:

- *What did I ask for?* — `DeclaredGpus()`, static, from the step meta staged by
  the generator and read back into `context.params["gpus"]` by the bootstrap.
- *What did I get?* — `DetectGpus()`, probed on the execution host through
  `external_shell`. That shell is the relay under a container runtime and the
  local shell under mamba/native, which is what makes one implementation correct
  everywhere. Under a partial allocation or a MIG slice the two answers differ,
  and the second is the one a tool sizing its own offload needs.

Plus the seam that removes the last documented reason to branch on the runtime:
a step that declared a GPU gets `--nv` / `--gpus all` on its tool container for
free, in the right dialect, without duplicating a flag the caller already passed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.coms.terminals import ShellResult
from metasmith.env import Runtime
from metasmith.models.libraries import (
    ContextData,
    ContextPath,
    ExecutionContext,
    Gpus,
    Size,
)
from metasmith.models.solver import Dependency, Endpoint


class ScriptedShell:
    """Returns canned stdout lines and records what it was asked."""

    def __init__(self, out: list[str] | None = None, raises: Exception | None = None):
        self.out = out or []
        self.raises = raises
        self.calls: list[str] = []

    def Exec(self, cmd: str, timeout=None, history: bool = False) -> ShellResult:
        self.calls.append(cmd)
        if self.raises is not None:
            raise self.raises
        return ShellResult(out=list(self.out), err=[])


def _dep(name: str) -> Dependency:
    return Dependency(properties={name}, parents=set())


def _context(tmp_path: Path, runtime: Runtime, params: dict, shell=None, image_dep=None):
    sif = tmp_path / "tool.sif"
    sif.write_bytes(b"\x00\x01\x02\x03")
    uri = "/hpc/home/containers/tool.sif"
    cp = ContextPath(local=sif, external=Path(uri), container=Path(uri))
    cd = ContextData(input_group=[cp], endpoint=Endpoint(properties={"image"}), type_name="image")
    image_dep = image_dep or _dep("image")
    return ExecutionContext(
        _inputs=[{image_dep: cd}],
        _get_output_paths=lambda *a: None,
        external_shell=shell or ScriptedShell(),
        external_cwd=Path("/hpc/home/work"),
        external_agent_home=Path("/hpc/home/msm_home"),
        _environment=runtime,
        params=params,
    )


# --------------------------------------------------------------------------
# what did I ask for
# --------------------------------------------------------------------------

class TestDeclaredGpus:
    def test_absent_reads_as_none(self, tmp_path):
        # every non-GPU step, and every workspace staged before GPU support
        ctx = _context(tmp_path, Runtime.DOCKER, {"cpus": 4})
        assert ctx.DeclaredGpus() == (Gpus.NONE, None)

    def test_required_with_memory(self, tmp_path):
        ctx = _context(tmp_path, Runtime.DOCKER, {"gpus": {"gpus": "required", "gpu_memory_gb": 40.0}})
        toggle, mem = ctx.DeclaredGpus()
        assert toggle is Gpus.REQUIRED
        assert mem is not None and mem.value_gb == 40.0

    def test_optional_without_memory(self, tmp_path):
        ctx = _context(tmp_path, Runtime.DOCKER, {"gpus": {"gpus": "optional", "gpu_memory_gb": None}})
        assert ctx.DeclaredGpus() == (Gpus.OPTIONAL, None)

    def test_unknown_toggle_degrades_to_none_rather_than_crashing(self, tmp_path):
        ctx = _context(tmp_path, Runtime.DOCKER, {"gpus": {"gpus": "sometimes"}})
        assert ctx.DeclaredGpus()[0] is Gpus.NONE


# --------------------------------------------------------------------------
# what did I get
# --------------------------------------------------------------------------

class TestDetectGpus:
    def test_reports_per_device_memory(self, tmp_path):
        shell = ScriptedShell(["msm_gpu 8192", "msm_gpu 8192"])
        ctx = _context(tmp_path, Runtime.APPTAINER, {}, shell=shell)
        found = ctx.DetectGpus()
        assert len(found) == 2
        assert all(f.value_gb == 8.0 for f in found)

    def test_no_nvidia_smi_is_an_empty_answer_not_an_error(self, tmp_path):
        ctx = _context(tmp_path, Runtime.MAMBA, {}, shell=ScriptedShell([]))
        assert ctx.DetectGpus() == []

    def test_unrelated_shell_noise_is_ignored(self, tmp_path):
        # the relay shell interleaves its own chatter; only tagged lines count
        shell = ScriptedShell(["connecting...", "msm_gpu 24576", "done"])
        ctx = _context(tmp_path, Runtime.APPTAINER, {}, shell=shell)
        found = ctx.DetectGpus()
        assert len(found) == 1 and found[0].value_gb == 24.0

    def test_probe_runs_on_the_execution_host_shell(self, tmp_path):
        shell = ScriptedShell([])
        ctx = _context(tmp_path, Runtime.APPTAINER, {}, shell=shell)
        ctx.DetectGpus()
        assert len(shell.calls) == 1
        assert "nvidia-smi" in shell.calls[0]

    def test_shell_failure_degrades_to_empty(self, tmp_path):
        ctx = _context(tmp_path, Runtime.DOCKER, {}, shell=ScriptedShell(raises=RuntimeError("relay down")))
        assert ctx.DetectGpus() == []


# --------------------------------------------------------------------------
# automatic per-runtime GPU flags
# --------------------------------------------------------------------------

DECLARED = {"gpus": {"gpus": "required", "gpu_memory_gb": 8.0}}


class TestAutomaticGpuArgs:
    @pytest.mark.parametrize(
        "runtime,expected", [(Runtime.DOCKER, ["--gpus", "all"]), (Runtime.APPTAINER, ["--nv"])]
    )
    def test_declaring_step_gets_its_runtime_dialect(self, tmp_path, runtime, expected):
        dep = _dep("image")
        ctx = _context(tmp_path, runtime, DECLARED, image_dep=dep)
        container = ctx.GetContainerModel(dep)
        assert container.extra_args[: len(expected)] == expected

    def test_non_declaring_step_gets_nothing(self, tmp_path):
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.APPTAINER, {"cpus": 4}, image_dep=dep)
        assert ctx.GetContainerModel(dep).extra_args == []

    def test_caller_supplied_flag_is_not_duplicated(self, tmp_path):
        # transforms that hardcoded --nv before this existed must keep working
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.APPTAINER, DECLARED, image_dep=dep)
        container = ctx.GetContainerModel(dep, args=["--nv", "--env", "FOO=bar"])
        assert container.extra_args.count("--nv") == 1
        assert container.MakeRunCommand().count("--nv") == 1

    def test_caller_args_are_preserved_alongside_framework_flags(self, tmp_path):
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.DOCKER, DECLARED, image_dep=dep)
        container = ctx.GetContainerModel(dep, args=["--shm-size=8g"])
        assert container.extra_args == ["--gpus", "all", "--shm-size=8g"]

    def test_flags_reach_the_emitted_run_command(self, tmp_path):
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.APPTAINER, DECLARED, image_dep=dep)
        cmd = ctx.GetContainerModel(dep).MakeRunCommand()
        assert "--nv" in cmd
