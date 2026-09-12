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


class TestDeclaredGpus:
    def test_absent_reads_as_none(self, tmp_path):
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


DECLARED = {"gpus": {"gpus": "required", "gpu_memory_gb": 8.0}}
OPTIONAL = {"gpus": {"gpus": "optional", "gpu_memory_gb": 8.0}}
HAS_GPU = ["msm_gpu 8192"]


class TestAutomaticGpuArgs:
    @pytest.mark.parametrize(
        "runtime,expected", [(Runtime.DOCKER, ["--gpus", "all"]), (Runtime.APPTAINER, ["--nv"])]
    )
    def test_declaring_step_gets_its_runtime_dialect(self, tmp_path, runtime, expected):
        dep = _dep("image")
        ctx = _context(tmp_path, runtime, DECLARED, shell=ScriptedShell(HAS_GPU), image_dep=dep)
        container = ctx.GetContainerModel(dep)
        assert container.extra_args[: len(expected)] == expected

    def test_non_declaring_step_gets_nothing(self, tmp_path):
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.APPTAINER, {"cpus": 4}, shell=ScriptedShell(HAS_GPU), image_dep=dep)
        assert ctx.GetContainerModel(dep).extra_args == []

    @pytest.mark.parametrize("runtime", [Runtime.DOCKER, Runtime.APPTAINER])
    def test_no_device_present_means_no_flags(self, tmp_path, runtime):
        dep = _dep("image")
        ctx = _context(tmp_path, runtime, OPTIONAL, shell=ScriptedShell([]), image_dep=dep)
        assert ctx.GetContainerModel(dep).extra_args == []

    def test_detection_is_probed_once_per_context(self, tmp_path):
        dep = _dep("image")
        shell = ScriptedShell(HAS_GPU)
        ctx = _context(tmp_path, Runtime.DOCKER, DECLARED, shell=shell, image_dep=dep)
        ctx.GetContainerModel(dep)
        ctx.GetContainerModel(dep)
        assert sum("nvidia-smi" in c for c in shell.calls) == 1

    def test_caller_supplied_flag_is_not_duplicated(self, tmp_path):
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.APPTAINER, DECLARED, shell=ScriptedShell(HAS_GPU), image_dep=dep)
        container = ctx.GetContainerModel(dep, args=["--nv", "--env", "FOO=bar"])
        assert container.extra_args.count("--nv") == 1
        assert container.MakeRunCommand().count("--nv") == 1

    def test_caller_args_are_preserved_alongside_framework_flags(self, tmp_path):
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.DOCKER, DECLARED, shell=ScriptedShell(HAS_GPU), image_dep=dep)
        container = ctx.GetContainerModel(dep, args=["--shm-size=8g"])
        assert container.extra_args == ["--gpus", "all", "--shm-size=8g"]

    def test_flags_reach_the_emitted_run_command(self, tmp_path):
        dep = _dep("image")
        ctx = _context(tmp_path, Runtime.APPTAINER, DECLARED, shell=ScriptedShell(HAS_GPU), image_dep=dep)
        cmd = ctx.GetContainerModel(dep).MakeRunCommand()
        assert "--nv" in cmd
