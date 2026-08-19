from __future__ import annotations

import json
from pathlib import Path

import pytest

import metasmith.agents.workflow_ops as _agents
from metasmith.agents import Agent, GpuRequirementError
from metasmith.constants import AgentPaths
from metasmith.coms.terminals import ShellResult
from metasmith.models.libraries import Gpu, Size
from metasmith.models.remote import Source


TASK_KEY = "testtask01"


class FakeShell:
    def __init__(self, manifest: dict, gpu_present: str = "", manifest_text: str | None = None):
        self.manifest = manifest
        self.gpu_present = gpu_present
        self.manifest_text = manifest_text
        self.calls: list[str] = []

    def Exec(self, cmd, timeout=None, history=False, quiet=False, **_) -> ShellResult:
        self.calls.append(cmd)
        if "workspace exists" in cmd:
            return ShellResult(out=["workspace exists"], err=[])
        if "launcher-present" in cmd:
            return ShellResult(out=["launcher-present"], err=[])
        if AgentPaths.GPU_MANIFEST in cmd:
            if self.manifest_text is not None:
                return ShellResult(out=self.manifest_text.splitlines(), err=[])
            return ShellResult(out=[json.dumps({"schema": 1, "steps": self.manifest})], err=[])
        if "nvidia-smi" in cmd:
            return ShellResult(out=[self.gpu_present] if self.gpu_present else [], err=[])
        return ShellResult(out=[], err=[])

    @property
    def launched(self) -> bool:
        return any(AgentPaths.LAUNCHER_FILE in c and "-e" not in c for c in self.calls)


class RecordingMover:
    sent: dict[str, str] = {}

    def __init__(self, *a, **kw):
        self._queued: list[tuple[Path, Path]] = []

    def QueueTransfer(self, src, dest):
        self._queued.append((Path(str(src.GetPath())), Path(str(dest.GetPath()))))

    def ExecuteTransfers(self, wait_for_complete=False):
        for src, dest in self._queued:
            if src.exists() and src.is_file():
                RecordingMover.sent[dest.name] = src.read_text()
        return []


def _step(name: str, order: int, toggle: str, gb: float | None):
    return {
        f"p{order:02}__{name}": {
            "step": order, "transform": name, "process": f"p{order:02}__{name}",
            "gpus": toggle, "gpu_memory_gb": gb,
        }
    }


@pytest.fixture
def agent(tmp_path, monkeypatch):
    home = tmp_path / "msm_home"
    (home / AgentPaths.STAGED / TASK_KEY / AgentPaths.INTERNALS / AgentPaths.TASK).mkdir(parents=True)
    (home / "relay").mkdir(parents=True, exist_ok=True)
    RecordingMover.sent = {}
    monkeypatch.setattr(_agents, "Logistics", RecordingMover)
    return Agent(home=Source.FromLocal(home))


def _run(monkeypatch, agent, manifest, gpus=None, gpu_present="", config=None, manifest_text=None):
    shell = FakeShell(manifest, gpu_present, manifest_text)

    class _AgentShell:
        def __init__(self, _agent): pass
        def __enter__(self): return shell
        def __exit__(self, *a): return False

    monkeypatch.setattr(_agents, "AgentShell", _AgentShell)
    cfg = config or agent.GetNxfConfigPresets()["slurm"]
    agent.RunWorkflow(TASK_KEY, config_file=cfg, gpus=gpus)
    return shell


class TestPreflightBlocksTheLaunch:
    def test_required_gpu_with_no_declaration_raises_before_launching(self, monkeypatch, agent):
        manifest = _step("prott5", 1, "required", 40.0)
        with pytest.raises(GpuRequirementError) as e:
            _run(monkeypatch, agent, manifest)
        assert "prott5" in str(e.value)
        assert RecordingMover.sent == {}

    def test_error_names_a_device_it_can_see(self, monkeypatch, agent):
        manifest = _step("prott5", 1, "required", 40.0)
        with pytest.raises(GpuRequirementError) as e:
            _run(monkeypatch, agent, manifest, gpu_present="GPU 0: NVIDIA GeForce RTX 3060 Ti")
        assert "RTX 3060 Ti" in str(e.value)

    def test_optional_only_proceeds_and_requests_no_gpu(self, monkeypatch, agent):
        manifest = _step("bwa", 1, "optional", 8.0)
        shell = _run(monkeypatch, agent, manifest)
        assert shell.launched
        cfg = RecordingMover.sent[AgentPaths.NXF_CONFIG]
        assert "xgpux" not in cfg, "no device declared -> no GPU config at all"

    def test_no_gpu_steps_at_all_is_untouched(self, monkeypatch, agent):
        shell = _run(monkeypatch, agent, {})
        assert shell.launched
        base = (agent.GetNxfConfigPresets()["slurm"]).read_text()
        assert RecordingMover.sent[AgentPaths.NXF_CONFIG] == base


class TestUnreadableManifest:
    def test_truncated_manifest_refuses_rather_than_skipping_the_check(self, monkeypatch, agent):
        truncated = '{"schema":1,"steps":{"p01__x":{"gpus":"required"'
        with pytest.raises(GpuRequirementError) as e:
            _run(monkeypatch, agent, {}, manifest_text=truncated)
        assert "could not be parsed" in str(e.value)
        assert RecordingMover.sent == {}


class TestDeclaredRunRendersRequests:
    def test_per_step_request_reaches_the_agent_config(self, monkeypatch, agent):
        manifest = _step("prott5", 1, "required", 40.0) | _step("bwa", 2, "optional", 8.0)
        shell = _run(monkeypatch, agent, manifest, gpus=Gpu(memory=Size.GB(80)))
        assert shell.launched
        cfg = RecordingMover.sent[AgentPaths.NXF_CONFIG]
        assert "withLabel: 'xgpux'" in cfg
        assert "withName: 'p01__prott5'" in cfg
        assert "withName: 'p02__bwa'" in cfg
        assert '" --gpus-per-node=1"' in cfg
        assert "--nodes=1 --ntasks=1" in cfg

    def test_spanning_request_renders_the_larger_count(self, monkeypatch, agent):
        manifest = _step("prott5", 1, "required", 40.0)
        _run(monkeypatch, agent, manifest, gpus=Gpu(memory=Size.GB(20)))
        assert '" --gpus-per-node=2"' in RecordingMover.sent[AgentPaths.NXF_CONFIG]

    def test_local_preset_gets_the_label_block_but_no_cluster_options(self, monkeypatch, agent):
        manifest = _step("prott5", 1, "required", 8.0)
        local = agent.GetNxfConfigPresets()["local"]
        _run(monkeypatch, agent, manifest, gpus=Gpu(memory=Size.GB(8)), config=local)
        cfg = RecordingMover.sent[AgentPaths.NXF_CONFIG]
        assert "withLabel: 'xgpux'" in cfg
        assert "clusterOptions" not in cfg


class TestGenericSchedulerInjection:
    def test_nested_params_do_not_clobber_each_other(self, monkeypatch, agent):
        monkeypatch.setattr(_agents, "AgentShell", None)
        shell = FakeShell({})

        class _AgentShell:
            def __init__(self, _agent): pass
            def __enter__(self): return shell
            def __exit__(self, *a): return False

        monkeypatch.setattr(_agents, "AgentShell", _AgentShell)
        agent.RunWorkflow(
            TASK_KEY,
            config_file=agent.GetNxfConfigPresets()["slurm"],
            params={"process_tries": 3, "process_clusterOptionsExtra": "--partition=bigmem", "slurmAccount": "st-x"},
        )
        import yaml
        sent = yaml.safe_load(RecordingMover.sent[AgentPaths.NXF_PARAMS])
        assert sent["process"] == {"tries": 3, "clusterOptionsExtra": "--partition=bigmem"}
        assert sent["slurmAccount"] == "st-x"
