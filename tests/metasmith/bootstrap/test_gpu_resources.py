from __future__ import annotations

import json

import pytest

from metasmith import agents as _agents
from metasmith.agents import (
    GpuRequirementError,
    _plan_gpu_requests,
    _render_gpu_config,
    _GPU_BEFORE_SCRIPT,
)
from metasmith.env import Environment, Runtime
from metasmith.models.libraries import GPU_LABEL, Gpu, Gpus, Resources, Size


class TestResourcesDeclaration:
    def test_default_is_none_and_renders_nothing_extra(self):
        r = Resources(cpus=4, memory=Size.GB(8))
        assert r.gpus is Gpus.NONE
        assert r.gpu_memory is None
        assert not r.wants_gpu
        assert r.AsNextflowFormat(is_config=True) == [
            "cpus = 4",
            "memory = { (2**(task.attempt-1)) * ('8.00 GB' as MemoryUnit) }",
        ]

    @pytest.mark.parametrize("toggle", [Gpus.OPTIONAL, Gpus.REQUIRED])
    def test_gpu_fields_accepted_and_still_not_rendered(self, toggle):
        plain = Resources(cpus=8, memory=Size.GB(32))
        gpu = Resources(cpus=8, memory=Size.GB(32), gpus=toggle, gpu_memory=Size.GB(40))
        assert gpu.wants_gpu
        assert gpu.AsNextflowFormat(is_config=True) == plain.AsNextflowFormat(is_config=True)

    def test_toggle_values_are_the_serialized_form(self):
        assert [g.value for g in Gpus] == ["none", "optional", "required"]


class TestGpuDevice:
    def test_devices_for_rounds_up(self):
        d = Gpu(memory=Size.GB(24))
        assert d.DevicesFor(Size.GB(24)) == 1
        assert d.DevicesFor(Size.GB(25)) == 2
        assert d.DevicesFor(Size.GB(48)) == 2
        assert d.DevicesFor(Size.GB(49)) == 3

    def test_no_ask_or_no_device_memory_means_one_device(self):
        assert Gpu(memory=Size.GB(24)).DevicesFor(None) == 1
        assert Gpu().DevicesFor(Size.GB(40)) == 1

    def test_request_flag_shapes(self):
        assert Gpu().MakeRequestFlag(2) == "--gpus-per-node=2"
        assert Gpu(type="a100").MakeRequestFlag(1) == "--gpus-per-node=a100:1"
        assert Gpu(flag="--gres=gpu:").MakeRequestFlag(4) == "--gres=gpu:4"
        assert Gpu(flag="--gres=gpu:", type="h100").MakeRequestFlag(2) == "--gres=gpu:h100:2"

    def test_site_flags_ride_with_the_request(self):
        assert Gpu(extra=["--partition=gpu"]).MakeRequestFlag(1) == (
            "--gpus-per-node=1 --partition=gpu"
        )

    def test_sockeye_dialect(self):
        assert Gpu(memory=Size.GB(32), extra=["--partition=gpu"]).MakeRequestFlag(1) == (
            "--gpus-per-node=1 --partition=gpu"
        )


def _manifest(*entries):
    out = {}
    for i, (name, toggle, gb) in enumerate(entries, start=1):
        out[f"p{i:02}__{name}"] = {
            "step": i,
            "transform": name,
            "process": f"p{i:02}__{name}",
            "gpus": toggle.value,
            "gpu_memory_gb": gb,
        }
    return out


class TestPreflight:
    def test_empty_manifest_is_a_no_op(self):
        assert _plan_gpu_requests({}, None) == {}
        assert _plan_gpu_requests({}, Gpu(memory=Size.GB(24))) == {}

    def test_required_without_declaration_raises_naming_the_transform(self):
        m = _manifest(("prott5", Gpus.REQUIRED, 40.0), ("bwa", Gpus.OPTIONAL, None))
        with pytest.raises(GpuRequirementError) as e:
            _plan_gpu_requests(m, None)
        assert "prott5" in str(e.value)
        assert "step 1" in str(e.value)

    def test_error_mentions_a_detected_device_when_one_is_there(self):
        m = _manifest(("prott5", Gpus.REQUIRED, 40.0))
        with pytest.raises(GpuRequirementError) as e:
            _plan_gpu_requests(m, None, lambda: "GPU 0: NVIDIA GeForce RTX 3060 Ti")
        assert "RTX 3060 Ti" in str(e.value)

    def test_optional_only_without_declaration_proceeds_with_no_requests(self):
        m = _manifest(("bwa", Gpus.OPTIONAL, 8.0))
        assert _plan_gpu_requests(m, None) == {}

    def test_device_large_enough_plans_one(self):
        m = _manifest(("prott5", Gpus.REQUIRED, 40.0))
        assert _plan_gpu_requests(m, Gpu(memory=Size.GB(80))) == {"p01__prott5": 1}

    def test_device_half_the_size_plans_two_and_warns(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(_agents.Log, "Warn", lambda msg: warnings.append(msg))
        m = _manifest(("prott5", Gpus.REQUIRED, 40.0))
        planned = _plan_gpu_requests(m, Gpu(memory=Size.GB(20)))
        assert planned == {"p01__prott5": 2}
        assert any("spans 2 devices" in w and "prott5" in w for w in warnings), warnings

    def test_single_device_does_not_warn(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(_agents.Log, "Warn", lambda msg: warnings.append(msg))
        m = _manifest(("prott5", Gpus.REQUIRED, 40.0))
        _plan_gpu_requests(m, Gpu(memory=Size.GB(80)))
        assert not any("spans" in w for w in warnings), warnings

    def test_exceeding_declared_device_count_raises(self):
        m = _manifest(("prott5", Gpus.REQUIRED, 160.0))
        with pytest.raises(GpuRequirementError) as e:
            _plan_gpu_requests(m, Gpu(memory=Size.GB(40), count=2))
        assert "prott5" in str(e.value)
        assert "only 2 are declared" in str(e.value)

    def test_optional_steps_are_planned_when_a_device_is_declared(self):
        m = _manifest(("prott5", Gpus.REQUIRED, 40.0), ("bwa", Gpus.OPTIONAL, 8.0))
        assert _plan_gpu_requests(m, Gpu(memory=Size.GB(80))) == {
            "p01__prott5": 1, "p02__bwa": 1,
        }


class TestRenderGpuConfig:
    def test_nothing_planned_emits_nothing(self):
        assert _render_gpu_config({}, Gpu(memory=Size.GB(40)), scheduler=True) == []

    def test_local_executor_gets_only_the_label_block(self):
        lines = _render_gpu_config({"p01__x": 1}, Gpu(memory=Size.GB(8)), scheduler=False)
        text = "\n".join(lines)
        assert f"withLabel: 'x{GPU_LABEL}x'" in text
        assert "clusterOptions" not in text, "a local executor has no scheduler to ask"

    def test_label_block_reexports_cuda_visible_devices(self):
        lines = _render_gpu_config({"p01__x": 1}, Gpu(memory=Size.GB(8)), scheduler=False)
        text = "\n".join(lines)
        assert f"beforeScript = '{_GPU_BEFORE_SCRIPT}'" in text
        assert "APPTAINERENV_CUDA_VISIBLE_DEVICES" in text

    def test_scheduler_block_restates_the_base_cluster_options(self):
        lines = _render_gpu_config({"p01__prott5": 2}, Gpu(memory=Size.GB(20)), scheduler=True)
        text = "\n".join(lines)
        assert "withName: 'p01__prott5'" in text
        assert "--nodes=1 --ntasks=1" in text
        assert "--account=${params.slurmGpuAccount ?: params.slurmAccount}" in text
        assert '" --gpus-per-node=2"' in text
        assert "params.process.clusterOptionsExtra" in text

    def test_gpu_account_falls_back_to_the_default_account(self):
        text = "\n".join(_render_gpu_config({"p01__x": 1}, Gpu(memory=Size.GB(8)), scheduler=True))
        assert "params.slurmGpuAccount ?: params.slurmAccount" in text

    def test_site_gres_dialect_reaches_the_emitted_string(self):
        device = Gpu(memory=Size.GB(40), type="a100", flag="--gres=gpu:")
        text = "\n".join(_render_gpu_config({"p01__x": 3}, device, scheduler=True))
        assert '" --gres=gpu:a100:3"' in text


class TestSlurmPreset:
    @staticmethod
    def _slurm() -> str:
        from metasmith.constants import MODULE_PATH
        return (MODULE_PATH / "nextflow_config/slurm.nf").read_text()

    def test_generic_injection_points_exist(self):
        src = self._slurm()
        assert "clusterOptions = null" in src
        assert "clusterOptionsExtra = ''" in src
        assert "slurmGpuAccount = null" in src

    def test_base_cluster_options_survive_and_extra_appends(self):
        src = self._slurm()
        line = [l for l in src.splitlines() if l.strip().startswith("clusterOptions = (")]
        assert len(line) == 1, src
        line = line[0]
        assert '?: "--nodes=1 --ntasks=1 --account=${params.slurmAccount}"' in line
        assert "params.process.clusterOptionsExtra" in line


class TestEnvironmentGpuArgs:
    def test_docker(self):
        assert Environment(image="x", runtime=Runtime.DOCKER).MakeGpuArgs() == ["--gpus", "all"]

    def test_apptainer(self):
        assert Environment(image="x", runtime=Runtime.APPTAINER).MakeGpuArgs() == ["--nv"]

    def test_mamba_inherits_the_host_devices(self):
        assert Environment(image="x", runtime=Runtime.MAMBA).MakeGpuArgs() == []

    def test_native_needs_no_wrapper_flags(self):
        env = Environment(image="x", runtime=Runtime.DOCKER, native=True)
        assert env.MakeGpuArgs() == []

    @pytest.mark.parametrize(
        "runtime,flag", [(Runtime.DOCKER, "--gpus"), (Runtime.APPTAINER, "--nv")]
    )
    def test_flags_land_in_the_run_command(self, runtime, flag):
        env = Environment(image="docker://x:1", runtime=runtime)
        env.extra_args = env.MakeGpuArgs()
        assert flag in env.MakeRunCommand()


class TestSiteGpuArgs:
    WSL = ["--bind", "/usr/lib/wsl:/usr/lib/wsl", "--env", "LD_LIBRARY_PATH=/usr/lib/wsl/lib"]

    def test_site_args_follow_the_runtime_switch(self):
        env = Environment(image="x", runtime=Runtime.APPTAINER, gpu_args=self.WSL)
        assert env.MakeGpuArgs() == ["--nv"] + self.WSL

    def test_site_args_apply_to_docker_too(self):
        env = Environment(image="x", runtime=Runtime.DOCKER, gpu_args=["--shm-size=8g"])
        assert env.MakeGpuArgs() == ["--gpus", "all", "--shm-size=8g"]

    def test_site_args_are_the_whole_answer_for_native(self):
        env = Environment(image="x", runtime=Runtime.DOCKER, native=True, gpu_args=["--x"])
        assert env.MakeGpuArgs() == ["--x"]

    def test_default_is_empty_so_a_normal_host_is_unchanged(self):
        assert Environment(image="x", runtime=Runtime.APPTAINER).MakeGpuArgs() == ["--nv"]

    def test_agent_round_trips_through_agent_yml(self, tmp_path):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        a = Agent(home=Source.FromLocal(tmp_path), gpu_args=list(self.WSL))
        assert Agent.Unpack(a.Pack()).gpu_args == self.WSL

    def test_legacy_agent_yml_without_the_key_still_loads(self):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        packed = Agent(home=Source.FromLocal("/tmp/x")).Pack()
        packed.pop("gpu_args")
        assert Agent.Unpack(packed).gpu_args == []
