"""The retry ladder escalates only what an exit code names, and the presets agree with it.

A timeout used to double memory, an OOM doubled time, and every other failure retried once
at twice the size. The presets and the per-step resources render one ladder from
`models/libraries/resources.py`, and these tests pin that the preset text is that ladder.
Nextflow's own reading of it is pinned in the docker lane.
"""

from __future__ import annotations

import pytest

from metasmith.agents.workflow_ops import (
    ParseHostSize, SCHEDULER_LOCAL_CPUS, _render_local_executor, _render_process_literals,
    _slurm_memory_gb,
)
from metasmith.constants import MODULE_PATH
from metasmith.models.libraries import Duration, Resources, Size
from metasmith.models.libraries.resources import MemoryLadder, RetryStrategy, TimeLadder


def _preset(name: str) -> str:
    return (MODULE_PATH / "nextflow_config" / f"{name}.nf").read_text()


class TestPresets:
    def test_slurm_renders_the_shared_ladder(self):
        src = _preset("slurm")
        assert "memory = " + MemoryLadder("params.process.memory") in src
        assert "time = " + TimeLadder("params.process.time") in src
        assert "errorStrategy = " + RetryStrategy() in src

    def test_local_retries_by_the_same_exit_codes(self):
        assert "errorStrategy = " + RetryStrategy() in _preset("local")

    @pytest.mark.parametrize("name", ["slurm", "local"])
    def test_error_strategy_owns_the_retry_limit(self, name):
        # maxRetries is read when the config is parsed, before the params file merges, so a
        # run that raised `tries` past it aborted on the attempt over the parse-time limit.
        assert "maxRetries = 99" in _preset(name)

    def test_slurm_ceilings_are_set(self):
        src = _preset("slurm")
        assert "maxDuration = '7days'" in src
        assert "maxMemory = '750 GB'" in src

    @pytest.mark.parametrize("key", ["cpus", "scratch", "clusterOptions"])
    def test_run_params_reach_the_process_keys(self, key):
        # A nested param read outside a closure keeps the preset's default whatever the run says.
        lines = [l.strip() for l in _preset("slurm").splitlines() if l.strip().startswith(f"{key} = ")]
        process_level = [l for l in lines if "params.process" in l]
        assert process_level and all(l.startswith(f"{key} = {{") for l in process_level), lines


class TestLadder:
    def test_memory_doubles_only_on_an_oom(self):
        ladder = MemoryLadder("'8 GB'")
        assert "in [125, 137] ? 2 : 1" in ladder
        assert "task.previousTrace" in ladder

    def test_time_doubles_only_on_a_timeout(self):
        assert "in [140] ? 2 : 1" in TimeLadder("'1h'")

    def test_every_rung_is_clamped(self):
        assert MemoryLadder("'8 GB'").rstrip().endswith("].min() }")
        assert "params.process?.maxMemory" in MemoryLadder("'8 GB'")

    def test_a_strict_memory_never_climbs(self):
        out = Resources(memory=Size.GB(8).SetStrict()).AsNextflowFormat(is_config=True)
        assert out == ["memory = '8.00 GB'"]

    def test_a_step_renders_both_ladders(self):
        out = Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)).AsNextflowFormat(is_config=True)
        assert out == ["cpus = 2", "memory = " + MemoryLadder("'8.00 GB'"), "time = " + TimeLadder("'1hours'")]

    def test_only_resource_and_interruption_exits_retry(self):
        rs = RetryStrategy()
        assert "[125, 137, 140, 143]" in rs
        assert "Integer.MAX_VALUE" in rs


class TestLocalExecutorSizing:
    def test_the_cgroup_bounds_the_host_memory(self):
        assert ParseHostSize(["64", str(512 * 1024**3), str(16 * 1024**3)]) == (64, 16 * 1024**3)

    def test_an_unbounded_cgroup_leaves_the_host_memory(self):
        assert ParseHostSize(["4", "1000", "max"]) == (4, 1000)

    def test_an_unreadable_probe_is_no_size(self):
        assert ParseHostSize(["oops"]) is None

    def test_a_login_node_gets_a_polite_share(self):
        lines = _render_local_executor(None, (64, 16 * 1024**3), None)
        assert f"executor {{ cpus = {SCHEDULER_LOCAL_CPUS}; memory = '13 GB' }}" in lines

    def test_a_driver_job_is_sized_from_its_allocation(self):
        lines = _render_local_executor(None, (192, 768 * 1024**3), (2, 16.0))
        assert "executor { cpus = 2; memory = '13 GB' }" in lines

    def test_the_run_params_win(self):
        lines = _render_local_executor({"cpus": 3, "memory": "5 GB"}, (64, 16 * 1024**3), None)
        assert "executor { cpus = 3; memory = '5 GB' }" in lines

    def test_nothing_known_renders_nothing(self):
        assert _render_local_executor(None, None, None) == []

    @pytest.mark.parametrize("spec,gb", [("8G", 8.0), ("16000M", 16000 / 1024), ("1T", 1024.0), ("x", None)])
    def test_slurm_memory_specs(self, spec, gb):
        assert _slurm_memory_gb(spec) == gb


def test_array_is_rendered_as_a_literal():
    assert _render_process_literals({"array": 50}) == ["", "process { array = 50 }", ""]
    assert _render_process_literals({"tries": 3}) == []
