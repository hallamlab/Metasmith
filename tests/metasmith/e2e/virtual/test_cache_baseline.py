from __future__ import annotations

import pytest

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import (
    linear_3step,
    mixed_cacheability,
    parallel_then_group,
)


FIXTURES = {
    "linear_3step": linear_3step,
    "parallel_then_group": parallel_then_group,
    "mixed_cacheability": mixed_cacheability,
}


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_baseline_fixture_workflows_produce_deterministic_outputs(
    name, tmp_path, virtual_runtime
):
    fixture = FIXTURES[name]

    task = fixture.build_task(tmp_path / "run1")
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.result_fingerprints, f"fixture {name} produced no outputs"

    clear_trace(virtual_runtime)

    task2 = fixture.build_task(tmp_path / "run2")
    snap2 = capture_run(virtual_runtime, task2)

    assert snap1.result_fingerprints == snap2.result_fingerprints, (
        f"fixture {name} is not deterministic across runs"
    )


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_cache_dir_populated_after_first_run(name, tmp_path, virtual_runtime):
    fixture = FIXTURES[name]

    task = fixture.build_task(tmp_path / "run1")
    snap = capture_run(virtual_runtime, task)

    assert snap.cache_state != (), (
        f"fixture {name}: cache_root was empty; the tasks promoted nothing and record_run "
        f"deposit any entries"
    )
    rel_files = {relpath for relpath, _ in snap.cache_state}
    assert any(p.endswith("cache.sqlite") for p in rel_files), (
        f"no cache.sqlite under task_cache/ for {name}; got {rel_files}"
    )
