"""Baseline tests — pin current `main` behavior before caching lands.

These tests MUST pass on the caching branch with no caching code present
(i.e. before S1 is implemented). They establish the "before" half of the
before/after harness: deterministic fixtures, full re-execution on rerun,
and no cache root on disk.

Each fixture is run via the virtual_runtime (no real Docker/Nextflow). The
RunSnapshot harness captures three observables; this file asserts the
shape of those observables today so that the caching PRs can show, against
the same fixtures, that the shape changed in a goal-aligned way.
"""

from __future__ import annotations

import pytest

from tests.cache._cache_harness import capture_run, clear_trace
from tests.cache.fixtures.cache_fixtures import (
    linear_3step,
    mixed_cacheability,
    parallel_then_group,
)

# `virtual_runtime` fixture is provided project-wide by tests/conftest.py.


FIXTURES = {
    "linear_3step": linear_3step,
    "parallel_then_group": parallel_then_group,
    "mixed_cacheability": mixed_cacheability,
}


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_baseline_fixture_workflows_produce_deterministic_outputs(
    name, tmp_path, virtual_runtime
):
    """Two runs of the same fixture produce byte-identical result files.

    Pure determinism check that pre-dates caching; the test fixtures depend
    on this to ever be able to compare cache-hit fingerprints against
    cache-miss fingerprints.
    """
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


# NOTE: `test_baseline_rerun_re_executes_everything` was removed here. It
# pinned the pre-caching "main" behavior (a rerun re-executes every step) and
# its own docstring stated it "will start failing once caching lands." Caching
# has landed, so the assertion is now intentionally false. The forward-looking
# behavior (rerun on identical inputs → executed_steps == () full hit) is
# covered green by tests/cache/test_cross_run.py, tests/cache/test_hit_miss.py,
# and tests/e2e/virtual/test_cache_e2e.py.


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_cache_dir_populated_after_first_run(name, tmp_path, virtual_runtime):
    """Post-S5: the first run of a cacheable fixture populates task_cache/.

    Replaces the obsoleted main-era assertion that task_cache/ would
    never exist. With promote_run wired into Agent.RunWorkflow, every
    cacheable transform's outputs deposit into the cache atomically.
    A non-empty cache_state is the smoking gun that the promote step
    fired and CacheStore.upsert wrote a row for every step.
    """
    fixture = FIXTURES[name]

    task = fixture.build_task(tmp_path / "run1")
    snap = capture_run(virtual_runtime, task)

    assert snap.cache_state != (), (
        f"fixture {name}: cache_root was empty; promote_run did not "
        f"deposit any entries"
    )
    # At least one cache.sqlite row + one out/ payload per cacheable step.
    rel_files = {relpath for relpath, _ in snap.cache_state}
    assert any(p.endswith("cache.sqlite") for p in rel_files), (
        f"no cache.sqlite under task_cache/ for {name}; got {rel_files}"
    )
