"""The trio's second run reuses its cache, and a re-solve does not break that.

The complaint that opened the annotation-trio investigation: a second run of
`annotation_trio_from_assembly` appeared to redo work it had already done. On
0.20.4 it did -- every input sourced from an upstream step took a fresh id on
each materialisation, so the first consumer downstream of two cache-served
producers could never hit. Repaired at 0.21.0 by 517d746 and 1343cca.

The second test is the GUI's shape rather than the API's: `generate` re-solves
the recipe every time, so a re-solve that mints different keys would re-fetch
every database on every run.
"""

from __future__ import annotations

import pytest

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.fixtures.trio import build_inputs, solve_trio


def test_a_second_run_of_the_same_task_executes_nothing(
    virtual_runtime, tmp_path, metasmith_libraries_root,
):
    task = solve_trio(
        metasmith_libraries_root, build_inputs(metasmith_libraries_root, tmp_path),
    )
    capture_run(virtual_runtime, task)
    clear_trace(virtual_runtime)
    second = capture_run(virtual_runtime, task)
    assert not second.executed_steps, (
        f"the second run re-executed {second.executed_steps}"
    )


def test_a_resolve_of_the_same_recipe_keeps_the_cache(
    virtual_runtime, tmp_path, metasmith_libraries_root,
):
    from metasmith.python_api import DataInstanceLibrary

    inputs = build_inputs(metasmith_libraries_root, tmp_path)
    capture_run(virtual_runtime, solve_trio(metasmith_libraries_root, inputs))

    clear_trace(virtual_runtime)
    resolved = solve_trio(
        metasmith_libraries_root, DataInstanceLibrary.Load(inputs.location),
    )
    second = capture_run(virtual_runtime, resolved)
    assert not second.executed_steps, (
        f"re-solving the same recipe re-executed {second.executed_steps}"
    )
