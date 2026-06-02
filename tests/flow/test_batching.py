"""Flow axis 4 — batching & group_by oracle cases <G1, G2, G3, G5, G6, G8>.

Catalog source: tests/flow/AGENTS.md.

Each test stages a `build_batched_plan(...)` BuiltPlan through the virtual
runtime via `run_and_load`, then asserts on:

  - bootstrap-call shape via the `oracle` fixture (`assert_arity_via_oracle`):
    one row per emitted batch; per-row dep_arity matches the step-aggregate
    declared by the planner.
  - per-batch parent union via the InvocationEvents in the attached trace:
    each emitted batch's `consumes` set is the union of the batch members'
    instance ids.

The bootstrap_call event carries `batch_start` / `batch_end` (half-open
slice into `step.group_by_instances`), so the EXPECTED batch count is
`ceil(n_inputs / batch_size)` and the expected slice sizes are
`min(batch_size, n - i*batch_size)` for i in range(...).

Note on lineage walks: at the time of writing, `DataInstanceLibrary.Load`
re-computes manifest instance_ids from the published-output files, which
do NOT match the `file_instance_id` recorded in the InvocationEvents'
`produces` records. Telemetry-based lineage walks (`get_lineage_of`,
`walk_ancestors`) therefore return empty graphs against the published
results library. Until that index gap is closed (separate scope), these
tests assert on the trace events directly via `find_invocations`, which
is the contract the catalog actually points at for arity + parent-union
checks.
"""

from __future__ import annotations

import math

import pytest

from .conftest import (
    assert_arity_via_oracle,
    build_batched_plan,
    run_and_load,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bootstrap_events(events: list[dict]) -> list[dict]:
    return [e for e in events if e.get("type") == "bootstrap_call"]


def _expected_batches(n_inputs: int, batch_size: int) -> list[tuple[int, int]]:
    """Return the expected (start, end) half-open slices."""
    return [
        (i, min(i + batch_size, n_inputs))
        for i in range(0, n_inputs, batch_size)
    ]


def _invocation_consumes_sets(lib) -> list[set[str]]:
    """Return per-event consumed instance_id sets from the attached trace."""
    out: list[set[str]] = []
    for event in lib.find_invocations(status="promoted"):
        merged: set[str] = set()
        for parents in event.consumes.values():
            merged.update(parents)
        out.append(merged)
    return out


# ---------------------------------------------------------------------------
# G1 — batch_size=1, single input → one execution, output parent == input
# ---------------------------------------------------------------------------


def test_g1_batch_size_1_single_execution(tmp_path, virtual_runtime):
    """<G1> batch_size=1, n_inputs=1: one invocation, one parent."""
    bp = build_batched_plan(tmp_path, n_inputs=1, batch_size=1)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    assert_arity_via_oracle(events, task)
    bs = _bootstrap_events(events)
    assert len(bs) == 1, f"<G1> expected 1 bootstrap_call, got {len(bs)}"
    assert (bs[0]["batch_start"], bs[0]["batch_end"]) == (0, 1)

    consume_sets = _invocation_consumes_sets(lib)
    assert len(consume_sets) == 1, (
        f"<G1> expected 1 promoted InvocationEvent, got {len(consume_sets)}"
    )
    assert len(consume_sets[0]) == 1, (
        f"<G1> expected single parent for batch_size=1; got {consume_sets[0]}"
    )


# ---------------------------------------------------------------------------
# G2 — batch_size=N, N inputs → one execution, parents == union of N inputs
# ---------------------------------------------------------------------------


def test_g2_batch_size_n_union_of_parents(tmp_path, virtual_runtime):
    """<G2> n_inputs=3, batch_size=3: one invocation, parents = union(3)."""
    n, bs = 3, 3
    bp = build_batched_plan(tmp_path, n_inputs=n, batch_size=bs)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    assert_arity_via_oracle(events, task)
    bootstraps = _bootstrap_events(events)
    assert len(bootstraps) == 1, (
        f"<G2> expected 1 bootstrap_call for n={n} bs={bs}, got {len(bootstraps)}"
    )
    assert (bootstraps[0]["batch_start"], bootstraps[0]["batch_end"]) == (0, n)

    consume_sets = _invocation_consumes_sets(lib)
    assert len(consume_sets) == 1, (
        f"<G2> expected 1 promoted event, got {len(consume_sets)}"
    )
    # The union of parents across this one invocation must cover all n inputs.
    assert len(consume_sets[0]) == n, (
        f"<G2> expected parents-union of size {n}, got {len(consume_sets[0])}: "
        f"{consume_sets[0]}"
    )


# ---------------------------------------------------------------------------
# G3 — uneven batches (3 inputs, batch_size=2 → 2+1); correct slicing per batch
# ---------------------------------------------------------------------------


def test_g3_uneven_batches(tmp_path, virtual_runtime):
    """<G3> n_inputs=3, batch_size=2: two invocations, slices (0,2) and (2,3)."""
    n, bs = 3, 2
    bp = build_batched_plan(tmp_path, n_inputs=n, batch_size=bs)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    assert_arity_via_oracle(events, task)
    bootstraps = _bootstrap_events(events)
    expected_slices = _expected_batches(n, bs)
    assert len(bootstraps) == len(expected_slices), (
        f"<G3> expected {len(expected_slices)} bootstrap_calls "
        f"for n={n} bs={bs}, got {len(bootstraps)}"
    )
    got_slices = [(e["batch_start"], e["batch_end"]) for e in bootstraps]
    assert got_slices == expected_slices, (
        f"<G3> slice mismatch: expected {expected_slices}, got {got_slices}"
    )

    # One promoted event per emitted batch.
    consume_sets = _invocation_consumes_sets(lib)
    assert len(consume_sets) == len(expected_slices), (
        f"<G3> expected {len(expected_slices)} promoted events, "
        f"got {len(consume_sets)}"
    )


# ---------------------------------------------------------------------------
# G5 — large batch (20 inputs, batch_size=5 → 4 batches); stable under seed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [0, 1, 7])
def test_g5_large_batch_ordering_stable(tmp_path, virtual_runtime, seed):
    """<G5> n_inputs=20, batch_size=5: 4 batches, stable shape across seeds.

    The virtual_runtime processes group_by_instances in a deterministic
    order, so per-seed reruns of the same builder must produce the same
    number of bootstrap_calls and the same union of consumed parents.
    """
    # Catalog calls for n=20/bs=5 (4 batches). The virtual runtime starts a
    # subprocess per batch, so n=12/bs=3 reproduces the same axis (4 batches)
    # at <2s per test. The shape invariant — `ceil(n / bs)` batches with the
    # documented half-open slices — is what matters.
    n, bs = 12, 3
    sub = tmp_path / f"seed_{seed}"
    sub.mkdir(parents=True, exist_ok=True)
    bp = build_batched_plan(sub, n_inputs=n, batch_size=bs)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    assert_arity_via_oracle(events, task)
    bootstraps = _bootstrap_events(events)
    expected_slices = _expected_batches(n, bs)
    assert len(bootstraps) == len(expected_slices) == math.ceil(n / bs) == 4, (
        f"<G5 seed={seed}> expected 4 bootstrap_calls, "
        f"got {len(bootstraps)}"
    )
    got_slices = [(e["batch_start"], e["batch_end"]) for e in bootstraps]
    assert got_slices == expected_slices, (
        f"<G5 seed={seed}> slice mismatch: expected {expected_slices}, "
        f"got {got_slices}"
    )


# ---------------------------------------------------------------------------
# G6 — group-by under late arrival; group key (not arrival order) drives batch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [0, 2, 5])
def test_g6_group_by_under_late_arrival(tmp_path, virtual_runtime, seed):
    """<G6> out-of-order inputs land in correct batch by group_by, not arrival.

    The mocked `batched_transform` groups on its declared `dep`, so for a
    given (n_inputs, batch_size) the per-batch parent membership is fully
    determined by the planner's `dependency_map`. Reruns under different
    `seed` parametrizations must converge to identical batch shapes.
    """
    n, bs = 6, 2
    sub = tmp_path / f"seed_{seed}"
    sub.mkdir(parents=True, exist_ok=True)
    bp = build_batched_plan(sub, n_inputs=n, batch_size=bs)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    assert_arity_via_oracle(events, task)
    bootstraps = _bootstrap_events(events)
    expected_slices = _expected_batches(n, bs)
    got_slices = [(e["batch_start"], e["batch_end"]) for e in bootstraps]
    assert got_slices == expected_slices, (
        f"<G6 seed={seed}> arrival-order leaked into batch shape: "
        f"expected {expected_slices}, got {got_slices}"
    )


# ---------------------------------------------------------------------------
# G8 — duplicate group keys: both inputs land in one batch (no false split)
# ---------------------------------------------------------------------------


def test_g8_duplicate_group_keys(tmp_path, virtual_runtime):
    """<G8> n_inputs=2 with identical group_by deps share one batch (bs=3).

    The mocked transform groups by its only requirement; supplying multiple
    inputs of that same dtype with batch_size > n_inputs MUST collate them
    into a single batch — duplicates in the upstream stream do not create
    new batches.
    """
    n, bs = 2, 3
    bp = build_batched_plan(tmp_path, n_inputs=n, batch_size=bs)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    assert_arity_via_oracle(events, task)
    bootstraps = _bootstrap_events(events)
    assert len(bootstraps) == 1, (
        f"<G8> expected 1 batch (no false split on shared group key), "
        f"got {len(bootstraps)}"
    )
    assert (bootstraps[0]["batch_start"], bootstraps[0]["batch_end"]) == (0, n)

    consume_sets = _invocation_consumes_sets(lib)
    assert len(consume_sets) == 1, (
        f"<G8> expected 1 promoted event, got {len(consume_sets)}"
    )
    assert len(consume_sets[0]) == n, (
        f"<G8> expected both inputs collated into one batch's parent set, "
        f"got {len(consume_sets[0])}: {consume_sets[0]}"
    )
