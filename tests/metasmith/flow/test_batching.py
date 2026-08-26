from __future__ import annotations

import math

import pytest

from .conftest import (
    assert_arity_via_oracle,
    build_batched_plan,
    run_and_load,
)


def _bootstrap_events(events: list[dict]) -> list[dict]:
    return [e for e in events if e.get("type") == "bootstrap_call"]


def _expected_batches(n_inputs: int, batch_size: int) -> list[tuple[int, int]]:
    return [
        (i, min(i + batch_size, n_inputs))
        for i in range(0, n_inputs, batch_size)
    ]


def _invocation_consumes_sets(lib) -> list[set[str]]:
    out: list[set[str]] = []
    for event in lib.find_invocations(status="promoted"):
        merged: set[str] = set()
        for parents in event.consumes.values():
            merged.update(parents)
        out.append(merged)
    return out


def test_g1_batch_size_1_single_execution(tmp_path, virtual_runtime):
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


def test_g2_batch_size_n_union_of_parents(tmp_path, virtual_runtime):
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

    # One batch carried every member, and the cache unit is the member: one
    # promoted event per member, each consuming its own input alone.
    consume_sets = _invocation_consumes_sets(lib)
    assert len(consume_sets) == n, (
        f"<G2> expected {n} promoted events (one per member), got {len(consume_sets)}"
    )
    assert all(len(c) == 1 for c in consume_sets), (
        f"<G2> a member's consumes must be its own input alone: {consume_sets}"
    )
    assert len(set.union(*consume_sets)) == n, (
        f"<G2> the members do not cover {n} distinct inputs: {consume_sets}"
    )


def test_g3_uneven_batches(tmp_path, virtual_runtime):
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

    consume_sets = _invocation_consumes_sets(lib)
    assert len(consume_sets) == n, (
        f"<G3> expected {n} promoted events (one per member), got {len(consume_sets)}"
    )


@pytest.mark.parametrize("seed", [0, 1, 7])
def test_g5_large_batch_ordering_stable(tmp_path, virtual_runtime, seed):
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


@pytest.mark.parametrize("seed", [0, 2, 5])
def test_g6_group_by_under_late_arrival(tmp_path, virtual_runtime, seed):
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


def test_g8_duplicate_group_keys(tmp_path, virtual_runtime):
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
    assert len(consume_sets) == n, (
        f"<G8> expected {n} promoted events (one per member), got {len(consume_sets)}"
    )
    assert len(set.union(*consume_sets)) == n, (
        f"<G8> the one batch's members do not cover both inputs: {consume_sets}"
    )
