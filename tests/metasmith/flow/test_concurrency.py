from __future__ import annotations

import pytest

from .conftest import (
    assert_arity_via_oracle,
    build_batched_plan,
    run_and_load,
)


def _bootstrap_signature(events: list[dict]) -> list[tuple[int, int, dict]]:
    sig: list[tuple[int, int, dict]] = []
    for e in events:
        if e.get("type") != "bootstrap_call":
            continue
        sig.append(
            (
                int(e["batch_start"]),
                int(e["batch_end"]),
                dict(sorted({str(k): int(v) for k, v in dict(e.get("dep_arity", {})).items()}.items())),
            )
        )
    return sig


@pytest.mark.parametrize("seed", [0, 3, 11])
def test_rc1_out_of_order_arrival_stable(tmp_path, virtual_runtime, seed):
    n, bs = 6, 2
    sub = tmp_path / f"rc1_seed_{seed}"
    sub.mkdir(parents=True, exist_ok=True)
    bp = build_batched_plan(sub, n_inputs=n, batch_size=bs)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    assert_arity_via_oracle(events, task)
    sig = _bootstrap_signature(events)
    expected_slices = [
        (i, min(i + bs, n)) for i in range(0, n, bs)
    ]
    got_slices = [(s, e) for (s, e, _) in sig]
    assert got_slices == expected_slices, (
        f"<RC1 seed={seed}> slice ordering drifted: "
        f"expected {expected_slices}, got {got_slices}"
    )
    arities = [d for (_, _, d) in sig]
    assert all(a == arities[0] for a in arities), (
        f"<RC1 seed={seed}> dep_arity drifted across slices: {arities}"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "<RC2> concurrent index mutation under real Nextflow channels is "
        "out of scope for the virtual runtime — the in-process executor "
        "runs batches serially, so the ConcurrentModification class of "
        "bug cannot surface here. Coverage lives under "
        "tests/e2e/docker/test_orchestrator_exec.py (RC2 case)."
    ),
)
def test_rc2_concurrent_index_mutation_xfail(tmp_path, virtual_runtime):
    n, bs = 4, 2
    bp = build_batched_plan(tmp_path, n_inputs=n, batch_size=bs)
    task, lib = run_and_load(virtual_runtime, bp)
    events = virtual_runtime.parse_trace()

    bootstraps = [e for e in events if e.get("type") == "bootstrap_call"]
    arities = [dict(e.get("dep_arity", {})) for e in bootstraps]
    assert len(arities) > 1
    assert all(arities[i] != arities[i - 1] for i in range(1, len(arities))), (
        "<RC2> serial runtime emits identical dep_arity per batch; "
        "real-channel mutation would diverge."
    )
