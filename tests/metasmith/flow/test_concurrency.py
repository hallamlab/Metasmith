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


@pytest.mark.parametrize("seed", [0])
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


