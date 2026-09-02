"""An admissible ceiling on the refiner's objective, for a branch and bound cutoff.

`refine_mcts` maximises `1000*e_score + lin_score` over the plans that differ from
the one it was handed only in which endpoint each step consumes. Both terms have a
ceiling computable from that plan alone, so the refiner can stop the moment its
incumbent reaches the ceiling instead of spending the rest of its budget.

**CAUTION** The ceiling must never fall below the true optimum. A ceiling that does
stops the refiner on a plan it would otherwise have improved, which moves the
fingerprints in `tests/solver/fingerprints.json`, `SOLVER_RNG_VERSION`, and the Rust
half with them. Every bound here is deliberately loose in the safe direction.
"""

from __future__ import annotations

from collections import deque
from math import log2

#: The weight `score_node` puts on the entropy term. The two must agree.
ENTROPY_WEIGHT = 1000


def max_concentration(k: int, m: int) -> float:
    """The largest `sum(p*log2(p))` reachable by m usages over at least k anchors.

    `solver_math.entropy` carries no minus sign, so the objective rewards a
    concentrated lineage assignment and peaks at 0.0 on a single anchor. With k
    anchors that no assignment can merge, the peak is the most lopsided split
    available: one anchor takes every usage the other k-1 do not need.
    """
    if k <= 1 or m <= 0:
        return 0.0
    k = min(k, m)
    counts = [m - k + 1] + [1] * (k - 1)
    return sum((c / m) * log2(c / m) for c in counts)


def min_anchor_count(candidate_sets: list[set]) -> int:
    """How many anchors no assignment can collapse together.

    Two slots whose candidate endpoints do not intersect must resolve to two
    different anchors. A greedy pairwise-disjoint subset is a lower bound on the
    number of anchors, which is the direction that keeps the ceiling admissible.
    """
    disjoint: list[set] = []
    for cand in sorted(candidate_sets, key=len):
        if not cand:
            continue
        if all(not (cand & seen) for seen in disjoint):
            disjoint.append(cand)
    return max(1, len(disjoint))


def min_depth_between(producer_inputs: dict, sources: set, targets: set) -> int:
    """The shortest producer-graph hop count from any source to any target.

    `score_node` divides this depth by the plan length, so the smallest depth over
    the pairs an assignment could pick bounds that term from below. Returns 0 when
    no source reaches any target, which the caller reads as "no better than the
    fallback distance of 1.0".
    """
    if not sources or not targets:
        return 0
    seen = set(sources)
    todo = deque((s, 0) for s in sources)
    while todo:
        e, d = todo.popleft()
        if d > 0 and e in targets:
            return d
        for parent in producer_inputs.get(e, ()):  # type: ignore[arg-type]
            if parent in seen:
                continue
            seen.add(parent)
            todo.append((parent, d + 1))
    return 0


def objective_ceiling(
    *,
    n_steps: int,
    n_usages: int,
    anchor_candidates: list[set],
    lineage_min_depths: list[int],
) -> float:
    """The highest score any plan reachable by input swaps can carry."""
    e_max = max_concentration(min_anchor_count(anchor_candidates), max(1, n_usages))
    if not lineage_min_depths:
        lin_max = 0.0
    else:
        n = max(1, n_steps)
        # `_max_distance_to` returns depth/len(steps) when the anchor is reachable
        # and 1.0 when it is not, so an unreachable anchor is the worst case and a
        # depth of 0 from `min_depth_between` has to read as that 1.0.
        terms = [(d / n if d > 0 else 1.0) for d in lineage_min_depths]
        lin_max = -sum(terms) / len(terms)
    return ENTROPY_WEIGHT * e_max + lin_max


def entropy_ceiling_reached(e_score: float, anchor_candidates: list[set], n_usages: int) -> bool:
    """Whether the dominant term is already maxed, leaving only the tiebreak.

    `lin_score` spans one unit against the entropy term's weight of 1000, so a plan
    at the entropy ceiling is within 0.1% of optimal even when the strict ceiling
    has not been met.
    """
    e_max = max_concentration(min_anchor_count(anchor_candidates), max(1, n_usages))
    return e_score >= e_max - 1e-12
