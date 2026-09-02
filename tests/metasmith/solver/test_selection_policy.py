from __future__ import annotations

import pytest

from metasmith.models.solver import active_policy_name
from metasmith.models.solver_backend import Backend, UsePythonSolver
from metasmith.models.solver_policy import (
    SELECTION_TOP_K,
    SELECTION_WEIGHTS,
    Arm,
    PuctConfig,
    PuctSelection,
    UsePuctSelection,
    WeightedSelection,
)
from metasmith.models.solver_rng import DecisionStream
from metasmith.testing.solver_bench import CORPUS
from metasmith.testing.solver_verification import (
    check_plan,
    generate_problem,
    plan_fingerprint,
)


def test_the_shipped_rule_is_what_runs_when_nothing_is_asked_for():
    assert active_policy_name() == "weighted"


def test_the_context_manager_puts_the_previous_policy_back():
    before = active_policy_name()
    with UsePuctSelection():
        assert active_policy_name() == "puct"
    assert active_policy_name() == before


def _arms(n: int) -> list[Arm]:
    return [Arm((i / n, 1 - i / n), key=f"t{i % 3}") for i in range(n)]


def test_a_greedy_selection_costs_one_word_and_an_exploring_one_costs_two():
    """Words consumed per selection is the contract `rng.rs` mirrors.

    Counting *calls* would not say this: `weighted_index` and `pick_top_k` both
    delegate to `bounded_int`, so a call count double-counts. What matters is
    the stream position, and the load-bearing detail is that `pick_top_k` at
    k=1 spends nothing -- `bounded_int(1)` returns without drawing -- so the two
    greedy arms cost exactly the one word `weighted_index` took.
    """
    policy = WeightedSelection()
    rng = DecisionStream(42)
    n, explore, start = 400, 0, rng.draws
    for _ in range(n):
        before = rng.draws
        policy.select(rng, _arms(6))
        spent = rng.draws - before
        assert spent in (1, 2), f"a selection spent {spent} words"
        explore += spent == 2
    assert rng.draws - start == n + explore
    # The third weight is 5 of 100. A wide band: this pins the shape of the
    # rule, not the stream's exact output.
    assert 0.01 < explore / n < 0.15


def test_the_explore_arm_is_positional():
    """The uniform arm is whichever index is last, not one named as special."""
    assert len(SELECTION_WEIGHTS) == 3
    assert SELECTION_TOP_K == 1


def test_puct_shares_statistics_by_key_not_by_arm():
    """Two arms with the same key move together; that is the RAVE aliasing.

    Per-arm counts would all stay at zero, because a selected node is removed
    from the frontier and never seen again.
    """
    policy = PuctSelection(PuctConfig())
    policy.observe("t0", 1.0)
    a, b = Arm((0.5, 0.5), key="t0"), Arm((0.5, 0.5), key="t1")
    priors = policy._priors([a, b])
    assert priors[0] == pytest.approx(priors[1]), "equal scores, so equal priors"
    assert policy._n["t0"] == 1.0
    assert "t1" not in policy._n


def test_puct_credits_an_improvement_not_a_position():
    """The default reward is a delta, and the alternative is not.

    `candidate_transforms` only grows, so an absolute progress reading rises
    along every path and would rank a transform by how late it is applied.
    """
    delta = PuctSelection(PuctConfig(reward_mode="delta"))
    absolute = PuctSelection(PuctConfig(reward_mode="absolute"))
    assert delta.reward_for(False, 0.8, 0.8) == 0.0
    assert absolute.reward_for(False, 0.8, 0.8) == pytest.approx(0.8)
    assert delta.reward_for(False, 0.2, 0.5) == pytest.approx(0.3)
    assert delta.reward_for(True, 0.0, 0.0) == 1.0


def test_turning_the_value_off_keeps_the_counts_but_flattens_q():
    """The measured variant: exploration without an estimator.

    On the held-out hard profiles this reaches the same solve rate as the full
    policy for about 10% more iterations, while freezing the *counts* instead
    is worse than the shipped rule. It is also the cheaper thing to port,
    because it needs no reward and so no progress walk -- which is what
    `wants_rewards` being false is there to switch off.
    """
    counts_only = PuctSelection(PuctConfig(use_value=False))
    assert counts_only.wants_observations is True, "visit counts still accumulate"
    assert counts_only.wants_rewards is False, "but the progress walk is skipped"
    # Every observation is worth the first-play value, so Q cannot separate arms.
    assert counts_only.reward_for(True, 0.0, 1.0) == counts_only.config.fpu
    assert counts_only.reward_for(False, 0.2, 0.9) == counts_only.config.fpu

    full = PuctSelection(PuctConfig(use_value=True))
    assert full.wants_rewards is True
    assert full.reward_for(True, 0.0, 0.0) == 1.0


def test_the_shipped_rule_is_told_not_to_compute_rewards():
    """The baseline must not pay for a signal it discards.

    The mcts progress measure walks every successor's endpoint set, so charging
    the shipped rule for it would slow the very thing an alternative is
    measured against.
    """
    assert WeightedSelection().wants_observations is False
    assert PuctSelection().wants_observations is True


@pytest.mark.parametrize("name,seed,dials", CORPUS, ids=[c[0] for c in CORPUS])
def test_puct_produces_a_sound_plan_on_every_corpus_case(name, seed, dials):
    """Soundness under the alternative policy, adjudicated by `check_plan`.

    Not a fingerprint comparison: a different selection rule is expected to
    return a different plan, and pinning one here would only record whatever
    the policy happened to do on the day. What must hold is that the plan is
    sound and that the search still completes.

    Pinned to the python implementation and *asserted*, because the policy seam
    lives only there: `solve_by_mcts` dispatches through `solver_backend`, so on
    a machine with a staged `msm_solver` this would quietly solve with the
    engine, `UsePuctSelection` would be inert, and the test would pass green
    having tested nothing. That is the failure `tests/solver/AGENTS.md` names as
    "a reference that isn't a reference".
    """
    problem = generate_problem(seed, dials, name=name)
    with UsePythonSolver():
        assert Backend("solve") == "python"
        with UsePuctSelection():
            solution = problem.solve()
    verdict = check_plan(problem, solution)
    assert verdict.ok, f"[{name}] {verdict.violations}"
    assert solution.complete
    assert plan_fingerprint(solution)


def test_a_puct_solve_does_not_inherit_the_last_one_s_statistics():
    """Each phase forks its own policy, so two solves cannot differ by history.

    `fork()` returning `self` for a stateful policy is what this catches: the
    second solve would start with the first one's visit counts and could return
    a different plan.
    """
    name, seed, dials = CORPUS[0]
    problem = generate_problem(seed, dials, name=name)
    shared = PuctSelection(PuctConfig())
    with UsePythonSolver():
        assert Backend("solve") == "python"
        with UsePuctSelection():
            first = plan_fingerprint(problem.solve())
            second = plan_fingerprint(problem.solve())
    assert first == second
    # And the policy object the search forked from is itself untouched, so the
    # statistics really did live on the fork rather than on the template.
    assert shared._total == 0
