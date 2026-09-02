"""Which node the search expands next, as a swappable rule.

Both search phases -- the mcts frontier of candidate applications and the
refiner's frontier of swapped plans -- pick their next node the same way, which
is why `SELECTION_WEIGHTS` is one constant shared by both. This module makes
that rule an object so a second rule can be measured against it without either
one being conditional on the other.

`WeightedSelection` is the shipped rule and the default: a three-way weighted
draw, two arms greedy on a score channel and one uniform. It is a transcription,
not a reimplementation -- the draws it consumes, in the order it consumes them,
are part of the decision contract that `solver_rng.py` and `rng.rs` both carry.

`PuctSelection` is the alternative under evaluation. Two things about this
search make textbook PUCT inapplicable as written, and both shape what is here:

**Nothing is visited twice.** A selected node is removed from the frontier and
never returns, so there is no per-node visit count for a confidence radius to
divide by. Statistics are therefore shared across arms by a *key* rather than
held per arm -- the transform being applied, which recurs throughout a search.
That is the AMAF/RAVE move: when per-node evidence is too sparse to estimate
anything, estimate per action-identity instead and accept the aliasing.

**Nothing is rolled out.** There is no playout and no return, so `Q` cannot be a
backed-up outcome. It is fed instead by a progress signal the caller observes
directly after expanding, which is why `observe` is separate from `select` --
the reward is not knowable at selection time.

Scores arrive on whatever scale a phase happens to use: the mcts channels are
already in [0, 1] while the refiner's are unbounded below and dominated by a
term multiplied by a thousand. `_normalized` rescales per selection against the
frontier's own range, so the prior means the same thing in both phases and the
`c_puct` that tunes one is not silently wrong for the other.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from math import exp, sqrt
from typing import Hashable, Sequence

__all__ = [
    "Arm",
    "SelectionPolicy",
    "WeightedSelection",
    "PuctSelection",
    "PuctConfig",
    "SELECTION_WEIGHTS",
    "SELECTION_TOP_K",
    "ActivePolicy",
    "SetSelectionPolicy",
    "UsePuctSelection",
    "ResetSelectionPolicy",
]

SELECTION_WEIGHTS = (75, 20, 5)
SELECTION_TOP_K = 1


@dataclass(frozen=True)
class Arm:
    """One candidate on the frontier: its score channels and its sharing key.

    `scores` is what the shipped rule ranks on and is not negotiable -- those
    are the exact channels, in the exact order, that the decision contract
    names. `prior_scores` exists because one of those channels is not usable as
    a prior: the refiner's second channel is `score*valid` over a score that is
    never positive, so an invalid state's 0.0 outranks every valid one. The
    shipped rule keeps that behaviour, bug and all, because reproducing it is
    the point; a new policy should not inherit it by accident, so it is given
    the same intent -- score, and validity -- encoded so that better is higher.
    """

    scores: Sequence[float]
    key: Hashable
    prior_scores: Sequence[float] | None = None

    @property
    def priors_from(self) -> Sequence[float]:
        return self.scores if self.prior_scores is None else self.prior_scores


class SelectionPolicy:
    name: str = "?"

    #: Whether `observe` is worth calling. A policy that learns nothing should
    #: not make the search compute a reward for it: the progress measure the
    #: mcts phase feeds in walks every successor's endpoint set, and charging
    #: the shipped rule for a number it discards would slow the very baseline
    #: an alternative is being measured against.
    wants_observations: bool = False

    @property
    def wants_rewards(self) -> bool:
        """Whether the caller should compute the progress pair `observe` needs.

        Separate from `wants_observations` because a policy can want the visit
        counts without wanting a value: counting is free, and the progress walk
        that produces the reward is not.
        """
        return self.wants_observations

    def select(self, rng, arms: list[Arm]) -> int:
        raise NotImplementedError

    def select_from(self, rng, items: list, scores_of) -> int:
        """Select over items whose score channels `scores_of` extracts.

        The entry point for a rule that needs no key and no prior, and it exists
        for cost rather than taste. Building an `Arm` per frontier entry per
        selection measures ~15x the cost of reading one channel out of a list,
        and the shipped rule discards every field but the scores -- so routing
        it through `select` made the *default* path materially slower for
        nothing. `solver_bench`'s contract is that fingerprints match and the
        time drops, so a decision-identical slowdown is still a regression.

        The default implementation is correct for any policy; `WeightedSelection`
        overrides it with one that allocates what the old code allocated.
        """
        return self.select(rng, [Arm(scores_of(x), None) for x in items])

    def observe(self, key: Hashable, reward: float) -> None:
        """Report what selecting `key` turned out to be worth, in [0, 1]."""

    def reward_for(self, solved: bool, before: float, after: float) -> float:
        """Turn a caller's observation into a reward in [0, 1].

        Lives here rather than at the call site because how an observation is
        credited is part of the policy's configuration, not of the search.
        """
        return 1.0 if solved else max(0.0, after - before)

    def fork(self) -> "SelectionPolicy":
        """A sibling carrying the same configuration and no statistics.

        Each search phase forks its own: the mcts frontier and a refiner call
        are separate searches asking different questions of the same keys, and
        pooling their evidence would let one phase's experience steer the other.
        """
        return self


class WeightedSelection(SelectionPolicy):
    """The shipped rule. Two greedy arms and one uniform, drawn 75/20/5.

    The draw sequence is the contract: `weighted_index` first, then either
    `pick_top_k` on the chosen channel or `bounded_int` over the frontier.
    `pick_top_k` at k=1 reduces to an argmax that consumes no further word,
    because `bounded_int(1)` returns without drawing.
    """

    name = "weighted"

    def select(self, rng, arms: list[Arm]) -> int:
        p_i = rng.weighted_index(SELECTION_WEIGHTS)
        if p_i < len(SELECTION_WEIGHTS) - 1:
            return rng.pick_top_k([a.scores[p_i] for a in arms], SELECTION_TOP_K)
        return rng.bounded_int(len(arms))

    def select_from(self, rng, items: list, scores_of) -> int:
        """The same rule, allocating exactly what it did before the seam.

        One list of floats on a greedy arm and nothing at all on the uniform
        one -- which is why the uniform branch reads `len(items)` rather than
        building anything first.
        """
        p_i = rng.weighted_index(SELECTION_WEIGHTS)
        if p_i < len(SELECTION_WEIGHTS) - 1:
            return rng.pick_top_k(
                [scores_of(x)[p_i] for x in items], SELECTION_TOP_K
            )
        return rng.bounded_int(len(items))


@dataclass
class PuctConfig:
    c_puct: float = 1.5
    #: Prior weight per score channel. Defaults keep the shipped rule's relative
    #: emphasis on its two greedy arms (75:20) rather than inventing a new one.
    channel_weights: tuple[float, ...] = (75 / 95, 20 / 95)
    #: Softmax temperature over the normalized, combined channels.
    temperature: float = 0.5
    #: Value assigned to a key with no observations yet.
    fpu: float = 0.5
    #: Breadth of the final draw. 1 is a pure argmax and makes the search
    #: deterministic in the seed; above 1 restores seeded diversity.
    top_k: int = 1
    #: Probability (in units of 1/1000) of ignoring the index and drawing
    #: uniformly, the role the shipped rule's third arm plays.
    epsilon_milli: int = 0
    #: Exponential recency weight for Q. 1.0 is a plain running mean; below 1
    #: lets a key that has stopped paying off decay back toward the prior.
    decay: float = 1.0
    #: Whether Q is estimated at all. With this off the visit counts still
    #: accumulate but every arm's value stays at `fpu`, so selection is the
    #: prior under a visit-count penalty and nothing is learned.
    #:
    #: Measured, not hypothetical: on the held-out hard profiles that variant
    #: reaches the same solve rate for about 10% more iterations, while
    #: freezing the *counts* instead collapses the search to worse than the
    #: shipped rule. The exploration term is what carries this policy; the
    #: estimator is a small bonus. Turning this off also removes the only
    #: expensive thing the search gained -- the progress walk that feeds the
    #: reward -- which is what makes it the cheaper thing to port.
    use_value: bool = True
    #: How a caller's before/after progress pair becomes a reward.
    #:
    #: ``"delta"`` credits the *improvement* an action made. ``"absolute"``
    #: credits the state it left behind, which sounds equivalent and is not:
    #: a state's unlocked-transform set only ever grows, so absolute progress
    #: rises monotonically along every path and Q ends up ranking transforms by
    #: how late they tend to be applied rather than by whether they help.
    reward_mode: str = "delta"


class PuctSelection(SelectionPolicy):
    """PUCT with statistics shared by key, and a caller-supplied reward."""

    name = "puct"
    wants_observations = True

    def __init__(self, config: PuctConfig | None = None):
        self.config = config or PuctConfig()
        self._w: dict[Hashable, float] = {}
        self._n: dict[Hashable, float] = {}
        self._total: int = 0

    def fork(self) -> "PuctSelection":
        # `type(self)`, not the class name: a subclass that overrides the reward
        # or the observation would otherwise fork into the base class and the
        # override would silently stop applying for the whole search.
        return type(self)(self.config)

    @property
    def wants_rewards(self) -> bool:
        return self.config.use_value

    def reward_for(self, solved: bool, before: float, after: float) -> float:
        if not self.config.use_value:
            return self.config.fpu
        if solved:
            return 1.0
        if self.config.reward_mode == "absolute":
            return max(0.0, min(1.0, after))
        return max(0.0, min(1.0, after - before))

    def reset(self) -> None:
        self._w.clear()
        self._n.clear()
        self._total = 0

    def observe(self, key: Hashable, reward: float) -> None:
        d = self.config.decay
        if d >= 1.0:
            self._w[key] = self._w.get(key, 0.0) + reward
            self._n[key] = self._n.get(key, 0.0) + 1.0
        else:
            self._w[key] = self._w.get(key, 0.0) * d + reward
            self._n[key] = self._n.get(key, 0.0) * d + 1.0
        self._total += 1

    def _priors(self, arms: list[Arm]) -> list[float]:
        cfg = self.config
        # `min` over the arms, not `max`: every arm is indexed at every channel
        # below, so taking the widest arm's arity would index past a narrower
        # one. Nothing today builds a mixed-arity frontier; this is cheap and
        # the failure would be an IndexError from inside a search.
        n_ch = min(len(cfg.channel_weights), min(len(a.priors_from) for a in arms))
        combined = [0.0] * len(arms)
        for ch in range(n_ch):
            col = [float(a.priors_from[ch]) for a in arms]
            lo, hi = min(col), max(col)
            span = hi - lo
            w = cfg.channel_weights[ch]
            for i, v in enumerate(col):
                combined[i] += w * ((v - lo) / span if span > 0 else 0.5)
        tau = cfg.temperature if cfg.temperature > 0 else 1e-9
        peak = max(combined)
        raw = [exp((v - peak) / tau) for v in combined]
        total = sum(raw)
        if total <= 0:
            return [1.0 / len(arms)] * len(arms)
        return [r / total for r in raw]

    def select(self, rng, arms: list[Arm]) -> int:
        cfg = self.config
        if cfg.epsilon_milli > 0:
            if rng.bounded_int(1000) < cfg.epsilon_milli:
                return rng.bounded_int(len(arms))
        priors = self._priors(arms)
        # sqrt(1 + total) rather than sqrt(total): at the first selection of a
        # phase the bonus would otherwise be identically zero for every arm,
        # collapsing the whole index onto the (constant) first-play value.
        explore = sqrt(1.0 + self._total)
        index: list[float] = []
        for a, p in zip(arms, priors):
            n = self._n.get(a.key, 0.0)
            q = (self._w[a.key] / n) if n > 0 else cfg.fpu
            index.append(q + cfg.c_puct * p * explore / (1.0 + n))
        return rng.pick_top_k(index, cfg.top_k)


_policy: SelectionPolicy | None = None


def ActivePolicy() -> SelectionPolicy:
    global _policy
    if _policy is None:
        _policy = WeightedSelection()
    return _policy


def SetSelectionPolicy(policy: SelectionPolicy | None) -> SelectionPolicy | None:
    global _policy
    previous = _policy
    _policy = policy
    return previous


def ResetSelectionPolicy() -> None:
    SetSelectionPolicy(None)


@contextmanager
def UsePuctSelection(config: PuctConfig | None = None):
    previous = SetSelectionPolicy(PuctSelection(config))
    try:
        yield
    finally:
        SetSelectionPolicy(previous)
