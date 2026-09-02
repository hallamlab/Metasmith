# PUCT as the solver's selection policy — proof of concept

*Status: proof of concept. PUCT is implemented, measured and opt-in. It is **not** the default and
nothing about the shipped behaviour has changed.*

## The question

The solver chooses which node to expand next with a fixed three-way weighted split — 75% greedy on
one static heuristic, 20% greedy on a second, 5% uniform random — at two sites, the mcts phase and
the refiner, sharing one constant. Both heuristics are computed once by a single backward
breadth-first walk before the search begins and are never revised, so nothing the search discovers
can change what it does next.

Can a PUCT rule replace that split without losing plan soundness, and find plans in fewer
expansions?

**Short answer.** Yes — sound on every case tried, 640/640 held-out instances solved against the
baseline's 627, with smaller plans and about a third fewer expansions. But an ablation shows the
gain comes from PUCT's *exploration* term applied to the heuristics the solver already had, **not**
from the learned value estimate, which is nearly inert at this iteration budget. That distinction
is in the mechanism section below and it should govern how much of this is worth porting.

## What was built

`src/metasmith/models/solver_policy.py` makes the selection rule an object, chosen the way the
Rust/Python solver backend already is: a module-level setter and a context manager, never an
environment variable. `WeightedSelection` is the shipped rule, transcribed rather than rewritten,
and remains the default. `PuctSelection` is the alternative.

Two properties of this search make textbook PUCT inapplicable as written, and both shape the
implementation:

**Nothing is visited twice.** A selected node is removed from the frontier and never returns, so
there is no per-node visit count for a confidence radius to divide by. Statistics are therefore
shared across arms by *key* — the transform being applied — rather than held per arm. That is the
AMAF/RAVE move: when per-node evidence is too sparse to estimate anything, estimate per
action-identity and accept the aliasing.

**Nothing is rolled out.** There is no playout and no return, so `Q` cannot be a backed-up outcome.
It is fed instead by a progress signal the caller observes after expanding, which is why `observe`
is separate from `select`.

The prior comes from the two heuristics the solver already computes, normalized over the frontier
and combined at the shipped rule's own 75:20 emphasis. Nothing new is precomputed.

## Correctness is the gate

Soundness is adjudicated by `check_plan`, which shares no code with the search — the asymmetry that
lets it judge output from an implementation not yet trusted. It checks conformance of every
binding, that every consumed endpoint is produced, ordering, absence of cycles, exactly one target
application, and that every binding descends from its declared lineage constraint.

- **No plan from either policy was refused, on any case in this work.**
- The full `tests/metasmith/solver` axis passes with the policy left at its default: **158 passed,
  237 skipped, 1 xfailed** — 141 of those were passing before this work and 17 are the new
  `test_selection_policy.py`.
  (The skips are the `python_solver` and engine lanes, which need a staged `msm_solver` binary; none
  is staged on this host, so the whole panel — and this entire comparison — runs on the Python
  implementation.)
- **The default path was decision-identical but 19% slower, and that had to be fixed.** An
  adversarial review caught it and it reproduced immediately: 5.13s → 6.11s over 336 solves of the
  seven non-`sink` profiles, non-overlapping across repeats. The cause was building a frozen `Arm`
  dataclass per frontier entry per selection — roughly 15× the cost of reading one channel out of a
  list — on a path that discards every field but the scores. The template check missed it because
  `run_templates` times library load and plan assembly too, diluting a search-only regression to
  +0.5%. `solver_bench`'s docstring is the standard: *the fingerprints must match **and the time
  must drop**, or the change is reverted* — a decision-identical slowdown is still a regression.
  Fixed with a `select_from` entry point that allocates what the pre-seam code allocated (one list
  of floats on a greedy arm, nothing at all on the uniform one). After the fix, six interleaved
  rounds give HEAD 5.155s mean against patched 5.185s — **+0.6%, fully overlapping**, with the
  patched tree faster in two of the six. The seam is now free.
- The default path is verified unchanged three ways: fingerprints on the generated corpus,
  fingerprints on all eleven shipped templates, and template wall clock against the pre-change
  numbers (25.514s → 25.654s, +0.5%, noise on a shared box).

## The benchmark had to be built first

The shipped corpora do not exercise the selection policy. `CORPUS` and `STRESS_CORPUS` solve in
7–13 mcts iterations against a budget of 256, and across 1,120 generated cases spanning the eight
differential profiles the median is 5–9 iterations. Only the `sink` profile reaches the budget —
and the cases that reach it are exactly the cases the baseline fails to solve.

So the honest axis here is **solve rate on hard instances**, not wall clock on easy ones. Tuning
used problem seeds 0–19; the results below are from seeds 40–79, which were never run during
development.

## Results

*Held-out: eight profiles, problem seeds 40–79, solve seeds 42 and 7. Python solver both sides,
same problem objects, same seeds.*

**Soundness: no plan from either policy was refused by `check_plan`.**

| | baseline | PUCT |
|---|---|---|
| rows solved | 627 / 640 | **640 / 640** |
| rows only this policy solved | 0 | 13 |

**Read that per problem, not per row, because the two sides do not have the same corpus size.**
PUCT at `top_k=1` draws no randomness, so its 640 rows are 320 distinct runs — each problem solved
once and counted twice — while the baseline genuinely gets two attempts per problem. The thirteen
gained rows are **8 distinct problems**, and of those the baseline solves 3 under one of its two
seeds. So the defensible statement is:

> **PUCT solves 5 problems out of 320 that the baseline never solves under either seed, and loses
> none.**

The five are `cyclic-61`, `sink-48`, `sink-68`, `sink-77`, `sink-78`. The other three
(`cyclic-53`, `sink-56`, `sink-64`) are ones the baseline gets under one seed and misses under the
other, which is the seed lottery PUCT replaces with a deterministic answer rather than a capability
it lacked.

That is a real capability gain and a much smaller one than "640/640 versus 627/640" suggests. A
95% binomial interval on 320/320 is [0.989, 1.0], so a PUCT failure rate around 1% is entirely
consistent with this data — the corpus cannot show better than that.

Ten of the thirteen gained rows are in the `sink` profile, the one place the search is under load;
three are in `cyclic`. No row was lost.

On the **627 cases both solved**, which is the only comparison over a common set:

| | baseline | PUCT | change |
|---|---|---|---|
| mcts iterations | 7,064 | 4,606 | −34.8% |
| refiner iterations | 2,695 | 1,128 | −58.1% |
| plan steps | 3,803 | 3,655 | **−3.9%** |
| wall clock | 34.5s | 2.4s | −93.2% |

PUCT used fewer iterations on 413 of 627, more on 20, the same on 194. Its plan was **smaller on 23
cases, larger on 3, and identical in size on 601** — and byte-identical by fingerprint on 490 of
627. So it is mostly finding *the same plan with less work*, not a different one.

**The wall-clock ratio is largely an artifact of the harness and should not be quoted.** Over all
640 cases the raw figures are 129.4s → 2.8s, a 46× ratio — but 73% of the baseline's total is spent
on rows it never solves, and **90.0s of that is six 15-second timeout caps, i.e. the harness
constant rather than either algorithm**. Raise the cap and the ratio grows without bound; it is
measuring my `--timeout` flag. The paired figure on the common set (34.5s → 2.4s) is the honest one
and is itself inflated by the refiner being reached less often. The iteration counts are the
durable measurement here; the seconds are not.

### The eleven shipped templates

These are the only cases in the panel that are real workflows over real transform libraries, and
they are what a claim about "the solver" has to survive.

**PUCT is sound on 11/11 and returns the identical plan on 10/11.** Total solve time 29.2s → 19.9s
(−32%); mcts iterations 236 → 227.

The one template whose plan changed is `isolate_assembly_from_long_reads` (20 → 11 mcts
iterations). Both plans pass `check_plan`. **Soundness is not the same as appropriateness** — for a
real workflow, two sound plans may select different tools, and nothing in this harness adjudicates
which is the better science. That single case would need a human to look at it before anyone
adopted this.

`metagenomics_from_paired_reads`, the one expensive template at 24.1s → 17.3s, is worth reading
carefully: its refiner runs the full 256 iterations under *both* policies and it returns the same
plan, so the 6.9s saved is entirely per-iteration cost — PUCT is walking cheaper intermediate
states, not doing less refinement. That is a smaller and less interesting win than the generated
corpus suggests, and it is the realistic one.

### The reward shape is what the result rests on

Eight `sink` cases, holding everything else fixed:

| reward | solved | iterations | wall clock |
|---|---|---|---|
| baseline (weighted split) | 7 / 8 | 120 | 20.0s |
| PUCT, `delta` | **8 / 8** | 130 | **0.1s** |
| PUCT, `absolute` | 6 / 8 | 343 | 20.8s |

The bandit formula is the same in both PUCT rows. Crediting an action by the *improvement* it made
rather than by the state it left behind is the difference between beating the baseline and losing
to it.

### Robustness

Twelve hyperparameter configurations were tried on the tuning slice (c_puct ∈ {1.5, 4, 12} ×
top_k ∈ {1, 3} × epsilon ∈ {0, 5%}). **All twelve solved 120/120 with no unsound plan**, in
1,350–1,865 iterations. The result is not knife-edge on any hyperparameter, which matters more than
which cell won. `top_k=1` was best on both iterations and plan size; `c_puct` and epsilon barely
registered. Chosen: `c_puct=1.5, top_k=1, epsilon=0, reward=delta`.

Given the ablation, that insensitivity now reads differently: `c_puct` scales a term whose *shape*
is what matters, and epsilon adds diversity to a rule that already has some from the visit-count
penalty. The one parameter that would matter — whether the value is estimated at all — is the one
the grid did not vary, and it is the subject of the section above.

## Which part of PUCT is doing the work — and it is not the estimator

The obvious way this result could be overstated is if the bandit machinery were inert and the gain
came from somewhere else. It partly is. Three arms differing only in what `observe` records, on 80
held-out `sink`/`cyclic` cases:

| arm | solved | iterations | steps/solve | wall clock |
|---|---|---|---|---|
| baseline (weighted 75/20/5) | 74 / 80 | 2,633 | 8.50 | 81.4s |
| PUCT, full | **80 / 80** | 963 | 7.08 | 0.58s |
| PUCT, **Q forced flat** (N still counts) | **80 / 80** | 1,058 | 7.11 | 0.74s |
| PUCT, **N frozen** (prior argmax only) | 55 / 80 | 5,619 | — | 83.5s |

**The learned value is close to inert.** Holding Q at its first-play value for every arm costs
about 10% on iterations and nothing at all on solve rate. Whatever this change is winning, it is
not winning it by estimating action values.

**The visit-count term is what matters.** Freeze N — leaving a pure argmax over the prior — and the
search collapses to 55/80, *worse than the shipped rule it was meant to beat*, while burning more
than twice the iterations.

Instrumenting the chosen configuration over 2,586 selections makes it starker. Q's spread across
the frontier is **exactly 0.0 on 83.8%** of selections, and at the chosen arm it is `0.0` on 50.5%
and `fpu` on 46.8% — so on 97% of selections Q is a two-valued *"have I tried this transform yet"*
flag rather than a value. The residual advantage of the full estimator over counts-only is 276
iterations across the whole 120-row tuning slice, and **72.5% of that comes from a single problem**
(`sink-14`, counted twice because PUCT is deterministic). Only 24 of 120 rows differ at all.

Worse, the reward is time-correlated in the way this design was supposed to avoid: pooled mean
reward by observation index runs 0.10, 0.18, 0.20, 0.21, 0.28, 0.35, 0.41, 0.47, then falls to
0.27, 0.15, and is **0.000–0.002 from index 14 onward**. That is the same "credits position, not
contribution" failure the `delta` mode was introduced to fix, milder but present — and in the
refiner it is structural, because `improved` is a new-global-best flag against a monotonically
rising incumbent, so it is 1 early and 0 late by construction.

So the honest mechanism is: **UCB-style exploration applied to the heuristics the solver already
had**, not a value estimate learned during search. The `sqrt(N_total)/(1+N(key))` factor forces the
search to stop re-selecting the same transform family and to spread across the frontier; the priors
are the same `distance` and `opportunity` numbers the baseline uses. Q is a small bonus on top.

Two things follow. First, the answer to "can PUCT drive this solver" is yes, but the interesting
half of PUCT — the estimator — has nothing to work with here: at a 256-iteration budget with
statistics aliased across transforms, there is not enough evidence to separate arms, which is
exactly what the earlier instrumentation showed when Q spread sat at 0.0 for the median selection.
Second, the shipped rule's crude randomization is doing real work. Pure greedy on its own
heuristics is far worse than the 75/20/5 split; the split's alternation and its 5% uniform arm are
supplying diversity that a naive "better" rule throws away.

## What actually changed the outcome

Two defects in the first implementation, both found before any number was reported:

**The progress signal was measuring the wrong thing.** It read the minimum distance-to-target over
the transforms a state had unlocked. But that candidate set only ever grows, so the reading rises
monotonically along every path, and crediting an action by it ranks transforms by how *late* they
are typically applied rather than by whether they helped. Instrumenting the policy showed the
consequence directly: Q spread across the frontier was 0.0 at the median, so every arm carried the
same value. It was replaced with a goal-count heuristic — the fraction of the *target's*
requirements satisfiable from what the state has.

That repair is what made the policy usable, but the ablation above is the more honest reading of
it: what the fix bought was not a working estimator so much as a reward that stops actively
misinforming one. Q went from carrying nothing to carrying about 10% of the iteration saving, and
it is still a two-valued flag on 97% of selections. Both statements are true and the second is the
one that should govern decisions.

**The refiner's second score channel inverts.** It is `score*valid` over a score that is never
positive (`solver_math.entropy` returns Σp·log₂p with no negation), so an invalid state's `0.0`
outranks every valid one. The shipped rule keeps this, because reproducing it is the point; the new
policy was inheriting it by accident through the prior. `Arm.prior_scores` now carries the same
*intent* — score, and validity — encoded so that better is higher.

Worth stating plainly: **repairing this buys nothing measurable.** A weighted rule ranking its
second arm on `(score, valid)` instead of `score*valid` reproduces the baseline's numbers exactly —
113/120 solved, 3,089 mcts and 923 refiner iterations, 7.062 steps per solve. The repair is
justified on its own terms and is not part of the win. The quirk itself is a live oddity in the
shipped rule and deserves its own ticket rather than being smuggled in with a policy change.

## If any of this were to be adopted, port the smaller thing

The ablation changes what is worth building. The **counts-only** variant reaches the same solve
rate for about 10% more iterations, and it is dramatically cheaper to port:

- no `Q` state, no `observe`, no reward shaping;
- **no `progress_of`** — the goal-count heuristic that walks every successor's endpoint set, which
  is the only genuinely new per-expansion cost this work introduced;
- nothing new precomputed: the prior is a softmax over `distance` and `opportunity`, which the
  solver already computes.

What remains is a per-transform integer counter and one `sqrt` and division per arm per selection.
That is a much smaller change to hold bit-identical across the Rust and Python halves than the full
policy, and it avoids putting the reward path — the part with the most room for the two
implementations to disagree — on the decision path at all.

The recommendation is therefore: **if this is pursued, pursue the exploration term, not the
estimator.** Keep the full version only if a later change gives Q something to learn from — a
larger iteration budget, per-node rather than per-transform statistics, or a genuine rollout.

That variant is a first-class option rather than something to reconstruct: `PuctConfig(use_value=
False)`. It reproduces the ablation exactly (963 → 1,058 iterations on the same 80 cases, same
80/80 solve rate) and sets `wants_rewards` false, which is what switches the progress walk off.

## Limits

- **Python only.** No Rust port. No `msm_solver` binary is staged on this host, so the panel and
  the comparison both ran on the Python implementation. That makes the A/B a fair like-for-like,
  but no wall-clock number here describes the shipped path, which is roughly 15× faster.
- **Adoption would move every plan.** `tests/solver/fingerprints.json` would need regenerating and
  `SOLVER_RNG_VERSION` bumping, and the Rust and Python halves would have to move together, bit for
  bit. PUCT puts `ln`-class arithmetic on the decision path, which is the one category the wire
  already carries a dedicated probe op for because the two implementations link different libms.
- **At `top_k=1, epsilon=0` the search is deterministic.** The policy consumes no random draws, so
  the `seed` argument becomes inert — one plan across four seeds where the baseline gives four.
  Good for reproducibility, but it would fail
  `test_the_corpus_spans_both_sides_of_rng_sensitivity`, which requires seed-sensitive cases to
  exist. `top_k=3` recovers three distinct plans and `epsilon=5%` recovers two.
- **The mcts and refiner changes are not separated.** Both sites moved together; this work does not
  say how much each contributes. The ablation separates Q from the visit-count term but not the two
  call sites from each other.
- **The ablation is on 80 cases from two profiles**, not the full 640. It is decisive about Q being
  worth ~10% rather than being the mechanism, but the exact figure should not be quoted precisely.
- **The generated corpus is not the shipped workload.** It is what exists in enough volume to
  measure. The eleven templates are the realistic cases and are reported separately.

## Reproducing

Harnesses are under `/home/tony/.claude/jobs/a15841b8/tmp/puct/` (node-local): `tune.py` for the
hyperparameter grid, `verify.py` for the A/B sweep, `analyze.py` for the report above,
`templates_ab.py` for the shipped templates. The policy itself is exercised by
`tests/metasmith/solver/test_selection_policy.py`.
