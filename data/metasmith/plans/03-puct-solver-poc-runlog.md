# PUCT as the solver's selection policy — a proof of concept

## Context

The metasmith plan solver currently chooses which node to expand by a fixed three-way weighted
split: 75% greedy on one static heuristic, 20% greedy on a second, 5% uniform random. Both
heuristics are computed once before the search and never revised, so nothing the search discovers
can override them. The question is whether a PUCT selection rule — the AlphaZero-family index that
combines a learned value estimate with a prior and a visit-count exploration term — can replace
that split without losing plan soundness, and ideally while finding plans in fewer expansions.

## What you said

> as a POC, try transpiling the solver to use PUCT

> there should be a panel of tests that were used for the rust solver migration, use those to verify correctness

> objective is show that PUCT can be used as the solver's implementation with equivalent or better performance (so do a bit of tuning on performance, but correctness is paramount)

## Issues

- I1. The selection policy mixes two static per-transform heuristics at a fixed global ratio, so
  evidence gathered during the search cannot influence which node is expanded next.
- I2. The explore arm is uniform over the whole frontier, so exploration is undirected — it does
  not concentrate where the search is uncertain.
- I3. No statistic survives a node's expansion. A node is popped and discarded, so there is no
  visit count and no backed-up value anywhere in the search.
- I4. The shipped benchmark corpora solve in 7–13 mcts iterations against a budget of 256, so they
  cannot distinguish one selection policy from another; an A/B run on them measures noise.
- I5. Changing the selection rule changes every plan the solver produces, so the pinned
  fingerprints and the cross-language decision contract both move.

## High-level goals

- G1. Show whether PUCT can drive this solver at all, without producing unsound plans.
- G2. Show whether it finds plans in fewer expansions than the current split, on problems where
  the selection policy actually matters.
- G3. Leave the existing solver reachable and unchanged, so the comparison is honest and the
  change is reversible.

## Acceptance criteria

- `check_plan(...).ok` holds on 100% of cases PUCT solves, across the generated corpus, the stress
  corpus, a wide sweep, and the shipped templates. Soundness is the gate; nothing else matters if
  this fails.
- PUCT's solve rate is greater than or equal to the baseline's on the same corpus, judged against
  `exhaustive_solvable` / `forward_closure_solvable` where they return a verdict.
- The whole existing `tests/metasmith/solver` axis stays green with the policy left at its default.
- A load corpus exists on which the baseline uses a substantial fraction of its iteration budget,
  and PUCT's iteration count and wall clock on it are reported against the baseline's.
- The chosen hyperparameters are backed by a recorded sweep, not asserted.

## Tasks

T1. Capture the baseline panel and bench numbers (G2)
T2. Add a switchable selection-policy seam, default unchanged (G3)
T3. Implement PUCT selection behind the seam (G1)
T4. Verify PUCT correctness with check_plan across corpus and sweeps (G1)
T5. Tune c_puct, top-k, reward shape and FPU against the bench (G2)
T6. Adversarial review of the result by a fresh-context subagent (G1, G2)
T7. Write the POC report and journal the run (G1, G2, G3)
T8. Debrief

## Approach by task

**T1.** Run `solver_bench` on the generated corpus, the stress corpus and the shipped templates,
plus the solver test axis, on the unmodified tree. Because the shipped corpora turned out to be
trivial for the search, this task also builds a *load corpus*: sweep the differential profiles
across problem and solve seeds, record mcts iterations per case, and cut the high-iteration tail.
`Gotchas:` the standard library must be compiled first or the template cases silently skip; the
build script calls bare `python`, so it needs the msm env on PATH, not just `PYTHONPATH`.

**T2.** Introduce a policy object consulted by the mcts `select_node`, defaulting to the current
weighted split so that byte-for-byte behaviour is preserved when nothing is asked for. Selection
must stay inside `DecisionStream` primitives so the decision stream remains reproducible and the
existing RNG contract tests keep meaning something. `Gotchas:` the number of draws consumed per
iteration is part of the contract — a policy that consumes a different number of words shifts every
subsequent decision, which is expected for PUCT but must not leak into the default path.

**T3.** Build the prior from the existing `distance` and `opportunity` tables, normalized over the
frontier; maintain visit counts and a backed-up value keyed by transform; feed the value from an
observable progress signal already available in the loop. `Gotchas:` the frontier is a pool of
*applications* across timelines rather than one node's children, so the prior must be normalized
over what is actually being chosen between; a fully deterministic argmax would make every seed
produce the same plan and break the corpus test that requires rng-sensitive cases to exist.

**T4.** Soundness by `check_plan` over the corpora and a wide generated sweep; solve rate against
the oracles; the full solver axis under both policies. `Gotchas:` `Solution.complete` and
`check_plan(...).ok` are different questions and a plan can be one without the other; a `None` from
`exhaustive_solvable` is not a `False`.

**T5.** Sweep c_puct, the top-k breadth, the reward shape and the FPU seed against the load corpus,
holding soundness at 100%. `Gotchas:` tuning against the same corpus used to report the result is
how a number gets manufactured — hold out part of the corpus, or report the tuning set explicitly.

**T6.** Freeze the implementation, dispatch a reviewer at it with the numbers, triage findings
before editing anything.

**T7.** Report with the numbers and the honest limits. **T8.** Debrief.

## Callouts

- This is Python-only unless the result justifies a Rust port. No `msm_solver` binary is staged on
  this host, so the whole panel runs on the Python implementation anyway, which makes the A/B a
  fair like-for-like — but it means no wall-clock number here describes the shipped path.
- PUCT changes every plan, so `tests/solver/fingerprints.json` would need regenerating if this were
  ever adopted as the default. It is deliberately *not* being made the default here.

---

## Autopilot

**Guardrails.**
- The default policy path must stay bit-identical. Any change that moves a baseline fingerprint
  while the policy is unset is a bug, not an expected consequence.
- Soundness (`check_plan`) is the gate. A performance number from a run with any unsound plan in it
  is not reportable.
- Do not regenerate `tests/solver/fingerprints.json`. PUCT is opt-in for this POC.
- Do not delete or rewrite the existing selection code — add beside it.

**Failure modes I will not be around to catch.**
- Measuring on a corpus the search finishes in 8 iterations, and reporting the resulting noise as a
  win or a loss. Mitigated by the load corpus in T1.
- Tuning on the reporting corpus. Mitigated by holding out a disjoint slice.
- A green run that silently used the baseline policy because the switch did not take. Mitigated by
  asserting the active policy inside the measurement harness.

**Cadence.** No Monitor and no backup cron. Every step of this run is a harness-tracked background
job, and the harness notifies on both success *and* failure (three SIGTERM'd jobs were reported
that way). A Monitor would duplicate that channel and a cron would add nothing the notifications
do not already carry. Recording the omission because the skill's default is to arm both.

**Resume handles.**
- Worktree: `/home/tony/agentic_workspace/projects/metasmith/engine/solver` (branch `feat/solver`)
- Scratch + results: `/home/tony/.claude/jobs/a15841b8/tmp/puct/`
- Env: `PYTHONPATH="$PWD/src" mamba run -n msm …`
- Baselines: `baseline_corpus.json`, `baseline_stress.json`, `baseline_full.json`
- Load probe: `load_probe.json`

### Live state (T1–T5 done, T6 in flight)

Everything measured. Full axis green with the new tests: **157 passed, 237 skipped, 1 xfailed**
(141 pre-existing + 16 new). Report drafted at
`data/metasmith/plans/02-puct-selection-poc.md`. Waiting only on the adversarial reviewer, whose
brief names the Q-ablation as the thing most likely to overturn the headline — if PUCT-minus-Q
performs the same, the win is the visit-count-penalized prior rather than the learned value, and
the framing has to change. Do not finalize the report before that comes back.

Uncommitted: `solver.py` (modified), `solver_policy.py`, `test_selection_policy.py`,
`02-puct-selection-poc.md` (all new).

### Superseded state

T1–T3 complete. T5 (tuning) in flight; T4 (held-out verification) queued behind it.

**Implementation frozen.** `src/metasmith/models/solver_policy.py` is new; `solver.py` carries the
two call sites, the progress measure and the reward wiring. The default path is verified unchanged
three ways: fingerprints on the generated corpus, fingerprints on all 11 templates, and template
wall clock against the pre-change numbers (+0.5%, noise).

Load regime: the `sink` profile. Baseline on the tuning slice (sink+cyclic+pgroups, problem seeds
0–19, solve seeds 42 and 7, 120 cases): **113/120 solved, 0 unsound, 1 timeout, 3089 mcts
iterations**. That is the number PUCT has to match.

Spot checks so far are strongly positive — sound everywhere, fewer iterations on 11/11 easy cases,
and 6/6 on a hard `sink` slice where the baseline manages 2/6 — but these do not count as the
result.

**Held-out slice moved to problem seeds 40–79.** The original plan said 20–39, but my own spot
checks during development used `sink-22` and `sink-25`, which sit in that range, and the progress
measure was redesigned off instrumentation of `sink-22`. That is contamination however informally
it happened, so the reporting slice is being moved to seeds I have never run.

Next after the grid: pick a config, run `verify.py` on seeds 40–79 across all 8 profiles with the
oracles on, run `templates_ab.py`, then the adversarial review. Plan *size* is reported alongside
solve rate — a sound plan with three times the steps is not an improvement, and soundness alone
would not catch that.

### Run log

- **17:25** Env verified. No `msm_solver` staged → whole panel runs on the Python solver. Backend
  reports `python`.
- **17:26** Baseline on CORPUS: 8/8 ok, 0.035s total. On STRESS_CORPUS: 3/3 ok, 0.021s total,
  7–13 mcts iterations each. **Finding: neither shipped corpus exercises the selection policy.**
  This is the same fact the earlier analysis predicted from `max_iter=256` versus observed
  iteration counts, now measured. A load corpus is required before any A/B means anything.
- **17:28** `dev/libraries.sh -bm` failed with `python: command not found` — the script calls bare
  `python`. Re-run under `mamba run -n msm bash dev/libraries.sh -bm`, which succeeded: 10
  libraries compiled.
- **17:30** Template baseline: 11 templates, all sound, 25.5s total — of which
  `metagenomics_from_paired_reads` alone is 20.2s. Its split is **50 mcts iterations against 256
  refiner iterations**, and every other template runs the refiner once or twice. So on the one
  template that costs anything, the phase PUCT would replace uses a fifth of its budget while the
  refiner exhausts its own. This is the measured version of what `refine.rs`'s header already
  says, and it caps what a selection-policy change can win on wall clock.
- **17:35** Load probe, 1280 cases over the 8 differential profiles: p50 of **5–9 mcts iterations**
  everywhere, and only 6 of 1120 non-sink cases exceed 30. The `sink` profile is the exception —
  p50 17, p90 at the 256 cap. Cases that hit the cap are exactly the cases the baseline fails to
  solve. So solve rate on `sink` is the honest benchmark, not wall clock on the shipped corpus.
- **17:41** Seam landed. `solver_bench --no-templates` against the pre-change baseline reports no
  `!!` on any case: the default path is fingerprint-identical, twice (once after the seam, once
  after the reward refactor).
- **17:44** First PUCT smoke test: sound everywhere it solves, and on the 11 easy corpus cases it
  reaches the *same plan size* in **fewer iterations on 10 of 11**. But it fails `sink-22/s42`,
  which the baseline solves — and its apparent speed on the hard cases is an artifact of not
  solving them, so the refiner never runs. Solve rate, not wall clock, is the thing to fix.
- **17:52** **Reward defect found by reading, before it was measured.** `progress_of` reads
  `candidate_transforms`, which only ever grows, so absolute progress rises monotonically along
  every path and Q would rank transforms by how late they are typically applied rather than by
  whether they help. Reworked to credit the *delta* a step made, keeping the source→successor
  pairing that the old flattening comprehension threw away. Both shapes are now a config value and
  the grid tests both rather than my assuming which is right. Killed the tuner that had already
  imported the old module rather than let it report on code that no longer exists.
- **17:55** Tuner and the pytest axis are running concurrently, so **the wall-clock column in this
  coarse pass is contaminated**. Solve rate and iteration count are not, and they are what the
  tuning decision rests on; final timings will be re-measured with nothing else on the box.
- **18:05** Instrumented the policy on a hard case rather than guessing at why it underperformed.
  The hypothesis under test — that the softmax prior degenerates as the frontier grows — was
  **wrong**: the frontier is small (median 19 arms, max 37, spanning ~4 distinct transforms), so
  normalization was never the problem. What the instrumentation did show is that **Q spread across
  the frontier was 0.0 at the median**: every arm carried the same value, so Q contributed nothing
  and the search was running on prior and visit-count alone. Recording this because the cheap
  measurement refuted the expensive theory, and the second defect was invisible from the code.
- **18:12** Root cause: the delta of the old progress measure was almost always exactly zero. The
  minimum distance over `candidate_transforms` rarely moves on a single application, so nearly
  every reward was 0 and Q was flat. Replaced it with a goal-count heuristic — the fraction of the
  *target's* requirements satisfiable from `state.have`, with the distance term demoted to a
  sub-requirement tie-break. Q spread is now nonzero on 30–50% of selections.
- **18:30** Found and fixed a defect I had introduced myself: the refiner's channel 1 is
  `score*valid`, whose sign inverts, and I was feeding it straight into the PUCT prior — so the
  new policy was inheriting the old bug through the back door. Added `Arm.prior_scores` so the
  shipped rule keeps ranking on the exact channels the decision contract names while PUCT is given
  the same *intent* encoded correctly (score, and validity as an indicator). Default path
  re-verified fingerprint-identical after the change.
- **18:34** Full solver axis on the default policy: **141 passed, 237 skipped, 1 xfailed** in
  11m43s. The skips are the `python_solver` and engine lanes, which need a staged binary.
- **18:36** Implementation frozen. Killed and restarted the grid twice more for importing stale
  code; the second kill also killed its own replacement, because the new background job's wrapper
  carries `tune.py` in its argv and matched my own `ps | grep` pattern. Kill by PID in a separate
  invocation from the launch.
- **19:10** First grid cell, on the tuning slice and against a baseline of 113/120 solved / 3089
  iterations / 69.4s: the **default PUCT config solves 120/120 in 1350 iterations and 0.77s**.
  Treating the wall-clock ratio with suspicion until plan sizes are checked — a policy that
  "solves" more by returning degenerate plans would look exactly like this, and `check_plan` calls
  every sound plan sound regardless of size. Added plan-size-per-solve to the tuner before
  believing any of it.
- **19:12** The grid's second cell — the `absolute` reward — ran for nine minutes without
  finishing, against 0.77s for `delta`. That is the design argument confirmed rather than
  contradicted, but it makes the full cross-product unaffordable, so `absolute` moved to its own
  small run and the main grid is delta-only.
- **19:30** Tuning grid, 12 delta-reward configurations on the tuning slice against a baseline of
  113/120 solved, 2833 iterations, 7.06 steps per solve. **Every one of the 12 solves 120/120 with
  no unsound plan**, in 1350–1865 iterations. The result is therefore not knife-edge on any
  hyperparameter, which matters more than the winner. `top_k=1` beats `top_k=3` on both iterations
  and plan size; `c_puct` and epsilon barely register. Chosen: `c_puct=1.5, top_k=1, epsilon=0,
  reward=delta` — 1350 iterations, **6.77 steps per solve**.
  **The plan-size column is the one that matters**: PUCT's plans are *smaller* than the baseline's,
  so the extra solve rate is not being bought with bloat, which soundness alone would not catch.
- **19:33** Baseline totals move slightly between identical runs (2833 vs 3089 iterations, 1 vs 2
  timeouts). Not nondeterminism in the search: a timed-out case contributes no iteration count, so
  which cases trip the wall clock changes the sum. PUCT hits no timeouts, so its totals are stable.
- **19:36** Confirmed a property with two faces: **at `top_k=1, epsilon=0` PUCT consumes no random
  draws at all**, so the search is fully deterministic and the `seed` argument stops doing
  anything — one plan across four seeds where the baseline produces four. Good for
  reproducibility; it would also fail `test_the_corpus_spans_both_sides_of_rng_sensitivity` if
  adopted as the default, and makes a user-facing knob inert. `top_k=3` recovers 3 distinct plans
  and `epsilon=5%` recovers 2, so diversity is available if wanted.
- **19:55** **Held-out result, 640 cases on problem seeds 40–79 across all eight profiles.** No
  plan from either policy refused by `check_plan`. Baseline solves 627/640; PUCT solves **640/640**
  — 13 gained, **0 lost**. On the 627 both solved: mcts iterations −34.8%, refiner iterations
  −58.1%, plan steps **−3.9%** (smaller, not larger), wall clock −93.2%. PUCT returns the
  *identical* plan on 490 of 627, a larger plan on 3, a smaller one on 23. 73% of the baseline's
  total wall clock went on cases it never solved, so the raw 129.4s→2.8s ratio flatters PUCT and
  the both-solved figure is the one to quote.
- **19:58** Reward-shape comparison on 8 `sink` cases, documenting the design claim: `delta` solves
  8/8 in 130 iterations and 0.1s; `absolute` solves 6/8 in 343 iterations and 20.8s; the baseline
  solves 7/8. The reward shape, not the bandit formula, is what the result rests on.
- **20:05** Templates, the only realistic cases in the panel: **PUCT sound on 11/11, identical plan
  on 10/11**, 29.2s → 19.9s. The one plan that changed is `isolate_assembly_from_long_reads`; both
  versions pass `check_plan`, but soundness is not appropriateness and a human would need to look
  at that one. On `metagenomics_from_paired_reads` — the only expensive template — the refiner runs
  its full 256 iterations under both policies and the plan is identical, so the 6.9s saved is
  purely cheaper intermediate states. That is the realistic win and it is far smaller than the
  generated corpus implies.
- **20:20** Full solver axis re-run including the new tests: **157 passed, 237 skipped, 1 xfailed**
  in 12m22s. The 16 new tests pin the seam's contract — that the shipped rule is the default, that
  a greedy selection costs one RNG word and an exploring one costs two (`pick_top_k` at k=1 spends
  nothing), that statistics are shared by key rather than per arm, that the delta reward credits an
  improvement where the absolute one credits a position, and that the shipped rule is told not to
  compute rewards. Two of these tests were wrong on the first pass and the code was right: I
  counted primitive *calls*, but `weighted_index` and `pick_top_k` both delegate to `bounded_int`,
  so a call count double-counts. Draw counts are the contract; call counts are not.
- **20:35** **Ablation, and it partly refutes my own headline.** Three arms differing only in what
  `observe` records, over 80 held-out sink/cyclic cases. Full PUCT 80/80 in 963 iterations. **Q
  forced flat: still 80/80, 1058 iterations** — the learned value is worth ~10% on iterations and
  nothing on solve rate. **N frozen (prior argmax only): 55/80**, worse than the baseline's 74/80
  and burning more than twice the iterations. So the mechanism is the visit-count exploration term
  over the *existing* heuristics, not the estimator. Reframed the report's headline accordingly
  before reporting it, rather than letting "PUCT works" stand as if the bandit's interesting half
  were doing the work. Ran this myself rather than waiting for the reviewer, since I would have had
  to verify any such finding independently anyway.
  Second-order finding worth keeping: the shipped 75/20/5 rule's crude randomization is doing real
  work — pure greedy on its own heuristics is much worse than the split.
- **20:45** Promoted the ablation's winner to a first-class option, `PuctConfig(use_value=False)`,
  rather than leaving the recommended variant reachable only through a subclass in a scratch
  script. It needs no reward, so `wants_rewards` is false and the progress walk — the only
  expensive thing this work added to the search — is skipped entirely. Verified it reproduces the
  ablation exactly: 963 → 1,058 iterations, 80/80 both ways. Default path re-verified identical;
  policy tests now 17 and green. This is completing the recommendation, not widening the task: the
  point of the POC is to say what is worth porting, and the thing worth porting should be the thing
  that is actually there.
- **21:05** **Adversarial review returned nine findings; triage below.** It is a good review and it
  landed several real hits.
  - **F5 VERIFIED, fixed — the most serious.** The default path was decision-identical but **19%
    slower** (5.13s → 6.11s over 336 solves, non-overlapping). Reproduced independently against a
    pristine HEAD tree before touching anything. Cause: a frozen `Arm` dataclass built per frontier
    entry per selection, ~15× the cost of the list build it replaced, on a path that uses only the
    scores. My template check missed it because `run_templates` times library load and plan
    assembly too, diluting it to +0.5% — a genuinely instructive miss: I verified the cheap thing
    and thought I had verified the expensive one. Fixed with a `select_from` entry point that
    allocates what the pre-seam code allocated.
  - **F2 VERIFIED, fixed.** My two solve-based tests called `problem.solve()` with no backend pin,
    so on a machine with a staged binary they would run the *engine*, `UsePuctSelection` would be
    inert, and they would pass having tested nothing. `tests/solver/AGENTS.md` names this exact
    failure twice and I had read it. Now wrapped in `UsePythonSolver()` with
    `assert Backend("solve") == "python"` inside.
  - **F1 VERIFIED, already self-found, and the review sharpens it.** Q is a two-valued flag on 97%
    of selections; the estimator's whole residual is 276 iterations of which 72.5% is one problem;
    and the reward is time-correlated (mean reward ≈0 past observation index 14), which is a milder
    form of the very failure `delta` was introduced to fix.
  - **F3, F4, F6 VERIFIED — restated in the report.** The wall-clock ratio is substantially the
    harness `--timeout` constant (90 of the baseline's 129s is six 15s caps); the corpus is half
    the size it looks because PUCT is deterministic; the capability claim is **5 distinct problems
    in 320**, not 13 rows in 640.
  - **F9 VERIFIED, fixed.** `fork()` returned the base class rather than `type(self)`, so every
    ablation subclass silently forked into the parent. `_priors` used `max` arity where `min` is
    the safe guard. The refiner bypassed `reward_for`, so `use_value=False` would not have
    flattened its Q — the two call sites disagreed about what one config meant.
  - **F8 accepted as clean.** Soundness stands (240 PUCT solves, 0 dead steps, 0 checker notes);
    default fingerprint parity 384/384; the iteration-order invariant is untouched. Also useful:
    the `prior_scores` repair **buys nothing measurable** — a corrected weighted rule reproduces
    the baseline exactly — so it is not part of the win and the underlying quirk wants its own
    ticket.
  - **F7 partially accepted.** Several of my tests were weak or self-confirming. Rewrote the two
    worst; the draw-count test the reviewer singled out as good is kept as-is.
  - Re-verified after every fix: fingerprints identical, 17/17 policy tests green, and PUCT's own
    numbers unmoved (963 / 1058 iterations, same as before the fixes).
- **18:50** **Measurement bias found in my own code, before any number was reported.** The reward
  block ran unconditionally, so the shipped rule was paying to compute a progress signal it
  discards — and since that signal walks every successor's endpoint set, it was slowing the exact
  baseline PUCT is measured against. Gated it behind `wants_observations` (and the refiner's
  `prior_scores` construction with it). Verified against the *original* template numbers rather
  than only fingerprints: 25.514s → 25.654s, +0.5% on a shared box, no fingerprint moved. Timings,
  not just plans, now describe an unmodified baseline.
- **18:15** First real result, three `sink` cases at the default config:
  `sink-22` baseline 256 iterations → PUCT **37**; `sink-16` baseline 145 → PUCT **39**; and
  `sink-14`, which **the baseline fails to solve at all**, PUCT solves in **37**. All sound.
  This is the first evidence the change buys capability rather than only speed.
- **18:40** On frozen code, a wider spot check. Easy corpora (CORPUS + STRESS, 11 cases): PUCT is
  sound on all 11 and uses **fewer mcts iterations on 11 of 11**, reaching the same plan size. Six
  `sink` cases at solve seed 42: the baseline solves 2 and fails 4; **PUCT solves all 6**, and on
  the two the baseline also solves it cuts 256→37 and 145→39 iterations, with refiner iterations
  on `sink-22` dropping 256→54. Still a spot check, not the held-out measurement.
