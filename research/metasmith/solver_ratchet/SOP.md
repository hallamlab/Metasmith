# Ratchet SOP — optimising the solver's node-selection policy

You are one arm of a parallel ratchet. Read this once, then work.

## What you are optimising

`msm_solver` searches backwards from a target for a plan. Twice per solve it must pick one
node from a frontier: once in the MCTS phase, once in the refiner. That choice is the only
thing you may change. It lives in **`src/workflow_solver/src/policy.rs`** and nowhere else.

The shipped rule draws one of three arms — two exploit arms reading a score channel, one
uniform explore arm — and takes the top-scoring node. It is `Kind::Weighted`.

## The two commands

    score                 build, gate, measure, print a table, log the result
    submit <label> "<one-line claim>"    re-score from clean, commit, record the finding

`score` takes about ten seconds. Run it as often as you like — it is the fastest way to
learn anything here. It handles the build, the correctness gates, the benchmark and the log.
Nothing else is your problem.

Useful flags: `score --policy weighted` (sanity check: must print TIE), `score --puct
"c_puct=2.0,fpu=0.3"` to pass configuration, `score --set full` for the expensive set (only
when asked — it is slow).

## What you must never do

- **Do not run cargo, docker, pytest, git, or mamba.** `score` does all of it. A hand-rolled
  build will not match what is measured.
- **Do not edit anything outside `src/workflow_solver/src/`.** Not the tests, not the
  harness, not the payloads, not another worktree.
- **Do not change behaviour when `MSM_SOLVER_POLICY` is unset.** That is gate 1 and it fails
  hard. Your work must be reachable only through the policy env var.
- **Do not measure wall clock or tune toward it.** Three of you share this box; timings are
  meaningless here. `score` ranks you on deterministic counts and the supervisor re-times the
  winner alone afterwards.

## The env-var contract — identical for every arm

> `MSM_SOLVER_POLICY` selects the rule: `weighted` (default, unchanged behaviour) or `puct`.
> `MSM_SOLVER_PUCT` optionally carries `c_puct`, `temperature`, `fpu`, `use_value` as
> comma-separated `k=v`. Unset means the binary behaves exactly as it does today.

An unrecognised policy name is refused rather than defaulted, so a typo cannot silently
report a measurement of the incumbent under your name.

## How you are ranked

Over 81 frozen wire payloads, in **two families that are not interchangeable**:

- **real** (17) — 13 rungs of a real metagenomics workflow from 3 to 68 targets, plus 4 real
  metagenomics arms. This is what ships.
- **sink** (64) — generated pathologies. A stress signal, not the objective.

The keys, in order:

1. **real-workflow steps must not regress.** A config that lengthens real plans is a LOSS
   however many generated cases it buys. Round 1 found exactly that trade — `epsilon_milli=200`
   solves one more `sink` case and costs 9 ladder steps and 6 metagenomics steps — and it is
   not a win. Plan length is what the refiner turns superlinearly into wall clock.
2. **real-workflow steps lower** is a WIN outright.
3. then **total solved**, then total steps, then iterations.

`score` prints both families in separate columns and gives WIN / TIE / LOSS against the baseline
and the current ratchet head, plus exactly which payloads moved.

## The gates

Both run inside `score`, before any number is reported.

- **Gate 1, default path.** With the policy unset, every reply must be byte-identical to the
  recorded baseline on all 81 payloads.
- **Gate 2, witness.** Every complete plan you emit is adjudicated by the *reference*
  binary's proved witness, not your own build. A plan the witness rejects is not a result.

A failing gate exits non-zero and prints the cause on the first line. Compiler errors come
back verbatim.

## Working notes

- `RefinerState.swapped_in` and the transform id are the keys `select` hands you. Nothing is
  revisited — the frontier is a plain vector popped by swap-remove — so per-node statistics
  are impossible; anything stateful must accumulate against those coarser keys.
- `select` takes accessor closures, not slices, so the default rule still allocates only in
  the iterations that need it. Keep that property.
- The Python reference implementation is `src/metasmith/models/solver_policy.py` —
  `PuctSelection` and `PuctConfig`. Read it. Do not edit it; it is a different backend.
- Read the shared log before you start. Another arm may have already tried your idea, and a
  recorded negative result is worth as much as a win.

## When you have something

`submit <label> "<claim>"` — even for a LOSS. A negative result nobody wrote down gets
re-tried by the next round. Keep iterating after a submit; you may submit more than once.

Report back to the supervisor in a few lines: what you tried, what `score` said, and what you
would try next.
