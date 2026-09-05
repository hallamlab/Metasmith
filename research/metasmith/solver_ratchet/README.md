# solver_ratchet

## Purpose & Contents

The harness that measured the solver's node-selection policy, and the frozen corpus it measured
against. Use it to hold any future selection change to the same gates and the same numbers.

The finding this produced is `data/metasmith/plans/09-the-selection-ratchet.md`. The
per-run record of everything three parallel agents tried, wins and losses alike, is
`results/findings.md`.

Not here: what the policies do. That is `src/workflow_solver/src/policy.rs`, whose comments carry
the mechanism and the measured reason for every default.

## The two commands

    score                              build, gate, measure, print a table, append to the log
    submit <label> "<one-line claim>"  re-score from clean, commit, record the finding

`score` takes about twelve seconds. It builds the engine, runs both gates, measures 81 frozen wire
payloads, ranks the result against the baseline and the current head, and names the payloads that
moved. `SOP.md` is the one page a worker reads.

**CAUTION** `score` builds with a host cargo from the `msmrust` environment and loosens `lto` and
`codegen-units` through the environment, into a per-worktree target directory. It never stages the
binary. Replies are byte-identical to the shipping profile -- checked on all 81 payloads, and
re-checked at the end of the run -- but a number that is going to be published should be confirmed
against a `dev/metasmith.sh -be` build.

## The gates

Both run inside `score`, before any number is reported.

1. **Default path.** With `MSM_SOLVER_POLICY` unset every reply is byte-identical to
   `results/baseline.json`. A change that moves the shipped rule is a regression, not a candidate.
2. **Witness.** Every complete plan is adjudicated by the *reference* binary's proved witness. A
   candidate does not get to be its own judge.

## The corpus

81 wire payloads in `payloads/`, in two families that are not interchangeable.

- **real (17)** -- 13 rungs of the metagenomics workflow from 3 to 68 targets, built by
  `build_ladder.py`, plus the 4 metagenomics arms of `../witness_sweep/duplicate_work.py`. The
  ladder reproduces `data/metasmith/plans/04-puct-on-large-workflows.md` step for step at every
  shared rung, which is the evidence it is the same ladder.
- **sink (64)** -- generated pathologies from `build_sink.py`, held-out problem seeds 100-115. The
  shipped rule fails 24 of them.

**CAUTION** Rank real-workflow plan length first. A policy that solves more generated cases by
lengthening real plans measured as a WIN under a solved-count-first rule and is not one -- the real
workflows are what ships. `harness.verdict` encodes this.

**CAUTION** Neither `CORPUS` nor `STRESS_CORPUS` can see a selection change. They solve in 7-13
iterations against a budget of 256. That is why this corpus exists.

## Regenerating

    python build_ladder.py <outdir> --probe --emit     # the 13 real rungs
    python build_sink.py <outdir> "26:101,40:103,..."  # generated cases, n_types:seed
    python runner.py --bin <binary> --payloads <dir> --set fast --out baseline.json

Run each under `PYTHONPATH="$PWD/src" mamba run -n msm`, and cap any speculative solve with
`systemd-run --user --scope -p MemoryMax=8G`.
