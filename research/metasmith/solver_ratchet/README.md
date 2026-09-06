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

`score` takes about fifteen seconds. It builds the engine, runs both gates, measures 81 frozen wire
payloads, ranks the result against the baseline and the current head, and names the payloads that
moved. `SOP.md` is the one page a worker reads.

**CAUTION** `score` builds with a host cargo from the `msmrust` environment and loosens `lto` and
`codegen-units` through the environment, into a per-worktree target directory. It never stages the
binary. Replies are byte-identical to the shipping profile -- checked on all 81 payloads, and
re-checked at the end of the run -- but a number that is going to be published should be confirmed
against a `dev/metasmith.sh -be` build.

## The gates

Both run inside `score`, before any number is reported.

1. **The adjudicator has not moved.** `src/solver_witness/` and `src/solver_witness_audit/` hash
   to `results/witness-digest.txt`, which was recorded when the Lean proof was last adjudicated.
   A round that changes the judge has measured nothing. This replaced a gate that demanded the
   default path stay byte-identical, which was the right shape while PUCT was opt-in behind an
   env var and became vacuous the moment it shipped as the only rule.
2. **Witness.** Every complete plan is adjudicated by `msm_solver check`. Gate 1 is what makes it
   safe for the candidate to run its own checker.

**CAUTION** `results/baseline.json` now records the *shipped* engine, so a fresh `score` reads
TIE. `results/pre-adoption-weighted.json` is the retired weighted rule on the same 81 payloads —
57 solved, 700 real steps — kept because it is the only machine-readable record of what adoption
was measured against.

## The corpus

81 wire payloads in `payloads/`, in two families that are not interchangeable.

- **real (17)** -- 13 rungs of the metagenomics workflow from 3 to 68 targets, built by
  `build_ladder.py`, plus the 4 metagenomics arms of `../witness_sweep/duplicate_work.py`. The
  ladder reproduces `data/metasmith/plans/04-puct-on-large-workflows.md` step for step at every
  shared rung, which is the evidence it is the same ladder.
- **sink (64)** -- generated pathologies from `build_sink.py`, held-out problem seeds 100-115. The
  retired weighted rule failed 24 of them; the shipped rule solves all 64.

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
