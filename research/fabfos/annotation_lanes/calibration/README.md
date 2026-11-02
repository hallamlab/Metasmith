# What one annotation call is worth, in nats

## Purpose & Contents

This directory measures how often each of the four chosen lanes is wrong, as a function
of that lane's own score, and turns the answer into the single weight the GPR table
carries. It holds the finding and the parts of it a reader cannot get from the code.

The scripts run in order under `mamba run -n msm`. Each names its own inputs and outputs
in its header. Numbers live in `out/`; the shipped artifact is
`ref::lane_calibration`, at `data/fabfos/processed/lane_calibration/`.

| file | what it does |
|---|---|
| `_cal.py` | the cohort, the lane readers, the EC projection |
| `build_labeled.py` | one row per call, with that call's score and a correct flag |
| `fit_calibration.py` | the per-lane fit, on ORF-grouped held-out folds |
| `emit_reference.py` | the fit as a versioned reference artifact |
| `reparse_run.py` | one run's GPR table rebuilt from its cached lane files |
| `_legacy.py` | the pre-calibration weighting, frozen, for before/after |
| `before_after_eydallin.py` | the two eydallin runs, compared |

## Why a log-likelihood ratio

The four lanes reported an HMM bitscore, a GMM confidence, a BLAST score ratio and a kNN
vote, and the pooling treated them as interchangeable. A precision would not have fixed
that: a precision is a posterior, so it carries the benchmark's own base rate inside it
and cannot move to a corpus with a different one. Curated K-12 is not dark matter.

`llr = logit(P(correct | score)) - POOL_LAM0` leaves the part that is a property of the
evidence. Two calls about one reaction then add, in log-odds and in no other coordinate,
which is what the pooling already did with them.

**CAUTION** `POOL_LAM0` is both the prior the fit subtracts and the prior the pooling
adds back, and that identity is load-bearing: it makes a reaction carrying one assertion
come out at exactly that assertion's own calibrated accuracy. Subtracting the
benchmark's own call prevalence instead — the first cut here — floors 58% of the CLEAN
and ProteinBERT lanes to zero and pins the pooled belief into [0.947, 1]. It reads
"worse than the average call" as "argues the reaction is absent", and those are
different claims.

## The finding

Two of the four lanes' scores predict nothing, and the reason differs.

| lane | model | llr (nats) | P(correct) at the median call | held-out z for the slope |
|---|---|---|---|---|
| kofam | constant | 6.14 | 0.959 | 0.62 |
| clean | sloped | 2.35 – 7.07 | 0.975 | 5.09 |
| uniref50 | constant | 8.87 | 0.997 | 0.23 |
| pbert | sloped | 2.96 – 6.16 | 0.959 | 6.40 |

**KOfam's bitscore carries no information about correctness once the call is above
threshold.** Its ten score deciles run 14.3%, 2.4%, 5.6%, 2.4%, 0.8%, 2.4%, 4.0%, 3.2%,
1.6%, 4.8% error — no trend, and the top decile is worse than the middle. That is what a
per-family threshold doing the calibration already looks like. UniRef50 is a constant for
the plainer reason: 3 errors in 1,086 calls leaves no slope to fit.

The slope is kept only where it clears its own noise. Comparing two mean held-out
log-losses made kofam sloped on a 0.001 margin carried by three folds of five, which is a
coin flip dressed as a decision. The test is the paired per-call gain against a
standard error clustered on the ORF, because two calls on one ORF are not two
independent observations.

Between-lane spread at the median is 3.8x, against a single hand-picked constant of 1.0
for every lane before.

## What the benchmark can and cannot support

Adjudicable ORFs are 1,300 of DH10B's 4,468, by exact sequence md5 against Swiss-Prot
K-12 entries carrying a level-4 EC. The error counts — 52, 85, 3, 142 — are what limits
each fit, not the call counts.

- **The lane files are the run's, not the bundle's.** `data/fabfos/benchmarks/lane_dh10b`
  holds a replication under its own ORF ids, and its DIAMOND output predates the `bsr`
  column the uniref50 lane scores on. A calibration fitted on anything but the production
  score calibrates a number the pipeline does not emit.
- **CLEAN's abstain point was tuned against this same truth family.** The curve is
  conditioned on the score, so the tuning moves which calls exist rather than the
  score-to-error relation — but that lane's fit is not a clean holdout.
- **ProteinBERT retrieves against Swiss-Prot and the truth set is Swiss-Prot**, so most
  of these ORFs find themselves. That populates the top of the score range and leaves the
  bottom thin. It bounds where the curve has support; it does not bias it.
- The fit is at **EC level**. Reaction level would measure the same fan-out dilution the
  mapper's `1/F_n` divisor already applies, so moving to it means removing that divisor
  in the same change.

## Rebuilding a table costs no annotator time

Every run keeps its six lane files beside the GPR table under version control, and the
embedding parquet is among them. `reparse_run.py` re-runs the lane logic and the kNN vote
against those cached vectors; nothing touches a queue and no GPU is involved. K-12, DH1,
DH10B and the eydallin clones each rebuild to exactly the row count their published table
carries, which is what makes a before/after a comparison of weightings rather than of
tables.

Seven of the thirteen runs have their lane files materialised locally. AG1 is not one of
them and does not need to be: it has no proteome of its own and borrows DH1's table
through `derive_ag1_denovo.py`.

## The before/after is a null, and the benchmark is why

On the AG1 de-novo channel — the only eydallin path that reads the weight column at all —
both weightings correlate with the measured phenotype at rho +0.07 (p 0.62) and +0.05
(p 0.74). Reaction membership is identical at 11,248, rank agreement is rho 0.965, and
the glycogen pathway tops both.

Half the cohort produces a delta of exactly zero. A clone adds a second copy of reactions
its host already asserts, and pooling one more assertion onto a saturated reaction moves
it by nothing, so a Spearman over 54 values of which 24 to 30 are tied at zero cannot
resolve a modest reweighting either way. Read the null as *this benchmark cannot see
it*.

**CAUTION** `benchmarks/eydallin_fba/` cannot see it either, for a different reason: it
carves the frame by `channel.nunique() >= min_lanes` and never reads the weight column.
`cohort_delta_panel.py` and `twopoint_cohort.py` run on the curated `gem_gpr` channel,
which is a uniform assertion. A null from any of those is a statement about what they
consume.

## What calibration made visible

Calibrated assertions are worth 3 to 9 nats where the retired constant was worth 1, so
the pooled log-odds inflate about sevenfold and the float64 tie point at ~37 arrives
after five corroborating assertions rather than forty. On K-12 that took the reactions
tied at exactly `E = 1.0` from 2 of 11,203 to 51, and `tau` moved from 0.25 to 0.5 to
bring it back to 18.

That is an independence problem, not a `tau` problem. The pool adds assertions as if they
were independent, and the retired constant was silently discounting for the fact that
they are not. Five calibrated calls reach certainty because the model says five
independent 99%-accurate calls are certainty — and lanes trained on overlapping databases
are not independent. Raising `tau` hides this. The fix is a correlation term the assertion
key cannot express.
