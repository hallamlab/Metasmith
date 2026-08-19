# LASER: ECSPr vs genome-scale flux balance

Scores ECSPr and FBA against LASER's experimental outcomes on the **design axis**,
and against each other. `benchmark_design.html` is the argument; `report.py`
builds `benchmark_report.html` from the executed tables in `out/`.

## What this file is for

The reasoning that is not recoverable by reading the scripts: why the metric is
shaped the way it is, and which choices silently decide the answer. Per-script
behaviour lives in each module's docstring; the numbers live in `out/*.tsv`.

## The de-novo arms in `out/` are STALE, and by how much

Everything under `out/` was computed against the k12 and dh10b de-novo GPR tables as
they were before those hosts were rebuilt. Both have changed, and not slightly: they
carried three lanes and no ProteinBERT, over an ORF namespace no other host shared.
k12 went from 25,799 rows over 2,137 ORFs to 61,715 over 4,311, and its reaction set
from 10,478 to 14,122.

So every `denovo_ev` and `denovo_uni` number in `out/` — `T2_headline.tsv` through
`T15`, `scalars_all.parquet`, the matrices, and the figures and report built from them
— describes tables that no longer exist. The `gem` and `fba_*` arms are untouched: they
read `gpr_gem.parquet`, which changed shape but not content.

Two things follow. The de-novo arms move in BOTH directions and the direction is not
predictable from here: a third more reactions raises coverage, while the fourth lane
changes every ORF's belief denominator, and `unit` is a median over the new weights so
the size of one added gene copy moves too. And there is no incremental path — `cache/`
is gitignored and empty, and `score.py` rebuilds every table by globbing it, so
refreshing one arm means re-running all eight ECSPr units and the FBA and trivial arms
before the scoring chain.

Re-running is deliberately not done here, so the rebuild stays attributable. When it
happens: leave `POOL_SEED` alone, or every head-to-head p-value in the report is void.

## Why the design axis, and not the obvious metric

Ranking the measured target among a metabolite panel cannot work here.
**LASER has no negatives on the metabolite axis** — a paper records what its
authors chose to assay, never that nothing else moved, and no panel construction
recovers a negative class that was never sampled. Rayleigh monotonicity compounds
it: ECSPr's two-terminal conductance rises on any addition, so "the target went
up" is guaranteed rather than informative.

Holding the target fixed and varying the *design* does have negatives: the
designs nobody built. Each real design is scored against `POOL_N` synthetic edit
sets drawn from the pooled LASER universe, size-matched, and pushed through the
identical `apply_edits` policy. Size matching is what neutralises monotonicity —
a counterfactual adds as many edges as the real design.

This is affordable only because one `measure_leak` solve returns a draw for
**every** metabolite: the pool is N designs per method, not K per condition.

## The other question: does the method answer at all

`response_hist.py` (T15) asks something smaller and separate, and needs none of
the machinery above. On the same cell — a real design at the target its own paper
measured — what does each method *return*? ECSPr answers on 86% of the 99 matched
cells, flux balance on 25%; the rest of FBA's mass is a single spike at solver
tolerance.

That is **resolution, not accuracy**, and it does not soften the design-axis
result: a method that always answers can still answer no better than the trivial
baseline, which is what T14 found. Each method is judged against *its own*
measured band, never an assumed tolerance — ECSPr's is the jittered-baseline
floor `run_arms` caches per solve, and FBA's is measured here from the designs
whose adds are all native and whose deletions all miss, since cobra then gets a
model identical to the base one and every nonzero delta is noise. Those designs
also bound how bad it gets: 44% return exactly zero, but the worst is 1.23
mmol/gDW/h — a degenerate LP relocating a target's maximum with no model change
at all.

## Four decisions that decide the answer

- **Abstention.** A target that is not a node of an arm's graph leaves the
  denominator; it does not score zero. Scoring it as zero lets the arm with the
  smaller graph win by abstaining, and it is the difference between the GEM arm
  reading 98 conditions and 159.
- **Matched comparison.** Any claim against the trivial baseline must be made on
  the conditions *both* scored (`T14`). The baseline never abstains, so a pooled
  comparison silently gives it a different denominator — and reverses the sign
  of the margin on this dataset.
- **Null resolution.** The binned null resolves p only to `1/(n_bin+1)`. When
  that floor exceeds the BH threshold `α/m`, the binned test cannot reject at any
  effect size. The size-regressed residual (all N counterfactuals) is therefore
  the primary, and every table prints the floor beside the binned column.
- **Counterfactual zeros.** A counterfactual that fails to create the target
  contributes a genuine zero to the null, not a missing value. Dropping those
  would leave the null made only of designs that *did* create it — the opposite
  of the intended comparison.

## Landmines

- `add_mnxr` / `del_mnxr` in `extraction.tsv` are **comma** delimited. Splitting
  on `;` collapses every multi-reaction design to one token and halves the
  no-heterologous-add stratum. `common.assert_census` asserts the delimiter and
  re-derives the whole census; it fails loudly rather than drifting.
- The FBA scaffold must insert heterologous reactions **before** building demand
  reactions. Targets like lycopene exist in no *E. coli* GEM and only become
  species once a graft creates them; the other order leaves every de-novo target
  unscoreable and reads as an FBA coverage failure.
- Gating the scaffold's demands at `(0,0)` is a correctness requirement, not a
  speed trick: 117 open demands raise unmodified iML1515's growth from 0.877 to
  1.384. The scaffold gate checks base growth is unchanged before any scoring LP.
- `ecspr_evidence.compute_E` fails conservation on the de-novo table because
  `raw_score` is float32. Cast to float64 **in the driver**; never loosen the
  library tolerance.
- The counterfactual pool is seeded once in `common.POOL_SEED` and shared by
  every arm and by FBA. Regenerating it per arm voids every head-to-head p-value
  comparison in the report.
- Both bake versions are mixed by necessity — the answer key is v1, the only
  direction table is v2. `verify_bake_join.py` bounds the damage (orientation
  disagreement, plus the v1 sha256 against `TIER4_FREEZE.md`) and must pass
  before any solve. **It cannot currently run**: it reads
  `data/fabfos/benchmark/reference_tier4`, a pin since retired and not on disk,
  so the gate has to be re-pointed at the bake's own atom pairs before this
  cohort is solved again.

## Order

    build_refs.py → verify_bake_join.py → run_arms.py / run_fba.py
      → score.py → score_extras.py → score_matrix.py → make_figures.py → report.py

`run_arms.py` and `run_fba.py` shard per unit into `cache/`, flush through a tmp
sibling, and resume from the design ids already on disk. Both need the
`fabfos:local` image — it is the only environment here with cobra, and running
both methods under one interpreter keeps the numerics comparable.

`response_hist.py` hangs off the same shards and is not in that chain: it reads
`cache/` and renders in seconds under `figure-net`, with no solver and no
container. Its figure goes to `cache/` and so is not tracked — the script and
`out/T15_response_resolution.tsv` are what survive, and re-rendering is cheaper
than archiving.
