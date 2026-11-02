# Verdict: do not adopt as a scoring study

The proposed test — compute `delta I_eff` to ethanol per insert, compare to measured
outcomes — is well posed and cannot be run on this paper. The reason is a single clean
fact:

> The one insert with a phenotype has no mappable edge. The three inserts with mappable
> edges have no phenotype.

That is not a coverage gap the annotation lanes could close. It is the study's shape.

## The 2×2

| | mappable to an atom-mapped edge | not mappable |
|---|---|---|
| **phenotype** | *(empty)* | H10_Phage |
| **no phenotype** | H10_BK, ENV_F1, ENV_M1 | *(empty)* |

Measured against the bake, the three nulls are genuinely visible to the method: 83, 8 and
16 atom-mapped MNXR respectively. H10_Phage is a 359 bp N-terminal fragment of a phage
minor structural protein with no EC, no reaction and no metabolite. The authors do not
claim a metabolic mechanism; they float regulatory RNA, localised ethanol tolerance,
plating efficiency, and antibiotic export against neighbours. None of those is an edge.

So the scoreable rows are three predictions against three measured nulls, and the only real
signal is unpredictable in principle. Best case is 1/4, and the one "hit" would be
`ENV_M1` predicted-zero / measured-zero — a control passing, not a result.

## Every cell is confounded anyway

Fragments are "not full-length genes". A truncated β-ketoacyl synthase most likely has no
catalytic activity at all, so `H10_BK` measuring zero says nothing about whether the
network is right — it says the protein was never made functional. Scoring a null against a
non-functional protein tests cloning, not topology. The same applies to `ENV_F1` and
`ENV_M1`; `ENV_M1` additionally has **no nucleotide alignment**, its annotation resting
entirely on BLASTX of reading frames.

## Statistical power

n = 4 inserts, 1 condition, 3 non-zero Y rows. Compare what is already in
`BUILD_studies.json`:

| study | y_rows | gpr_rows | conditions |
|---|---|---|---|
| laser | 7765 | 3257 | 382 |
| keio | 750 | 230 | 166 |
| forsberg | 18 | 6 | 6 |
| **freedman2023** | **3** | **3** | **1** |

Agreement at this size is a coin flip and disagreement is uninterpretable. There is no
condition list to build: the paper has one medium. The serial re-inoculations are
enrichment rounds, not distinct conditions.

## What it would additionally cost

No host background exists. Every `gpr_gem.parquet` under `data/fabfos/runs/` is an
E. coli one. Adopting this study means building a *Ruminiclostridium cellulolyticum*
H10 GEM and its `gpr_gem.parquet` from scratch — the largest single piece of work here, in
service of three Y rows.

## What the paper is actually worth

Three things, none of which require adopting it:

1. **The share-not-total argument.** Working out why this study cannot be scored produced
   the Rayleigh-monotonicity point in `README.md`: for GOF-only arms a two-terminal
   `delta I_eff` is non-negative by construction and its sign is uninformative. That
   applies to `laser`, `forsberg`, `fa_supply`, `aromatic` and `pg_anionic` — every `gof`
   study already in the set — and is worth checking against how they are currently scored.

2. **A template for a fermentation-product study.** Constant substrate uptake with
   redistribution across competing secreted sinks is the cleanest possible CLR observation.
   The schema drafted here is reusable; only the paper is wrong.

3. **A negative result about random-fragment GOF screens generally.** Enrichment selects
   for fitness, and fitness in a plasmid library selects heavily for things that are not
   metabolic edges — regulatory fragments, tolerance, plasmid maintenance (`ENV_M1` carries
   a Phd antitoxin, which is plausibly why it enriched at all). Any benchmark built from
   enrichment-based screens should expect its strongest hits to be exactly the ones the
   method cannot see.

## If a fermentation-redistribution study is still wanted

Look for one with: a defined-gene (not fragment) library, a sequenced host with an existing
GEM, more than one medium, and per-clone product quantification. The Charles-lab PHA work
(Cheng & Charles 2016, per-clone GC of 27 cosmid clones) has the per-clone quantification
and random large inserts, but a *phaC*-null host means it measures rescue rather than
redistribution. That trade-off is the one to shop against.
