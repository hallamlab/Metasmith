# Genome-scale flux balance as the independent method

Two scripts, both on iML1515 with a `DM_glycogen_c` demand and growth pinned at 0.9x max.
`fba_glycogen.py` is the five-gene probe that established *why* a bound edit registers at
all; `fba_eydallin_cohort.py` runs the whole Eydallin cohort and scores it against ECSPr.
Conventions and their failure modes are in each module's docstring; `out/*.tsv` holds the
numbers.

## What this file is for

The one thing neither script says: what the comparison found, and which part of it is a
property of flux balance rather than of this cohort.

## FBA's ceiling is resolution, and then it is direction

39 of the 86 screened genes carry an iML1515 gene (b-number join, exact); the other 47 are
regulators, signalling proteins and uncharacterised ORFs the model does not represent.
Of the 39, the obligate-flux arm answers 23 — 14 go infeasible, 2 return solver noise.

**Both natural overexpression conventions are one-sided, and for different reasons.**
Capacity relaxation cannot raise the optimum because growth-pinned maximum glycogen is
limited by glucose uptake rather than by any internal capacity: no gene is rate-limiting
and all 39 deltas are zero to 1e-9. Obligate flux cannot raise it either, because WT
already sits at that ceiling, so every gene reads DOWN. Its 17/23 is exactly what a
constant "down" predictor scores on the same 23, and the rank correlation against Fig. 1
is *negative* (Spearman -0.34, n = 23).

So FBA does not predict this phenotype. It reproduces the deficient majority and gets
glgC and glgA — the two strongest measured increases — backwards.

## Against ECSPr, on ECSPr's 45

| | ECSPr two-point | ECSPr ratio | FBA (force) |
|---|---|---|---|
| all 45 conditions | 10 | 30 | 14 |
| the 19 FBA answers | 5 | 13 | 14 |

ECSPr's ratio metric is the only arm that is signed by construction and it is the only one
that beats the base rate. FBA answers 19 of ECSPr's 45 and 4 genes outside it (erfK, gltI,
gntT, pstC — transport and cell-wall reactions ECSPr's atom-mapped universe drops); ECSPr
reaches 16 that iML1515 has no gene for, all through the de-novo channel.

## Choices that would move the numbers

- Host is K-12 iML1515, not DH1/AG1 — no strain GEM is on disk in cobra-loadable form.
  Gene identity travels by b-number, so the join is exact even though the model is not.
- `GROWTH_FRAC = 0.9` is inherited from `fba_glycogen.py`. It sets how much carbon is left
  for glycogen and therefore every delta's magnitude, but not its sign: the ceiling
  argument above holds at any fraction.
- `VMIN` (floor capacity for a reaction the reference leaves at zero) and `FOLD` set the
  size of the perturbation for off-in-WT genes. `--sweep` runs the grid; accuracy stays
  between 13/15 and 17/23 across it, which is the always-down base rate throughout.
