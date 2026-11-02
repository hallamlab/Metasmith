# Genome-scale FBA built from GPR tables

## Purpose & Contents

This directory constructs FBA models from the project's own GPR tables and scores them
against the Eydallin 2010 glycogen screen. It holds what the constructed-model arm found
and which parts of that are properties of flux balance rather than of this cohort.

Conventions and their failure modes live in each module's docstring. Numbers live in
`out/`. This file states the finding and nothing that a module already states.

| file | what it does |
|---|---|
| `common.py` | paths and the measured phenotype |
| `kornberg.py` | the 2010 medium as an exchange-bound set, with the Methods quoted |
| `build_tables.py` | the cohort as `ecspr::conditions`, `ecspr::gem_tests` and `ecspr::media` |
| `build_models.py` | the two host models, built through `ecspr.model.fba` |
| `fba_cohort.py` | three overexpression conventions across 86 clones |
| `score.py` | sign accuracy and Spearman against Fig. 1 |
| `reduced_costs.py` | the same ranking from one LP instead of 3,096 |

Run them in that order under `mamba run -n ecspr`. Each module's first docstring line is
its command.

**THE CONSTRUCTION IS NO LONGER HERE.** The carve, the MetaNetX insertion, the four
admission rules, the cycle closure and the gapfill are `ecspr.model.fba` and
`ecspr.model.mnx`, and `ecspr fba` runs the whole arm from the three tables
`build_tables.py` writes. That is the point of the move rather than a tidy-up: the
conductance arm reads the same conditions table, so a construction measured both ways is
the same construction and not two implementations that agreed by hand.

## The finding

Constructing the models from GPRs, running the medium the experiment used, and adding a
second annotation channel does not rescue FBA on this phenotype. The obligate-flux arm is
a constant-down predictor in both channels, in both media, at every growth pin. Its sign
accuracy equals the base rate of an always-down constant predictor to three decimal places
in all twelve configurations.

| channel | medium | pin | answered | accuracy | base rate | Spearman | Spearman before |
|---|---|---|---|---|---|---|---|
| gem | Kornberg | 0.1 | 19 | 0.789 | 0.789 | -0.344 | -0.348 |
| gem | M9 | 0.1 | 18 | 0.778 | 0.778 | -0.385 | -0.408 |
| denovo | Kornberg | 0.1 | 13 | 0.923 | 0.923 | 0.253 | 0.132 |
| denovo | M9 | 0.1 | 13 | 0.923 | 0.923 | 0.231 | 0.209 |

No Spearman here reaches p < 0.11. The GEM channel's rank correlation is negative, which
reproduces the prior arm's -0.34 on a different host, a different medium and a differently
built model. `out/score.tsv` holds all 36 configurations.

The last column is what this arm reported before the construction moved into the library
and before the GPR tables were migrated onto a calibrated `llr`. The two host models are
byte-identical across the move -- same reaction set, same order, same bounds -- and every
answered-clone count and gene list is unchanged. What moved is five de-novo `force` rows,
and the next section says why.

## The reference state is degenerate, so `force` depends on what was solved before it

`force` reads a pFBA reference flux vector and pins the clone's reactions at twice what
that vector runs them at. THE VECTOR IS NOT UNIQUE. Solving the de-novo host at pin 0.1 in
Kornberg gives two alternative optima of identical total flux, 567.735734 either way,
differing on 63 of 2,196 reactions -- and which one GLPK returns depends on the warm-start
basis, that is, on which pins were solved before it on the same model object.

Measured on `glgP`, de-novo, Kornberg, one model object per row:

| pins solved, in order | glgP at pin 0.1 |
|---|---|
| 0.9, 0.5, 0.1 | 5.347381 |
| 0.5, 0.1 | infeasible |
| 0.1 | infeasible |

So five of 3,096 rows differ between a run that walks the pin grid and a run that does not,
and the committed `degenerate` verdicts for `glgP` and `malP` were the artefact of the
walk. This is a defect in the `force` convention rather than in either implementation, and
it caps how much any digit of its Spearman is worth. The conclusion does not rest on those
digits -- accuracy equals the base rate in all twelve configurations either way -- but a
future arm that reads a reference flux vector should take a deterministic vertex, not
whichever one the solver is warm on.

## An MNXR is only meaningful inside one MNXref release

The frame carries its own `metanetx.reaction` annotation on 2,752 of its 2,755 reactions,
and it is the wrong crosswalk. It is whichever MNXref release BiGG published
`iECDH1ME8569_1439` against; the GPR tables are in this project's pinned MetaNetX 4.5. The
two agree on 1,520 reactions and **disagree on 1,231 of 2,751**.

Carving with the frame's own annotation keeps 1,095 reactions where the pinned crosswalk
keeps 1,720, and nothing raises: the model is simply wrong and smaller, and gapfill then
repairs it back into something that reports plausible numbers. `ecspr.model.fba.crosswalk`
therefore takes a curated GPR table as its source and the frame's annotation only as a
fill-in, counts the disagreement in the census, and `carve` refuses outright when a
condition's assertions reach no reaction of the frame at all.

**This is why the curated table is staged for the de-novo channel too.** It is not a
background there -- no de-novo condition names it -- it is the only source of a crosswalk in
the right release.

## Two of the three conventions cannot answer at all

**`relax` is a bit-exact no-op.** Capping every reaction at its reference flux and doubling
the clone's caps moves the optimum by less than 1e-6 for every clone in both channels. The
prior arm read this as glucose-uptake limitation. That reading is wrong. Kornberg supplies
carbon well past the point where uptake binds, and growth-pinned maximum glycogen rises
from 0.95 to 6.42 mmol/gDW/h at a 0.9 pin when the medium changes. The no-op survives that
change, so the cause is the convention rather than the carbon budget. An LP optimum is
non-decreasing in every bound, so this arm can only ever predict up or flat.

**`add` has nothing to add.** Each clone carries one chromosomal *E. coli* ORF, so every
reaction a clone asserts, the host asserts through the same gene. Run as set algebra
through `ecspr fba` -- where a clone's condition is the host's rows unioned with that
clone's -- the 91 conditions carve to 13 distinct models, and the largest movement in the
objective across all 534 clone tests is 4e-7 percent. Eleven clone constructions therefore
do change the model and none of them changes the answer. On the GEM channel the arm is
vacuous by construction; on
the de-novo channel the 50 clones with absent assertions name MNXR the curated GEM never
carried, which the insertion rules already refused when the host was built. The convention
would only bind on a heterologous library.

That is also why `ecspr fba` implements `add` and only `add`. It is the one convention that
reads a GPR as evidence about PRESENCE, which is what a conditions table can say. `relax`
and `force` are claims about capacity, and a condition's `factor` is evidence in nats.

## What each channel reaches

| | clones with a GPR | answered by `force` | reached only here |
|---|---|---|---|
| gem | 34 of 86 | 19 | cydC, ddg, dos, gltI, gntT, metH, pfs, ppk, pstC, rutF, serB, talA |
| denovo | 55 of 86 | 13 | clpA, csrD, gpp, recQ, spoT, yfaY |

The union answers 25. The de-novo channel reaches six regulators and signalling proteins
the curated GPR maps to no reaction, which is the same coverage argument ECSPr makes for
its own de-novo channel. It answers fewer clones overall, because its host model is smaller
and more of its forced solves go infeasible.

The four strongest glycogen phenotypes are absent from both answer sets. glgC at 453% of
wild type and glgA at 328% come back degenerate, moving the optimum by under 1e-8, and the
next two by magnitude have no GPR at all. The reference state already runs the glycogen
reactions at the ceiling, so requiring twice their reference flux requires nothing. The
prior arm answered three of them and got all three backwards. Unanswered is the better
failure.

## Building a model from a GPR table

A GPR table carries gene-to-reaction evidence in MetaNetX space. It carries no
stoichiometry and no bounds. Both channels therefore use `iECDH1ME8569_1439` as the
stoichiometric frame and differ in which of its reactions they keep.

The GEM host removes what the AG1 host GPR does not assert. That is one reaction, GTPDPK,
withheld for relA1. `check_ag1_identity.py`'s declared list and the table's own coverage
agree, and `build_models.py` asserts against both by name.

The de-novo host carves the frame to lane-supported reactions, inserts lane-asserted MNXR
the GEM never named, closes the energy-generating cycles insertion creates, gapfills back
to biomass and glycogen feasibility, then closes cycles again.

| lanes | kept | carved | inserted | gapfilled | growth on M9 | glycogen at pin 0.1 |
|---|---|---|---|---|---|---|
| curated | 2754 | 1 | - | - | 0.982 | 8.449 |
| 1 | 1720 | 1035 | 417 | 58 | 0.246 | 4.976 |
| 2 | 1260 | 1495 | 195 | 104 | 0.576 | 7.517 |
| 3 | 1035 | 1720 | 129 | 173 | 0.853 | 8.130 |

530 frame reactions carry no gene rule and survive every carve, because no annotation lane
can speak to them.

Growth does not fall as the carve deepens. Gapfill draws on the reactions the carve removed
and a deeper carve leaves it more to draw on, so the three de-novo hosts are not a
monotone series. Read each as its own model.

**CAUTION** THE GAPFILL RUNS IN M9 FOR EVERY MODEL, whatever medium it is later measured
in, which makes each host a claim about the organism rather than about the broth. Repairing
in Kornberg instead is not equivalent and not merely different: it draws 53 reactions
rather than 58, and the cycle closure that follows then removes a direction the repair was
using, taking growth from 0.063 back to zero. `ecspr fba --gapfill-media` is that choice,
and leaving it unset gives each medium its own model.

**CAUTION** `min_lanes` defaults to 1 here and to 2 everywhere else in this project. No two
annotation lanes agree on any reaction of the glycogen pathway. GLGC, GLCS1, GLBRAN2 and
PGMT each rest on a single lane, so a two-lane carve deletes the phenotype under study and
only gapfill restores it.

## What this arm can and cannot see of an evidence weighting

`min_lanes` is a LANE COUNT. It admits a reaction when that many distinct channels assert
it, and it reads no weight at all, so the per-call confidence `ref::lane_calibration` puts
in the `llr` column is invisible to it. That was true of the whole arm and it is why the
earlier version of this page said a null from here is a statement about what it consumes.

`carve_threshold` is the column that changes it. It is a floor on the pooled belief `E_r`,
which reads the calibrated `llr` and a condition's dosage `factor`, so more evidence can put
a reaction INTO the model. That is the only route by which either reaches the LP: a weight
never moves a bound, because raising a bound that was not binding is a bit-exact no-op.

## Insertion needs more than MetaNetX's own balance flag

Inserting 131 lane-asserted MNXR reversibly raises host growth by 41%. Four admission rules
and a cycle closure bring that to 1.5%, and three of the four were found by measurement
rather than anticipated.

- MetaNetX balances against MetaNetX protonation and BiGG protonates differently, so a
  reaction can leave MetaNetX balanced and land unbalanced. Each of seven such reactions
  alone raises growth from 0.982 to 1.384 by acting as a free proton sink.
- The formula-and-charge bridge tier maps both epimers of an epimerase onto one frame
  metabolite. Substrate and product cancel and a free proton sink survives.
- A reversible carboxy-lyase fixes CO2 for free. The reverse of `co2_c + dhap_c = h_c +
  ohpb_c` alone is worth 0.982 to 1.102.

`ecspr.model.mnx` carries the rules. `build_from` calls the cycle closure twice, because
gapfill returns partners the carve had taken away and reopens a cycle the first pass shut.

## One LP instead of 3,096, and it does not pay

A reduced cost from the baseline solve is the rate at which the objective moves per unit of
forced flux, so it ranks every clone from one LP where `force` needs one per clone per
medium per pin. `reduced_costs.py` runs it. It does not reach significance under any of the
eight constructions it reports -- signed or unsigned, summed or maximised, all clones or
only the 14 with a non-zero dual -- in either channel. The best is rho +0.232 at p = 0.16.

A note carried into this scope from an earlier session recorded rho 0.68 at p 0.005 for this
rung. It does not reproduce, and the GEM host it would have run on is byte-identical to the
one used then, so the difference is in the construction rather than in the model. The
script that produced the earlier number was never committed, so which construction it used
is not recoverable. Treat 0.68 as unsupported.

## Relation to the other FBA arm

`research/fabfos/vs_flux_balence/` runs stock iML1515 with edited bounds. This arm builds
its models from GPRs, uses the strain's own GEM, and runs the medium the screen used. The
two arms agree on the finding. That agreement is the useful part: the negative result
survives changing the host, the medium and the construction.

Matched conditions make the agreement a construction check. On M9 at a 0.9 pin the two
arms answer 14 genes in common, agree on the sign for all 14, and their percent
differences correlate at rho = 0.886. A constructed model that reproduces a stock model
under the same settings is built right, whatever the settings are worth.

## Choices that would move the numbers

- `FOLD` and `VMIN` set the size of the perturbation for a gene the reference leaves at
  zero. They are inherited from the prior arm, where a sweep held accuracy at the base rate
  throughout.
- Kornberg closes `EX_nh4_e` and withholds free cysteine, because yeast extract is the only
  nitrogen source and the paper attributes its cysI, cysP and serB hits to a
  cysteine-deficient medium. The de-novo hosts grow poorly under it, at 0.235 per hour at
  one lane against 0.246 on M9, because the carve removes the amino-acid catabolism that
  releases nitrogen.
- Gapfill is an L1 relaxation rather than the exact minimum-cardinality MILP, because GLPK
  is the only solver in this environment.
- Which pins are solved, and in what order. See the degeneracy section above.
