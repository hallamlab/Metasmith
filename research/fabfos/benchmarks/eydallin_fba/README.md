# Genome-scale FBA built from GPR tables

## Purpose & Contents

This directory constructs FBA models from the project's own GPR tables and scores them
against the Eydallin 2010 glycogen screen. It holds what the constructed-model arm found
and which parts of that are properties of flux balance rather than of this cohort.

Conventions and their failure modes live in each module's docstring. Numbers live in
`out/`. This file states the finding and nothing that a module already states.

| file | what it does |
|---|---|
| `common.py` | paths, GPR loaders, the bridge to `laser/vs_gem`'s MetaNetX machinery |
| `kornberg.py` | the 2010 medium as an exchange-bound set, with the Methods quoted |
| `mnx_insert.py` | MNXR to cobra reactions, with four admission rules and cycle closure |
| `build_models.py` | the four host models |
| `fba_cohort.py` | three overexpression conventions across 86 clones |
| `score.py` | sign accuracy and Spearman against Fig. 1 |

Run them in that order under `mamba run -n ecspr`. Each module's first docstring line is
its command.

## The finding

Constructing the models from GPRs, running the medium the experiment used, and adding a
second annotation channel does not rescue FBA on this phenotype. The obligate-flux arm is
a constant-down predictor in both channels, in both media, at every growth pin. Its sign
accuracy equals the base rate of an always-down constant predictor to three decimal places
in all twelve configurations.

| channel | medium | pin | answered | accuracy | base rate | Spearman |
|---|---|---|---|---|---|---|
| gem | Kornberg | 0.1 | 19 | 0.789 | 0.789 | -0.348 |
| gem | M9 | 0.1 | 18 | 0.778 | 0.778 | -0.408 |
| denovo | Kornberg | 0.1 | 13 | 0.923 | 0.923 | 0.132 |
| denovo | M9 | 0.1 | 13 | 0.923 | 0.923 | 0.209 |

No Spearman here reaches p < 0.09. The GEM channel's rank correlation is negative, which
reproduces the prior arm's -0.34 on a different host, a different medium and a differently
built model. `out/score.tsv` holds all 36 configurations.

## Two of the three conventions cannot answer at all

**`relax` is a bit-exact no-op.** Capping every reaction at its reference flux and doubling
the clone's caps moves the optimum by less than 1e-6 for every clone in both channels. The
prior arm read this as glucose-uptake limitation. That reading is wrong. Kornberg supplies
carbon well past the point where uptake binds, and growth-pinned maximum glycogen rises
from 0.95 to 6.42 mmol/gDW/h at a 0.9 pin when the medium changes. The no-op survives that
change, so the cause is the convention rather than the carbon budget. An LP optimum is
non-decreasing in every bound, so this arm can only ever predict up or flat.

**`add` has nothing to add.** Each clone carries one chromosomal *E. coli* ORF, so every
reaction a clone asserts, the host asserts through the same gene. All 479 de-novo clone
assertions appear in the host's own assertion set, and all 386 the host model lacks are
MNXR the curated GEM never carried, which the insertion rules already refused when the host
was built. On the GEM channel the arm is vacuous by construction. The convention would only
bind on a heterologous library.

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
wild type, glgA at 328%, glgP at 60% and glgB at 26% all come back degenerate, moving the
optimum by under 1e-8. The reference state already runs those reactions at the glycogen
ceiling, so requiring twice their reference flux requires nothing. The prior arm answered
three of them and got all three backwards. Unanswered is the better failure.

## Building a model from a GPR table

A GPR table carries gene-to-reaction evidence in MetaNetX space. It carries no
stoichiometry and no bounds. Both channels therefore use `iECDH1ME8569_1439` as the
stoichiometric frame and differ in which of its reactions they keep.

The GEM host removes what the AG1 host GPR does not assert. That is one reaction, GTPDPK,
withheld for relA1. `check_ag1_identity.py`'s declared list and the table's own coverage
agree, and `build_models.py` asserts against both.

The de-novo host carves the frame to lane-supported reactions, inserts lane-asserted MNXR
the GEM never named, closes the energy-generating cycles insertion creates, gapfills back
to biomass and glycogen feasibility, then closes cycles again.

| lanes | kept | carved | inserted | gapfilled | growth on M9 | glycogen at pin 0.1 |
|---|---|---|---|---|---|---|
| curated | 2755 | 1 | - | - | 0.982 | 8.449 |
| 1 | 1720 | 1035 | 417 | 58 | 0.246 | 4.976 |
| 2 | 1260 | 1495 | 195 | 104 | 0.576 | 7.517 |
| 3 | 1035 | 1720 | 129 | 173 | 0.853 | 8.130 |

530 frame reactions carry no gene rule and survive every carve, because no annotation lane
can speak to them.

Growth does not fall as the carve deepens. Gapfill draws on the reactions the carve removed
and a deeper carve leaves it more to draw on, so the three de-novo hosts are not a
monotone series. Read each as its own model.

**CAUTION** `min_lanes` defaults to 1 here and to 2 everywhere else in this project. No two
annotation lanes agree on any reaction of the glycogen pathway. GLGC, GLCS1, GLBRAN2 and
PGMT each rest on a single lane, so a two-lane carve deletes the phenotype under study and
only gapfill restores it.

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

`mnx_insert.py` carries the rules. `build_models.py` calls the cycle closure twice, because
gapfill returns partners the carve had taken away and reopens a cycle the first pass shut.

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
  is the only solver in this environment. `--milp` selects the exact form.
