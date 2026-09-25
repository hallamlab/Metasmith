# E4: how the GEM lanes differ from metaGEM and from E5

## Purpose & Contents

E4 rebuilds metaGEM's genome-scale models (GEMs) from metaGEM's own published protein bins, in two lanes. This file records every known difference between each lane and what it is compared against, with the evidence for each, and the full run's counts and parity. It holds findings only. `drivers/e4_gems.py` is the source of truth for the commands, and the env files under `library/resources/e4/` pin the images. The per-bin tables are in `results/e4/`.

- **Reproduction lane** (`--lane repro`): metaGEM's methods and materials, as close as fir allows.
- **Modern lane** (`--lane modern`): E5's GEM transforms, unchanged, for comparison with E5.

Both lanes start from the same 14,105 protein bins (`drivers/e4_published_proteins.tsv`). These are the files metaGEM's `carveme` rule read. They are CheckM's `genes.faa`, so Prodigal ran in single mode with a per-bin translation table. E4 does not rerun metaGEM's QC, assembly, binning or refinement.

## Reproduction lane against metaGEM

metaGEM's versions come from its repository at d5c6eed0 (2020-06-29), the state it was in when it packed the Tara GEMs. `metaBAGpipes_env.yml` there pins every package. The `diamond>=2.0.6` pin in later commits dates from 2021-01 and does not describe the published GEMs.

These match metaGEM exactly:
- CarveMe 1.2.2 and framed 0.5.1. The published GEMs say "built with CarveMe version 1.2.2".
- DIAMOND 0.9.30, build `h56fc30b_0`, as the biocontainers image of the same build string.
- The BiGG data. `carveme_init` fetched it from CarveMe's `master`. That only worked between 2019-03 and 2020-09-16, because it also fetched two RefSeq tables `master` held in that window alone. Every file `carve` reads in protein mode was byte-identical on `master` from 2017-10 to 2021-03-03, and is byte-identical to the 1.2.2 wheel's copy.
- The DIAMOND database. `carveme_init_db` runs `carveme_init`'s `diamond makedb` on DIAMOND 0.9.30. The `.dmnd` CarveMe 1.2.2 ships predates 0.9.30 and does not open in it, so metaGEM must have rebuilt it too.
- The commands, argument for argument: `diamond blastp --more-sensitive --top 10`, `carve -g M8 -v --mediadb media_db.tsv --fbc2`, and both `memote` commands with the same five skips.
- The medium: metaGEM's `media_db.tsv`, medium M8.
- MEMOTE 0.9.13. Its `test_find_medium_metabolites` raises a TypeError on every model. That is a 0.9.13 bug, so metaGEM's results carry it too.

These differ:

| Item | metaGEM | E4 | Why | Effect |
|---|---|---|---|---|
| MILP solver | CPLEX 12.8 | CPLEX 22.2 | fir has no other CPLEX, and 12.8 cannot be obtained | carve's MILP picks a different optimum. See the parity table. |
| Python stack under CarveMe | Python 3.6.7, pandas 1.0.1, numpy 1.18.1, scipy 1.4.1 | Python 3.12, pandas 2.2.3, numpy 1.26.4, scipy 1.13.1 | CPLEX 22.2's Python API on fir exists for 3.12 only | not separated from the solver's effect. pandas orders tied gene scores. |
| framed | 0.5.1 as released | 0.5.1 with a 5-line patch | Python 3.12 and CPLEX 22 reject 0.5.1 as released | none intended. The patch changes container types only: two `collections.abc` imports, lists where CPLEX 22 rejects tuples and sets, and `html.escape` for the removed `cgi.escape`. Without the last, framed drops every SBML note without failing. |
| python-libsbml | unpinned, from PyPI in 2020 | 5.20.4 | no Python 3.12 build of an older one | not measured. It reads and writes the SBML only. |
| MEMOTE's dependencies | unpinned `pip install --user` | PyPI as it stood on 2019-12-05, the 0.9.13 release day | reproducible, and the closest thing to an unpinned install of that era | not measured |
| DIAMOND threads | all cores of metaGEM's node | all 192 cores fir reports, in a 4-CPU job | CarveMe passes no `--threads` | none. DIAMOND's output does not depend on threads. |

CAUTION: carve is not deterministic on CPLEX 22.2. Re-carving the same DIAMOND hits three times changed 0 to 25 reactions per bin, with identical genes and gene rules (4 bins). An exact match with the published models is out of reach on this solver.

### Measured parity

The 20 ordinary smoke bins (4 per study) were carved with metaGEM's commands and compared with metaGEM's published GEMs by `drivers/e4_parity.py` (fir, 2026-09-22). The values are medians over the 20 bins, with the range in parentheses.

| Annotation | Reactions Jaccard | Metabolites Jaccard | Genes Jaccard | Shared reactions with a different gene rule |
|---|---|---|---|---|
| DIAMOND 2.1.9 | 0.801 (0.630–0.913) | 0.870 (0.723–0.954) | 0.920 (0.877–0.978) | 7.4% |
| DIAMOND 0.9.30, metaGEM's | 0.836 (0.683–0.944) | 0.895 (0.750–0.970) | 0.975 (0.919–0.996) | 2.1% |

The remaining gap sits in carve's reaction selection and gapfill, not in annotation. With DIAMOND 0.9.30, 97.5% of genes agree, but about 16% of reactions do not.

The metasmith smoke run of both lanes (runs `6nwwFX7P` and `l1yFJbPC`, fir, 2026-09-22) measured the same 20 bins, plus the two bins whose solves stalled on CarveMe 1.6.1:

| Comparison | Reactions Jaccard | Metabolites Jaccard | Genes Jaccard | Shared reactions with a different gene rule |
|---|---|---|---|---|
| Reproduction lane against metaGEM, 20 bins | 0.836 (0.683–0.944) | 0.896 (0.750–0.970) | 0.975 (0.919–0.996) | 2.1% |
| Reproduction lane against metaGEM, all 22 | 0.824 (0.448–0.944) | 0.892 (0.635–0.970) | 0.977 (0.919–1.000) | 2.0% |
| Modern lane against metaGEM, all 22 | 0.386 (0.236–0.499) | 0.508 (0.360–0.609) | 0.705 (0.591–0.752) | 27.4% |
| Modern lane against the reproduction lane, all 22 | 0.388 (0.245–0.507) | 0.516 (0.384–0.615) | 0.706 (0.591–0.755) | not computed |

All 22 bins finished in both lanes, including the two that stalled on CarveMe 1.6.1.

## Full run

Both lanes ran over all 14,105 bins on fir, 2026-09-22 to 2026-09-25, in 7 chunks of 2,015 (`drivers/e4_chain.sbatch`). `drivers/e4_tally.sbatch` counted the products and compared every model with metaGEM's published GEM for its bin.

| | metaGEM | Reproduction lane | Modern lane |
|---|---|---|---|
| Models | 14,087 | 14,102 | 14,097 |
| MEMOTE results | not compared | 14,088 | 14,097 |
| Bins with no model | 18 | 3 | 8 |

metaGEM published no GEM for 18 of its 14,105 protein bins. Both lanes built a model for all 18. They are karlsson2013 `ERR260137_bin.8.p`, `ERR260166_bin.15.s`, `ERR260175_bin.22.s`, `ERR260193_bin.16.s`, `ERR260227_bin.13.s`, `ERR260256_bin.31.s` and `ERR260269_bin.6.p`, sunagawa2015 `ERR598968_bin.63.s`, `ERR599010_bin.4.p`, `ERR599032_bin.37.p`, `ERR599102_bin.23.s`, `ERR599109_bin.22.s` and `ERR599156_bin.36.o`, and bissett_base `ERR671910_bin.15.o`, `ERR671913_bin.3.o`, `ERR671914_bin.8.s`, `ERR671925_bin.6.o` and `ERR687894_bin.4.o`.

Every bin either lane missed has a published GEM:
- Reproduction lane, no model: sunagawa2015 `ERR599115_bin.46.s`, `ERR599130_bin.8.o` and `ERR599162_bin.33.s`.
- Reproduction lane, a model but no MEMOTE result: karlsson2013 `ERR260138_bin.8.p`, `ERR260148_bin.22.p`, `ERR260182_bin.14.s`, `ERR260232_bin.11.p`, `ERR260233_bin.17.s` and `ERR260263_bin.23.o`, and sunagawa2015 `ERR598980_bin.21.s`, `ERR598988_bin.35.p`, `ERR598993_bin.48.s`, `ERR599038_bin.54.p`, `ERR599063_bin.9.p`, `ERR599064_bin.27.s`, `ERR599139_bin.46.s` and `ERR599146_bin.4.s`. MEMOTE runs for 2 h, with one retry at 4 h. All three in chunk 1 stopped at the 4 h limit.
- Modern lane, no model: karlsson2013 `ERR260174_bin.4.s`, and sunagawa2015 `ERR598965_bin.21.o`, `ERR598978_bin.7.o`, `ERR598984_bin.22.p`, `ERR598993_bin.59.s`, `ERR599057_bin.15.s`, `ERR599057_bin.42.s` and `ERR599176_bin.8.s`.

CAUTION the archives keep `results/` and the workflow files, not the Nextflow logs. So the cause of each missing model is not recorded. The reproduction lane caps carve at 6 h with no retry, and the modern lane caps each SCIP solve at 600 s.

Parity against metaGEM's published GEMs, as medians over the bins both sides have, with the range in parentheses:

| Lane | Bins | Reactions Jaccard | Metabolites Jaccard | Genes Jaccard | Shared reactions with a different gene rule |
|---|---|---|---|---|---|
| Reproduction | 14,084 | 0.840 (0.446–0.993) | 0.902 (0.544–1.000) | 0.976 (0.813–1.000) | 2.1% |
| Modern | 14,079 | 0.409 (0.165–0.688) | 0.531 (0.289–0.791) | 0.706 (0.455–0.818) | 25.7% |

The full run matches the smoke bins (0.836 reactions and 0.975 genes on 20 bins). No model is identical to its published GEM, in either lane. In the reproduction lane, 214 models reach reaction Jaccard 0.95. The median model has 1,345 reactions in both the reproduction lane and metaGEM, and 1,287 in the modern lane. The medians hold per study:

| Study | Bins | Reproduction reactions / genes | Modern reactions / genes |
|---|---|---|---|
| bissett_base | 269 | 0.832 / 0.971 | 0.412 / 0.707 |
| karlsson2013 | 4,127 | 0.840 / 0.973 | 0.416 / 0.707 |
| korem2015 | 154 | 0.820 / 0.961 | 0.442 / 0.688 |
| li2019 | 172 | 0.870 / 0.972 | 0.432 / 0.700 |
| sunagawa2015 | 9,362 | 0.839 / 0.977 | 0.405 / 0.705 |

The modern lane's karlsson2013 and sunagawa2015 rows cover one and four fewer bins, respectively.

### Storage

WARNING the chunk archives are the only copy of E4's products. `/scratch/phyberos/metagem/e4_gems_archive/<lane>/chunk<N>.<key>.tar.zst` holds each lane's `results/` with its lineage index, `_metadata/index.yml`, and the workflow files. After a lane was archived, its cache entries and its run directory were removed. The two lanes would have left about 720K inodes in the cache against about 330K of room, so Tony chose archiving per chunk (2026-09-22). A new plan over the same bins recomputes every step. It cannot reuse the archive.

## Modern lane against E5's GEM lane

The modern lane plans with E5's GEM library as `e5_pilot.py` loads it for `--solver open`: `carveme_from_orfs` on CarveMe 1.6.6 with SCIP, then `memote_score` on MEMOTE 0.17.0. The medium is the same M8 table (`research/metasmith_libraries/carveme_m8_medium.tsv`). It holds metaGEM's 74 M8 compounds exactly, each with uptake bounded at 100.

These differ from E5:

| Item | E5 | E4 modern lane |
|---|---|---|
| Genomes | E5's own four MAG sets: DAS Tool or MAGScoT, each dereplicated by dRep or skANI | metaGEM's published MAGs, refined by metaWRAP |
| Gene calls | `pprodigal -p meta` on E5's MAGs | metaGEM's proteins: CheckM's Prodigal, single mode |
| Samples | E5's corpora | metaGEM's five studies |

## Modern lane against metaGEM

These are the version effects the two E4 lanes isolate. Both lanes read the same proteins.

- CarveMe 1.6.6 searches its own BiGG protein set (26,727 sequences) against 1.2.2's 84,674, with a newer universe and gene–reaction table.
- SCIP replaces CPLEX, with a 600 s cap on each solve.
- Gapfill runs as a second call with no gene scores. It adds the fewest reactions, not the best-supported ones. `carveme_from_orfs.py` explains why SCIP needs the split.
- MEMOTE 0.17.0 replaces 0.9.13, and runs through `memote_score.py` instead of metaGEM's two commands.
