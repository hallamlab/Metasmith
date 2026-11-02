# E4's GEM counts and parity

## Purpose & Contents

These tables are E4's full-run results: which bins got a model in each lane, how each model compares with metaGEM's published GEM for the same bin, and MEMOTE's results for all three. The `ablation_*` tables score E4's ablation ladder over a 1,408-bin subset. `findings/E4_REPRODUCTION.md` holds the medians and what they mean. The models themselves live only in the chunk archives, `e4_gems_archive/<lane>/chunk<N>.<key>.tar.zst`, because each lane's cache entries and run directory were removed after archiving. The ablation's models live in its own rung archives. All the archives are now on chinook only. `findings/E4_REPRODUCTION.md` § Storage says where, and how to restore them.

| File | Rows | What it is |
|---|---|---|
| `tally.tsv` | 14,105 bins | per bin: whether metaGEM published a GEM, and the model and MEMOTE counts per lane (0 or 1) |
| `parity_repro.tsv` | 14,102 bins | reproduction lane against metaGEM, one row per bin with a model |
| `parity_modern.tsv` | 14,097 bins | modern lane against metaGEM, one row per bin with a model |
| `quality_metagem.tsv` | 14,015 bins | metaGEM's published MEMOTE 0.9.13 results, per test |
| `quality_repro.tsv` | 14,088 bins | the reproduction lane's MEMOTE 0.9.13 results, per test |
| `quality_modern.tsv` | 14,097 bins | the modern lane's MEMOTE 0.17 total and section scores. The lane archived no per-test result. |
| `quality_sample.tsv` | 200 bins | 40 bins per study re-scored four ways, one row per bin and run |
| `ablation_parity.tsv` | 23,907 rows | each ablation rung's model against metaGEM (`against=metagem`) and against the rung above (`against=previous`), one row per bin, rung and reference |
| `ablation_summary.tsv` | 102 rows | medians and interquartile ranges of `ablation_parity.tsv`, pooled and per study |
| `ablation_universe.tsv` | 1,408 bins | the share of metaGEM's reactions in CarveMe 1.6.6's universe, and parity with metaGEM over the reactions both universes hold, for rungs R1, B and U |
| `ablation_memote.tsv` | 14,080 rows | MEMOTE 0.17 total and section scores for metaGEM's GEM and every rung, one row per bin and source. `note` is `no model` where a rung built none. |
| `ablation_memote_rewritten.tsv` | 7,040 rows | as `ablation_memote.tsv`, for metaGEM's GEM and rungs R0 to U after each model is rewritten through CarveMe 1.6.6's SBML writer |
| `ablation_rewrite_fidelity.tsv` | 7,040 rows | what each rewrite changed: per check, the count of elements that differ, and the formulas, charges, SBO terms and annotation namespaces before and after |

A per-test column holds MEMOTE's metric, and `<test>_n` holds the count or value it came from.

A parity row with only `no published GEM` is one of the 18 bins metaGEM built no GEM for.

## Provenance

`drivers/e4_tally.sbatch` wrote all three (job 61479987, 2026-09-25, checkout 0843e646). `e4_tally.py` traces each product through its archive's lineage index to its protein bin. `e4_parity.py` compares reaction, metabolite and gene sets, the gene rule of every shared reaction, and the flux bounds.

`drivers/e4_quality.sbatch` wrote the three `quality_<source>.tsv` (job 61520978, 2026-09-25). `drivers/e4_quality_sample.py` staged the sample, and `e4_quality_sample.sbatch` scored it (jobs 61526575 and 61527887, 2026-09-25). `drivers/e4_quality_compare.py` summarises all four.

`drivers/e4_ablation.py` wrote `ablation_parity.tsv`, `ablation_summary.tsv` and `ablation_universe.tsv` (checkout 283c2dbb). Its `extract` wrote every rung's models out of the lane archives on fir (job 62192031, 2026-09-29), and `compare`, `summary` and `universe` read them. R0 and M come from E4's chunk archives. The other rungs come from `e4_chain.sbatch` over the `e4abl` home (jobs 61930928 and 62074730, 2026-09-28 to 2026-09-29), with these run keys:

| Rung | Lane | Run key |
|---|---|---|
| R1 | `repro` | `ADbK45My` |
| B | `bigg` | `DzdkPUul` |
| U | `universe` | `tJn4eR3h` |
| V | `version` | `BDE02pUu` |
| G | `gapfill` | `Oc4ZKujV` |
| D | `diamond` | `WYZoxeEB` |
| S | `scip` | `fHy9Gtxj` |

`drivers/e4_ablation_memote.sbatch` scored metaGEM's GEMs and rungs R0 to S (jobs 62448450 and 62448866, 2026-10-01, checkout b5e5cee0) over models from a fresh `extract` of the rung archives. `e4_ablation_memote.py collect` wrote `ablation_memote.tsv`, taking M's rows from `quality_modern.tsv`. `figures/make_figures.py` draws E4's figures from these tables.

The same sbatch with `REWRITE_IMAGE` set to the stock CarveMe 1.6.6 image (reframed 1.6.0) wrote the two rewrite tables (jobs 62505042 and 62505653, 2026-10-01, checkout c8b0c61a). `drivers/sbml_rewrite.py` holds the rewrite and its checks.
