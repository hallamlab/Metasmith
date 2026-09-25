# E4's GEM counts and parity

## Purpose & Contents

These tables are E4's full-run results: which bins got a model in each lane, how each model compares with metaGEM's published GEM for the same bin, and MEMOTE's results for all three. `findings/E4_REPRODUCTION.md` holds the medians and what they mean. The models themselves live only in the chunk archives on fir, `/scratch/phyberos/metagem/e4_gems_archive/<lane>/chunk<N>.<key>.tar.zst`, because each lane's cache entries and run directory were removed after archiving.

| File | Rows | What it is |
|---|---|---|
| `tally.tsv` | 14,105 bins | per bin: whether metaGEM published a GEM, and the model and MEMOTE counts per lane (0 or 1) |
| `parity_repro.tsv` | 14,102 bins | reproduction lane against metaGEM, one row per bin with a model |
| `parity_modern.tsv` | 14,097 bins | modern lane against metaGEM, one row per bin with a model |
| `quality_metagem.tsv` | 14,015 bins | metaGEM's published MEMOTE 0.9.13 results, per test |
| `quality_repro.tsv` | 14,088 bins | the reproduction lane's MEMOTE 0.9.13 results, per test |
| `quality_modern.tsv` | 14,097 bins | the modern lane's MEMOTE 0.17 total and section scores. The lane archived no per-test result. |
| `quality_sample.tsv` | 200 bins | 40 bins per study re-scored four ways, one row per bin and run |

A per-test column holds MEMOTE's metric, and `<test>_n` holds the count or value it came from.

A parity row with only `no published GEM` is one of the 18 bins metaGEM built no GEM for.

## Provenance

`drivers/e4_tally.sbatch` wrote all three (job 61479987, 2026-09-25, checkout 0843e646). `e4_tally.py` traces each product through its archive's lineage index to its protein bin. `e4_parity.py` compares reaction, metabolite and gene sets, the gene rule of every shared reaction, and the flux bounds.

`drivers/e4_quality.sbatch` wrote the three `quality_<source>.tsv` (job 61520978, 2026-09-25). `drivers/e4_quality_sample.py` staged the sample, and `e4_quality_sample.sbatch` scored it (jobs 61526575 and 61527887, 2026-09-25). `drivers/e4_quality_compare.py` summarises all four.
