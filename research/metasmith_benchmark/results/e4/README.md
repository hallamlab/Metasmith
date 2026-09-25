# E4's GEM counts and parity

## Purpose & Contents

These tables are E4's full-run results: which bins got a model in each lane, and how each model compares with metaGEM's published GEM for the same bin. `findings/E4_REPRODUCTION.md` holds the medians and what they mean. The models themselves live only in the chunk archives on fir, `/scratch/phyberos/metagem/e4_gems_archive/<lane>/chunk<N>.<key>.tar.zst`, because each lane's cache entries and run directory were removed after archiving.

| File | Rows | What it is |
|---|---|---|
| `tally.tsv` | 14,105 bins | per bin: whether metaGEM published a GEM, and the model and MEMOTE counts per lane (0 or 1) |
| `parity_repro.tsv` | 14,102 bins | reproduction lane against metaGEM, one row per bin with a model |
| `parity_modern.tsv` | 14,097 bins | modern lane against metaGEM, one row per bin with a model |

A parity row with only `no published GEM` is one of the 18 bins metaGEM built no GEM for.

## Provenance

`drivers/e4_tally.sbatch` wrote all three (job 61479987, 2026-09-25, checkout 0843e646). `e4_tally.py` traces each product through its archive's lineage index to its protein bin. `e4_parity.py` compares reaction, metabolite and gene sets, the gene rule of every shared reaction, and the flux bounds.
