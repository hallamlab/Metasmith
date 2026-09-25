# E2's AMBER results

The two tables here are the only copy of E2's scores outside `fir:/scratch/phyberos/cami/metasmith/task_cache`,
which is itself the only copy of E2's computed products — E2's run directories are gone. Everything else about
E2 (assemblies, BAMs, bins, CheckM2) still lives only in that cache.

| file | rows | what it is |
| --- | --- | --- |
| `e2_amber_summary.tsv` | 996 = 249 samples x 4 binners | one row per (sample, binner), AMBER's genome-binning summary |
| `e2_amber_bin_metrics.tsv` | 26,928 | one row per bin |

`arm`, `run_key` and `sample` are prepended by the extractor; every other column is AMBER's own. Each row's
sample field was cross-checked against the sample resolved from its shard before the row was emitted.

## Provenance

- AMBER `cami-amber 2.0.7--pyhdfd78af_0`, `binning type = genome`, `rank = NA`.
- Short arm `MjMN02CK`, 208 samples. Long arm `VgUw0A7c`, 41 samples.
- Extracted from transform `4UdrtQp8` only, 249 shards, `tombstoned_at is null`.

## The headline metric

`f1_score_bp` — the harmonic mean of AMBER's per-genome average purity and average completeness, bp-based.
Verified as `2PR/(P+R)` with `P = precision_avg_bp` and `R = recall_avg_bp`, max deviation 0.000e+00 over all
996 rows. It is **not** `f1_score_per_bp`, not the `_seq` variants, and not `_cami1`.

Medians, reproduced from this file:

| arm | COMEBin | MetaBAT2 | SemiBin2 | DAS Tool |
| --- | --- | --- | --- | --- |
| short (n=208) | 0.1905 | 0.1385 | 0.1260 | 0.0889 |
| long (n=41) | 0.9034 | 0.4138 | 0.4715 | 0.1951 |

**The short and long medians are not a read-length comparison.** 100 of the 208 short samples are `strain`,
the hardest dataset for every binner. Per-dataset short medians for COMEBin run strain 0.121, mousegut 0.299,
marine 0.456, airskinurogenital 0.476, gastrooral 0.538. Reporting "short 0.19 vs long 0.90" as a read-length
effect is an overclaim; it is substantially a dataset-mix effect.

## Two traps if you re-extract from the cache

- **Take AMBER only from transform `4UdrtQp8`.** A second transform named `amber`, `h2g7GAwU`, is also in the
  cache and is not tombstoned. Its long arm holds 82 shards for 41 samples — two batches written 7,800 s apart.
  The early batch gives a long DAS Tool median of 0.9105; the late batch gives 0.1951 and is bit-identical to
  `4UdrtQp8`. Extracting "the amber shards" by name silently mixes a superseded batch into the numbers.
- **Identify products by their header, never by the dtype suffix.** The suffix differs per arm
  (`-9lwRaNZx`/`-WrPKXJz9` short, `-sdxbXLKo`/`-zZwGkfvB` long), so a suffix glob returns 41 of 249 and drops
  the entire short arm.

Open any metasmith `cache.sqlite` as `file:<path>?immutable=1`. The database is in WAL mode, and a plain open
creates `-shm`/`-wal` sidecars and bumps the `task_cache/` root mtime.

## What is not here

E1's side. nf-core/mag ships no scorer of any kind, so E1 was never scored at scale — AMBER exists for E1 on
`sample_0` only (COMEBin 0.885, MetaBAT2 0.478, SemiBin2 0.428, DAS Tool 0.181, long arm). These tables are
therefore E2's results, not a comparison.
