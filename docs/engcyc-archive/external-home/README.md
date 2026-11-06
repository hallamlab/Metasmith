# external-home

## Purpose & Contents

This topic answers Tony's question of 2026-11-06:

> and we need to decide where to place the the external datasets like CAMI, tara oceans, etc

It holds where the external datasets sit now and the home decided for them. An external dataset is one the lab did not generate. Locations and per-file detail live in the DVC tables, because this repository is public.

## Status on 2026-11-06

| Group | Datasets | Size | Where now |
|---|---|---|---|
| CAMI benchmarks | 7 | 1.4 TB | On chinook in the external-data folder, one subfolder per CAMI release, with one `manifest.tsv`. |
| metaGEM reproduction sets | 5 | 5.2 TB | On chinook in a personal workspace backup, as fetched, beside the fetch scripts and run outputs. |
| Groundwater benchmark (Pratama 2026) | 1 | 0.8 TB | On fir scratch and in a personal workspace backup on chinook. |
| Cyanoverse | 1 | 3.8 TB | On fir and in the manuscripts collection. Its reads are public SRA runs. |
| Host chassis genomes, amplicon test set | 3 | 3 GB | On altair, Sockeye and in a chinook project folder. |
| Ocean time series and ocean recruitment runs | 2 | unsized | The 2025 sharknado index lists them at the sharknado root, outside the lab data folder. The backup volume does not hold them. Most runs are public. |
| Earlier Engcyc build | 1 | unsized | Same as the row above. JGI assemblies with annotation output. |
| Tara subset | not in the manifest | about 0.3 TB | On the backup volume and in the older partial copy on chinook. Public ENA runs. |
| GEM catalog | 1 | not fetched | Candidate only. |

The external-data folder on chinook also holds reference databases and tool folders beside CAMI.

## Open questions

The queue is in `STACK.md`.
