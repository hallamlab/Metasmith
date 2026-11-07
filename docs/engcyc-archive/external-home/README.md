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

## Home

Each external dataset gets one folder, `Resources/external_data/<Collection>/<dataset_id>/`, in the chinook Projects collection. The folder holds the files as published and one `manifest.csv` in the received-data schema. CAMI already sits in this shape and needs only its `manifest.csv`. Move the read files of a set that sits in a workspace backup into its folder by per-file rename. Leave the fetch scripts and run outputs in the backup.

Tony, 2026-11-07, on "one folder per external dataset under the chinook external-data folder, as CAMI is now?":

> external-home q1, yes

## Copies of public reads

Keep the lab's copy of every public read set it already holds, gzipped as it is. Give each a `manifest.csv` with the accession of every file, so a lost copy can be fetched again. Fetch no set only to complete the archive.

Tony, 2026-11-07, on "keep a copy of public reads, or keep an accession manifest only?":

> we have the public reads and tehy are zipped, we can keep our copies

## Sets outside the backup

The ocean time series, the ocean recruitment runs and the earlier Engcyc build sat at the sharknado root, outside the lab data folder. The sharknado index lists their names. The backup volume does not hold them, and no other copy is known. Mark the three `missing` in the manifest. Their public runs can be fetched again by accession, and the IMG assemblies by taxon ID. Their old annotation output is lost.

Tony, 2026-11-07:

> we also have the sharknado index sqlite db on chinook. if you cant find what you are looking for there, then it is gone.

## Open questions

None. The topic is closed.
