# remaining-datasets

## Purpose & Contents

This topic answers Tony's question of 2026-11-06:

> status on the rest of the files to archive?

It holds the archive's status, the batches for the datasets still to move, and the decisions on how to move them. Sizes come from a read-only listing of the backup volume on 2026-11-06, pinned in DVC at `data/engcyc/sources/worker01/`. Dataset names, locations and per-file detail live in the DVC tables, not here, because this repository is public.

## Status on 2026-11-06

Of the 106 datasets in the Engcyc manifest:

| Group | Datasets | Size | State |
|---|---|---|---|
| Moved by this work | 5, in 6 entries | 0.65 TB | On chinook in the new shape, checksum-verified, each with `manifest.csv`. |
| Already in the new shape before this work | 11 | 3.8 TB | On chinook as received-data entries. They need a `manifest.csv` only. |
| On chinook elsewhere | 8 | 1.7 TB | In personal backup folders or project folders, not as received-data entries. |
| Not on chinook | 14 | 0.2 TB, 2 unsized | On fir, Sockeye, altair or another Globus endpoint only. Mostly small lab 16S and fosmid sets. |
| External | 20 | 11.3 TB | Public datasets. The `external-home` topic decides their home. |
| On the backup volume only | 47 | 36.2 TB raw | The batches below. Five Sakinaw sets of 0.2 TB are in flight: gzip on altair runs now, then upload and manifests. |
| Missing | 1 | unknown | No location known. |

## Batches for the backup volume

Each batch keeps one entry per dataset. "Compressed" moves as it is. "Uncompressed" is read files (fastq, sff, fna, qual, fasta) that get gzip first. "Other" is centre output beside the reads: annotation tables, alignments, assemblies.

| Batch | Datasets | Total | Compressed | Uncompressed | Other |
|---|---|---|---|---|---|
| Small sets | 13 | 0.9 TB | 0.5 | 0.3 | 0.1 |
| Stratified lakes, Sakinaw remainder included | 6 | 1.3 TB | 0.4 | 0.9 | 0.0 |
| Hakai and LTSP | 6 | 3.1 TB | 2.6 | 0.1 | 0.4 |
| Methanome | 1 | 3.5 TB | 3.0 | 0.6 | 0.0 |
| Line P | 5 | 4.4 TB | 3.0 | 1.1 | 0.3 |
| Hydrocarbon | 6 | 5.2 TB | 3.6 | 1.6 | 0.0 |
| Saanich | 4 | 8.7 TB | 3.6 | 1.9 | 3.2 |
| OMZ single cells and cruises | 6 | 9.1 TB | 7.9 | 0.5 | 0.7 |

The processed-data folders (2.6 TB, mostly old pipeline output) are not counted. Two listed locations did not resolve and need their paths corrected in the manifest.

## Route

The backup volume has a read-only Globus endpoint. It needs no admin window and no shell on the backup host.

1. Transfer compressed and other files from that endpoint straight to the dataset's chinook entry, with checksum verification.
2. Transfer uncompressed read files from that endpoint to altair. Run `pigz -p 4` at nice 19 on each file, as Tony approved on 2026-11-06. Transfer the `.gz` files to the same chinook entry.
3. Write `manifest.csv` from the Globus checksums.

Altair has 7.1 TB free, which holds the uncompressed part of any one batch.

## Decisions

Tony, 2026-11-06:

> For the existing ones, you don't have to touch them at all, except for to add a CSV manifest in addition to the existing YAML ones

> For the new updates, it would be good if we kept most of this together and updated rather than adding more and more, since we have the ability to do so. So for instance, we could combine the three biofactorial ones and maybe the FabFos and SCADC. Those could be like some FOSMID project.

> We do want to keep external data sets that we did not generate separate.

> let's just do the raw inputs for these and skip the blast tab and stuff.

The design that follows:

1. Leave every dataset already on chinook where it is. Add a `manifest.csv` beside it. Keep any `manifest.yml`.
2. Upload a dataset that is not on chinook into a project entry, not one entry per dataset. Add later deliveries of the same project to that entry and extend its `manifest.csv`. The fosmid sets (biofactorial, fabfos, ScaDC) form one fosmid project entry.
3. Move only the raw inputs from the backup volume: reads as delivered and the centre's small QC reports. Leave annotation tables, alignments, assemblies and analysis work folders on the backup volume. The `derived-archive` topic decides their home.
4. Keep each external dataset apart from the lab's own data, even when the lab manifest marks it internal. HOT is external.

## Open questions

The open questions are queued in `STACK.md` under this topic.
