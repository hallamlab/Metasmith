# remaining-datasets

## Purpose & Contents

This topic answers Tony's question of 2026-11-06:

> status on the rest of the files to archive?

It holds the archive's status, the project entries for the datasets still to move, and the decisions on how to move them. Sizes come from a read-only listing of the backup volume on 2026-11-06, pinned in DVC at `data/engcyc/sources/worker01/`. Dataset names, locations and per-file detail live in the DVC tables, not here, because this repository is public.

## Status on 2026-11-06

Of the 106 datasets in the Engcyc manifest:

| Group | Datasets | Size | State |
|---|---|---|---|
| Moved by this work | 5, in 6 entries | 0.65 TB | On chinook in the new shape, checksum-verified, each with `manifest.csv`. |
| Already in the new shape before this work | 11 | 3.8 TB | On chinook as received-data entries. They need a `manifest.csv` only. |
| On chinook elsewhere | 10 | 1.7 TB | In personal backup folders or project folders, not as received-data entries. |
| Not on chinook | 12 | 0.2 TB, 2 unsized | On fir, Sockeye, altair or another Globus endpoint only. Mostly small lab 16S and fosmid sets. They join the project entries below. |
| External | 20 | 11.3 TB | Public datasets. The `external-home` topic decides their home. |
| On the backup volume only | 47 | 29 TB of raw inputs | The project entries below. Five Sakinaw sets of 0.2 TB are in flight: gzip on altair runs now, then upload and manifests. |
| Missing | 1 | unknown | No location known. |

## Project entries

Each entry is `Received_raw_data/<Project>/`. Each delivery is a subfolder in it, named `YYYY-MM-DD_<Type>_<Centre>`. Each entry has one `manifest.csv`. Its `dataset_id` column tells the deliveries apart. Sizes count raw inputs only.

| Entry | Contents | TB |
|---|---|---|
| Sakinaw_Lake | The 6 entries made on 2026-11-06, renamed in. The 454, single-cell and pyrotag sets in flight. The fosmid pools go to Fosmids. | 0.8 |
| Pitch_Lake | The JGI entry made on 2026-11-06, renamed in. | 0.08 |
| Saanich_Inlet | JGI metagenomes, JGI iTags, the 2008 to 2012 16S time series. | 5.5 |
| Line_P | JGI metagenomes, JGI iTags, Hakai 16S and 18S. | 3.4 |
| Oxygen_Minimum_Zones | Single cells, Eastern Tropical South Pacific, Eastern Tropical North Pacific, Mediterranean deep chlorophyll maximum. | 5.7 |
| South_Atlantic_Dark_Ocean_2013 | The R/V Knorr KN210-04 metagenomes, metatranscriptomes, iTags and cruise metadata. The lab's JGI proposal, PI Hallam. | 0.5 |
| Hakai | JGI and Genome Quebec metagenomes, two amplicon sets. | 1.2 |
| Long_Term_Soil_Productivity | O'Connor Lake and Williams Lake soil metagenomes. | 1.5 |
| Hydrocarbon | Survey metagenomes, the degrading-culture metagenomes, coal-bed metagenomes, oil and gas well pyrotags. | 5.3 |
| Methanome | Site metagenomes. | 3.5 |
| Stratified_Lakes | Powell, Kivu and Kabuno Bay, Malili lakes. | 0.8 |
| Fosmids | Every fosmid library, one subfolder per library. | 1.3 |
| Lignin | Ligninase screen and in-situ metagenomes. | 0.3 |
| Cyanogroup_16S | AB48 consortia and Spirulina purification 16S. | 0.004 |
| One small entry each | Beaver gut, phosphorus-removal bioreactor, demolition range, wastewater, biofuel experiments, Antamina mine, methanotroph single cells, collaborations. | 0.3 |

Work the entries smallest first.

## Samples shared across entries

A fosmid library and a metagenome made from one physical sample sit in different entries. Give them the same `sample_id` in both manifests. Keep `sample_id` unique across the whole archive, so that a match always means one physical sample.

## Route

The backup volume has a read-only Globus endpoint. It needs no admin window and no shell on the backup host.

1. Transfer compressed files from that endpoint straight to the chinook entry, with checksum verification.
2. Transfer uncompressed read files from that endpoint to altair. Run `pigz -p 4` at nice 19 on each file, as Tony approved on 2026-11-06. Transfer the `.gz` files to the same chinook entry.
3. Write `manifest.csv` from the Globus checksums.

Altair has 7.1 TB free, which holds the uncompressed part of any one entry.

## Decisions

Tony, 2026-11-06:

> For the existing ones, you don't have to touch them at all, except for to add a CSV manifest in addition to the existing YAML ones

> For the new updates, it would be good if we kept most of this together and updated rather than adding more and more, since we have the ability to do so. So for instance, we could combine the three biofactorial ones and maybe the FabFos and SCADC. Those could be like some FOSMID project.

> We do want to keep external data sets that we did not generate separate.

> let's just do the raw inputs for these and skip the blast tab and stuff.

> Fold today's Pitch Lake and Sakinaw entries into their project folders. -> yes
> Put every fosmid library in one Fosmids entry. -> yes, as long as its marked internally (subfolder structure)
> Put the ScaDC metagenome under Hydrocarbon -> sure, but need to indicate the fosmid and metagenome links where they are the same sample. this needs to be clear somewhere

The design that follows:

1. Leave every dataset already on chinook where it is. Add a `manifest.csv` beside it. Keep any `manifest.yml`.
2. Upload a dataset that is not on chinook, or on the backup volume only, into a project entry, not one entry per dataset. Add later deliveries of the same project to that entry and extend its `manifest.csv`. All fosmid libraries form one Fosmids entry, one subfolder per library. Fold the Pitch Lake and Sakinaw entries made on 2026-11-06 into their project entries.
3. Move only the raw inputs from the backup volume: reads as delivered and the centre's small QC reports. Leave annotation tables, alignments, assemblies and analysis work folders on the backup volume. The `derived-archive` topic decides their home.
4. Keep each external dataset apart from the lab's own data, even when the lab manifest marks it internal. HOT is external. The Knorr 2013 cruise data is internal: the lab's JGI proposal.

## Open questions

None. The topic is closed.
