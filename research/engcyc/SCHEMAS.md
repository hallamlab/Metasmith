# Engcyc dataset tables

## Purpose & Contents

This file defines the tables that list the candidate datasets for Engcyc. It holds:

1. The dataset manifest, `data/engcyc/manifest.tsv`.
2. The rule for the per-dataset metadata tables in `data/engcyc/metadata/`.
3. The amplicon datasets and their overlap with metagenome datasets.

The manifest and the metadata tables carry internal paths and collaborator data. Keep them in DVC under `data/engcyc/`. Keep this file free of paths and sample details, because the git remotes are public.

## Terms

- **Dataset**: one study with its own sample or library list. It is one manifest row.
- **Collection**: a group of datasets, such as CAMI II or the Hallam fosmid collection.
- **Modal**: the kind of sequence data. It is one of `metagenome`, `genome`, `fosmid` or `amplicon`.

## 1. Dataset manifest

The manifest has one row per dataset. Every column is a string. An empty cell means "not known".

| Column | Holds |
|---|---|
| `dataset_id` | Short stable key in snake case. It is also the metadata table's file stem. |
| `name` | Human-readable name. |
| `collection` | Grouping above the dataset. |
| `modal` | `metagenome`, `genome`, `fosmid` or `amplicon`. |
| `description` | One or two sentences: environment, design and what was sequenced. |
| `origin` | `internal`, `external` or `collaborator`. |
| `accession` | Public IDs, such as a BioProject, ENA study, Zenodo record or JGI proposal. Empty when the data is not public. |
| `locations` | Every place the data lives, as `node:path` entries separated by `; `. |
| `n_samples` | Number of samples, runs or libraries, as the source counts them. Fosmid rows count libraries. |
| `size_bytes` | Disk size of the raw inputs. |
| `size_source` | `measured` (a live listing) or `recorded` (taken from a doc). |
| `status` | `available`, `archived`, `missing` or `candidate`. |
| `metadata_table` | Bare filename of the metadata table, resolved inside `data/engcyc/metadata/`. Empty when the dataset has no metadata. |
| `existing_results` | Free text on how far processing got. |
| `notes` | Free text: caveats, places searched, data-use flags. |

Status values:

- `available`: the raw inputs are on a node that metasmith can read now.
- `archived`: the raw inputs exist in cold storage, such as chinook or the sharknado volume. A restore is needed before a run.
- `missing`: the dataset is known, but its sequence data is not found.
- `candidate`: a public dataset that Engcyc may add. Nothing is downloaded yet.

Write `description`, `existing_results` and `notes` in short declarative sentences, in simple present or past tense, with no semicolons.

## 2. Metadata tables

A metadata table is a copy of the metadata that already exists for a dataset. Write it as TSV, named `<dataset_id>.tsv`.

1. Keep the source's own columns and values. Add no columns.
2. Select the dataset's rows when one source table covers several datasets, such as the FosDB sheets or the ASPIRE cohort list.
3. Join two sources only when both describe the same libraries or genomes, such as the FosDB sheet and the library inventory.
4. Give a dataset with no metadata no table. Leave its `metadata_table` cell empty.

A run or file list that came with the study counts as metadata. A directory listing made during a survey does not.

**CAUTION** The manifest and the tables are read-only DVC hardlinks. Run `dvc unprotect data/engcyc/manifest.tsv data/engcyc/metadata` before you edit or rebuild them. Run `dvc add` on both afterwards.

Run `research/engcyc/scripts/build_metadata.py` to rebuild the tables. It also rewrites the `metadata_table` column of the manifest. Copy a new source into `data/engcyc/sources/` before you add it to the script.

Run `research/engcyc/scripts/validate_manifest.py` after every manifest edit. It fails on a schema error. It warns when a table's row count differs from `n_samples`, because a table can count files, runs or plate sets. Explain each such difference in `notes`.

## 3. Amplicon datasets

Amplicon datasets are a modal of their own in the manifest. ASPIRE processed five of them. The rest are raw reads in cold storage.

Datasets with both amplicon and metagenome data:

- **Saanich Inlet**: the metagenomes and the 2008–2012 16S time series share cruises and depths. JGI iTags exist for the same program.
- **AB48**: the 16S samples and the metagenome come from the same consortium, but not from the same sample.
- **Line P**: metagenomes, JGI iTags and 2023 16S/18S runs. The overlap in stations and years is not checked.
- **Mine enrichments**: metagenomes and 16S runs from the same enrichment program. Shared samples are not checked.
