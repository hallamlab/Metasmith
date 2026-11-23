# Annotation tables for the CPE3 assemblies

## Purpose & Contents

This file explains the annotation tables under `/scratch/phyberos/beth_mp/tables/v1/` on fir. It
covers the files in each assembly folder, the features, every annotation lane and its
confidence unit, the Pathway Tools tables, the status checks, and where the raw outputs live.

Each assembly ran through two pipelines:

- **mp**: the original MetaPathways 4.0.3.
- **port**: the metasmith port of MetaPathways, plus extra annotation tools.

A **lane** is one tool run against one reference, such as DIAMOND against UniRef50. The tables
join every lane of both pipelines into one place per assembly.

## Files in each assembly folder

| File | What it holds |
|---|---|
| `annotations_wide.parquet`, `.tsv.gz` | One row per feature. The best hit of each lane, in three columns per lane. |
| `annotations_long.parquet`, `.tsv.gz` | One row per hit. Every hit of every lane, ranked. |
| `pgdb_port/`, `pgdb_mp/` | Gene-level tables from each pipeline's Pathway Tools databases (PGDBs). |
| `cds_coordinate_mismatches.tsv` | Every CDS that the two pipelines did not call with identical coordinates. |
| `lane_presence.tsv` | One status per lane: did it run, and did it find anything. |
| `summary.tsv` | Counts of features and hits per lane. |
| `lane_legend.tsv` | The lane table below, in machine-readable form. |

Open Parquet with `arrow::read_parquet()` in R or `pandas.read_parquet()` in Python. The
`.tsv.gz` files hold the same data as tab-separated text.

**CAUTION** The long tables hold millions of rows, more than Excel opens. So do the wide tables
of the two parent assemblies. Use R or Python for them.

## Features

A feature is one called object on a contig. Its `feature_id` is
`type:contig:start-end:strand`, for example
`CDS:cpe3_r2a_cu6__hybrid_metaspades__contig_000117:26966-27790:-`. Contig names are the
assembly's own, in both pipelines. The two pipelines' calls share a row only when their
coordinates agree exactly.

| Feature type | Called by |
|---|---|
| `CDS` | Both pipelines |
| `rRNA`, `tRNA` | Both pipelines, and Bakta |
| `promoter` | PromoTech |
| `ncRNA`, `tmRNA`, `regulatory_region`, `CRISPR`, `crispr-repeat`, `crispr-spacer`, `oriC`, `oriT` | Bakta |
| `element` | The element lanes. `element_class` says which kind: `virus`, `provirus`, `plasmid`, `integron`, `MGE` or `BGC`. |

## The wide table

The fixed columns come first.

| Column | Meaning |
|---|---|
| `feature_type`, `feature_name` | The type, and the name the caller gave it, such as a tRNA product |
| `element_class` | For an element row, its kind |
| `contig`, `start`, `end`, `strand`, `length` | Position on the assembly |
| `bin` | The UniteM bin (MAG) that holds the contig, if any |
| `pipelines` | `mp`, `port`, or `mp;port` when both called the feature |
| `sources` | The callers that produced the feature |
| `port_id` | The port's ORF name, `<contig>_<n>` |
| `mp_id` | MetaPathways' ORF name, `C<contig number>-G<gene number>` |
| `coord_status`, `coord_partner` | How the two pipelines' calls compare (see below) |
| `elements` | The elements that contain this feature |

Three columns per lane follow: `<lane>_id`, `<lane>_desc` and `<lane>_conf`. They hold the
lane's best hit, the hit's description and its confidence. Four rules apply:

- The pathway lanes (`*_pathologic`) list every reaction and every pathway of the ORF, joined
  by `;`.
- The `gpr` and `gpr7` lanes give the reaction that the most methods agree on. `_desc` lists
  those methods and `_conf` counts them.
- The contig lanes (`contig_lcastar`, `metabuli`) are copied onto every feature on the contig.
- The element lanes are copied onto every feature inside the element.

**CAUTION** geNomad sometimes calls a whole contig a plasmid. In `hybridspades_cpe3_r2a_cu6`
it called the 830 kb contig_000001 a plasmid with score 0.94, so every feature on that contig
carries the plasmid label. Check `genomad_conf` and the element length before you trust the
label.

## The long table

| Column | Meaning |
|---|---|
| `feature_id`, `feature_type` | The feature the hit belongs to. Contig lanes use `contig:<name>`. |
| `lane` | The lane |
| `rank` | 1 is the lane's best hit for the feature, by its confidence |
| `id`, `desc`, `conf` | As in the wide table |
| `key` | The feature's name in the tool's own output |
| `extras` | The tool's other columns for the hit, as JSON |

## Lanes

The `_conf` column holds the confidence measure that the last column of each table names. None
means the tool gives no score, and the cell is blank. BSR is the blast score ratio: the hit's
bitscore divided by the ORF's bitscore against itself. It runs from 0 to 1.

### Protein lanes, MetaPathways

| Lane | What it is | `_id` | `_desc` | `_conf` |
|---|---|---|---|---|
| `swissprot`, `uniref50`, `uniref90`, `cazy`, `eggnog`, `metacyc` | FAST search against that protein database | Matched protein | Its product | BSR, higher is better |
| `mp_product` | MetaPathways' chosen product for the ORF | Matched protein | Its product | None |
| `mp_lca` | Lowest common ancestor of each database's hits, NCBI taxonomy | Taxon | The database. Rank 1 is the one the product came from. | None |

### Protein lanes, port

| Lane | What it is | `_id` | `_desc` | `_conf` |
|---|---|---|---|---|
| `diamond_uniref50` | DIAMOND against UniRef50 | UniRef50 cluster | Cluster name | BSR, higher is better |
| `kofam` | KofamScan. Only hits above the KO's own threshold. | KO | KO definition | HMM bitscore, higher is better |
| `clean` | CLEAN enzyme prediction | EC number | Empty | 0 to 1, higher is better |
| `deepec` | DeepEC enzyme prediction | EC number | Empty | None |
| `ezpred` | EZpred enzyme prediction at every EC level. Only scores of 0.1 or more. | EC number, at any level | The EZpred head that made the call | Probability, higher is better |
| `proteinbert` | Reaction transfer from similar reference proteins, by the ProteinBERT language model | MetaNetX reaction | The reference protein | Vote share 0 to 1, higher is better |
| `esm_c` | As `proteinbert`, by the ESM-C language model, inside `gpr7`. Only ORFs with a reference protein at cosine 0.985 or more. | MetaNetX reaction | The reference protein | Vote share 0 to 1, higher is better |
| `esmc_transfer` | ESM-C second opinion, outside the `gpr` consensus (see below) | MetaNetX reaction | Its EC numbers | Cosine to the nearest reference protein, higher is better |
| `gpr` | Reaction calls merged from kofam, CLEAN, UniRef50 and ProteinBERT | MetaNetX reaction | The methods that agree | Number of methods that agree, of 4 |
| `gpr7` | As `gpr`, plus DeepEC, EZpred and ESM-C | MetaNetX reaction | The methods that agree | Number of methods that agree, of 7 |
| `deeptfactor` | DeepTFactor. Only predicted transcription factors. | `TF` | Empty | Probability, higher is better |
| `amrfinderplus` | AMRFinderPlus antimicrobial resistance genes | Gene symbol | Gene name and class | % identity to reference, higher is better |
| `bacmet`, `megares`, `vfdb`, `tcdb` | DIAMOND against metal and biocide resistance (BacMet2), antimicrobial resistance (MEGARes), virulence factors (VFDB) and transporters (TCDB) | Matched protein | Its title | Bitscore, higher is better |
| `pathofact` | PathoFact antimicrobial resistance prediction | Resistance gene | Resistance category | None |
| `eggnog_mapper` | eggNOG-mapper best ortholog | Seed ortholog | Gene name and description | Bitscore, higher is better |
| `interproscan` | InterProScan domains and families | Signature | Its description and InterPro entry | E-value, lower is better |

### ESM-C second opinion

`esmc_transfer` gives an ORF the reactions of the reference proteins nearest to it in ESM-C
embedding space. The references are 222,019 Swiss-Prot 2026_02 proteins. The lane takes the 30
nearest and keeps those at cosine 0.97 or more. The `gpr` consensus does not use this lane.

ESM-C cosines crowd near 1, so the cut-off decides how far to trust a call. The table gives the
precision of the top hit against KofamScan and UniRef50, measured on `hybridspades_cpe3_r2a_cu7_8`.
Filter `esmc_transfer_conf` at the cut-off you need.

| Top-hit cosine at least | Precision | ORFs with a call |
|---|---|---|
| 0.95 | 0.44 | 70% (below the floor, so not in the tables) |
| 0.97 | 0.55 | 31% |
| 0.975 | 0.68 | 19% |
| 0.98 | 0.85 | 10.7% |
| 0.985 | 0.91 | 6.7% |
| 0.99 | 0.94 | 4.1% |

In the long table, `extras` names the reference protein (`landmark`), its rank among the 30
nearest (`neighbour`) and how many of the 30 carry the reaction (`landmarks`).

### Pathway lanes

| Lane | What it is | `_id` | `_desc` | `_conf` |
|---|---|---|---|---|
| `mp_pathologic`, `port_pathologic` | Pathway Tools PathoLogic on the whole community | MetaCyc reactions | MetaCyc pathways | None |
| `mp_pathologic_mag`, `port_pathologic_mag` | PathoLogic on the bin that holds the ORF | MetaCyc reactions | MetaCyc pathways | None |

### RNA and promoter lanes

| Lane | What it is | `_id` | `_desc` | `_conf` |
|---|---|---|---|---|
| `silva` | MetaPathways rRNA against SILVA SSU and LSU. Only rRNAs that pass MetaPathways' filters. | Deepest taxon | SILVA lineage | % identity, higher is better |
| `mp_trna` | MetaPathways tRNAscan-SE | tRNA type | Anticodon | Infernal score, higher is better |
| `rrna_gtdb` | Port rRNA against GTDB SSU | Deepest taxon | GTDB lineage | % identity, higher is better |
| `aragorn` | Port ARAGORN tRNA calls | tRNA product | Anticodon | None |
| `bakta_ncrna` | Bakta non-coding features | Feature name | Product and database links | None |
| `promotech` | PromoTech promoters | `promoter` | Empty | Probability, higher is better |

### Element lanes

| Lane | What it is | `_id` | `_desc` | `_conf` |
|---|---|---|---|---|
| `genomad` | geNomad viruses, proviruses and plasmids | Sequence name | Virus taxonomy, or a plasmid's conjugation and resistance genes | Virus or plasmid score 0 to 1, higher is better |
| `virsorter2` | VirSorter2 viral regions | Sequence name | Best viral group | Maximum score 0 to 1, higher is better |
| `integronfinder` | IntegronFinder integrons | Replicon and integron | Integron type and its parts | Best e-value, lower is better |
| `mobileelementfinder` | MobileElementFinder insertion sequences and transposons | Element name | Element type | Identity, fraction 0 to 1, higher is better |
| `antismash` | antiSMASH biosynthetic gene clusters | `<contig>.region<NNN>` | Cluster products | None |

### Contig taxonomy lanes

| Lane | What it is | `_id` | `_desc` | `_conf` |
|---|---|---|---|---|
| `contig_lcastar` | LCA* vote over UniRef50, Metabuli and rRNA, on GTDB | Deepest taxon | GTDB lineage | p-value, lower is better |
| `metabuli` | Metabuli contig classification, on GTDB | Deepest taxon | GTDB lineage | Score 0 to 1, higher is better |

## Pathway Tools tables

`pgdb_port/pgdb_genes.*` and `pgdb_mp/pgdb_genes.*` hold one table per pipeline for the
assembly. Each table holds the community PGDB and every bin PGDB. The `entity` column is
`community` or the bin name. There is one row per ORF, reaction and pathway. An ORF with no
reaction, or a reaction in no pathway, keeps one row with those cells empty.

The columns are `orf_id`, `product`, `reaction`, `reaction_name`, `ec`, `enzrxn_name` (the
enzyme), `pathway`, `pathway_name` and `represented_by`. The port's PathoLogic input carries EC
numbers only, so its `product` is empty. Read `enzrxn_name` instead.

MetaPathways builds its community PGDB from one representative ORF per distinct product name.
Its `ptools/orf_map.txt` lists the ORFs each representative stands for. The table copies each
representative's rows onto every ORF it stands for, and `represented_by` names the
representative. A blank `represented_by` means the PGDB holds the ORF itself. The bin PGDBs and
the port PGDBs are not collapsed this way.

**CAUTION** In the port PGDBs of `hybridspades_cpe3_r2a_cu9_11`, Pathway Tools cut the 3,113 ORF
names longer than 52 characters to their last 52 characters, in each gene's accession and
synonyms. `orf_id` keeps the full name from the gene's common name, but the cut names are what
you see if you open those PGDBs in Pathway Tools.

`pgdb_manifest.tsv` gives one row per PGDB, with its gene, reaction and pathway counts. Its
`status` is `ok`, `EMPTY` (the PGDB holds no genes) or `MISSING` (a bin has no PGDB).

## Coordinate comparison

`coord_status` in the wide table compares the two pipelines' calls of each CDS, rRNA and tRNA.

| Status | Meaning |
|---|---|
| `match` | Both pipelines called the same coordinates. One row holds both. |
| `same_stop_diff_start` | CDS only. The other pipeline called the same stop codon with a different start. |
| `overlap` | The other pipeline called an overlapping feature on the same strand |
| `unique` | Only this pipeline called a feature here |
| `not_compared` | Only one pipeline calls this feature type, such as promoters |

`coord_partner` gives the `feature_id` of the other pipeline's call. `cds_coordinate_mismatches.tsv` lists every
CDS that is not a `match`, with both pipelines' ORF names and the start and end differences in
bases.

## Lane status

`lane_presence.tsv` gives each lane one status.

| Status | Meaning |
|---|---|
| `ok` | The lane ran and found hits |
| `partial` | A bin lane is missing some bins' PGDBs. `detail` names them. |
| `EMPTY` | The tool's output exists but holds no hits. An example is IntegronFinder finding no integron. |
| `not_applicable` | The assembly has no bins |
| `MISSING` | The tool's output is absent. Its columns are absent from the tables. |
| `UNMAPPED` | The tool's hits matched no feature |

## Raw outputs on fir

The tables read these directories and do not change them. `<sample>` is the assembly name, such
as `hybridspades_cpe3_r2a_cu6`.

- Assemblies: `/scratch/phyberos/beth_mp/inputs/<sample>.fasta`
- Bin assignments: `/scratch/phyberos/beth_mp/mp4/mag_maps/<sample>.tsv`
- MetaPathways: `/scratch/phyberos/beth_mp/mp4/runs/<sample>/<sample>/`. The cu6 pilot is at
  `/scratch/phyberos/beth_mp/mp4/out/hybridspades_cpe3_r2a_cu6/`. Look in
  `results/annotation_table/`, `blast_results/`, `results/rRNA/`, `results/tRNA/`,
  `results/pgdb/` and `ptools/`.
- Port: `/scratch/phyberos/beth_mp/port/metasmith/runs/<run key>/results/`, one subfolder per
  output type. Each assembly has seven runs, one per tool group.
  `/scratch/phyberos/beth_mp/jobs/tables/run_keys.tsv` lists each assembly's run keys.
- Scripts: `/scratch/phyberos/beth_mp/jobs/tables/`. `tables.sbatch` rebuilds one assembly's
  folder.

### ESM-C embeddings

The raw ESM-C embeddings are kept as raw data. Each assembly's folder is
`/scratch/phyberos/beth_mp/port/metasmith/task_cache/1e/<folder>/out/` and holds three files:

- `*-UnN4blDx.parquet`: one row per ORF, the mean over its residues of the final ESM-C layer, in
  columns `dim_0` to `dim_1151`.
- `*-GbXmuFYU.csv`: the ORF name of each row, in the same order.
- `*-x1S1kK8l.npy`: the means of layers 34, 35 and 36, which EZpred reads.

| Assembly | Folder |
|---|---|
| `hybridspades_cpe3_r2a` | `20b7275ec52b8bb1b95972fc426965d892e581748c17d27e6b25d9d5543354e7d8` |
| `hybridspades_cpe3_r2a_cu1_5` | `20a41eba81430368f83144c3e6e8573724f71f73f785332eb729ddefda95a78c45` |
| `hybridspades_cpe3_r2a_cu6` | `20c014cc1cb2c03fc942a680193480cb392a3831a2e4aeba7fa180dddc2b7276c2` |
| `hybridspades_cpe3_r2a_cu7_8` | `20b5933b71e08a5a2f9778ac96d5760d8a8ba1be7ca1b0ad6060303e1c2bd684d0` |
| `hybridspades_cpe3_r2a_cu9_11` | `20db8d589ea35ae3b78cae54dff95d651d0f87b48e80ea5a1119b5c78d42acbf74` |
| `operams_cpe3_r2a` | `20cb9cfdf64bffb64096d840ef754520ea2b7f390db879cb2263e05230f68fbbd9` |
| `operams_cpe3_r2a_cu1_5` | `20ce6a70feb0f58598bd7a6996b748bec4177495df0fff34d9952610e26746dd8a` |
| `operams_cpe3_r2a_cu6` | `2069c600453a0d9082e0e8ccd5cf0f6dd50ab2477b5d9f13ffb2a6b7d239e87e32` |
| `operams_cpe3_r2a_cu7_8` | `209f952d1e911a413f5ee532f260510c43d85297746c162689b6651fce41efd230` |
| `operams_cpe3_r2a_cu9_11` | `207b96c444b67857829086be48971c74d55e6fb57b83ebc59e87f48e02f86f183f` |
| `shortspades_cpe3_r2a` | `20d54284386b8cadf53a43b029daba74031ba8df5e7d902459cc86b17cc4056005` |
| `shortspades_cpe3_r2a_cu1_5` | `203be897aa77f5ea123f73042c1e6f7c4f7f11b2236cae66f46573babbde1ae105` |
| `shortspades_cpe3_r2a_cu6` | `20d2083417f99b11cfe8aac40d1a9e28aa6a9f54f42902a07fdcfa23783d142494` |
| `shortspades_cpe3_r2a_cu7_8` | `2060b7e4b1412761988346b998ea640c9092a7675076a58d0a73cd683367513b9e` |
| `shortspades_cpe3_r2a_cu9_11` | `20b9f5f24add10339123ac5f620d801b3c52c499aa11cf4b563abdfa3bedd303ee` |

The two parent assemblies get their folders when their runs finish.
