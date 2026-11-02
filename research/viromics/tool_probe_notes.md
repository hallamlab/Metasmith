# Tool probe journal — the missing viromics transforms

Session of 2026-09-02. Purpose: establish, by running the tools, what each
missing step of Antonio's pipeline actually **requires as input** and **emits as
output**, so the transform schemas in the plan are grounded rather than guessed.

Method: biocontainers images pulled to a local docker cache, run against a
synthetic 12-record pooled contig FASTA (`pooled.fna`: three phage genomes —
lambda NC_001416.1, phage P22 NC_001895.1, T4 NC_000866.4 — replicated across
two fake samples x two fake assemblers, headers in the pipeline's
`SAMPLE|assembler|id` form). Scratch at `/home/tony/scratch/viromics_probe/`.
Nothing here is a result about the data; it is a result about file shapes.

Reference for the contract: `pipeline_steps.yml` (this directory) and
`research/metasmith_libraries/viromics/reference/` (Antonio's 40-row xlsx and
his `01_`..`15_` shell scripts).

---

## What the scripts pin down before any tool is run

Read off `05_combined_contigs.sh` … `15_filter_votu_10kb.sh` verbatim. These are
facts about the *pipeline*, not about the tools.

| step | command | in | out |
|---|---|---|---|
| 5 | `seqkit replace -p '^(.+)' -r 'SAMPLE\|asm\|$1'` then `cat` >> one file; `seqkit seq -m 1000` | every sample's `contigs.fasta` + `final.contigs.fa` | `all_assemblies_combined.min1kb.fasta` |
| 6 | `mmseqs easy-cluster … --min-seq-id 0.95 -c 0.80 --cov-mode 0`; then `seqkit replace -r 'vPre_$1'` | pooled ≥1 kb FASTA | `viral_precluster_rep_seq.renamed.fasta` |
| 7 | `genomad end-to-end --cleanup --splits 8` | precluster reps | `*_virus.fna`, `*_virus_summary.tsv`, `*_virus_genes.tsv` |
| 8 | inline python (pandas+Biopython) → `seqkit grep -f` | `*_virus_genes.tsv` + `*_virus.fna` | `kept_contig_ids.txt`, `viral_contigs.filtered_before_checkv.fna` |
| 9 | `checkv end_to_end -d checkv-db-v1.5` | filtered viral FASTA | `viruses.fna` (+ quality/completeness/contamination tsv) |
| 10 | `mmseqs createdb` → `cluster --min-seq-id 0.95 -c 0.80 --cov-mode 1` → `createtsv` → `createsubdb` → `convert2fasta` | `checkv/viruses.fna` | `vOTU_clusters.tsv`, `vOTU_representatives.fna` |
| 11 | `seqkit fx2tab -n -l` \| awk bucket counts | vOTU reps | counts ≥1/≥5/≥10 kb (stdout) |
| 12 | inline python regex over geNomad gene annotations | vOTU reps + step-7 `*_virus_genes.tsv` | `vOTUs.filter3.no_large_mobile.fasta`, `removed_large_mobile_vOTUs.txt`, `filter3_summary.txt` |
| 13 | same awk as 11, before/after | both FASTAs | `final_vOTU_summary.txt` |
| 14 | `checkv end_to_end` again | curated vOTUs | `quality_summary.tsv` etc. |
| 15 | `VIBRANT_run.py -f nucl -no_plot -d DB -m FILES` | curated vOTUs | `VIBRANT_phages_*/*.phages_lytic.fna`, `*.phages_lysogenic.ffn` |
| (15b) | `seqkit seq -m 10000` | curated vOTUs | `…min10kb.fasta` — the DRAM-v / geNomad-taxonomy / iPHoP input |
| (14b) | python merge of CheckV `quality_summary.tsv` + VIBRANT lifestyle, ≥5 kb | both | `Supplementary_Data_2_virus_overview_vOTU_ge5kb.tsv/.xlsx` |

Two consequences that are schema decisions, not tool facts:

1. **Step 12 reaches back to step 7's gene table**, across the CheckV trim and
   the vOTU clustering. The gene table is keyed on *precluster* contig ids and
   the vOTU reps are named for whichever member won clustering — the script
   relies on both being the same id string. Any transform pair must carry that
   lineage explicitly.
2. **CheckV is run twice** on different inputs (steps 9 and 14) and only the
   FASTA is used from the first, only the tables from the second. One transform
   with all products serves both, but the two targets must be distinguishable
   by lineage or they collapse.

---

## Probes

### P0. The cross-sample pooling shape already has a precedent in the library

Not a tool probe, but it settles the structural blocker that `pipeline_steps.yml`
records as "a different sample_type, not a missing tool", so it belongs first.

`transforms/pangenome/ppanggolin.py` collects **every sample** of a run into one
object, and does it with declared lineage rather than a special sample type:

- `data_types/pangenome.yml` declares a contentless root type, `pangenome`
  (`properties: {logistics: to group genomes into a pangenome}`).
- `pangenome_heatmap_from_assembly.py` mints it once with
  `lib.AddValue("pangenome.json", {...}, "pangenome::pangenome")`, hangs every
  per-sample input off it (`parents={pan}`), keeps `sample_type` at the
  per-sample type, and lists `pangenome.json` in `shared_input_paths` so the
  sample mask does not hide it.
- `ppanggolin.py` then takes `pan` as a requirement, its per-sample inputs with
  `parents={pan}`, and declares `group_by=pan` — so one task arrives holding
  every sample's file, and `context.InputGroup(gbk)` is the pool.
- `context.SourceOf(item, name)` recovers which sample each file came from,
  which is exactly what step 5's `SAMPLE|assembler|id` rename needs.

`metagenomics/instrain_compare.py` is the second instance of the same shape,
grouping on `binning::derep_mag_ref`.

**So steps 5-25 do not need a new sample_type.** They need one new root type —
call it a vOTU study — minted in the template, with the sample's read pair as
its child, and one collecting transform (step 5) whose `group_by` is that root.
Everything downstream of step 5 is then ordinary per-object work whose lineage
root happens to be the study rather than a sample.

### P1. MMseqs2 — steps 6 and 10

Image `quay.io/biocontainers/mmseqs2:17.b804f--hd6d6fdc_1` (1.3 GB). Ran both of
Antonio's invocations against `pooled.fna` (12 records, 1.0 Mbp), seconds each.

**Step 6, `easy-cluster`** — one command, three files beside a chosen prefix:

    <prefix>_rep_seq.fasta     representatives, one record per cluster
    <prefix>_cluster.tsv       2 cols, no header: <representative_id>\t<member_id>
                               (the representative appears as its own member)
    <prefix>_all_seqs.fasta    every sequence, cluster-blocked, headers repeated

**Step 10, the low-level path** (`createdb`→`cluster`→`createtsv`→`createsubdb`
→`convert2fasta`) produced the SAME two useful artifacts —
`vOTU_clusters.tsv` (identical 2-col shape) and `vOTU_representatives.fna` —
plus **19 mmseqs database sidecar files** (`viral_db*`, `viral_clusters.0..3`,
`vOTU_representatives{,.dbtype,.index,.lookup,.source,_h,_h.dbtype,_h.index}`).
None of the sidecars are wanted; a transform copies the two and drops the rest.

So **one transform shape serves both steps**: FASTA in, (rep FASTA, cluster TSV)
out. They differ only in `--cov-mode` (0 vs 1), which is a parameter, not a
shape. `easy-cluster` reaches the same place in one call and is what step 6
already uses; using it for step 10 as well would collapse the two.

Two facts that constrain the schema:

- **Headers survive verbatim, pipes included.** `VSG1W5|metaSPAdes|NC_001416.1_0`
  round-trips through both paths, so the sample provenance minted at step 5 is
  still readable at step 12. `easy-cluster`'s `_rep_seq.fasta` appends a
  **trailing space** to each header; the low-level `convert2fasta` does not.
  Anything joining on the id must strip.
- Clustering is genuinely cross-sample: all four copies of lambda (2 samples x 2
  assemblers) landed in one cluster with a single representative. That is the
  vOTU definition and it is why the pooling cannot be moved after this step.

### P2. SeqKit — steps 5, 11, 13, 26 and the ≥10 kb cut

Image `docker.io/staphb/seqkit:2.13.0` (already the library's `seqkit.env`).
Every step the pipeline calls "SeqKit" is one of five subcommands:

| use | command | out |
|---|---|---|
| rename with sample+assembler (5) | `seqkit replace -p '^(.+)' -r 'SAMPLE\|asm\|$1'` | FASTA on stdout |
| length filter (5, 15b, 26) | `seqkit seq -m <N>` | FASTA on stdout |
| id subset (8) | `seqkit grep -f ids.txt` | FASTA on stdout |
| per-record lengths (11, 13) | `seqkit fx2tab -n -l` | 2-col TSV, no header: `id\tlength` |
| assembly stats (5) | `seqkit stats -a --tabular` | 1-row TSV, 19 cols with header (file format type num_seqs sum_len min_len avg_len max_len Q1 Q2 Q3 sum_gap N50 N50_num Q20(%) Q30(%) AvgQual GC(%) sum_n) |

The size-bucket counts of steps 11/13 are `fx2tab | awk`, i.e. a derived table,
not a tool output. `seqkit seq -m` on a FASTA warns
`you may switch on flag -g/--remove-gaps` and still works.

### P3. geNomad — what the existing transform is throwing away (steps 7, 8, 12, 19)

Image `antoniopcamargo/genomad:1.11.0`, the library's own pin. Read off
`genomad/_paths.py`, so this is the tool's own statement of where things land.
Note the binary is at `/opt/conda/bin/genomad` and the image entrypoint is
`_entrypoint.sh` — `docker run … bash` fails without one or the other.

`<out>/<prefix>_summary/` holds nine files; the four this pipeline needs are

    <prefix>_virus.fna          the viral contigs        (step 8 reads it)
    <prefix>_virus_genes.tsv    per-gene annotations     (steps 8 and 12 read it)
    <prefix>_virus_summary.tsv  ALREADY A PRODUCT
    <prefix>_plasmid_summary.tsv ALREADY A PRODUCT

and the siblings not needed here are `_virus_proteins.faa`, `_plasmid.fna`,
`_plasmid_genes.tsv`, `_plasmid_proteins.faa`, `_summary.json`.

`<prefix>_taxonomy.tsv` is **not** in the summary directory — it is
`<out>/<prefix>_annotate/<prefix>_taxonomy.tsv` (step 19's named output).
**`--cleanup` does not delete it**: the only thing `annotate`'s cleanup removes
is the `_mmseqs2` subdirectory (`modules/annotate.py:210`). So the transform can
declare it as a product while keeping the `--cleanup` the library already passes.

### P4. CheckV — steps 9 and 14

Image `quay.io/biocontainers/checkv:1.0.3--pyhdfd78af_0` (Antonio ran v1.0.1;
the outputs are the same). `checkv end_to_end <input.fna> <outdir> -d <db> -t N`.
No `-o` flag: the output directory is positional.

Files written (from `checkv/modules/*.py`):

    viruses.fna            host-trimmed viral sequences   <- step 9's only consumer
    proviruses.fna         the excised proviral portions
    quality_summary.tsv    <- step 14's output, and half of Supplementary Data 2
    completeness.tsv
    contamination.tsv
    complete_genomes.tsv
    (intermediates: aai.tsv, diamond.tsv, gene_features.tsv, tr.fna, checkv_reps.fna
     -- removed by --remove_tmp)

`quality_summary.tsv` has exactly 14 columns, verified against the source
(`modules/quality_summary.py:168`): contig_id, contig_length, provirus,
proviral_length, gene_count, viral_genes, host_genes, checkv_quality,
miuvig_quality, completeness, completeness_method, contamination, kmer_freq,
warnings — which is the `cols` list Antonio's `14_summarize…` script selects,
so his Supplementary-Data-2 builder needs no column beyond this file plus
VIBRANT's lifestyle call.

`checkv download_database <dest>` exists as a subcommand and resolves the
version from `https://portal.nersc.gov/CheckV/CURRENT_RELEASE.txt`, so the
`downloadCheckvDB` transform is a two-line body in the `ref_genomad.py` shape.
It also honours the `CHECKVDB` environment variable when `-d` is absent.

### P5. CoverM — steps 16 and 39, and it is a COLLECTING transform

Image `quay.io/biocontainers/coverm:0.7.0--hcb7b614_4`. Ran Antonio's step-16
command for real against `pooled.fna` with 6,000 synthetic read pairs.

    coverm contig --coupled R1.fq.gz R2.fq.gz --reference votus.fna \
      --mapper minimap2-sr -m trimmed_mean covered_fraction count \
      --min-read-percent-identity 0.95 --min-read-aligned-percent 0.75 \
      -t N -o out.tsv

**One TSV, one row per contig, and three columns PER SAMPLE**, named for the
reference and the R1 file:

    Contig | <ref>/<R1> Trimmed Mean | <ref>/<R1> Covered Fraction | <ref>/<R1> Read Count

`--coupled` takes an arbitrary number of R1/R2 pairs in one invocation, and each
adds its own column block. **That is the cross-sample abundance matrix** — it is
produced by one task holding every sample's reads plus the one shared vOTU
reference, so it is `group_by=<the pooled study>` with `InputGroup(reads)`, the
`ppanggolin` shape again, not a per-sample transform that something merges later.
minimap2 2.30 and samtools 1.22.1 are bundled in the image; nothing else is needed.
`coverm genome` (step 39) is the same tool and the same output shape with
`--genome-fasta-files`/`-d` instead of `--reference`.

### P6. CCTyper — step 22

Image `quay.io/biocontainers/cctyper:1.8.0--pyhdfd78af_1`.
`cctyper <input.fna> <outdir> --prodigal meta -t N --db <path>` (or `CCTYPER_DB`).
Positional in/out, like CheckV.

Files it writes (from the package source):

    crisprs_all.tab, crisprs_near_cas.tab, crisprs_orphan.tab, crisprs_putative.tab
    crisprs.gff
    cas_operons.tab, cas_operons_orphan.tab, cas_operons_putative.tab
    genes.tab, hmmer.tab, blast.tab, type_dict.tab, probability_test.tab
    (plus a spacers/ directory of per-array FASTAs, and prodigal/hmmer logs)

The spacer FASTA step 23 blasts is the `spacers/` directory, not a single file;
the transform concatenates it into one non-redundant FASTA, which is what
Antonio's "non-redundant spacer DB" means.

### P7. vConTACT3 — step 20, and it does not need DRAM-v

Image `quay.io/biocontainers/vcontact3:3.1.4--pyhdfd78af_1`.
`vcontact3 run` takes **either** `-n <nucleotide.fna>` (it calls genes itself
with pyrodigal-gv) **or** the `-p proteins -g gene2genome -l lengths` trio
Antonio used. The `-n` route removes the dependency on DRAM-v's `genes.faa` and
on building a gene2genome map, so it is one requirement instead of three.

Output filenames present in the package source include `final_assignments.csv`
(the genome-by-genome table), `nodes.csv` / `edges.csv` (the network),
`ani_summary.tsv`, `ani_closest.tsv`, `votu2ref.tsv`, `species.tsv`,
`completeness.csv`, `performance_metrics.csv`, `db_versions.tsv`. The
`genome_by_genome_overview.csv` / `viral_cluster_overview.csv` names in
Antonio's table are vConTACT **2** names; v3 renamed them, so the xlsx cannot be
transcribed literally into the products.

### P8. MetaPop — step 18 is blocked on mapping, not on packaging

**Corrected.** An earlier reading of this probe called MetaPop blocked because
`quay.io/biocontainers/metapop:1.0.2--hdfd78af_1` carries no `metapop` console
script and no python at all. It carries no python because it does not need any:
`/usr/local/bin/MetaPop.R` is the whole driver, a standalone Rscript that takes
everything on argv. Its own `-help` states the contract — mandatory `-dir` (a
directory of BAMs), `-assem` (the contigs), `-ct` (the counts normalization
file); optional `-genes` (a prodigal FASTA, which the library already produces,
so the missing prodigal binary is not a blocker either); `-cov 70` and `-dep 10`
as the per-contig breadth and depth filters. samtools and bcftools are on PATH.
The `--input_samples/--reference/--norm` spelling in the pip documentation is the
python wrapper's, not this driver's.

What actually blocks it is the mapping. MetaPop takes ONE `-assem` and a
directory of BAMs that must all be against it — that is what makes its FST and
macrodiversity cross-sample — and the library's BAMs are per sample against that
sample's own assembly, so they cannot be pooled. Either a cross-mapping transform
is added (every sample's clean reads to the frozen set), or MetaPop runs per
sample and keeps only within-sample microdiversity. That is a topology decision,
not a packaging one.

### P9. VIBRANT — step 15

Image `quay.io/biocontainers/vibrant:1.2.1--hdfd78af_4` — Antonio's exact
version. `VIBRANT_run.py -i <fna> -f nucl -folder <out> -t N -no_plot -d <DB> -m <FILES>`.
Note `-d` and `-m` are **two separate paths**, the HMM `databases/` and the
`files/` directory; the transform's reference product must carry both, so it is
one directory type with two children, not two reference types.

`<out>/VIBRANT_<stem>/` contains six subdirectories — `VIBRANT_phages_<stem>`,
`VIBRANT_results_<stem>`, `VIBRANT_HMM_tables_parsed_<stem>`,
`VIBRANT_HMM_tables_unformatted_<stem>`, `VIBRANT_figures_<stem>`,
`VIBRANT_log_<stem>`. The two that matter:

`VIBRANT_phages_<stem>/` — nine files, `<stem>.phages_{combined,lytic,lysogenic,circular}.{fna,ffn,faa}`.
Antonio's lifestyle table is built from `phages_lytic.fna` (virulent) and
`phages_lysogenic.ffn` (temperate, after stripping a `_fragment…` suffix and
deduplicating). **His script reads the `.ffn` for the lysogenic half and the
`.fna` for the lytic half** — the `.fna` for lysogenic also exists, so this is
his choice, not a constraint; a transform should emit the pair consistently and
derive the table itself.

`VIBRANT_results_<stem>/` — `VIBRANT_genome_quality_<stem>.tsv`,
`VIBRANT_integrated_prophage_coordinates_<stem>.tsv`,
`VIBRANT_AMG_individuals_<stem>.tsv`, `VIBRANT_AMG_counts_<stem>.tsv`,
`VIBRANT_AMG_pathways_<stem>.tsv`, `VIBRANT_annotations_<stem>.tsv`,
`VIBRANT_machine_<stem>.tsv`, `VIBRANT_complete_circular_<stem>.tsv`. The
prophage-coordinates file is the second half of the xlsx's step-15 output line.

Every filename is stem-interpolated, so the transform must derive the stem from
its input path exactly as `genomad.py` already does.

### P10. iPHoP — step 21

Image `quay.io/biocontainers/iphop:1.3.3--pyhdfd78af_0` (4 GB).
`iphop predict --fa_file <votus.fna> --out_dir <dir> --db_dir <db> -t N -m 90`.
`-m/--min_score` is the confidence cut (75-100, default 90) — Antonio's ">=90"
is the default, so it needs no special handling.

Two output files, both named for the score threshold:

    Host_prediction_to_genus_m<score>.csv    <- the one Antonio keeps
    Host_prediction_to_genome_m<score>.csv
    Detailed_output_by_tool.csv              (per-tool blast/crispr/php/wish/rafah evidence)

Because the filename carries the threshold, a transform that hardcodes `-m 90`
can name its product deterministically; one that exposes the threshold cannot,
and must glob.

`iphop add_to_db --fna_dir <MAG fastas> --gtdb_dir <GTDB-Tk output dir> --db_dir
<base db> --out_dir <new db>` is a **second transform**, not a flag: it consumes
the run's own MAGs and the GTDB-Tk output directory (which the library's
`gtdbtk.py` already produces as `taxonomy::gtdbtk_raw`) and emits a new database
directory. Antonio's "+174 MAGs via GTDB-Tk de novo decorated tree" is exactly
this call, so the shape is already available in the library.

### P11. dRep and CheckM2 — steps 36 and 37, if they are in scope

`quay.io/biocontainers/drep:3.6.2--pyhdfd78af_0`:
`dRep dereplicate <work_dir> -g <genomes...> -pa 0.9 -sa 0.95 -comp 50 -con 10`.
The work directory is positional and holds everything; `--genomeInfo` accepts a
precomputed completeness/contamination CSV, which is how a CheckM2 result is fed
in without dRep re-running CheckM itself. Note `--S_algorithm skani` is
available in 3.6 — the library's `skani_dedup.py` substitute is the same
algorithm reached without dRep.

`quay.io/biocontainers/checkm2:1.1.0--pyh7e72e81_1`:
`checkm2 predict -i <folder of bins> -o <outdir> -x fna --threads N --database_path <db>`.
Writes `quality_report.tsv`. Antonio's `QS = completeness - 5x contamination`,
drop QS<50 cut is arithmetic over two of its columns, not a flag.

### P12. Blocked, and not on shape

Three of the four entries here have since been answered; they are kept with
their answers because the reasoning that dissolved them is the design.

- **MetaPop (18)** — still open, and see the corrected P8: the blocker is the
  mapping (one `-assem`, a directory of BAMs all against it), not the image.
- **KEGG Mapper (25)** — answered. It is a web application with nothing to
  install, and the substitute is the library's existing
  `chunkOrfsForAnnotation -> kofamscan -> merge_kofamscan` chain, reached by
  calling genes on the frozen viral set (`viromics/prodigal_gv.py`). The KO ->
  pathway roll-up this entry wanted as a `lib::` script is not built either: it
  is a join over the emitted tables, done after the DAG.
- **DRAM-v on vOTUs (24)** — answered, and the answer was a third option this
  entry did not consider. The vOTU FASTA does not become a `contig_batch`, and
  VirSorter2 does not gain a second requirement shape; instead
  `sequences::contig_fasta` was declared as the contract both a batch and the
  frozen set satisfy, and VirSorter2, geNomad and VIBRANT were all widened to it
  in one line each. `dramv.py`'s first requirement moved from
  `sequences::assembly` to that same contract, so the per-sample and frozen-set
  VirSorter2 runs cannot have their outputs paired across.
- **BinSanity (32) and ABAWACA+ESOM (33)** are two more binners on the MAG lane,
  where the library already runs three. Nothing was probed; they are inventory.

---

## Images pulled, and what they cost

All in the local docker cache after this session (`docker image ls`):

    quay.io/biocontainers/mmseqs2:17.b804f--hd6d6fdc_1
    quay.io/biocontainers/checkv:1.0.3--pyhdfd78af_0
    quay.io/biocontainers/coverm:0.7.0--hcb7b614_4
    quay.io/biocontainers/vibrant:1.2.1--hdfd78af_4
    quay.io/biocontainers/cctyper:1.8.0--pyhdfd78af_1
    quay.io/biocontainers/vcontact3:3.1.4--pyhdfd78af_1
    quay.io/biocontainers/iphop:1.3.3--pyhdfd78af_0
    quay.io/biocontainers/metapop:1.0.2--hdfd78af_1   (usable; see corrected P8)
    quay.io/biocontainers/checkm2:1.1.0--pyh7e72e81_1
    quay.io/biocontainers/drep:3.6.2--pyhdfd78af_0
    docker.io/staphb/seqkit:2.13.0
    antoniopcamargo/genomad:1.11.0                    (the library's own pin)

Every one of these needs a `resources/env/<tool>.env` with the digest pinned and
a comment naming the tag and what was verified in it, per the library's
authoring rules. The digests are not recorded here because a pin belongs beside
the env file, not in a journal.

The transform table this session was for is in the plan, not here.
