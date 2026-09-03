# Filling in the viromics mocks

Every transform under `src/metasmith_libraries/transforms/viromics/` declares its
requirements, its products and its grouping for real, and its protocol touches
its outputs and runs no tool. This file is what the next session needs to replace
each protocol body: the command, the files to copy out, the container, and the
filename trap already known about it.

**What the mocks prove and what they do not.** A solved plan proves the types
line up and the planner reaches every target. It proves nothing about whether
CheckV likes the input or whether VIBRANT wrote the file we expect. Treat every
output path below as a claim to check against a real run, not as a fact.

**And one check could not be run at all.** The intended proof that a mock touches
every product it declares is a direct run of it, but `msm run` (and the
`RunTransform` API under it) raises before reaching the protocol on every one of
these transforms:

    KeyError: lin entry carries no KEY: the orchestrator did not route this member

`bootstrap._get_output_paths` names an output through `payload.output_file_name`,
which requires a lineage entry the orchestrator stamps with a member key;
`direct_run._build_lineage` does not supply one. `output_signature` does not help
— it is not consulted on that path. The failure is also self-masking: the
`except` arm in `bootstrap.ExecuteStep` calls `context.Output(d)` to report which
outputs are missing, which raises the same KeyError and swallows the original
error, so the log says only `failed with error`. Fix that before trusting any
direct-run result here, and note that a suite which passes because its mocks
never call `context.Output()` proves nothing either.

Two things are deliberately absent from every mock and have to be added with the
protocol, not before it:

- **`env::` requirements.** No mock declares one, because `lib.GetType` fails at
  build time on a type that does not exist and six `.env` files ahead of a
  settled shape is the wrong order of work. Each entry below names the container
  the env file should carry.
- **`ref::` requirements and their download transforms.** Five of these tools
  need a database. They are `labels=["local"]` one-time costs that gate first
  use, and iPHoP's is ~4 GB compressed and much larger unpacked, so start them
  early rather than last.

Adding either changes the DAG's step count. The count this plan produced is a
floor.

---

## The three callers

### `viromics/vibrant.py` — step 15

    quay.io/biocontainers/vibrant:1.2.1--hdfd78af_4      (Antonio's exact version)

    VIBRANT_run.py -i <contigs.fna> -f nucl -folder <out> -t N -no_plot \
        -d <DB/databases> -m <DB/files>

`-d` and `-m` are two separate paths — the HMM `databases/` directory and the
`files/` directory — so the reference is ONE directory type with two children,
not two reference types.

It runs **once per sample**, on `sequences::contig_batch`, in both its roles: it
is a caller (it emits `vibrant_candidate_virus`) and it is the annotator step 15
wanted. See *Where each tool runs* below for why it is not re-run on the frozen
set.

Everything under `<out>/VIBRANT_<stem>/` is stem-interpolated from the input
filename, exactly as `genomad.py` already handles. Copy out:

| product | file |
|---|---|
| `vibrant_genome_quality` | `VIBRANT_results_<stem>/VIBRANT_genome_quality_<stem>.tsv` |
| `vibrant_amgs` | `VIBRANT_results_<stem>/VIBRANT_AMG_individuals_<stem>.tsv` |
| `vibrant_lifestyle_table` | derived: lytic from `VIBRANT_phages_<stem>/<stem>.phages_lytic.fna`, lysogenic from `…phages_lysogenic.fna` |
| `vibrant_candidate_virus` | derived: contig id, interval, `vibrant`, score |

Two notes on the lifestyle table. Antonio's script reads the `.ffn` for the
lysogenic half and the `.fna` for the lytic half; the `.fna` exists for both, so
that is his choice rather than a constraint — emit the pair consistently and
derive the table here. And VIBRANT's `_fragment_N` suffix has to be stripped and
the results deduplicated, which is the same id-normalisation the candidate
adapter does.

### geNomad and VirSorter2 — already real

`metagenomics/taxonomy/genomad.py` and `functionalAnnotation/virsorter2.py` are
not mocks. Their `candidate_virus` adapters are written and read their source
tables by column name, asserting with the observed header rather than falling
back to an index. **Both column sets are the part of this work least verified
against a real run** — no geNomad or VirSorter2 image was available locally — so
the first real run is where they get confirmed:

- geNomad reads `seq_name`, `length`, `coordinates`, `virus_score` from
  `<prefix>_virus_summary.tsv`.
- VirSorter2 reads the first present of `seqname`/`seqname_new`,
  `trim_bp_start`/`full_bp_start`, `trim_bp_end`/`full_bp_end`, and
  `max_score`/`trim_pr_max`/`trim_pr`/`pr_full` from `final-viral-boundary.tsv`.

`genomad.py` also now copies `<prefix>_annotate/<prefix>_taxonomy.tsv`. That file
is NOT in the summary directory, and `--cleanup` does not remove it — cleanup
touches only the `_mmseqs2` subdirectory (`genomad/modules/annotate.py:210`).

### The id conventions, which are the actual work

The three callers agree only that a call is an interval on a contig:

| caller | how it names a call |
|---|---|
| geNomad | `<contig>\|provirus_<start>_<end>`, coordinates repeated in a column |
| VirSorter2 | `<contig>\|\|full` or `<contig>\|\|partial` |
| VIBRANT | `<contig>_fragment_N` |
| (CheckV, later) | `<contig>_1` for a provirus fragment |

Reducing all four to one contig id plus a numeric interval is the substance of
the real adapters. One decision is deliberately left open: whether the interval
is 1-based inclusive (geNomad's `coordinates`) or 0-based half-open. Pick one,
state it in `viromics.yml`'s `candidate_virus` description, and convert on the
way in.

The CheckV suffix matters beyond CheckV: Antonio's step-12 filter derives a
contig id by stripping a trailing `_[0-9]+` from a gene id, which collides with
`_1`.

---

## The merge, and the only FASTA written after assembly

### `viromics/merge_candidate_calls.py`

    docker.io/staphb/seqkit:2.13.0                        (already env::seqkit.env)

No single command. The body is: read every `candidate_virus` table in the group;
deduplicate identical calls; union overlapping calls on the same contig from
earliest start to latest end; `seqkit subseq` the merged intervals out of the
contig batch each call came from; prefix headers with the sample label; write one
pooled FASTA plus the provenance table.

The mock already exercises the two lineage lookups the real body needs, and they
are the part to get right:

    sample  = context.SourceOf(call_table, pair)     # the sample label
    contigs = context.SourceOf(call_table, batch)    # what the coordinates are on

**Never pair grouped slots by index.** Two slots of one group arrive in arbitrary
order; `SourceOf` is the only thing that relates them, and pairing positionally
is this library's documented quiet failure.

One thing that is already done for you: `logistics/splitContigsForAmr.py` writes
batch headers as `<assembly stem>~<contig id>`, so the calls arrive already
namespaced by sample. Decide whether the merge re-prefixes with the `read_pair`
label or keeps that one; do not do both.

`candidate_call_provenance` is the join key for everything the DAG does not do:
one row per frozen contig giving source sample, source contig, merged interval
and which callers claimed it. vOTU abundance is
`sequences::assembly_per_contig_coverage` joined through this table and
`votu_cluster_table`, and without this table that join is impossible.

---

## The descriptive tables

### `viromics/contig_length_table.py` — step 5

    docker.io/staphb/seqkit:2.13.0

    seqkit fx2tab -n -l -g <frozen.fna>     # id, length, GC; 2-3 col TSV, no header

Add the header on emit. `seqkit seq -m N` warns `you may switch on flag
-g/--remove-gaps` on a FASTA and still works, if a length cut is ever needed for
a cost reason — but it must not become a consumed product.

### `viromics/mmseqs_precluster.py` (step 6) and `viromics/mmseqs_votu.py` (step 10)

    quay.io/biocontainers/mmseqs2:17.b804f--hd6d6fdc_1    (1.3 GB)

    mmseqs easy-cluster <frozen.fna> <prefix> tmp \
        --min-seq-id 0.95 -c 0.80 --cov-mode 0      # precluster
    mmseqs easy-cluster <frozen.fna> <prefix> tmp \
        --min-seq-id 0.95 -c 0.80 --cov-mode 1      # vOTU

`easy-cluster` writes `<prefix>_cluster.tsv` (2 columns, no header,
`representative<TAB>member`, the representative listed as its own member),
`<prefix>_rep_seq.fasta` and `<prefix>_all_seqs.fasta`. Copy out the TSV only.

Antonio ran step 10 through the low-level path
(`createdb → cluster → createtsv → createsubdb → convert2fasta`), which produces
the same two useful artifacts plus **19 database sidecar files**. `easy-cluster`
reaches the same place in one call; use it for both.

The trap: `_rep_seq.fasta` appends a **trailing space** to every header, where
the low-level `convert2fasta` does not. Nothing here consumes that file, which
is the point — but anything that ever joins on an id from it must strip.

Headers survive verbatim through both paths, pipes included, so the sample
provenance minted at the merge is still readable downstream.

### `viromics/checkv.py` — steps 9 and 14, one run

    quay.io/biocontainers/checkv:1.0.3--pyhdfd78af_0      (Antonio ran 1.0.1)

    checkv end_to_end <frozen.fna> <outdir> -d <db> -t N

**The output directory is positional. There is no `-o`.**

Copy out `contamination.tsv`, `quality_summary.tsv`, `completeness.tsv` and
`complete_genomes.tsv` from `<outdir>`. `quality_summary.tsv` has exactly 14
columns (`modules/quality_summary.py:168`), which is the full column list
Antonio's Supplementary-Data-2 builder selects — so that table plus VIBRANT's
lifestyle call is the whole of Supplementary Data 2.

Do not add a second CheckV transform for the contamination half. `end_to_end` is
`contamination → completeness → complete_genomes → quality_summary` over one
`tmp/`, sharing one prodigal-gv and one DIAMOND pass; splitting the two rows into
two transforms repeats that pass for nothing.

`checkv download_database <dest>` is a subcommand and resolves the version from
`https://portal.nersc.gov/CheckV/CURRENT_RELEASE.txt`, so `downloadCheckvDB` is a
two-line body in the `ref_genomad.py` shape. It also honours `CHECKVDB` when `-d`
is absent.

---

## The annotation fan

### `viromics/vcontact3.py` — step 20

    quay.io/biocontainers/vcontact3:3.1.4--pyhdfd78af_1

    vcontact3 run -n <frozen.fna> -o <outdir> -d <db> -t N

Use `-n`, not the `-p proteins -g gene2genome -l lengths` trio Antonio used: the
`-n` route calls genes itself with pyrodigal-gv, which removes the dependency on
DRAM-v's `genes.faa` and on building a gene2genome map — one requirement instead
of three. `-p/-g` is also mutually exclusive with `-n` and disables ANI export,
which is why `vcontact3_ani` is a product but not a driver target.

Copy out `final_assignments.csv`, and `nodes.csv` + `edges.csv` as the network
directory. The `genome_by_genome_overview.csv` / `viral_cluster_overview.csv`
names in Antonio's xlsx are vConTACT **2** names and do not exist in 3.1.4.

Run it once, on everything. There is no path that reuses protein clusters against
a subset of genomes: `-p/-g` skips gene calling but not clustering, and the
parquet intermediates are skipped only on an exact re-run.

### `viromics/iphop_add_to_db.py` and `viromics/iphop_predict.py` — step 21

    quay.io/biocontainers/iphop:1.3.3--pyhdfd78af_0       (4 GB)

    iphop add_to_db --fna_dir <MAG fastas> --gtdb_dir <GTDB-Tk output dir> \
        --db_dir <base db> --out_dir <new db>
    iphop predict --fa_file <frozen.fna> --out_dir <dir> --db_dir <new db> -t N -m 90

`-m/--min_score` defaults to 90, which is Antonio's cut, so hardcode it — and
note that the output filenames carry the threshold
(`Host_prediction_to_genus_m90.csv`, `Host_prediction_to_genome_m90.csv`, plus
`Detailed_output_by_tool.csv`). A transform that exposes the threshold must glob;
one that hardcodes it can name its products deterministically.

**One model change is still owed here.** `add_to_db --gtdb_dir` wants GTDB-Tk's
output *directory*, and `metagenomics/taxonomy/gtdbtk.py` declares only
`taxonomy::gtdbtk`, the per-contig classification TSV. The type
`taxonomy::gtdbtk_raw` exists in `taxonomy.yml` with no producer. So either
`gtdbtk.py` gains that product and `iphop_add_to_db.py` requires it instead of
`taxonomy::gtdbtk`, or the add_to_db body reconstructs the directory from the
TSV. The first is correct and is a protocol change to a heavily used transform,
which is why it was left out of the mock pass.

The three collected slots — quality bins, cluster table, GTDB-Tk — are joined on
bin id inside the task, not by lineage. GTDB-Tk runs on each binner's bins while
the quality pool is the aggregator's output, so no single quality bin has one
GTDB-Tk ancestor to recover with `SourceOf`. GTDB-Tk's `user_genome` column names
the bin file; that is the join.

Use `binning_local::cluster_table` to add only the `is_centroid_95` bins, not
every quality bin. Note that this library dereplicates within a sample, not
across the survey, so "the survey's dereplicated MAGs" is not the same set as
Antonio's 174.

### `viromics/cctyper.py` — step 22

    quay.io/biocontainers/cctyper:1.8.0--pyhdfd78af_1

    cctyper <bin.fna> <outdir> --prodigal meta -t N --db <path>

Positional in and out, like CheckV. `CCTYPER_DB` works instead of `--db`.

Copy out `crisprs_all.tab` and `cas_operons.tab`. The spacers are **a directory**
(`spacers/`, one FASTA per array), not a file — concatenate it into one
non-redundant FASTA, tagging each header with its array and its bin. That
concatenation is what Antonio's "non-redundant spacer DB" means.

Runs per bin with `batch_size=50`; the `AsBatch` loop is already in the mock.

### `viromics/blast_spacers_to_contigs.py` — step 23

    blastn -task blastn-short -query <all_spacers.fna> -subject <frozen.fna> \
        -outfmt 6 -evalue 1e-5 ...

`blastn-short` matters: spacers are ~30-40 bp and the default task's word size
misses them. The library's only other blastn transform,
`amplicon/blast_map_asvs.py`, is ASV-shaped and is a reference for the env, not
for the parameters.

Antonio's acceptance rule (mismatches and coverage) belongs in the emitted
columns, not in a filter — emit every hit with its identity, alignment length and
mismatch count, and let the join decide.

### `viromics/prodigal_gv.py` — step 25's only new piece

    prodigal-gv -p meta -i <frozen.fna> -a <orfs.faa> -f gff -o <orfs.gff>

prodigal-gv, not the library's `pprodigal -p meta`, and this is a correctness
point rather than packaging: many phage recode TAG or TGA, and the standard
tables truncate their genes. geNomad and CheckV both call viral genes with
pyrodigal-gv for the same reason.

It produces `sequences::orfs`, so everything after it —
`logistics/chunkOrfsForAnnotation.py`, `functionalAnnotation/kofamscan.py`,
`functionalAnnotation/merge_kofamscan.py` — is the library's existing chain,
unchanged. That is why step 25 needed no annotation transform.

---

## Where each tool runs, and why it is not negotiable

One rule decides it. **Cross-sample tools run on the frozen set** — MMseqs2
defines a vOTU by clustering across samples, and CheckV, vConTACT3, iPHoP, the
spacer BLAST and prodigal-gv → KOfam all want one catalogue. **Per-contig
annotators run once per sample** — geNomad, VirSorter2, VIBRANT and DRAM-v report
per contig, and a frozen contig is an interval of a contig they already
annotated, so their tables reach a vOTU through
`viromics::candidate_call_provenance` rather than by a second run.

That is a scientific call with a mechanical enforcement, and the mechanism is the
part to not undo. Lineage constraints in this planner are **ancestral, not
immediate**. The frozen set descends from every contig batch that fed the merge,
so a constraint like `parents={batch}` on the merge's `candidate_virus` slots is
satisfied by a frozen-set caller run as well as by the per-sample one — which
makes the merge eligible to consume its own output. The planner does reject the
cycle, but it pays to discover it, and the price is not linear: measured on this
target set, with geNomad reachable on the frozen set the solve took 43 s; adding
VIBRANT there as well, it stopped finding any complete plan at all, returning
zero steps in four seconds.

So the four per-contig annotators require `sequences::contig_batch`, and the
frozen type satisfies no general contig contract — every consumer of it names
`viromics::dereplicated_candidate_virus` explicitly. An earlier pass introduced
`sequences::contig_fasta` as a shared contract both a batch and the frozen set
satisfied; that is exactly the shape that reopens the cycle, and it was removed.
If a future tool needs to run on both, give it two transform files rather than
one widened requirement.

The cost, stated plainly: an annotation is computed on the parent contig, not on
the trimmed viral interval. For Antonio's step-8 host-gene fraction that is the
better input, since the host genes are on the untrimmed contig. Elsewhere it is a
caveat to carry into the comparison, alongside the same trade already made for
abundance in rows 16 and 39.

## Two things the next session should fix first

**An orphaned metaSPAdes run.** The solved plan carries a `spades` step whose
outputs nothing consumes. It is not merely wasteful in a mock DAG — on real data
it is a whole second assembly of every sample, for nothing. It appears only when
a per-sample target outside the viral lane is named: the viral lane alone solves
in 36 steps with no spades, and adding any one of `annotation::dramv_distill`,
`taxonomy::metabuli` or `annotation::dram_annotations` — each pinned to the
megahit assembly — brings it in and takes the solve from 4 s to ~18 s. Pinning
the targets harder does not remove it, so the binding is upstream of them: some
`sequences::assembly` slot inside the chain (`prodigal`, `metabuli` and
`dramv`'s batch lineage are the candidates) is free to bind the other assembler.
`metagenomics_from_paired_reads` does not have this problem, so it is solvable —
diff the two drivers' plans rather than guessing.

**The target list is short on purpose, and must stay short.** A target is not a
request for a file; it is a slot the planner has to satisfy consistently with
every other slot. Naming all forty of Antonio's outputs made the solve take
minutes and then return no plan at all; naming the nine that nothing else pulls
in solves the same graph in a second and produces every one of those outputs
anyway. Before adding a target, check with

    python research/viromics/probe_targets.py

that it is not already reached, and re-run that probe after adding one. The same
script's `--bisect` and `--one-per-app` modes are how the current list was found.

## Still open

**MetaPop, step 18.** Not written as a mock, because the decision it waits on is
a topology decision. `/usr/local/bin/MetaPop.R` is a standalone Rscript driver
taking `-dir` (a directory of BAMs), `-assem`, `-ct` and optionally `-genes` on
argv, with samtools and bcftools on PATH — so the tool is not the problem. Every
BAM must be against the ONE `-assem`, and this library's BAMs are per sample
against that sample's own assembly. Either a cross-mapping transform is added
(every sample's clean reads to the frozen set — which is also what would make
vOTU abundance match Antonio's CoverM numbers), or MetaPop runs per sample and
keeps only within-sample microdiversity.

**A threshold sweep cannot be a runtime knob.** `context.params` is populated in
`bootstrap.py` only from the Nextflow step metadata file, which carries `res`
(cpus/memory/attempt), `gpu` and `rootfs`. There is no channel for user-supplied
values — the existing `context.params.get("min_length", 500_000)` in
`select_chromosomal_contigs.py` and `context.params.get("amr_contig_batch_bp", …)`
in `splitContigsForAmr.py` read as knobs and always take their defaults. So a
sweep is expressible only as N transform instances decided at authoring time, or
by re-running the join outside the DAG against these tables. The second is the
point of the design and it is cheap.
