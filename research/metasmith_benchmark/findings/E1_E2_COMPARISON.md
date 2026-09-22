# Comparing E1 and E2 on their outputs

The question this answers: **on the same input samples, does metasmith reproduce nf-core/mag's results?**

It is answerable at all because the two arms share their inputs by construction — `build_samplesheet.py`
generates E1's sample sheets from `e2_cami.enumerate_arms()`, so both pipelines read the same files from
`/scratch/phyberos/cami/work`. Everything below compares outputs, not configurations. The tool-and-parameter
audit is a separate document, `TOOL_FOR_TOOL.md`, and it is not what this is.

AMBER is deliberately **not** the instrument here. nf-core/mag ships no scorer of any kind, so E1 was never
scored at scale — AMBER exists on E1's side for `sample_0` only. Any reproduction claim must rest on outputs
*both* pipelines already produce. E2's AMBER tables are committed at `../results/e2/` and fold into the
comparison as one more column when E1's side is eventually scored.

## What the evaluation actually covers

Four strands, and the leftover parity tasks are the smallest of them.

1. **Compute the comparison metrics** — the table below. This is the body of the work, and almost all of it is
   standalone scripts over files that already exist.
2. **Decide how the numbers are allowed to be read.** Three open parity questions change the interpretation
   without changing a number: E1 long drops lambda-phage reads and E2 does not (T28); COMEBin runs 12 threads
   on E1 and 48 on E2, on the best-scoring binner on both arms, with no user-facing seed and no documented
   thread-invariance (T29); and E1 long's MetaBAT2 bins come from a hand rerun at jgi 2.17 against nf-core's
   pinned 2.15 — a deviation that is **E1's, not E2's**. Each is a caveat on a specific row, not a blocker.
3. **Optionally add a scored column.** T24, the long-arm AMBER re-score, is ~41 × 37 s plus one tar extraction.
   It is now a nice-to-have rather than a gate, by decision: whatever AMBER gives folds in on a later run.
4. **Produce the deliverable**, shaped like Spanish Lakes — see below.

### Operating constraints while the evaluation runs

- **E4 is on hold.** The chunk chain was cancelled at chunk 10; chunk 10's driver is still running under its
  own guard and will bank its 500 MAGs, after which nothing further launches. **Its stage needs a
  `delete_stage` reclaim by hand once it completes** — the chain would have done that and is gone.
- **Inodes are the shared budget, and this is why the hold matters.** The project quota is 1,000,000 and sits
  near 784,000. Extracting E1's bins and assemblies from the tars re-accrues exactly the inodes E1's
  retirement released, so extraction and E4's remaining chunks were competing for the same resource. With E4
  held, that budget is the evaluation's.
- **E3 is still running and is this session's job, not the evaluation's.** Nothing about it needs to be
  touched.
- **Do not delete `/scratch/phyberos/cami/work`** — it is the CAMI source data, not a work tree.
- **`cami/metasmith/task_cache` is the only copy of every E2 product except the AMBER tables**, which are now
  in git. Read it, never write to it, and open its sqlite as `file:<path>?immutable=1`.

### The deliverable's shape

Model it on the Spanish Lakes tree, `fir:/home/phyberos/project-rpp/spanish_lakes/metagenomics/`. There is no
manuscript and no page there — the tree *is* the deliverable and the reader starts at its root `README.md`.
The conventions worth copying:

- Flat product-named directories, with `_logs/<runkey>/{logs,manifests,metadata}` as the provenance carrier.
- **Numbers carry their command or their evidence.** A figure without the invocation that produced it is not
  reportable.
- **Verification by content, not counts.** Name-diffing caught two real gaps that a count check had passed.
- **Absence is recorded, not left blank.** Spanish Lakes has a `binner_census.tsv` with a row for every
  sample × binner pair carrying the tool's own reason where nothing was produced, because "a silent absence and
  a genuine zero look identical in a bin count." That applies directly here: E1 long's MetaBAT2 produced no
  bins at all under B19, and that must read as a recorded reason, not a zero.
- **Caveats are `##` headings, not footnotes**, and misleading-but-kept data stays and gets explained.
- Fan-outs ship as tarballs, one per sample, never loose files — the allocation has an inode cap.

`research/aspire/campaigns/r1/publish_r1.py` is the working template for generating such a tree and its README.

## The metrics

| metric | what computes it today | what to write | input it consumes |
| --- | --- | --- | --- |
| total assembly length | `src/metasmith_libraries/transforms/assembly/assembly_stats.py:178` (`seqkit stat --all --tabular`) | one seqkit call per arm | contigs FASTA |
| contiguity (N50 and friends) | same file `:179-182`; the full `--all` row is preserved under `_raw_seqkit`, so L50, Q1/Q2/Q3, min/avg/max and sum_gap come free | same call | contigs FASTA |
| % reads mapped | `assembly_stats.py:177` — `mapped / total (QC-passed reads + QC-failed reads)` from `samtools flagstat -O tsv` | flagstat pass over each arm's BAM | E2 `e2::binning_bam`; E1 the published BOWTIE2_ASSEMBLY_ALIGN BAM (`save_assembly_mapped_reads = true`) |
| % of contigs binned | nothing. `research/cami/score_reference_amber.py:174-198` counts kept/unbinned/not-in-assembly to stderr only | ~30 lines; report by count **and** by bp | assembly FASTA + each arm's contig-to-bin table |
| MAG contiguity | nothing. QUAST_BINS ran on E1 long only, pre-config-fix, archived inside `QUAST.tar` | `seqkit stat --all --tabular bins/*.fa`, one row per bin | a directory of bin FASTAs |
| N MAGs total | nothing as a metric | a count under a quality gate; the repo's own precedent is `-comp 50 -con 10` (`bench/drep_study.py:22`) | CheckM2 tables + bin list |
| completeness / redundancy | **both arms already have it.** E2: `e2/checkm2.py:25-39`, run on all four bin sets. E1: nf-core `BIN_QC:CHECKM2_PREDICT` + `CONCAT_CHECKM2_TSV` | normalisation only — no tool run | two CheckM2 `quality_report.tsv`-shaped tables |
| MAG pairing + ANI distribution | the skani call exists (`pratama_mag_recovery.py:63-67`), the pairing does not | best-hit selection, tie policy, censoring-aware distribution, per-sample scoping | two directories of bin FASTAs, one per pipeline, per sample |
| runtime per process | raw traces exist; no aggregator reads `realtime` (`ops/runtime.py:148 parse_trace` tallies states only) | group by `name`, sum `realtime` — **but see the runtime trap** | `nxf_trace.tsv` (E2), `reports/trace.$JOB.tsv` (E1) |
| processes / transforms in plan | E2 already printed by `_common.py:268-272 print_plan` — short 15 steps, long 14. E1 via `e1_nfcore/dot_to_msm.py` | a count on `dot_to_msm.load()` | `.dot` / trace tsv |
| total runtime excluding queue | nothing | `realtime` excludes queue, `duration` includes it; wall-excluding-queue is a merged-interval union over task start→complete | trace tsv, both arms |

## Write these as standalone scripts, never as transforms

`research/cami/score_reference_amber.py` is the pattern and it should be copied close to line for line. It is
stdlib-only, imports no metasmith, pins its container images by apptainer-cache filename so it needs no engine,
takes every external input as a plain CLI path, and — the important part — **solves the external-input problem
by discrimination rather than assumption**: `_read_contig_to_bin` identifies the contig column by membership in
the assembly's own header set and fails loudly when no column matches, and the BAM is asserted to belong to the
assembly by sampling 100k records. Its docstring states its own argument: it is a transcription of the in-plan
transform running the same command in the same image, *"so the two arms' results are comparable by construction
instead of by inspection."*

Check each new metric for assembly-agnosticism before writing it, the way the AMBER scorer had to be. Assembly
length, N50, MAG contiguity and ANI are all intrinsically agnostic. "% of contigs binned" is too, because
numerator and denominator come from the same arm.

### Why not a transform

Two reasons, and the second is the hard one.

**The namespace wall.** `library/data_types/e2.yml:2-3` says it outright: every property carries `e2`, so no
standard-library transform can bind to these types. `assembly_stats.py` requires `sequences::assembly`; E2
produces `e2::megahit_assembly`. They are disjoint by construction — so the existing transform is adaptable to
neither arm, not just to E1. Both arms' products are plain files that someone has to open, which is exactly the
situation the reference-scorer pattern exists for.

**Editing a transform re-keys its cache, and E2's cache is the only copy of E2.** `transforms.py:122-125` sets
`protocol_sig` from `read_text()` of the entire file and `cache_decisions.py:41` folds it into the signature, so
a comment change orphans the step and everything downstream. Four places are dangerously inviting:

- `e2/amber.py:38-57` already parses every contig-to-bin table for the sample. Adding "% contigs binned" there
  is the most natural edit in the repository and would re-key E2's AMBER products.
- `e2/checkm2.py:19-22` already stages every bin into `bins/`. Adding `seqkit stat` costs one line and re-keys
  ~29,542 CheckM2 products.
- `e2/bowtie2_binning_bam.py:23-24` already writes `bowtie2.log`, which carries the overall alignment rate, and
  merely never publishes it. Recover that number with a flagstat pass or by grepping the step's `.command.log`.
- `src/metasmith_libraries/transforms/assembly/assembly_stats.py:174-184` — adding keys re-keys every consumer
  across every project in the monorepo. The repo already has the right instinct: `assembly_stats_pratama.py` is
  a copy, not an edit.

A new transform file is safe from re-keying but buys nothing: it would have to re-derive E2's inputs through the
planner, and E2's plan is finished and cached.

## Runtimes: where they are, and the three places the record was wrong

**The evidence is captured and committed.** `../results/e2/e1e2_slurm_jobs.psv` holds 14,367 rows — every E1
and E2 Slurm job in the 2026-09-12 → 09-18 window, with `Submit`, `Start`, `End`, `Elapsed`, `ReqCPUS`,
`AllocCPUS`, `ReqMem` and `WorkDir`. Run-key split: E1 short 7,107, E1 long 1,452, and E2 `WfOlaqLT` 2,324,
`sxDeVO5L` 1,341, `33hlLu8Q` 467, `F1yIPPmC` 235, `MjMN02CK` 211, `VgUw0A7c` 170. Fuller dumps, including the
per-step `MaxRSS` rows, are on fir at `/scratch/phyberos/bench/evidence/`.

**`WorkDir` is the only field that attributes a task to a run key or to E1 short/long. Do not drop it.**

Three corrections to what the campaign record said:

1. **`sacct` retention was never the problem.** It reaches back to **2026-07-01**, roughly 83 days, and every
   E1 and E2 row is present. The earlier "retention no longer reaches the DRAM-family jobs" finding was almost
   certainly this trap: `sacct` rejects a date range wider than about a month with
   `sacct: error: Too wide of a date range in query` **and exits 0**, so a wide query piped to `wc -l` reads
   as one row, or as zero with stderr suppressed — indistinguishable from "the data is gone." Probe per-month,
   never in one wide query. (The 2026-07-01 boundary may simply be the account's first day of use — the oldest
   artifact on scratch is dated 2026-07-02 — so retention is *at least* 83 days and may be longer.)
2. **metasmith never wrote a nextflow trace, so deleting E2's run directories cost nothing.** The reports block
   is commented out in `workflow.config.nf:60,85`, with the reason inline: nextflow treats it as an
   unrecognized config option and drops it. Slurm is not a degraded fallback for E2 — it is the only source,
   and it is intact.
3. **E1 *does* have a real nextflow trace, and it is the best single artifact in this audit.**
   `/scratch/phyberos/bench/e1/short/reports/` survives untarred: nine `trace.<jobid>.tsv` (the final one
   6,959 rows), nine `report.*.html`, nine DAGs. Columns include `realtime` (queue-excluded), `duration`
   (queue-included), `%cpu`, `peak_rss`, and `status=CACHED` so resumed tasks can be excluded. E1 long's trace
   is inside `e1_long.tar` but sits near the *front*, so it extracts without a full read:
   `tar xf … e1_long.tar -C <dest> long/reports long/.nextflow.log long/out/pipeline_info`.

### What is still genuinely tricky

**E2's scored runs are cache-warm and E1's are cold.** `MjMN02CK` dispatched only 2 distinct steps because 13
of its 15 were served from cache; the work happened under `WfOlaqLT` and siblings. So "steps run" is not
comparable across arms, and a per-transform runtime must be summed over the run that actually executed each
step, not over the run that produced the scored output.

**Do not use shard mtimes for duration.** Measured on one shard: `.command.sh` → `.command.out` mtime delta is
688 s where Slurm `Elapsed` is 507 s — the delta spans Submit→End and **overstates runtime by 36%**. Since
"total runtime excluding queue" is one of the asks, this would inflate E2's cost against E1. Use Slurm
`Start`→`End`. Shards are excellent for *attribution* — `.command.sh` line 2 carries the step number, line 3
the transform name, `.command.out` lines 4-5 the Slurm job id and the run key — and that job id is the join
key back to the Slurm row. (Line 2 can be ~900 KB; read it with `cut`, not `head`.)

**E1 short is nine resumed nextflow sessions**, most `FAILED` and resumed. Summing driver elapsed times badly
overcounts, because each resume re-reports cached tasks. E1 long completed in one session, `59634610`,
07:12:58. E2's drivers give totals directly: `e2_short` 23:01:45, `e2_long` 14:59:33.

**`-X` and `MaxRSS` are mutually exclusive.** `MaxRSS` lives only on the `.batch` step, so an `-X` dump returns
it blank. Two queries, not one.

Two instruments that do not work here at all:

- **`sacct --format=TotalCPU` is blind on this cluster** — `00:00:00` for every job, RUNNING and COMPLETED
  alike, because CPU accounting is not gathered. The working "is it stuck" instrument is
  `srun --jobid=<id> --overlap -n1 ps -eo etime,time,pcpu,rss,comm --sort=-time`.
- **`nxf.log` is UTC and Slurm is local PDT**, a seven-hour offset. DEBUG `ProcessConfigBuilder` and
  `Creating process` lines are process *definition*, not submission — only `Submitted process >` counts.

## Plan shape, counted

From job names, which carry the full process path on E1 and the step number plus transform name on E2.

**E1 short: 25 distinct `NFCORE_MAG` processes.** FASTP, FASTQC_RAW, FASTQC_TRIMMED, MEGAHIT, GUNZIP, BOWTIE2
BUILD/ALIGN, JGISUMMARIZE, METABAT2, SEMIBIN, COMEBIN, SEQKIT_STATS, SPLIT_FASTA, FASTATOCONTIG2BIN,
RENAME_PRE/POSTDASTOOL, DASTOOL, CHECKM2_PREDICT, CONCAT_CHECKM2_TSV, MAG_DEPTHS, MAG_DEPTHS_SUMMARY, PRODIGAL,
QUAST, BIN_SUMMARY, MULTIQC.

**E1 long: 28**, the same with PORECHOP_ABI, CHOPPER, NANOPLOT_RAW/FILTERED, FLYE and MINIMAP2 INDEX/ALIGN
replacing the short-read front end, plus QUAST_BINS and CONCAT_QUAST_SUMMARY.

**E2 short: 15 transforms** — p01 fastp, p02 fastqc_raw, p03 fastqc_trimmed, p04 megahit, p05
bowtie2_binning_bam, p06 comebin, p07 semibin2, p08 metabat2, p09 gold_standard, p10-p12 checkm2, p13 das_tool,
p14 checkm2, p15 amber. **E2 long: 14** — p01 porechop_abi, p02 chopper, p03 flye, p04 minimap2_binning_bam,
p05 comebin, p06 semibin2, p07 metabat2, p08 gold_standard, p09-p11 checkm2, p12 das_tool, p13 checkm2,
p14 amber.

**The 25-vs-15 gap is mostly not a real difference in work done.** `TOOL_FOR_TOOL.md:396-400` already records
why: one metasmith `assembly_stats` step is a composite of four tools against four separate nf-core processes,
and *"a process-count comparison of this stage is meaningless in either direction."* The number is cheap; making
it mean something is not. Report it with the composite steps expanded, or not at all.

**One unresolved E1-short caveat.** Over the whole 09-12 → 09-18 window E1 short shows a `QUAST` count of 20
against 208-626 for every other short process, and no `QUAST_BINS` or `CONCAT_QUAST_SUMMARY`. Its *final*
session's trace shows zero QUAST tasks, consistent with `skip_quast = true`. The likely reconciliation is that
early sessions ran QUAST before that flag was set, which is a sequencing artifact rather than an incomplete
run — but it is inferred, not established. Settle it before presenting E1 short as a complete nf-core/mag run.

## The ANI pairing metric

Three-quarters of the machinery exists in pieces that have never been assembled.

`pratama_mag_recovery.py:63-67` runs `skani dist --ql <list> --rl <list> --min-af 15`, which is the
cross-*set* shape this needs and the only use of it in the repo. Output columns, confirmed against real output:
`Ref_file Query_file ANI Align_fraction_ref Align_fraction_query Ref_name Query_name`. It also carries a subtle
requirement any new script inherits, explained at its lines 8-15: **skani scores one genome per input file**, so
a pooled MAG reference scores each *scaffold* separately and fragments a multi-contig MAG's ANI across its own
contigs. Split to one file per MAG first, always.

The four `bench/` dereplicators use `skani triangle --sparse` — all-vs-all *within* one pool. That is the wrong
shape for pairing (it would answer "how many distinct genomes across both arms", not "which E1 MAG is which E2
MAG"), though it is worth computing as a second view, and its clustering logic is unusually well validated.

**No best-pairing selection exists anywhere.** The headline numbers in `PROVEN.md` for the published-MAG
comparison — 71 of 92 matched, 65 of 132 high-confidence, ANI 85.52-100.00 — were computed by hand after the
run. Four decisions have to be made, and they are scientific rather than technical:

1. **Unpaired MAGs are censored, not zero.** skani filters hard before you see a row: 132 pairs out of 117,300
   possible in the published-MAG case. A histogram of paired ANI alone is a conditional distribution and reads
   far too optimistic. Report it beside a match rate in both directions.
2. **Mutual-best-hit is not a matching.** Two E2 bins can both best-hit one E1 bin, and bins genuinely split and
   merge between pipelines — that is signal, not noise. Emit the many-to-many table *and* a separate one-to-one
   assignment column, so a reader can see how much one-to-one-ness was imposed.
3. **Scope per sample.** The arms assembled the same reads independently; cross-sample pairs are noise. Derive
   the sample on both sides and refuse a cross-sample pair explicitly, the way the AMBER scorer refuses a
   cross-mapped BAM.
4. **Which bin set.** Each arm has four. That is 4x4 unless pinned, and everything already published pins DAS
   Tool. Note that DAS Tool scores below its best input on 208 of 208 short samples, so a DAS-Tool-only pairing
   is defensible but narrow.

For the distribution, `src/metasmith_libraries/resources/lib/bsr_histogram.py` is a close template: best-hit per
group via `groupby().max()`, similarity converted to distance, min/median/max printed, binned histogram with an
axis label stating the direction of the scale.

## Traps in the source data

- **Take AMBER only from transform `4UdrtQp8`.** A second, un-tombstoned transform named `amber`, `h2g7GAwU`,
  holds a superseded long-arm batch whose DAS Tool median is 0.9105 against the real 0.1951.
- **Identify cached products by header, never by dtype suffix** — the suffix differs per arm, so a suffix glob
  returns 41 of 249 and silently drops the short arm.
- **Open any metasmith `cache.sqlite` as `file:<path>?immutable=1`.** It is in WAL mode; a plain open creates
  sidecars and bumps the `task_cache/` root mtime. (That mtime keys nothing — verified: `stat_leaf_id` is never
  reached from the caching layer, and no payload in cami's cache references a `task_cache` path — but the rule
  stands because writing inside a cache is forbidden.)
- **`/scratch/phyberos/cami/work` is the CAMI source data, not a work tree.** Simulated reads, gold-standard
  assemblies, ground truth; 1.44 TiB across 15,070 files, every one single-linked. Every AMBER score on either
  arm depends on it. Do not delete it.
- **DAS Tool writes the full FASTA header as its contig id**, which silently dropped all 65,569 rows on the
  reference scorer's first run. On E2's side, `--write_unbinned` files every leftover contig under a bin
  literally named `unbinned`, and lists some contigs twice.
- **nf-core's contig-to-bin table is four-column with a `binner` discriminator**; its unbinned rows arrive with
  an empty binner and must be dropped by name.
- **E2's CheckM2 is 29,542 one-row files whose `Name` column is an opaque content hash** — not joinable to
  sample or binner without a lineage resolution nobody has performed.
- **E2 has no assembly-statistics product at all.** No quast, metaquast or assembly_stats among its 66
  transform names; megahit and flye emit only `.fa`. Assembly length and N50 are a job, not a read.
- **E1's outputs are inside two tars** — see the next section for where they actually are, which is not where
  the campaign record said. Extracting from them re-accrues inodes E1's retirement released, which competes
  directly with E4's remaining chunks.

## What survives of E1

Three figures in the campaign record were wrong and are corrected here.

**The archives are not at `/scratch/phyberos/bench/e1/{long,short}`.** Those paths hold live files: `long/`
contains only `kept_inputs/` (the 126 GB), and `short/` contains only rotated nextflow logs, traces and reports
(the 139 MB). The archives are in `/scratch/phyberos/bench/archive/`:

| archive | bytes | members |
| --- | --- | --- |
| `e1_long.tar` | 35.5 GB | 13,787 |
| `e1_short_out.tar` | 99.1 GB | 57,253 |

**"Don't spend a 126 GB sequential read" was protecting against a cost that does not exist.** A full `tar tf` of
`e1_long.tar` runs in **144 seconds**, exit 0, 13,787 lines — measured, inline, bounded. The short archive
reached 67% in 400 s, so a full listing is a ~10-minute single-core job. Neither archive has a saved manifest;
both archive scripts piped `tar tf | wc -l` and discarded the listing.

**"E1 short's `out/` was deleted" is misleading.** `out/` was tarred first and the tar exists, so E1 short's
bins, CheckM2 tables, depth tables and summaries are all recoverable. Only `work/` (177,092 inodes, 1.72 TB)
was destroyed without an archive.

### Tier A — a head-to-head comparison that is already computed, on both sides, today

This is the strongest thing in the campaign and it needs no compute at all. E1 was scored post-hoc by
`score_reference_amber.py` on one short sample and one long sample, all four binners; E2 scored the same two
samples as part of its 249. Both sides are plain files on disk. `f1_score_bp`:

| sample | binner | E1 | E2 |
| --- | --- | --- | --- |
| marine_sample_0 (short) | MetaBAT2 | 0.1542 | 0.1553 |
| | SemiBin2 | 0.1421 | 0.1381 |
| | COMEBin | 0.5391 | 0.5394 |
| | DAS Tool | 0.2235 | 0.2295 |
| plant_associated_long_nano_sample_0 | MetaBAT2 | 0.4778 | 0.4680 |
| | SemiBin2 | 0.4277 | 0.4450 |
| | COMEBin | 0.8849 | 0.9054 |
| | DAS Tool | 0.1805 | 0.1159 |

Seven of the eight agree to within ~0.02. DAS Tool on the long arm is the one visible split. n=2 samples, so
this is an existence proof rather than a distribution — but it is a *matched* one, same inputs, same scorer,
same image.

E1 side: `/scratch/phyberos/reference_amber/marine_sample_0/{MetaBAT2,SemiBin2,COMEBin,DASTool}/results.tsv`
and `/scratch/phyberos/reference_amber_long_b19/plant_associated_long_nano_sample_0/…`. A first long pass at
`/scratch/phyberos/reference_amber_long/` carries three binners only — no MetaBAT2, for the B19 reason below.

### Tier B — E1's long arm, after one ~3-minute extraction

Verified present as members of `e1_long.tar`, under `long/out/`: `GenomeBinning/QC/checkm2_summary.tsv` (all
bins, all 41 samples) and 5,064 per-bin CheckM2 outputs; bin FASTAs for COMEBin (2,058), SemiBin2 (1,336) and
DAS Tool (565); `GenomeBinning/depths/` contig and bin depth tables; `QC/quast_bin_summary.tsv` and a nested
`QUAST.tar`; `Assembly/FLYE/*.assembly_info.txt` × 41 with contig counts, lengths and coverage, plus 985
per-assembly QUAST files; MultiQC with `multiqc_checkm2.yaml`, `multiqc_quast.yaml` and `multiqc.parquet`;
`pipeline_info/` with the run's own params and software versions. Separately, `long/b19/` holds
`contig_to_bin_map.tsv` covering all 41 assemblies × 4 binners, and `long/reports/` holds E1 long's own
execution trace.

**`GenomeBinning/MetaBAT2/bins/` is an empty directory**, and that is B19 rather than a packing error:
`jgi_summarize_bam_contig_depths` at the default `--percentIdentity 97` counted almost no nanopore read as well
mapped, every contig went to `lowDepth`, and MetaBAT2 wrote no bin. E1 long's MetaBAT2 bins exist **only** in
`long/b19/<sample>/metabat2_bins/`, from the hand rerun at `--percentIdentity 80` **and at jgi 2.17 rather than
nf-core's pinned 2.15**. So any long-arm MetaBAT2 comparison must use the b19 bins and must carry that
deviation — and note the deviation is E1's, not E2's.

### Tier C — E1's short arm, after a ~10-minute extraction

Confirmed in the partial listing: `GenomeBinning/QC/checkm2_summary.tsv` plus 28,582 per-bin CheckM2 outputs,
`GenomeBinning/bin_summary.tsv` (the cross-binner summary), `GenomeBinning/contig_to_bin/contig_to_bin_map.tsv`
(all four binners, all 208 samples, one file), MetaBAT2 (4,288) and DAS Tool (2,595) bins, depth tables, and
per-sample DAS Tool summaries.

`out/Assembly/` and `out/multiqc/` **survived** — 835 and 164 members respectively. That was inferred here
before; it is now listed.

### Both archives are now fully listed, and the manifests are permanent

Job `60958550` listed both tars end to end and left `e1_long.manifest.txt` and `e1_short_out.manifest.txt`
beside them in `/scratch/phyberos/bench/archive/`. **Query those files rather than the tars** — a member
question is now a `grep`, not a sequential read. Listing cost 37 s for the long tar and 425 s for the short
one, both far below the "don't spend a sequential read" rule this document previously carried.

| | long (13,787 members) | short (57,253 members) |
| --- | --- | --- |
| `out/Assembly/` | 1,151 | 835 |
| `out/multiqc/` | 33 | 164 |
| `.bam` / `.bai` | **0 / 0** | **0 / 0** |
| bin FASTAs COMEBin | 1,975 | 7,593 |
| bin FASTAs SemiBin2 | 1,335 | 6,699 |
| bin FASTAs DAS Tool | 564 | 2,594 |
| bin FASTAs MetaBAT2 | **0** (see B19 above) | 4,287 |

Bin counts here are regular files matching `<binner>/bins/*.fa[sta][.gz]`, so they run slightly below the
Tier B figures above, which counted directory entries too.

### E1's BAMs are gone — measured, no longer a contradiction

`control.config:160` sets `save_assembly_mapped_reads = true`, and the earlier reading inferred from the 99 GB
archive size that 208 BAMs at ~3.35 GB each could not fit. The manifests settle it directly: **zero `.bam` and
zero `.bai` members in either archive.** Not an inference from size — an absence from a complete listing.

So for both arms, any metric requiring E1's alignments means regenerating them. That is ~640-700 GB for the
short arm's 207 and a smaller long-arm equivalent, and it is the single most expensive line item in the
metric table. **Check MultiQC first** — `out/multiqc/multiqc_data/` survives on both arms and the short arm's
plot list includes `bowtie2_pe_plot`, so a per-sample alignment rate may already be recorded. The open
question is *which* bowtie2 that is: nf-core/mag runs both a host/phiX removal alignment and
`BOWTIE2_ASSEMBLY_ALIGN`, and only the former is normally in MultiQC. The long arm's MultiQC carries QUAST
and CheckM2 only, no mapping section at all, so a long-arm mapping rate is not free by this route.

### What long-arm AMBER would need, if it is ever wanted

Almost everything is on disk: 41 Flye assemblies and 41 self-mapped BAMs with indexes in `kept_inputs/`; all
41 `reads_mapping.tsv.gz` truth files; the AMBER, samtools and polars images; the scorer and the gold-standard
vote library. Measured runtime is **37 s per sample** (`sacct -X -j 59893504`, 8 cpu / 48 G).

Three things are stale in the existing recipe. `contig_to_bin_map.tsv` is no longer on disk and must come out
of the tar — the only genuinely missing input. `$W` points into the deleted `work/` and should be repointed at
`kept_inputs/`. `$TRUTH` is hardcoded to one sample, and a 41-task array needs a sample-to-truth-path map over
two corpora whose directory layouts differ; the path template has only ever been exercised on
`plant_long_read_nano`.

**Keep the per-assembly pre-filter when generalising to an array.** Flye names contigs `contig_1, contig_10, …`
*per assembly*, and sample_0 shares 3,068 of its 3,135 contig names (97.9%) with `toy_humangut_long_sample_0`.
Membership filtering across the merged 41-assembly map would admit other samples' rows. The
`awk -F'\t' '$1==AID'` filter on `assembly_id` is what prevents a repeat of defect B21.

## Suggested order

Cheapest first, and the early items force you to solve "enumerate each arm's bin directory", which every later
item needs.

1. MAG contiguity — one seqkit call, identical on both arms.
2. Assembly length and N50 — same call on the contigs FASTA. Recomputing both arms with one tool is more
   comparable than mixing E1's archived QUAST numbers with seqkit numbers.
3. N MAGs — trivial once (1) and the CheckM2 tables are in hand; the only real decision is the quality gate.
4. % reads mapped — **the one metric E1 cannot supply from its archives**, because neither tar holds a single
   BAM. Read `out/multiqc/multiqc_data/` first (extracted by job `60959768` to `archive/e1_reports/`): if its
   bowtie2 section is the assembly alignment rather than host removal, the short arm is free and only the long
   arm needs work. Otherwise this metric costs ~640-700 GB of regenerated alignments on the short arm alone,
   and is the item most worth dropping or scoping down. E2's own BAMs are 1.87-4.89 GB each and are in cache.
5. % of contigs binned — small, three documented traps above.
6. Completeness / redundancy — no tool run; the work is normalising E1's one concatenated table against E2's
   ~29,542 one-row files and choosing the comparison unit.
7. Runtime per process and total-excluding-queue — medium, and carrying the runtime trap above.
8. Plan-shape counts — cheap to compute, hard to make meaningful. Read `TOOL_FOR_TOOL.md:396-400` first: one
   metasmith `assembly_stats` step is a composite of four tools against four separate nf-core processes, and
   *"a process-count comparison of this stage is meaningless in either direction."*
9. ANI pairing and distance distribution — largest by a wide margin.

## Unverified

- Whether nf-core/mag's `SEQKIT_STATS` passes `--all`, hence whether E1 already has N50 for free. No copy of
  `mag.nf` exists in this repository; settle it against upstream at revision `56abab5b`.
- Whether `e2::binning_bam` products are still present per-sample in cami's cache. The step is an intermediate,
  not a plan target; the per-run byte totals are consistent with the BAMs being cached, but presence was not
  checked.
- skani's internal ANI screen threshold. Heavy pre-filtering was inferred from 132 pairs out of 117,300, not
  from any statement in the repo.
