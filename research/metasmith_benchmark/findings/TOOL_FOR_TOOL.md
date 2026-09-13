# METASMITH SIDE of acceptance criterion 12's tool-for-tool diff.
# Generated 2026-09-12T22:25:21Z from /home/tony/scratch/launch_frozen (the frozen launch checkout).
# The nf-core side is L3's process list. Keep both here so the diff is one command.
# CAUTION grepping this out of a dry run can return NOTHING when the flags are wrong,
# and an empty grep is indistinguishable from a failed one. Read the raw output first.

## R2, CAMI short-read parity arm:  run --corpus cami --variant variant --no-dedup
Plan OK -- 18 steps across 229 samples, key=me0OiKOi
   1. seqkit_reads                 -> ['sequences::read_qc_stats']
   2. bbduk                        -> ['sequences::clean_short_reads', 'sequences::discarded_short_reads']
   3. megahit                      -> ['sequences::megahit_assembly', 'sequences::megahit_assembly_graph']
   4. prodigal                     -> ['sequences::gff', 'sequences::orfs']
   5. assembly_stats               -> ['alignment::bam', 'sequences::assembly_per_bp_coverage', 'sequences::assembly_per_contig_coverage', 'sequences::assembly_stats']
   6. comebin                      -> ['binning::comebin_contig_to_bin_table', 'sequences::comebin_bin_fasta']
   7. semibin2                     -> ['binning::semibin2_contig_to_bin_table', 'sequences::semibin2_bin_fasta']
   8. metabat2                     -> ['binning::metabat2_contig_to_bin_table', 'sequences::metabat2_bin_fasta']
   9. cami_contig_truth            -> ['binning::contig_gold_standard_table']
  10. checkm                       -> ['taxonomy::checkm_stats']
  11. checkm                       -> ['taxonomy::checkm_stats']
  12. checkm                       -> ['taxonomy::checkm_stats']
  13. amber                        -> ['binning::amber_bin_metrics', 'binning::amber_results']
  14. amber                        -> ['binning::amber_bin_metrics', 'binning::amber_results']
  15. amber                        -> ['binning::amber_bin_metrics', 'binning::amber_results']
  16. das_tool                     -> ['binning::das_tool_contig_to_bin_table', 'binning::das_tool_summary', 'sequences::das_tool_bin_fasta']
  17. checkm                       -> ['taxonomy::checkm_stats']
  18. amber_das_tool               -> ['binning::das_tool_amber_bin_metrics', 'binning::das_tool_amber_results']

## R2 long-read lane:  run --corpus cami --variant variant --with-long-read
Plan OK -- 17 steps across 172 samples, key=TlWFSiQB
   1. seqkit_reads                 -> ['sequences::read_qc_stats']
   2. flye_raw                     -> ['sequences::flye_raw_assembly']
   3. prodigal                     -> ['sequences::gff', 'sequences::orfs']
   4. assembly_stats               -> ['alignment::bam', 'sequences::assembly_per_bp_coverage', 'sequences::assembly_per_contig_coverage', 'sequences::assembly_stats']
   5. comebin                      -> ['binning::comebin_contig_to_bin_table', 'sequences::comebin_bin_fasta']
   6. semibin2                     -> ['binning::semibin2_contig_to_bin_table', 'sequences::semibin2_bin_fasta']
   7. metabat2                     -> ['binning::metabat2_contig_to_bin_table', 'sequences::metabat2_bin_fasta']
   8. cami_contig_truth            -> ['binning::contig_gold_standard_table']
   9. checkm                       -> ['taxonomy::checkm_stats']
  10. checkm                       -> ['taxonomy::checkm_stats']
  11. checkm                       -> ['taxonomy::checkm_stats']
  12. amber                        -> ['binning::amber_bin_metrics', 'binning::amber_results']
  13. amber                        -> ['binning::amber_bin_metrics', 'binning::amber_results']
  14. amber                        -> ['binning::amber_bin_metrics', 'binning::amber_results']
  15. das_tool                     -> ['binning::das_tool_contig_to_bin_table', 'binning::das_tool_summary', 'sequences::das_tool_bin_fasta']
  16. checkm                       -> ['taxonomy::checkm_stats']
  17. amber_das_tool               -> ['binning::das_tool_amber_bin_metrics', 'binning::das_tool_amber_results']

================================================================================
THE DIFF TABLE. Metasmith side is measured. nf-core side is PENDING L3's process list.
================================================================================

Both keys above reproduce the recorded launch baseline exactly (me0OiKOi, TlWFSiQB), which is
also the fifth confirmation today that the frozen tree has not moved under this session's edits.

Read the step lists as TOOLS, not as steps. Three things inflate the metasmith count and have no
nf-core counterpart by design:

  * `cami_contig_truth` is this campaign's gold-standard bridge, not a pipeline stage. The
    reference arm's equivalent is `score_reference_amber.py`, which runs the same vote script.
  * the four `checkm` instances are ONE tool fanned out over three raw bin sets plus the
    consolidated set. nf-core runs CheckM2 once per bin set too, under `postbinning_input='both'`.
  * the four `amber*` instances are the scorer, which nf-core does not run at all. That is the
    gap `score_reference_amber.py` closes.

| stage | metasmith (R2) | nf-core/mag (R1, control.config) | status |
|---|---|---|---|
| read QC report      | seqkit_reads          | FASTQC_RAW + FASTQC_TRIMMED | cosmetic, not a parity claim |
| trimming            | bbduk                 | fastp                       | DEVIATION, bounded: `too_short` 0, `low_quality` 0, 38 reads dropped for Ns, adapters on 8,226 of 33.3 M |
| contaminant removal | none                  | bowtie2 phiX removal        | DEVIATION, measured NO-OP: 127 of 16,647,376 pairs, 0.00% |
| assembly, short     | megahit               | MEGAHIT                     | PARITY |
| assembly, long      | flye_raw              | Flye                        | PARITY; no control arm wired yet (L3 costing) |
| gene calling        | prodigal              | PRODIGAL                    | PARITY |
| depth profile       | assembly_stats (minimap2) | bowtie2 (short) / minimap2 (long) | DEVIATION on the SHORT lane only, and the ONLY one of the four that can move an AMBER score |
| binning             | metabat2, semibin2, comebin | MetaBAT2, SemiBin2, COMEBin | PARITY, three binners each |
| refinement          | das_tool              | DAS Tool                    | PARITY; `--score_threshold` default IS 0.5 on both, so never a difference |
| bin quality         | checkm                | CheckM2                     | DEVIATION, tool substitution, not on the scored path |
| taxonomy            | severed               | disabled                    | PARITY by decision, stated as a deviation from both defaults |
| cross-sample derep  | off (`--no-dedup`)    | absent from control.config  | PARITY |
| AMBER scoring       | amber x3 + amber_das_tool | score_reference_amber.py  | post-hoc, criterion 8's sanctioned shape |

NF-CORE SIDE, FILLED IN 2026-09-12 from L3's process list (union of two real executions under
this config) with two corrections of my own from the source at the pinned revision 56abab5b:

  FASTQC_RAW, FASTP, FASTQC_TRIMMED
  CAT_FASTQ                                   (concatenate a sample's runs)
  BOWTIE2_PHIX_REMOVAL_BUILD/_ALIGN
  MEGAHIT, GUNZIP
  QUAST                                       (per ASSEMBLY -- 2 tasks for 2 assemblies in the
                                               pilot trace, not per bin; kept deliberately)
  PRODIGAL                                    (mag.nf:278, per assembly)
  BOWTIE2_ASSEMBLY_BUILD/_ALIGN               (own-read mapping; binning_map_mode='own')
  jgi_summarize_bam_contig_depths, METABAT2_METABAT2
  SEMIBIN_SINGLEEASYBIN
  COMEBIN_RUNCOMEBIN
  SEQKIT stats, SPLIT_FASTA, RENAME
  DASTOOL_FASTATOCONTIG2BIN, DASTOOL_DASTOOL
  DEPTHS
  BIN_QC:CHECKM2_PREDICT + BIN_QC:CONCAT_CHECKM2_TSV     source-confirmed, not yet run
  FLYE                                                    long lane, no control rung launched yet

  NOT RUN, each traced to its gate rather than to an absence in a log: CAT (mag.nf:517 gates the
  whole branch on `cat_db || cat_db_generate`, both unset), GTDB-Tk (skip_gtdbtk), Prokka
  (skip_prokka), ALE (skip_ale), BUSCO and CheckM1 (run_busco/run_checkm false), SPAdes,
  SPAdesHybrid, MetaMDBG, MaxBin2, CONCOCT, MetaBinner.

CORRECTION 1, and it is why a union of two runs is not a process list. L3's list carried
"CAT/pack DB staging". CAT does not run under this config -- that entry came from the
`-profile test` execution in the union, because the test profile supplies a toy `cat_db`.
Taxonomy on one arm only is the asymmetry this campaign already decided against, so this one
mattered: it would have put a taxonomic classifier in the reference arm's column against a
severed lane on ours.

CORRECTION 2, and it saves the AMBER pass a flag. The per-binner contig-to-bin tables were
never work-dir-only. `mag.nf:467` transposes the post-binning channel, splitFastas each bin for
its headers and collectFiles the result to `GenomeBinning/contig_to_bin/contig_to_bin_map.tsv`
via `storeDir`, which is UNCONDITIONAL. It is one four-column table --
`assembly_id contig_id binner bin_id` -- covering every binner, DAS Tool included, because
`binning_refinement/main.nf:72` stamps the refined bins `binner: 'DASTool'`. That is now the
scorer's input: `score_reference_amber.py --nfcore-contig-to-bin <file>` splits it into one
AMBER label per binner. `refine_bins_dastool_savecontig2bin` publishes a different intermediate
and is harmless but unnecessary. `save_assembly_mapped_reads = true` IS required, for the BAM.

STORAGE, narrowed by the same reading. With skip_prokka and skip_ale both true, the ONLY
remaining per-bin fan-out in this pipeline is that driver-side channel operation, which writes
one file. CheckM2 is per bin SET; QUAST is per assembly. So the ~60,000-task annotation
explosion the phase-2 review identified as the dominant storage term is switched off by
configuration. That is a narrowing, NOT a closure: rung 10 still has to measure
files-per-sample, counting directories separately, because no run has reached binning yet.

NF-CORE PROCESSES WITH NO METASMITH COUNTERPART, so the "every process has a row" check closes:

  QUAST                       assembly statistics. Cosmetic; not on the scored path. State as a
                              deviation of REPORTING, not of method.
  CAT_FASTQ                   concatenates a sample's multiple runs. CAMI ships one run per
                              sample, so it is a no-op here.
  GUNZIP, SPLIT_FASTA, RENAME, SEQKIT stats, DEPTHS      plumbing and per-bin bookkeeping.
  BOWTIE2_PHIX_REMOVAL_*      already a row above; the measured no-op.

Every other process maps to a row. Nothing in the table is empty except the AMBER row's
right-hand side, which is what score_reference_amber.py fills.

WHAT REMAINS: two entries are source-confirmed rather than observed -- CheckM2 (BIN_QC, which
rung 1 has not reached) and Flye (no long-read control rung launched). Confirm both from a real
trace, then the only thing left in this file is the numbers.

CAUTION when the AMBER numbers are reported, the DAS Tool row must come from
`binning::das_tool_amber_results` and not from whichever `amber_results` instance is first. That
distinct type exists for exactly this reason, and the campaign has already had one binner go
unscored for a whole gate because a positional check passed while the measurement was wrong.

================================================================================
L4 PASS, 2026-09-12. Metasmith side CONFIRMED FROM A REAL TRACE, plus three row
corrections. Every claim below was read off a task directory of run wqjsf1Et
(the pilot) or off the transform source in /home/tony/scratch/launch_frozen.
================================================================================

CONFIRMED BY RUNNING, not by planning. Pilot wqjsf1Et invoked steps 1-9 for real
(nxf_tasks.csv, one real task directory each): seqkit_reads, bbduk, megahit, prodigal,
assembly_stats, cami_contig_truth all COMPLETED exit 0 with products in results/;
metabat2 and semibin2 reached their tools and FAILED on subsample-size input (below);
comebin was ABORTED by a deliberate teardown before it started. Steps 10-18 (checkm x4,
amber x3, das_tool, amber_das_tool) are still plan-only on this side.

CORRECTION 3, and it inverts a row. "bin quality | checkm | CheckM2 | DEVIATION, tool
substitution" IS NOT A DEVIATION. The metasmith step is merely NAMED `checkm`: its env is
`docker://quay.io/hallamlab/external_checkm2:1.1.0` and
transforms/metagenomics/binning/checkm.py:65 invokes `checkm2 predict --genes -x faa`.
Both arms run CheckM2. The row is PARITY on the tool; only the VERSION is still
unconfirmed on the nf-core side. This is the table's own instruction -- read the step
lists as TOOLS, not as names -- applied to the table itself.
  CAUTION that same line ends `|| true`, so a checkm2 failure cannot fail the step. Check
  the product, never the exit code, when the CheckM2 row is finally reported.

CORRECTION 4, and it narrows the campaign's only AMBER-moving deviation. "depth profile |
assembly_stats (minimap2) | bowtie2 (short) / minimap2 (long)" understates what is shared.
BOTH arms compute MetaBAT2's depth with the SAME tool: nf-core runs
`jgi_summarize_bam_contig_depths` as its own process, and metasmith runs the identical
binary INSIDE the metabat2 step (transforms/metagenomics/binning/metabat2.py:39; observed
live in the pilot's stderr, "jgi_summarize_bam_contig_depths 2.17 (Bioconda)"). SemiBin2
and COMEBin take the BAM directly on both sides and run no depth tool of their own.
  So the deviation is ONLY the aligner that produces the BAM -- minimap2 here against
  bowtie2 there -- and NOT the depth computation on top of it. That is a narrower claim
  than the row makes, and it is still the one deviation of the four that can move an AMBER
  score. The rest of the row's difference is DECOMPOSITION: one nf-core process against
  one interior command of a metasmith step.

CORRECTION 5, on the "no metasmith counterpart" list. `SEQKIT stats` is listed there as
plumbing. It is not one-sided: metasmith runs seqkit twice, once as step 1
(`seqkit_reads` over the reads) and once inside assembly_stats
(`seqkit stat --all --tabular` over the assembly, transforms/assembly/assembly_stats.py:170).
Move it to a PARITY row. QUAST remains genuinely nf-core-only, so "a deviation of
REPORTING, not of method" stands for QUAST alone.
  For completeness, metasmith's step 5 `assembly_stats` is a composite of FOUR tools --
  minimap2 (align), samtools (sort/index/flagstat), bedtools genomecov (per-bp coverage)
  and seqkit stat -- against four separate nf-core processes
  (BOWTIE2_ASSEMBLY_BUILD/_ALIGN, DEPTHS, QUAST, SEQKIT). A process-count comparison of
  this stage is meaningless in either direction.

STILL UNCONFIRMED FROM A TRACE, and neither is L4's to close alone:
  CheckM2 (nf-core BIN_QC)  -- nf-core rung 1 has not reached binning; GenomeBinning/
                               contig_to_bin/ is still EMPTY and no BAM exists in the
                               outdir or the work dir as of 15:53. L3's lane.
  Flye                      -- no long-read control rung launched. The metasmith side's
                               flye_raw is also still plan-only.
  Metasmith checkm/amber/das_tool -- plan-only; rung 1 reaches them.

PILOT FINDING, and it is a statement about the tools rather than about the ramp. At
200,000 read pairs BOTH MetaBAT2 and SemiBin2 fail, exit 1, on a real megahit assembly of
that subsample. MetaBAT2's cause is in its own stderr: jgi reported only 25,489 of 400,075
reads well mapped (6.37%), and the depth matrix then carried
`[Warning!] Negative coverage depth is not allowed for the contig k141_3786, column 1:
-1.3731e+09`. That is a coverage-floor artifact of the subsample, not a chain defect, and
it means a 200k-pair pilot CANNOT validate steps 10-18 on either arm -- das_tool has no
three tables to consolidate. Anyone reusing this pilot size should expect the same and
size the pilot by binner viability rather than by wall clock.

IMAGE GAP FOUND AND CLOSED BEFORE THE RAMP. `das_tool:1.1.7--r44hdfd78af_1` was absent
ENTIRELY from /scratch/phyberos/cache/apptainer and `cami-amber:2.0.7` was present with no
.verified stamp. Cause: run_cami_metag.py's CONTAINERS list omitted "das_tool", so
`setup --run` never pulled it, and on a compute-node driver step 16 would have HUNG rather
than failed. Closed with Agent.MaterialiseImages('wqjsf1Et') from login3 -- fetched 2,
already_present 12 -- which reads the STAGED run's own env manifest and therefore cannot
drift from the source list the way CONTAINERS did.

L4 ADDENDUM 15:58. nf-core rung 1 control side, trace-confirmed as of this timestamp:
  MetaBAT2  CONFIRMED FROM A REAL TRACE. 50 bins written to
            GenomeBinning/MetaBaT2/bins/MEGAHIT-MetaBAT2-marine_sample_0.{1..50}.fa.gz
            plus discarded/{tooShort,lowDepth}.fa.gz, and
            GenomeBinning/depths/contigs/MEGAHIT-marine_sample_0-depth.txt.gz.
  SemiBin2, COMEBin  RUNNING (jobs 59545692, 59545702, 59545930).
  contig_to_bin/  STILL EMPTY, and that is expected rather than alarming: mag.nf:467
            collectFiles over the transposed post-binning channel, so the single
            four-column table lands only once every binner has finished. An empty
            directory here is not evidence of a missing step -- same trap the file
            already records for out_test1.
  CheckM2 (BIN_QC) and Flye  still source-confirmed only.

CAUTION FOR WHOEVER REPORTS THE METABAT2 ROW. The control's 50 bins were produced at
nf-core's min_contig_size = 1500. The metasmith arm's MetaBAT2 ran at --minContig 2500,
because the driver's `metabat2_min_contig=1500` pin never reaches the transform (it is
present in workflow.params.yml and absent from .command.metadata; the transform's own
default 2500 is what is used). So a MetaBAT2 bin-count or AMBER comparison drawn before
that pin is delivered is comparing two different contig floors, and the DAS Tool row
inherits the same confound through das_tool consuming metabat2's table. COMEBin and
SemiBin2 rows are not affected. Orchestrator owns the fix.

## PER-BINNER PARAMETER PARITY — the completed audit, 2026-09-12

Read from nf-core/mag 5.5.0's own `conf/modules.config` + `nextflow.config` at the pinned
revision, and from each tool's own `--help` inside the pinned image. Not from documentation.

**This audit had covered metabat2 only. Auditing one binner of three feels like auditing
the binners, and it is how the SemiBin2 rows below survived.**

| tool | parameter | nf-core/mag 5.5.0 | metasmith | status |
|---|---|---|---|---|
| MetaBAT2 | `-m` / minContig | `1500` (`min_contig_size`, floored at 1500) | was 2500 | **PINNED to 1500** |
| MetaBAT2 | `--seed` | `1` (`metabat_rng_seed`) | `1` | matches |
| MetaBAT2 | `-s` / minClsSize | not passed -> 200000 | not passed -> 200000 | **not a deviation** |
| MetaBAT2 | `--unbinned` | passed | not passed | extra OUTPUT only; binned assignments unchanged |
| SemiBin2 | `--min-len` | `1500` (`min_contig_size`) | not passed -> `--ratio` 0.05 of 2500 bp | **PINNED to 1500** |
| SemiBin2 | `--random-seed` | `1` (`semibin_rng_seed`) | **not passed -> system-chosen** | **PINNED to 1** |
| SemiBin2 | `--environment` | `global` (`semibin_environment`) | `global` | matches |
| SemiBin2 | `--sequencing-type` | `short_reads`/`long_reads` by platform | not passed | **OPEN, long-read lane only** |
| COMEBin | all args | none passed -> defaults | `-b min(usable_contigs, 1024)` | **not a deviation** (its default IS 1024) |
| DAS Tool | `--score_threshold` | `0.5` (explicit) | not passed -> `0.5` | matches |
| DAS Tool | `--search_engine` | `diamond` | `diamond` | matches |

**Two notes that change how these rows should be read.**

SemiBin2's `--min-len` is a different **rule**, not a different number: unset, SemiBin2
derives its floor from `--ratio` (0.05, relative to 2500 bp) rather than from a fixed
length. Matching the number is what makes the two arms answer the same question.

**Establishing the three non-deviations was worth as much as the three fixes.** Guessed,
metabat2's `-s`, COMEBin's `-b` and DAS Tool's threshold would each have gone into the
deviations table as a difference. COMEBin in particular looks like a deviation and is not:
the clamp only bites on inputs too small for COMEBin to run at all.

**CAUTION every pin in this table was INERT until 2026-09-12.** A driver's
`RunWorkflow(params=...)` reached nextflow only; `context.params` came solely from
`.command.metadata`. metabat2's `--seed` read correctly the whole time **because the
transform's own default happened to equal the pinned value**, which is precisely why
nobody checked `--minContig`. Verify a pin from the RENDERED command line in the task's
own `.command.out`, never from the driver source and never from `workflow.params.yml`.

**Which rung's numbers are usable.** Transform sources are staged PER RUN, while the params
fix lives in the engine overlay. So rung 1 (`iy8YLaGr`, staged before the SemiBin2 edit)
yields a parity-correct **MetaBAT2** row and a parity-correct **COMEBin** row, but its
SemiBin2 row — and therefore its DAS Tool row, which consumes all three tables — is not
comparable. Those wait for rung 10.

## metaGEM's own binning parameters, for criterion 6

From `config/config.yaml` in metaGEM's repository, with `workflow/rules/metabat_single.smk`
showing where each lands:

    minBin: 1500        -> metabat2 -m  (the CONTIG floor)
    metabatMin: 50000   -> metabat2 -s  (the BIN SIZE floor; metabat2 defaults to 200000)
    seed: 420           -> metabat2 --seed

**The names mean the opposite of what they suggest** — `minBin` is the contig floor and
`metabatMin` is the bin-size floor. So metaGEM's contig floor is also 1500, which is where
the whole campaign converges, but **its minimum bin size is 50000 against our 200000**, so
our arm discards bins metaGEM keeps and would under-count models in criterion 6.

**OPEN parity item for the metaGEM arm, not fixed:** `-s 50000` needs a
`metabat2_min_cls_size` param and a pin in `run_metagem.py`. It affects only that arm's
single-end lane (48 runs, unlaunched; the live li2019 paired lane bins with MetaWRAP), and a
second transform edit while three runs are in flight is not worth it. Recorded with the
exact value rather than as "check the binner settings".

---

## REFERENCE-ARM AMBER SCORES — delivered 2026-09-13, job 59654244

First real AMBER numbers in the campaign. `marine_sample_0`, nf-core/mag 5.5.0 under the pinned
`control.config`, AMBER 2.0.7, gold standard built through the read-truth bridge by
`score_reference_amber.py` — a transcription of `cami_contig_truth.py` and `amber.py` in the same
containers, so the two arms' `results.tsv` are comparable by construction rather than by inspection.

| label | precision_bp | recall_bp | f1_bp | accuracy_bp | ARI_bp | misclass_bp | bins |
|---|---|---|---|---|---|---|---|
| COMEBin | 0.7653 | **0.4161** | **0.5391** | 0.4588 | 0.8561 | 0.1165 | 105 |
| **DASTool** | **0.9737** | 0.1262 | 0.2235 | 0.2351 | **0.9670** | **0.0280** | 31 |
| MetaBAT2 | 0.8993 | 0.0843 | 0.1542 | 0.3156 | 0.4239 | 0.2642 | 50 |
| SemiBin2 | 0.8411 | 0.0776 | 0.1421 | 0.3618 | 0.8625 | 0.1075 | 77 |

**DAS Tool's row is the reported one**, per the principal's correction that the per-binner sets are
intermediates. Read it carefully: **three of its four quality measures are the best in the table** —
highest precision, highest ARI, lowest misclassification — and its recall is the second lowest. It
keeps 31 bins from 16,671 contigs where COMEBin keeps 105 from 62,459.

**So refinement buys purity at a large cost in completeness on this sample, and COMEBin alone beats
the refined set on f1 (0.539 against 0.224).** State that in the results rather than letting an f1
comparison imply DAS Tool underperformed; it is the purest set by every purity measure available.

MetaBAT2 is the weakest here on every measure (ARI 0.424, misclassification 0.264) — and that is the
reference arm's own MetaBAT2 at nf-core's pinned `-m 1500` and `--seed 1`, i.e. the settings our arm
now matches.

**Splitter accounting, all 193,896 rows placed:** 144,998 kept + 48,898 unbinned; COMEBin
62,459/62,459, MetaBAT2 34,562/34,562, SemiBin2 31,306/31,306, **DASTool 16,671/65,569** with the
remainder being the `DASToolUnbinned` pseudo-bin. Bridge quality: **398,702 of 400,744 contigs
voted, 28,686,328 of 28,686,328 mapped reads matched to truth.**

**STILL MISSING: the metasmith half for this sample.** Rung-1 run `iy8YLaGr` was reclaimed, so it
must come from `C1IM6IG3` or `WfOlaqLT`, both of which include `marine_sample_0`.

    CAUTION two column names that do NOT exist, and a missing awk key fails silently by printing
    the whole row: ARI is `adjusted_rand_index_bp`, never `ARI_bp`, and there is no `bin_counts`
    column at all -- the bin counts above are `wc -l` of metrics_per_bin.tsv minus its header.

    CAUTION the first run of this scorer emitted THREE labels and looked complete. All 65,569
    DAS Tool rows were dropped because DAS Tool writes the assembler's FULL FASTA HEADER as the
    contig id (`k141_10 flag=1 multi=2.5908 len=1055`) while the three raw binners write the bare
    name, and nf-core concatenates both shapes into one file. Trim at whitespace first. The scorer
    now also refuses to proceed when any binner present in the table contributes zero rows.

================================================================================
2026-09-13. WHICH ARM'S AMBER ROWS CAN BE JOINED BY IDENTIFIER, AND WHICH CANNOT.
The answer is per-transform, not per-library. Read this before pairing any number
in this file with any number in the other arm.
================================================================================

The sample identifier in a `results.tsv` comes from whatever the TRANSFORM passes as argv[4]
to `lib::cami_gold_standard.py`. Both paths call the SAME lib script -- its line 85 is
`f.write(f"@Version:0.9.1\n@SampleID:{sample_id}\n\n")` in both -- so the difference is
entirely in the caller, and reading the lib script tells you nothing about which name you get.

    STANDARD library                        E2's PINNED copy (WfOlaqLT, 33hlLu8Q)
    cami_contig_truth.py:33                 e2/gold_standard.py:25
      sample_id = Path(iasm.local).stem       sample = json.loads(imeta)["sample"]
      -> the ASSEMBLY PRODUCT FILENAME        -> the CAMI SAMPLE NAME from read_metadata

Measured on the artifacts, both sides:

    C1IM6IG3, standard library      @SampleID: 1-1-1.1fd5a140e91817fb-DVPQRF16
    WfOlaqLT, pinned e2 copy        @SampleID: strain_sample_26
                                               toy_hmp_airskinurogenital_sample_11
                                               toy_hmp_airskinurogenital_sample_14
                                               strain_sample_0
    reference arm, job 59654244     Sample:    marine_sample_0   (all four labels)

SO THE PAIRING IS FINE FOR THE ARMS THAT MATTER. WfOlaqLT and 33hlLu8Q label by CAMI sample
name, which is the same namespace `score_reference_amber.py` writes, so their AMBER rows join
the reference arm's directly on `Sample`. No mapping table and no transform edit is needed.
B21's 29188ea6 guard compares that @SampleID against read_metadata's `sample` -- the same
value -- so the guard is self-consistent on this path.

WHAT IS GENUINELY LOST: C1IM6IG3 only. It ran the standard library, so its ten leaf-stem
@SampleIDs carry no sample name, and the run is deleted, so its lineage cannot supply one
retrospectively. Job 59660273 tests whether the surviving cami `task_cache` manifests can map
an assembly instance back to a sample and so salvage its one valid row (prec 0.9734 /
ari 0.9703 / mis 0.0269). Until it returns, that row is real but unattributed.

CAUTION do not pair an 0.9734 metasmith row with an 0.9737 reference row because the numbers
are close. Two AMBER rows agreeing to three decimals is not evidence they describe the same
sample, and on C1IM6IG3 there is no identifier to check it against. ARI differs at the third
decimal (0.9703 vs 0.9670), which is what a genuinely different binning of the same sample
would also look like -- and what a different sample would look like too.

CAUTION reading `src/` tells you what the STANDARD library does, and a run stages its own
pinned transform tree at `<run>/_metasmith/task/transforms/`. Check there, per run, before
asserting a source-level mechanism applies to a live lane. Verifying a mechanism in a source
file is not verifying that a run reaches it -- the same error as the B12 long-read exposure
claim, which was also true of `src/` and false of the run.
