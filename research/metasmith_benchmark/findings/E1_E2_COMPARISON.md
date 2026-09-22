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

## The runtime trap

**E2's own run keys do not record the time E2's work took.** `MjMN02CK` carries 211 cache entries for 208
samples across ~12-15 steps — almost everything was served from cache and never re-tagged. The products that
actually executed sit under `WfOlaqLT` and its siblings, whose run directories were deleted waves ago.

So a runtime comparison sourced from `MjMN02CK`/`VgUw0A7c` measures cache-hit time, not compute time, and the
two differ by orders of magnitude. Decide explicitly which question the headline answers — *"time to produce
these results from scratch"* or *"time this invocation took"* — and source it from the run that did the work.
Nothing in the repo distinguishes them for you.

Two further instruments that do not work here:

- **`sacct --format=TotalCPU` is blind on this cluster.** It reads `00:00:00` for every job, RUNNING and
  COMPLETED alike, because CPU accounting is not gathered. Verified against COMPLETED jobs that plainly did
  work. `Elapsed`, `Submit`, `Start`, `End` are sound; the working "is it stuck" instrument is
  `srun --jobid=<id> --overlap -n1 ps -eo etime,time,pcpu,rss,comm --sort=-time`.
- **`nxf.log` is UTC and Slurm is local PDT**, a seven-hour offset. DEBUG `ProcessConfigBuilder` and
  `Creating process` lines are process *definition*, not submission — only `Submitted process >` counts. Use
  `sacct -X` for any per-job tally; without `-X` a tally inflates ~3x.

`drivers/step_refs.py:20` recovers a step name from a task dir via `#SBATCH -J nf-p\d+__(<transform>)_\(`. That
regex is the join key between a metasmith task and its transform, which is what runtime-per-process needs.

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
- **E1's outputs are inside two tars** on fir scratch with no off-site copy: `e1_long.tar` (13,787 members) and
  `e1_short_out.tar` (57,253 members). Any E1-side metric reads from inside them, and extracting re-accrues the
  inodes E1's retirement released — which competes directly with E4's remaining chunks.

## Suggested order

Cheapest first, and the early items force you to solve "enumerate each arm's bin directory", which every later
item needs.

1. MAG contiguity — one seqkit call, identical on both arms.
2. Assembly length and N50 — same call on the contigs FASTA. Recomputing both arms with one tool is more
   comparable than mixing E1's archived QUAST numbers with seqkit numbers.
3. N MAGs — trivial once (1) and the CheckM2 tables are in hand; the only real decision is the quality gate.
4. % reads mapped — small code, large I/O. E1 short's 208 BAMs are ~700 GB; E2's are 1.87-4.89 GB each.
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
