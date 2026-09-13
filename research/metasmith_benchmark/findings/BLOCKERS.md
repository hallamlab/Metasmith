# ACTIVE BLOCKERS — CAMI/Pratama/metaGEM benchmark campaign

Live tracker. **Rewrite rows in place; do not append a history** — the plan file's run log is
the append-only record, and a tracker that grows becomes a changelog nobody trusts.

Columns: what is blocked · why · who owns it · the exact next action. A row leaves this file
only when its next action is done AND verified by product.

Last updated 2026-09-13 04:45 PDT. **Wave 1 is LAUNCHED** — nine lanes running; the capability-proving pivot is over. The binding constraint is now BYTES (16.6 of 18.63 TiB), not inodes (46%).

---

## B1 — VirSorter2 database  ·  **CLOSED 22:18** — staged, env verified by capability

`virsorter setup -d /db -j 8` ran on login3 in **8 minutes** and passed its own verification:

    ok  Done_all_setup / group / hmm / rbs / conda_envs
    ok  conda_envs/671930f2   <- the name `virsorter run --db-dir /db` computes
    env imports: sklearn 0.22.1  pandas 1.2.5  numpy 1.23.5   (vs2.yaml's pins exactly)
    inodes at DEST: 28,732        11 GB at /scratch/phyberos/refs/virsorter2_2.2.4

**The env had to be BUILT under the `/db` bind, not staged.** snakemake 5.26 names a conda env
`md5(realpath(conda_prefix) + env_yaml_bytes)[:8]`, so the name depends on the mount path;
`downloadVirsorter2DB` builds it in a per-run work dir while the consumer always binds `/db`, and
those can never agree for any path. Conda envs are not relocatable — activation scripts and
shebangs bake the prefix — so a prebuilt tarball, including the lab's own
`virsorter2/virsorter2_data.tar.gz` on chinook, would NOT have fixed this.

**Retry scaling FIXED**: scaled retries now double, matching the transforms' own `2^(n-1)`, so a tail task that needs more than the first attempt's ceiling actually gets it — the flat `withName` block that replaced the preset's `task.attempt` closure had silently removed the ladder.

**Owner:** peer (`msm bench`). **Next action:** register `annotation::virsorter2_db` →
`/scratch/phyberos/refs/virsorter2_2.2.4` in **`STAGED_REFS_PRATAMA`**, not the shared dict —
an `annotation::` entry in `STAGED_REFS` makes every arm fail to plan with
`AssertionError: namespace [annotation] not found`, because the five registration sites load
different type-library lists. Add `annotation.yml` to the globals builder's list, re-measure keys
(CAMI's three must not move), verify `virsorter2` binds it as exactly one given leaf, and relaunch.
Afterwards apply the same `/db` bind to `downloadVirsorter2DB` itself — this run is its test.

## B3 — DRAM staging incomplete  ·  BLOCKS criterion 2's AMG calls  ·  CAUSE NOW PROVEN

`dramv` has RUN and FAILED against the incomplete database — three attempts, zero products:

    Error: Invalid file path or buffer object type: <class 'NoneType'>

DRAM-v handing a **null config entry** to a file reader: the all-null `DRAM.config` surfacing
ONE STEP LATER than its cause. Predicted hours before the step ran, now confirmed to the error
string. Its retries have not exhausted yet (ignored-step count still 1), so the swallow has not
happened — but when it does, the run reports complete with the AMG output silently absent.

**DOWNLOADS ARE DONE AND THE CONFIG IS POPULATING — the all-null state was a stage, not a
failure, as predicted.** `Pfam-A.full.gz` finished at exactly **23,991,810,911 bytes**, the
source's own Content-Length (measured, after two wrong guesses from me: first "~14 GB", then a
worry it might be 48). dbCAN landed. The log now reads `Moved kofam_ko_list to final
destination, configuration updated` and `Processing pfam`, and the config carries
**2 search databases** where it carried none. The five sheets come last and remain the gate.

**RESOLVED 22:09 — and the staging had ENDED EARLY AND SILENTLY 1.5 h before I looked, with
0 of 5 sheets. The watcher did not say so because it asked `pgrep -f stage_dram.sh`, which
matched its own remote `bash -c` wrapper's argv and reported ALIVE forever.** The stager's log
ends `DONE 2026-09-12T20:35:52` with `APPTAINER_EXIT=1`, and `ps -u phyberos -o args=` on
login3 showed no such process at all.

**The cause is a non-2xx body becoming the file, this campaign's documented failure shape.**
`prepare_databases` processes its `--select_db` list in order and RAISES on the first failure,
and dbcan sat ahead of the sheets:

    The subcommand ['hmmpress', '-f', '/db/dbCAN-HMMdb-V11.txt'] experienced an error:
    Error: File format problem in trying to open HMM file /db/dbCAN-HMMdb-V11.txt.
    Format tag is '<!DOCTYPE': unrecognized.

`dbCAN-HMMdb-V11.txt` is **19,313 bytes of HTML** — the upstream URL is dead. So kofam and pfam
landed (24 GB of Pfam-A.full, the mmseqs profile build, all of it real and kept) and everything
ORDERED AFTER dbcan — viral, peptidase, vogdb and **all five distillation sheets** — was never
reached. The sheets are not a separate download; they are written at the end of the same call.

**Fixed with the tool's own narrower subcommand rather than by re-running the whole thing.**
`DRAM-setup.py update_dram_forms --output_dir /db` fetches exactly the distillate/liquor forms
and updates the existing config in place. CAUTION: do NOT re-run `stage_dram.sh` to get them —
its first act is to copy the package's blank `CONFIG` over `/db/DRAM.config`, which would ERASE
the kofam and pfam entries that took hours to earn.

Verified BY CONTENT, not by exit code — all five are real TSVs with real headers, not HTML:

    genome_summary_form     580,242 B   gene_id|gene_description|module|sheet|header|...
    module_step_form        579,664 B   gene|ko|module|module_name|path|product_ids|...
    etc_module_database       2,378 B   module_id|module_name|complex|definition   (20 rows)
    function_heatmap_form    11,199 B   category|subcategory|function_name|...
    amg_database             21,569 B   KO|EC|PFAM|gene|module|metabolism|reference|...

and the three search databases plus `pfam_hmm` survived the update. **So `DRAM-v.py distill` can
now run and criterion 2's AMG calls are reachable.** A backup of the pre-update config is at
`DRAM.config.bak.1789276148`.

    CAUTION a glob for `etc_module_database*.tsv` matches NOTHING. DRAM's own filename is
    `etc_mdoule_database.20260912.tsv` -- an upstream typo, carried into the config value.
    My first content check reported that sheet as empty because of it; the file is fine.

**DEVIATION, stated rather than hidden: dbcan is ABSENT from the staged tree.** DRAM-v annotate
will produce no CAZyme annotations. The AMG calls need kofam, pfam and the amg_database sheet,
all of which are present. Re-adding dbcan needs a working URL, not a re-run.

**Owner:** orchestrator. **Next action:** none — closed. The Pratama relaunch picks the config
up at run time. VirSorter2 staging (B1) was released the moment this cleared.

## B4 — nf-core's `contig_to_bin_map.tsv` has not landed  ·  BLOCKS criterion 12's reference half

`mag.nf:467` gathers it driver-side via `collectFile` + `storeDir` only after EVERY binner
finishes. COMEBin was `CANCELLED` when L3's own `timeout -k 30 10800` wrapper SIGTERM'd a
healthy driver at exactly 3h00m00s; relaunching with `-resume` and a wall sized to the task.

**Owner:** L3 (`ad8899af8b5a916b0`), watch `buizv86av`. **Next action:** when the table
lands, `sbatch /scratch/phyberos/score_reference_rung1.sbatch`. Both risky joins are already
pre-verified against real bytes: BAM-to-assembly overlap 400,744 of 400,744, and the
read-name join closes after the mate-suffix strip. Pass `--nfcore-contig-to-bin`, never
`--contig-to-bin`, and `--lib` must name the FILE.

## B5 — **MEASURED, 4 of 10**: COMEBin at 48 cpus is 6h35m–9h37m, MaxRSS 51.53–61.85 GiB (32% of the 192 GB grant). The two figures previously on record are the two FASTEST tasks

CAMI rung 1's COMEBin (job 59548383, 48 cpus) has been **PENDING (Resources) since 16:35** —
queued, not stalled. A Pratama-arm COMEBin IS running healthily at 48 cpus, which gives a
48-cpu data point but not criterion 9's, since that names a CAMI sample. das_tool and the
MAG-level AMBER row both wait behind it.

**Owner:** L4 (`a6a51efe509e7c8f9`). **Next action:** `sacct MaxRSS` + elapsed once it
finishes. The reference arm's COMEBin rerun is running under a 16 h limit, which also
CLOSED a standing unknown: a task does auto-route past fir's 3 h band. For calibration nf-core's COMEBin on the same sample ran 36+ min healthily, so a
long COMEBin is not a stall; the real deadlock signature is zero `cluster_res` files.

## B6 — **RESOLVED on both sides.** 149,336 inodes reclaimed from nine dead runs + a one-inode squashfs replacing 424,034. The binning term stays unmeasured and is no longer a gate

Everything else is measured: ordinary task 9 inodes, cached shard ~6, per-bin product shard
130-170, reference-DB staging 16.5K-31.7K **once per agent home**. L4 measured a whole
1-sample non-binning chain at 302 files + 86 dirs. Quota **452,297 of 1,000,000** and climbing
as binning starts.

**NEW 2026-09-12: the per-bin term is far larger on real soil than on CAMI, and the CAMI
measurement alone would have under-projected it.** The inode law is `bins + 12` per checkm task,
confirmed exactly on two CAMI points (51->63, 57->69). The Pratama run recovers **146 bins
(metabat2) and 209 (semibin2)** on one groundwater run against CAMI's 51 and 57 — so a Pratama
checkm task is ~221 inodes, **3.2x** the CAMI figure, across 66 runs. Bin recovery is a property of
the sample, not of the pipeline, so a per-sample inode projection taken from CAMI does not transfer
to Pratama and must be measured per corpus.

**RESOLVED ON BOTH SIDES 2026-09-13, and neither side was the projection.** The gate was never
going to be closed by measuring the binning term more precisely — it was closed by removing two
fixed costs.

    reclaim of nine dead run dirs   866,253 -> 716,917   (149,336 freed, task_cache untouched)
    GTDB reps tree -> one squashfs   a further 424,034 available on deletion

`release232_skani_genomes.sqfs` is 192,112,726,016 bytes holding 199,923 genomes and 224,111
directories — **424,034 inodes inside, one on the quota.** Genome count verified against the tree
by the build's own check. `-noD -noF` is correct and its evidence is the 99.98%-of-uncompressed
figure: the payload is already-gzipped `.fna.gz`, so recompression would spend CPU for nothing.

Two figures I had wrong and had put in front of a budget question: the tree is **424,034** inodes,
not the 426,970 I was quoting (199,923 + 224,110 + root = 424,034 exactly; the excess was quota
accounting around the parent path), and the image did **not** build on node-local disk — its log's
line 2 writes straight to Lustre. Neither changes the decision; the first changes what the deletion
frees, and the second was an assumption about a script I never opened.

**The binning term remains genuinely unmeasured** and that is now a curiosity rather than a gate:
149,336 + 424,034 against a ~212K estimate for E1–E4 plus E4's ~308K chunked peak means wave 1 fits
with room, which is why the experimenter authorised launching before E4 and logged it as a
deviation. Wave 1 was submitted with the quota gate checked first (725,419, under the 800K stop).

**Owner:** this session. **Next action:** the tree deletion is HELD on e4_gtdbtest (59635386)
showing a classification row after `Traversing tree to determine classification method`, read from
the task's own log. Never the Slurm state — 2.6.1's post-placement ANI step is not gated by
`--skip_ani_screen`, so it can burn 33m44s of MSA masking and pplacer and then die on a missing
skani reference while its enclosing job exits 0:0. Watcher `bw92l3ega` emits on that line, on
`Reference genome missing from skani database`, and on a summary product — and deliberately not on
job state, since state is the one signal that cannot tell those apart.

**Superseded below, kept because how the gate was framed is worth more than the framing was:**

**Owner:** L4, binning began 16:35. **Next action:** `/scratch/phyberos/cami/l4_pilot/inode_census.sh iy8YLaGr`
once binning completes, files and dirs counted SEPARATELY — a `find -type f` census
under-reports the Lustre project quota by ~12% because directories are inodes.

## B8 — metaGEM's `-s 50000` is unpinned  ·  affects criterion 6

metaGEM's `config/config.yaml`: `metabatMin: 50000` (metabat2's `-s`), against metabat2's
default 200000 which we use. Our arm discards bins metaGEM keeps, so it under-counts models.
Its contig floor `minBin: 1500` now matches ours. CAUTION metaGEM's two names mean the
opposite of what they suggest.

**Owner:** orchestrator. **Gate:** low urgency — affects only the single-end lane (48 runs,
unlaunched); the live li2019 paired lane bins with MetaWRAP. **Next action:** add a
`metabat2_min_cls_size` param and pin 50000 in `run_metagem.py` before that lane launches.

## B9 — SemiBin2's `--sequencing-type` is not passed  ·  long-read arm only

nf-core passes `short_reads`/`long_reads` by platform; we pass neither. Irrelevant to the
short-read comparison, wrong for the CAMI long-read arm.

**Owner:** orchestrator. **Gate:** that arm is unlaunched. **Next action:** pass it from the
read metadata's `length_class` before the long-read arm launches.

## B10 — rungs 10 and 229 need authorization  ·  a gate, not a defect

Rung 10 is the first rung whose AMBER numbers are reportable, since rung 1's SemiBin2
predates the seed/min-len fix and DAS Tool consumes all three tables. Rung 229 is the
campaign's largest single compute commitment.

Rung 10's key against the CORRECTED shape is **`C1IM6IG3`** (12 steps / 10 samples),
measured twice in-tree by L4. The earlier `3zumz7xT` was the 18-step shape and is dead.

**Owner:** principal. **Next action:** rung 10 on the orchestrator's say-so once rung 1
reports; rung 229 explicitly the principal's.

---

## B11 — **FIXED AND VERIFIED at the level of use.** `carveme_from_orfs_cplex` renders 12 h / 16 GB / 4 cpu with BOTH doubling on `task.attempt`; the retry ladder is intact, not flattened

**FIXED AND VERIFIED AT THE LEVEL OF USE, 2026-09-13.** Read from the rendered `.command.run` of a
live task in `lE94xbfH` (E4 chunk 1), not from the transform's declaration — a Nextflow config
selector beats the declaration, so the source cannot answer this:

    #SBATCH -c 4        #SBATCH -t 12:00:00        #SBATCH --mem 16384M
    #SBATCH --nodes=1 --ntasks=1 --account=rrg-shallam-ab

and the selector in that run's own `workflow.config.nf`:

    withName: '.*__carveme_from_orfs_cplex' {
        cpus   = 4
        memory = { 16.GB * (2 ** (task.attempt - 1)) }
        time   = { 12.h  * (2 ** (task.attempt - 1)) }
    }

So the ladder is 16 GB / 12 h → 32 / 24 → 64 / 48 → 128 / 96.

**The wall.** 12 h against a measured distribution of min 131 s, p50 380 s, p90 1187 s and a worst
case just under 2 h — 6× the old wall, so the 5-of-100 biased loss goes to zero.

**The memory, which is the half I had to correct myself on.** My first CarveMe resource report was
survivorship-biased: I quoted headroom from COMPLETED tasks, and the **failing** tasks reach
**14.63 GiB of 16**. Attempt 1 therefore has only ~1.37 GiB of margin and some tasks will genuinely
retry. Attempt 2's 32 GB is **2.2× the observed failing peak**, so the ladder carries it. That is the
right shape — a tight first attempt retrying into headroom, rather than paying 32 GB across ~2,000
per-MAG tasks.

**And the retry ladder is intact rather than flattened, which is not automatic.** This project has a
recorded defect where a flat `withName` block REPLACES the preset's `task.attempt` closure and
silently removes the ladder: the block reads correctly, attempt 1 runs fine, and a task that needs
more never gets it. Here both values are closures over `task.attempt`, so the scaling is real. The
failure mode is invisible until something needs a second attempt, which is why this was worth
checking rather than assuming.

**Cause.** The transform declares `Duration(hours=2)`. Measured over 100 gapfill tasks in the live
li2019 run (job 59588214): min 131s, p50 380s, p90 1187s, and **5 of 100 FAILED at the wall**
(`ExitCode 140:0`, `Elapsed 01:59:1x`, `Timelimit 02:00:00`). 95 COMPLETED.

**Why it was missed.** Two single-bin probes were taken — smallest bin 32m00s, largest bin 39s —
and the inverted relationship between them was read as "cost does not track model size, 3.75x
headroom". The samples sat near p95 and p5 and bracketed nothing. The open question recorded at
the time ("could a middle-sized bin beat both?") is now answered: yes.

**Why it matters more than 5%.** The loss is BIASED, not random: it drops whichever MAGs are
hardest to gapfill, which are plausibly the fragmentary ones the comparison most wants to see.
Under retry-then-ignore the run reports complete with those models simply absent.

**Owner / gate.** The experimenter's driver work. **Next action:** raise the declared duration to
12h (fir band 2 — a 16h request is already confirmed to auto-route past band 1, job 59548173).
NOT changed mid-run: a transform edit retires that transform's cache shards, and the live run holds
140 models whose gapfills would all re-run.

## B12 — **CLOSED AT EXECUTION.** The preset heuristic misleads BOTH standard flye transforms on CAMI's synthetic Q40, but **no R1 lane runs either** — E2 long uses a pinned platform-keyed `e2/flye.py`, proven running past the disjointig stage

**CLOSED AT EXECUTION 2026-09-13. The pin works on real data, not just in the staged source.**
A live E2-long task (`33hlLu8Q/nxf_work/b2/fff5a226d45406f267600a5be4e114`), 6 minutes in, from
the tool's own stdout:

    04:17:10  Starting Flye 2.9.5-b1801
    04:17:41  Total read length: 3931523370 · Reads N50/N90: 3144 / 1228 · Minimum overlap 1000
    04:17:41  >>>STAGE: assembly  ·  Assembling disjointigs
    04:18:09  Counting k-mers:  0% .. 100%
    04:20:54  Filling index table (1/2)  0% .. 100%
    04:23:13  Filling index table (2/2)

**That is past the exact stage the HiFi preset died at** — `No disjointigs were assembled`, at
~9m43s, immediately after "Assembling disjointigs". This task counted k-mers to 100% and is
filling index tables. 41 tasks staged, 9 with log content, 42 queued, `--mem 65536M`.

Read profile confirms the long reads: 3.93 Gbp, N50 3,144, N90 1,228, consistent with the
2,789-2,801 bp medians measured across the CAMI long-read trees.

    STILL TO PROVE: a completed assembly's contig count and product size. This shows the preset
    is right, not that the assembly finishes.

    CAUTION 1 the production image is Flye **2.9.5-b1801**. The A/B that established B12 ran on a
    hand-pulled **2.9.6-b1802**, a different build. The preset logic is in the transform so the
    finding holds, but wall times are not comparable to arm B's 1h05m41s.

    CAUTION 2 there is NO `flye` invocation line to grep for. metasmith's protocol does not echo
    its command, so `.command.log` carries only the tool's own stdout. An hour was spent grepping
    for a rendered command that can never appear. The banner `Starting Flye 2.9.5-b1801` is the
    signal -- strictly better evidence than argv, the same way metabat2's `using minContig 1500`
    banner beat reading its command line.

**WIDENED 2026-09-13: this is BOTH flye transforms, and E2 long's live plan uses the one it was
not recorded against.** `flye.py` and `flye_raw.py` differ in **exactly three lines**, and none of
them is the preset logic:

    flye.py      requires sequences::clean_long_reads (parents={oreads}) -> sequences::flye_assembly
    flye_raw.py  consumes oreads directly                               -> sequences::flye_raw_assembly

The branch is byte-identical in both, and its arithmetic reproduces the failing command exactly:
`q = min(33, mean_quality)`, so CAMI's NanoSim `AvgQual 40.00` clamps to 33 and
`10**(-33/10) = 0.000501` — the literal `--read-error 0.000501` that failed twice.

**Cleaning does not rescue it**, which was the one thing that might have. `flye.py` takes reads that
have been through `porechop_abi` and `chopper`; neither rewrites the quality encoding, and chopper
filters *out* low-quality reads, so `mean_quality` can only rise. The `>= 20` branch fires either way.

    RETRACTED 2026-09-13, within the hour, and the retraction is mine. The paragraph below
    claimed E2 long is live-exposed. IT IS NOT. E2 long runs a PINNED `e2/flye.py`, verified in
    the executing copy at `runs/33hlLu8Q/_metasmith/task/transforms/{FVWXPAYrmdVA,P5SuNEsk9Zf1}/flye.py`
    (1,584 bytes, against the standard file's 1,863):

        line 13  # read quality: NanoSim writes a flat Q40 over reads that align at ~15% error (B12).
        line 14  MODE = { "OXFORD_NANOPORE": "--nano-raw", "PACBIO_HIFI": "--pacbio-hifi",
                          "PACBIO_CLR": "--pacbio-raw" }
        line 26  mode = MODE[json.loads(...)["platform"]]
        line 42  Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24))

    NO `mean_quality` anywhere, the preset comes from the declared platform, and the pin's own
    comment cites B12. `Size.GB(64)` is 1.96x the measured 32.68 GiB peak, so the memory concern
    is answered too. `task/data` holds no flye.py at all, so unlike memote there is no second
    copy to confuse the check.

    So B12 stands for the STANDARD `flye.py` and `flye_raw.py` and **no R1 lane runs either**.
    The recommendation I made -- take the preset from the declared platform rather than from a
    simulator's quality string -- had already shipped before I raised the alarm. I checked the
    standard transforms, found the defect genuinely present in both, and then asserted the live
    lane used one of them without looking at what that lane had staged. **Verifying a defect in
    a source file is not verifying that a run reaches it** -- the same artifact-versus-capability
    error this campaign keeps making, in the one direction that produces a false alarm rather
    than a false all-clear.

**Live exposure: E2 long (`33hlLu8Q`), whose step list is**
`porechop_abi chopper flye minimap2_binning_bam comebin semibin2 metabat2 das_tool gold_standard`
plus four `checkm2` and one `amber`. When `flye` fails, retry-then-ignore leaves that entire tail
with nothing to consume and **the run reports complete**. Chopper was still ahead of flye in the
queue when this was found, so it is predicted rather than discovered.

Both transforms also declare `memory=Size.GB(32)` and `duration=Duration(hours=18)`. The successful
`--nano-raw` arm measured **MaxRSS 32.68 GiB = 102.1% of the 32 GiB ceiling**, on one sample and not
the largest of 172; it survived only because 128 GB was allocated deliberately so an OOM could not
destroy the measurement. So even with the preset fixed the memory declaration is at the edge —
recommend 96-128 GB. The 18 h wall is ample against a 1h06m success.

**Cause.** `flye_raw.py` reads `mean_quality` from `seqkit AvgQual` and branches
`q>=20 -> --pacbio-hifi --read-error 10^(-q/10)`, else `--nano-raw`. CAMI's NanoSim-simulated
Nanopore reads carry a **flat synthetic Q40** — verified over the first 20,000 records, the set of
distinct quality characters is exactly `{I}`. So `q=min(33,40)=33`, the branch fires, and the CAMI
long arm assembles Nanopore data as PacBio HiFi.

**Not a bug in the heuristic's own terms** — the reads really do say Q40. The defect is that a
simulator's invented quality string is being used as an error-model signal.

**MEASURED 2026-09-12, and it is worse than "the assemblies differ" — the HiFi branch produces NO
ASSEMBLY AT ALL.**

    arm                                      result      elapsed   MaxRSS
    A  asis     --pacbio-hifi --read-error   FAILED      0:09:43   21.5 GiB
    C  hifionly --pacbio-hifi, no error hint FAILED      0:09:57   21.9 GiB
    B  nanoraw  --nano-raw                   SUCCEEDED   1:05:41   32.68 GiB

Both failures are identical — `ERROR: No disjointigs were assembled - please check if the read
type and genome size parameters are correct`, then `ERROR: Pipeline aborted`. Flye names the cause
itself. **So arm C settles what the first A/B could not: the PRESET is the cause, not
`--read-error`.** On CAMI's NanoSim reads the long arm would have failed EVERY Flye step and, under
retry-then-ignore, reported complete with no long-read assembly whatever.

Arm B's assembly, inspected rather than counted from the log: **3,186 contigs, 129,618,595 bp,
N50 96,428, longest 2,295,550, GC 63.51%, mean coverage 18x.**

**AND THE SAME RUN EXPOSED A SECOND DEFECT IN THE SAME TRANSFORM: its declared memory FAILS ON
EVERY SAMPLE MEASURED, 4 of 4.** `flye_raw.py` declares `Size.GB(32)` → `'32.00 GB'`, and both
Nextflow and Slurm read `G` as 1024-based (Slurm says so in its own submit note), so the ceiling is
**33,554,432 KiB**. Array 59610638 plus the original arm B:

    sample                    input     elapsed    MaxRSS                vs 32 GiB
    plant_nano_sample_0       1.87 GB   1:05:41    34,269,012 K = 32.68   102.1%
    plant_nano_sample_10      1.87 GB   1:02:56    35,142,964 K = 33.51   104.7%
    toy_humangut_sample_0     4.68 GB   1:10:29    35,552,636 K = 33.91   106.0%
    toy_humangut_sample_16    4.68 GB   1:05:45    34,873,732 K = 33.26   103.9%

All four ran at 8 cpus — the transform's own declared count — under a deliberate 128 GB allocation
so a kill could not destroy the measurement.

**THE FLATNESS IS THE RESULT, NOT THE RANGE: a 3.8% span across two datasets and a 2.5x difference
in input size.** Peak RSS is essentially input-independent here, so the peak is a fixed structure
(repeat graph / k-mer index) rather than something scaling with read volume. Wall clock is equally
flat, 1:02:56 to 1:10:29. So **64 GB is 1.9x the observed maximum with real headroom** and is what
E2 now declares; 96-128 GB, which I recommended off one sample, is more than the data asks for.

    CAVEAT all four are CAMI SIMULATED data at 25-29x coverage. A fixed allocation can still have
    a size above which it grows, and nothing here probes a much larger or more complex library.
    Well supported for this corpus; not a number to carry to a real long-read metagenome.

**And the heuristic is not merely reading a synthetic value — it is reading one wrong by three
orders of magnitude.** Final alignment error rates across all four samples:

    toy_humangut_sample_0   0.124991      toy_humangut_sample_16  0.123047
    plant_nano_sample_10    0.148195      plant_nano_sample_0     0.154408

12-15%, about Q8-Q9, against quality strings claiming Q40 = 0.01% — off by a factor of ~1000-1500.

**`OXFORD_NANOPORE_HQ` IS RULED OUT for both datasets**, since HQ presets assume a percent or two.
But the error rate does **NOT** settle ONT versus PacBio for CAMI III: ~12% fits ONT R9 and PacBio
CLR equally. The shape leans, from Flye's own read stats — `toy_humangut N50 4663 / N90 2637`
(ratio 1.77, tight, pbsim-like) against `plant_nano 2341 / 711` (ratio 3.29, long-tailed,
nanosim-like) — so CAMI III leans **PacBio CLR**. A lean from distribution shape, not a
measurement; nothing on disk names the simulator.

    CAUTION each log prints the alignment error rate TWICE as polishing iterates -- 0.199886 then
    0.124991 for toy_humangut sample_0, 0.228127 then 0.148195 for plant. The FINAL one is the
    rate; quoting the first overstates it by ~60%.

**Owner / gate.** The experimenter's driver work. **Next action:** take the preset from
`read_metadata`'s platform / `length_class`, never from a quality score — a simulator's quality
string carries no error-model information.

## B13 — WITHDRAWN: COMEBin's 4-hour declaration is overridden, verified on a live task

I raised this as a driver requirement and the peer was right that both drivers already cover it.
Verified on `iy8YLaGr`'s running COMEBin rather than left as an inference:

    #SBATCH -c 48   -t 72:00:00   --mem 196608M
    .command.metadata, first line:   res 48/192 GB/1

`make_slurm_config()`'s `withName: '.*__comebin' { cpus = 48; memory = '192 GB'; time = '3d' }`
reaches **both** Slurm and `context.params`, so `comebin.py`'s declared
`Duration(hours=4)` never gets to the scheduler. `e2_cami.py` and `e3_pratama.py` both launch
with that config.

**What is worth keeping is the general rule, not the blocker.** A Nextflow config selector beats
the transform's own declaration, and the protocol is told the SELECTOR's number — so a transform's
declared resources are **inert** wherever a selector matches its process name. E2 binds its own
`library/transforms/e2/comebin.py` declaring 16 cpus / 64 GB / 48 h; its process name also ends
`__comebin`, so it will run at 48 / 192 GB / 3 d and its own numbers do nothing. Anyone reading
that file to learn the thread count gets the wrong answer, and editing those numbers will look
effective and change nothing.

The good half: because the protocol reads the selector's value, COMEBin's torch thread pool stays
matched to the allocation at 48. That is not automatic — it holds because `cpus` and `memory`
arrive through `res`, the same channel that left every *other* param silently inert until the
bootstrap fix. Confirm from `.command.metadata`, never from the driver's dict.

**Owner:** none — withdrawn. Both rules are in `WAVE3_STANDING_ORDERS.md`.

## B14 — `fastp --detect_adapter_for_pe` + `--interleaved_in` exits 0 writing 20 bytes  ·  FIXED, VERIFIED

**Found by probe 59624780, reproduced in isolation against the pinned image.** With
`--interleaved_in` there is no `--in2`, so read2 adapter detection tries to open an empty
filename — `ERROR: Failed to open file: ` — and **fastp exits 0** having written a 20-byte
(empty) gzip. Four attempts, 23-37 s each. The transform's product check is the only thing that
caught it.

**Fixed by feeding E2 short the split R1/R2 pairs** (`--in1`/`--in2`, `--detect_adapter_for_pe`
kept), which is also nf-core's own shape, so it REMOVES a deviation rather than adding one.
**Verified by probe 59625914:**

    before   23-37 s, exit 0, product 20 bytes
    after    COMPLETED 19:40, reports success, product 4,546,836,430 bytes

And the peer has since made `fastp` **fail on a pipe error or empty output**, so the exit-0-with-
nothing shape cannot recur silently in that transform.

**CLOSED AT SCALE 2026-09-13, on live wave-1 tasks rather than a probe.** E2 short (`WfOlaqLT`,
15 steps) ran 208 fastp tasks against the 208 split pairs. First 38 to land:

    products 1,813,613,892 - 1,817,839,687 bytes, ZERO under 1 MB
    fastp's own report on one task: Read1 6,656,479 before / 6,656,479 after, same for Read2

Against the defect's 20-byte gzip that is a ~90-million-fold difference, so it is not a marginal
pass. The full rendered invocation, read from `.command.out` because **`.command.sh` carries only
the metasmith bootstrap and the tool name**:

    fastp --in1 .../strain_sample_23_R1.fastq.gz --in2 .../strain_sample_23_R2.fastq.gz --stdout
          --json ... --html ... --thread 6 --detect_adapter_for_pe
          -q 15 --cut_front --cut_tail --cut_mean_quality 15 --length_required 15
          2> fastp.log | gzip -c > ....fq.gz || status=$?

    --interleaved_in 0   --detect_adapter_for_pe 2   --in1/--in2/--stdout 2 each

**The fix gives up neither flag I expected it to have to.** `--detect_adapter_for_pe` is kept and
`--stdout` is kept; the broken combination is dissolved purely by supplying two mates, so
`--interleaved_in` has nothing to attach to. That is structurally stronger than remembering to omit
a flag. It also finally settles my retracted hypothesis from the other direction — `--stdout` is
right there in the working command, so the flag I originally suspected was never the problem.

Read counts unchanged before and after is the expected answer on this corpus, not a sign the filter
did nothing useful: CAMI is simulated and adapter-free, and the earlier bounding measurement put
fastp's whole effect at 38 reads dropped for Ns and 8,226 adapter trims out of 33.3 M.

    CAUTION my first hypothesis was that `--stdout` was unsupported with `--interleaved_in` in
    fastp 1.0.1. Tested in isolation, that pair works fine. Only reproducing the FULL flag set
    isolated the adapter flag. Reproduce the whole command, not your guess at the relevant part.

## B15 — E5 declares no GPU for CLEAN  ·  BLOCKS both E5 lanes  ·  cause proven, fix is a scope choice

**e5_pratama 59635698 FAILED at 37 s, exit 1:0**, after staging cleanly as `8Z7x3L7z` at 35 steps:

    GpuRequirementError: workflow requires a GPU but none was declared for this run;
    offending transforms: clean (step 8)

Proven from the run's own artifacts, not from the traceback: `workflow.gpu.json` renders
`{"p08__clean": {"gpus":"required","gpu_memory_gb":16}}`, `workflow.step_8.meta` carries
`gpu {"gpus":"required","gpu_memory_gb":16}` with `step_name clean`, and `grep -nE 'gpus|Gpu\('`
over **both** `e5_pilot.py` and `_common.py` returns nothing — no call site passes `gpus=`. So the
manifest is correct and the driver simply never declares it. Step 8 is
`functionalAnnotation/clean.py`, the enzyme-function predictor.

**e5_cami shares the driver and fails identically**, which is why this is a two-lane blocker rather
than one lane's bad luck.

**Owner:** the experimenter (driver + target set). **Two fixes, and they are different
experiments:** declare it — `RunWorkflow(..., gpus=Gpu(memory=Size.GB(16)))` via `stage_and_run` —
or mask `clean` out of the E5 target set. The second is worth checking first: E5 is the taxonomy and
iPHoP pilot, and CLEAN may be reached transitively as an accidental passenger, exactly as DAS Tool
entered the Pratama arm because iPHoP was its only consumer. If nothing in E5 reads CLEAN's product,
masking is free and a GPU lane is not.

**A GOOD failure, and worth saying so because most of tonight's were not.** Loud, fast, honest exit
code (1:0, not 0:0), names the transform and the step, and it fired inside `RunWorkflow` **before
any grid job was submitted** — so nothing is orphaned and no teardown is needed. `8Z7x3L7z` is
staged, so a relaunch resumes rather than restages.

    LATENT ENGINE DEFECT, in the error message itself, and it points the fix the wrong way. The
    message ends "a GPU does appear to be present on the target [NVIDIA-SMI has failed because it
    couldn't communicate with the NVIDIA driver...]" -- it quotes nvidia-smi's FAILURE as its
    evidence that a card is present. `_detect_gpu_on_target` (gpu.py, called at
    workflow_ops.py:365) is testing for non-empty output rather than a zero exit, so any error
    text reads as a detected GPU. `fc20637` is an ordinary cpubase node with no card at all. Left
    as written, that sentence tells the reader to declare a 16 GB GPU on a node that has none.
    Same family as every other false positive here, inverted: the instrument reports PRESENCE
    from a failure, where the usual shape is reporting ABSENCE from a broken probe.

## B16 — WITHDRAWN. Not a defect: the index is populated at `record_run`, by design, and the shards serve hits

**WITHDRAWN 2026-09-13 within the hour. I filed this as an engine defect and it is not one.**
Measured with the engine's own predicate, `caching/invocation.probe(root, bytes.fromhex(key))`:

    1e201b0f49f0  PROBE=HIT  manifest_files=3  out_files=3  tombstone=False
    1e204aaf9255  PROBE=HIT  manifest_files=3  out_files=3  tombstone=False
    1e20f3748c39  PROBE=HIT  manifest_files=3  out_files=3  tombstone=False
    1e2002587689  PROBE=HIT  manifest_files=3  out_files=3  tombstone=False
    1e207ae9457b  PROBE=HIT  manifest_files=3  out_files=3  tombstone=False
    ===> 5 of 5 shards SERVE a cache hit

**Why my evidence was worthless: `cache explain` and `cache list` read the sqlite index, and a
running workflow does not.** `ops/cache.py:explain_cache_entry` calls `CacheStore.probe` (index).
The runtime calls `python -m metasmith.caching.invocation` (`nextflow_codegen.py:33`,
`Orchestrator.groovy:621`), whose `probe()` (`invocation.py:102`) reads **only the shard** — no
tombstone file, a manifest, every file the manifest lists present. Index rows arrive at
`caching/promote.py:record_run` (line 331), which reads each task's cache record and calls
`index_shard` for every "promoted" record, and the runner calls it when the run **ends**. So
`found: False` on a live run's shard is expected, and it accounts for every observation I had,
including zero tombstones and the physical-minus-index gap.

**The error: I tested reachability with the wrong instrument and then trusted a passing positive
control to license the conclusion.** The control proved `explain` works — it could not prove
`explain` was the right question. Two paths read this store and I checked the one a human uses
rather than the one a task uses.

    WHAT SURVIVES, in weaker form: `cache [promoted]` reports the shard WRITE, and indexing waits
    for `record_run`. So a driver killed before `record_run` leaves its shards unindexed -- still
    served to `invocation.probe`, but invisible to `cache list`, `gc` and any accounting built on
    the index. That is worth knowing when reading a store whose run did not end cleanly, and it
    is the whole of what this row should have said.

**The 208 matched pairs stand**: WfOlaqLT's fastp products exist in both `nxf_work` and the store,
identical size, different inode, **625.5 GB duplicated** — and the cache copy is usable. So the
E2-short prune is sound on both existence and reachability, gated only on the three consumers
reaching 208 distinct successful indices.


**Proven 2026-09-13 with a passing positive control.** After each successful step `bootstrap.py`
promotes the products and logs `cache [promoted] member [n] key [...]`. The shard directory and the
product file land on disk. **The index row is never committed.**

    cache explain 1e201b0f49f0f973…   found: FALSE   <- shard dir exists, 4.24 GB
    cache explain 1e204aaf9255b999…   found: FALSE   <- shard dir exists, 1.69 GB
    cache list --run WfOlaqLT                  0 entries
    cache list --dtype e2::trimmed_short_reads 0 entries  (also 0 with --include-tombstoned)

    POSITIVE CONTROL, same command and --cache-root, key taken from the index:
    cache explain 1e20a9f4a00c89b7…   found: TRUE    origin imported, 25,345,024,406 bytes

So `explain` works and the key format is right; the 208 keys are genuinely absent from the index.

**The products ARE duplicated on disk and the duplicate is unusable.** Matched by filename across
`runs/WfOlaqLT/nxf_work` and `task_cache`: **208 of 208 names in both, identical size, different
inode, 625.5 GB**. A `du` says the bytes survive a prune; a lookup misses and re-runs.

**CONSEQUENCES**

1. **The E2-short prune gate must stay HELD regardless of consumer counts.** Deleting the fastp work
   dirs loses the products in every practical sense. The gate was designed around *existence*; the
   binding question is *reachability*.
2. **Wave 2 cannot serve any of wave 1's finished work from cache.** Every relaunch re-runs from
   scratch unless Nextflow's own `.nextflow/cache` covers it — a different mechanism, valid only
   within the same work tree.
3. **~625 GB of orphaned shards** sit in the cami store: written, never indexed, reachable by nothing.
4. **`cache [promoted]` is not evidence of a usable cache entry.** It reports the write, not the
   commit. Do not use that line as a success signal.

    THIS ALSO KILLS THE TOMBSTONE HYPOTHESIS and one of my own measurements.
    cami      1,934 entries / 889.9 GB indexed · 1,419.8 GB physical · TOMBSTONED 0
    pratama     226 entries / 767.9 GB indexed ·   704.3 GB physical · TOMBSTONED 0
    Zero tombstones in either store. And pratama's physical is LESS than its indexed bytes,
    because `size_bytes` records the LOGICAL object and an imported entry is a reference whose
    bytes live outside the store. So physical-minus-indexed was never a reconcilable quantity --
    one counts stored bytes including unlisted shards, the other counts logical objects including
    external ones. My "522 GB unaccounted" was an artifact of subtracting two different measures.

**Owner:** the experimenter (engine). **Next action:** find where the promotion path commits, and
whether the row is written to a transaction that is never flushed, or written to a store the CLI
reads from a different root. A shard whose `output_root` is under `task_cache/1e/...` while indexed
imports sit under `task_cache/imported/1e/...` is worth checking as the discriminator.

## B17 — `gold_standard` fails on mousegut: polars infers `genome_id` as f64 · **COSTS CRITERION 12 FIFTEEN SAMPLES** (15 distinct indices across 52 retry dirs; my first count of 17 was a dir count)

**Live in E2 short (`WfOlaqLT`) 2026-09-13. 17 of 226 non-stub tasks FAILED, all the same cause.**

    **193 exit 0 · 15 exit 1**, counted by DISTINCT TASK INDEX from the run's nxf.log.
    The 15 failures span **52 work dirs** -- those are retries, and my first report of
    "17 failed, 97 running" was a DIR count read mid-flight. Third time in one session that
    retry dirs inflated a count I sent out; count by index, never by directory.
    failing indices: 93 95 97 98 99 106 115 117 118 122 152 176 184 185 196
    all 15 ->  work/mousegut_short_read/   ·   all offending values begin with `denovo`

    FIX SITE, identified by the experimenter: `resources/lib/cami_gold_standard.py` **line 41**,
    the `pl.read_csv(reads_mapping_path, ...)` call that infers types. Read `genome_id` and
    `tax_id` as strings, and pin the corrected script in the E2 benchmark library rather than
    editing the standard one, per the no-in-place rule.

    E2 LONG CANNOT HIT THIS, and for a better reason than mousegut being absent from it: every
    other dataset's `genome_id` is NON-NUMERIC FROM ROW 1, so polars infers a string
    immediately and never attempts f64. Measured over the first 20k rows of each truth table:
      plant_long_read_nano    0 denovo   Otu9.0 · RNODE_76_length_3393_cov_4.30851
      toy_humangut_long_read  0 denovo   pASV1 · ASV387.11
      marine_long_read        0 denovo   Otu6 · Otu61.1
      strain_long_read        0 denovo   SP64_S79 · spades_MRSA7231_S7_L001
      mousegut_short_read   456 denovo   **1684221.0 · 174573.0**  <- numeric head, denovo later
    mousegut is the ONLY dataset where the inference can go wrong. Its gold_standard is p09 in
    WfOlaqLT and p08 in 33hlLu8Q, and no other live run has the step at all -- verified before
    scoping the watcher suppression to p09.
    all 17 failures ->  work/mousegut_short_read/
    all 17 offending values begin with  `denovo`

    polars.exceptions.ComputeError: could not parse `denovo9553.0` as dtype `f64`
      at column 'genome_id' (column number 2)

`cami_gold_standard.py` reads the per-read truth table and **polars infers `genome_id` as `f64` from
the early rows** — mousegut's ids look numeric, e.g. `190547.0` — then aborts on the first `denovo…`
string. Reported at 7,579 and 10,493 bytes into two different files, so it is wherever the first
`denovo` row happens to sit. The other 106 samples pass because their `genome_id` columns are
numeric throughout.

**FIX, named by polars' own error message: `schema_overrides={'genome_id': pl.Utf8}` on that read**,
or `infer_schema_length=None` to scan the whole column. A `genome_id` is an identifier and should
never have been inferred as a float.

**Impact is LOSS OF COVERAGE, not a wrong number** — which is the better of the two failure modes.
`gold_standard` is upstream of `amber`, so the downstream tasks never run rather than scoring against
a bad truth table. But criterion 12's AMBER comparison would be reported over 212 samples with 17
silently absent, and the run reports complete under retry-then-ignore.

**Cheap to recover:** `gold_standard` is a fast step, its inputs (`reads_mapping.tsv.gz` plus our own
assembly) are untouched, and 106 shards are already promoted — so a relaunch with the corrected
parser serves 106 from cache and recomputes 17.

    MOUSEGUT IS THE ODD DATASET IN EXACTLY THIS WAY TWICE. It is the only CAMI II set shipping a
    `gsa_mapping_new.tsv.gz` sibling, and this campaign already established the two disagree on
    TAXID while agreeing on genome_id. Now it is also the only one with `denovo` ids. Both quirks
    trace to whatever generated its references. Treat mousegut as the schema canary for any new
    parser over the CAMI truth tables.

**Owner:** the experimenter (library). **Next action:** the one-argument parser fix, then relaunch
E2 short; the 17 recompute and nothing else does.

## CLEARED TODAY — kept only so a reader can tell movement from stasis

- **B7, the Pratama MAG comparison — FIXED at the solve level.** MetaWRAP is Pratama's own
  binner and its bins were built, CheckM2-scored and then dropped, while the comparison
  against their 1,275 published MAGs ran off this library's own ensemble. A type-fenced
  parallel chain now dereplicates and scores MetaWRAP's own bins too, with three standalone
  leaf types so no slot gains a second producer and the two results rows have different
  names. Pratama arm now **38 steps, both chains resolving**, reproducible. Applied to the
  main checkout only — the frozen launch checkout stays untouched while runs are live and
  must receive this before any Pratama run that should include the parity chain.
  NOT YET RUN: this is a planning-level fix; the new protocols have never executed.

- **B2, the vConTACT3 unstage failure — CLOSED AT THE RIGHT LAYER.** The first fix turned
  `scratch` off for every cached twin, which bends the execution contract to accommodate one
  product's defect; the principal rejected it and the contract is not the thing to bend.
  The real cause: the published release ships upstream mmseqs build scratch holding **404
  dangling symlinks**, and a directory product with a dangling symlink cannot survive
  `nxf_fs_copy`'s dereferencing `cp -fRL`. `downloadVcontact3DB.py` now prunes that scratch
  in the protocol that builds the product and asserts none survives. Staged copy verified
  **6,925 → 1,057 inodes, 404 → 0 dangling, 5.1 → 3.2 GB** — 85% of that "database" was
  build scratch. The twin exemption is reverted in both drivers and the comment that told
  the next reader to re-add it now says why not.
- **CAMI 2+3 is ONE corpus again.** The short-read arms went 229 → 249; both arms verified
  to name the identical sample list name-for-name. The exclusion of CAMI III's 20 short-read
  samples was the orchestrator's narrowing, justified at the time by "no acceptance criterion
  asks for it" — which was wrong, criterion 7 names exactly those 20.

- **A driver's `params=` dict never reached any transform.** Parity pin inert campaign-wide.
  Fixed in `bootstrap.py`; **proven end to end three independent ways** — the rendered command line in two arms plus an independent watcher
  (`--minContig 1500`, MetaBAT2's own banner confirming `minContig 1500 ... random seed=1`).
- **SemiBin2's seed was system-chosen**, so our binning was not reproducible run to run.
  Now unconditional, plus `--min-len 1500` pinned. Reaches a relaunch only, not a live run.
- **The per-binner parity audit was one-of-three.** Completed against nf-core's own config;
  three apparent deviations proved not to be deviations.
- **The container pull list** was missing das_tool plus ten of Pratama's twenty-one images;
  an absent image on a compute-node driver HANGS rather than failing. All three arms now
  verified present and `.verified` from each run's own `workflow.env.json`.
- **AMBER/CheckM2 evaluated intermediates.** Corrected to the consolidated set only.
- **`gate_bindings.py` silently lost its strongest assertion** when per-binner scoring was
  retired: the distinctness check was guarded by `if scored:` and simply vanished from the
  output rather than failing or reporting N/A. Moved to the property's new home —
  `das_tool` must consume three DISTINCT tables — and every check now prints OK, FAIL or
  N/A so a check can never disappear again. Two new assertions added for the corrected
  deliverable: the MAG-level scorer binds the consolidated table, and checkm binds the
  consolidated bin set. Verified on all three CAMI arms.

## RELAUNCH SEQUENCE — preconditions and what each one PROVES

Four relaunches are queued behind live runs. Written down because the interlocks are not
obvious and a compaction would lose them. **None of these may start while its arm's current
run is live**: a grid job that outlives its driver keeps the compute and loses the cache entry,
so tearing down mid-flight converts finished work into wasted work.

**Order matters only where a precondition says so.**

### R-A · Pratama relaunch
**Preconditions, ALL required:** `d6UJuZgF` has ENDED · DRAM staged and verified BY CONTENT
(five sheets non-null) · VirSorter2 staged with its conda env built under a `/db` bind.
**Proves, in one run:** that `scratch = false` was NOT needed — the pruned vConTACT3 database
now survives the standard task contract (assert the twin's `.exitcode` is 0 and no
`NXF_SCRATCH=` line exists); that VirSorter2 runs on a compute node; that `dramv` produces AMG
calls. Those are three separate blockers closing on one launch.
**CAUTION** set `MSM_AGENT_HOME_PRATAMA=/scratch/phyberos/pratama2026/metasmith` or it stages
into the CAMI home, loses the task cache, and prints `Submitted` exactly like a healthy launch.

### R-B · metaGEM re-score
**Precondition:** `HQ5SrqFe` has ENDED. **Nothing else** — the fix is already in the transform.
**Proves:** MEMOTE scores for the 138+ models. Cheap: the reconstructions serve from cache and
only the ~1 min/model scoring re-runs, because editing `memote_score.py` retires ONLY its own
shards. **Do not** re-run CarveMe; that is the expensive half and it already succeeded 131/131.

### R-C · CAMI rung 10 or beyond
**Precondition:** the corrected target set, which rung 10 `C1IM6IG3` already carries.
**Proves:** the first REPORTABLE AMBER numbers. Rung 1 `iy8YLaGr` cannot give them — it was
staged before the SemiBin2 seed/min-len fix, and DAS Tool consumes all three binner tables, so
its consolidated row is confounded. Rung 1 remains valid for criterion 9's COMEBin RSS and the
binning inode census, which no binner parameter touches.

### R-D · frozen checkout receives B7
**Precondition:** ALL live runs ended. **Why it cannot be done now:** applying B7 to
`launch_frozen` needs a metadata rebuild there, which rewrites `_metadata` mtimes; every leaf id
hashes path+mtime, so every key moves and every live run's resume state is discarded.
**Rebuild SCOPED** (`python -m metasmith build transforms -t <data_types> -r <one library>`) —
`dev/libraries.sh -bm` rebuilds every library and moves keys that had no reason to move.

## KNOWN LATENT DEFECTS — diagnosed, deliberately not fixed, with their mechanism

Each was left because the fix costs more than the defect while three runs are in flight.
None is load-bearing for a current acceptance criterion.

- **`assembly_stats.py`'s bare `sequences::reads` requirement** is parented to shared
  metadata rather than to a specific assembly, so it once bound short reads to a long-read
  assembly. Closed operationally by giving every sample its own `read_metadata`; the root
  cause stands. Fixing it retires that transform's shards campaign-wide.
- **`withLabel: 'xlocalx'` carries an `errorStrategy = 'ignore'` with no retry** for the four
  real download steps. Fixing the label properly changes behaviour on every download lane
  mid-campaign.
- **`downloadVirsorter2DB` needs its output bound at `/db`** — the durable root-cause fix for
  B1. Scheduled for after the staging run proves the mechanism.
- **Naming `sequences::orfs` as an explicit target collapses the whole solve**, reproducibly
  and independently of the search budget. Left untargeted; DAS Tool pulls prodigal in anyway.
- **The launch-time executor ceiling check is blind** to executor blocks whose values are
  parameter references, which is the shape the Slurm preset uses. A future local step asking
  for more than 8 cpus still aborts at submit with no warning.
## NOTE ON THIS FILE — B12 and the three tail sections were restored 2026-09-12 23:40

I destroyed them with a careless write: a Python conditional expression that evaluated to
`s[:i] + head + body`, which truncated everything from B13's original position onward — and B12
plus CLEARED TODAY, RELAUNCH SEQUENCE and KNOWN LATENT DEFECTS all lived after it. The damaged
file is kept as `BLOCKERS.md.damaged`.

They are restored from the experimenter's own copy at
`research/metasmith_benchmark/findings/BLOCKERS.md`, timestamped 21:41, so **those four sections
are as of 21:41 and do not carry this evening's B1 and B3 closures** — which are current in this
file's own B1 and B3 rows above. Anything else written into them between 21:41 and 23:40 is lost.

    CAUTION this file is on scratch and is not version controlled. The peer's worktree copy is
    the only backup, and it exists only because the ledgers are mirrored there. Do not build a
    multi-step in-place rewrite out of one expression; slice, verify, then write.

