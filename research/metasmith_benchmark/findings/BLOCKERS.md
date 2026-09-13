# ACTIVE BLOCKERS — CAMI/Pratama/metaGEM benchmark campaign

Live tracker. **Rewrite rows in place; do not append a history** — the plan file's run log is
the append-only record, and a tracker that grows becomes a changelog nobody trusts.

Columns: what is blocked · why · who owns it · the exact next action. A row leaves this file
only when its next action is done AND verified by product.

Last updated 2026-09-13 06:00 PDT. **Wave 1 is LAUNCHED** — ten drivers running. BYTES are the binding axis (16.68 of 18.63 TiB, 89.5%, fitted ~0.24 TiB/h); inodes are 553K of 1M and FALLING as prunes land. All work-dir prunes are HELD until `prune_work.sbatch` keeps `.command.cache`.

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

**RE-SCOPED 2026-09-13 to E1 short (wave 1). L3 and its rung-1 run are gone; the reference arm is
now `/scratch/phyberos/bench/e1/{short,long}`, driver pids in `nextflow.pid`.**

**MEASURED, and the measurement is a trap worth naming:**

    /scratch/phyberos/bench/e1/short/out/GenomeBinning/contig_to_bin/
      total 8   -- ONLY . and ..   created 2026-09-13 02:40, EMPTY
    find bench/e1 -name contig_to_bin_map.tsv  ->  0   (positive control: bench/e1 holds short/ long/)

`storeDir` creates the directory when the channel operation is *declared*, not when it gathers. So
the path exists, is dated, and holds nothing — **a `test -d` or an `ls` of the parent reads as
landed.** Check for the FILE, and check it is non-empty. Same family as the campaign's other
completion proxies: a Slurm output file exists at task start, a directory is created before it is
populated.

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

**PROVEN COMPLETE 2026-09-13: the first assembly landed, and the MEMORY finding is the one B12 was
not written for.**

    product   1-1-1.090ba0c3d7a125f5-IzISt8qq.fa   117,295,515 B
    contigs   2,617     total 115,338,976 bp     N50 107,818
    job       nf-p03__flye_(1)  COMPLETED 00:32:45 at 16 cpus
    MaxRSS    **34,071,012K = 32.49 GiB**
    state     2 of 41 ok · 39 running · 0 failed

**Flye's own measurement closes the preset question for good:** `Alignment error rate: 0.228296` and
`0.152422` — **15.2% and 22.8% real error against a quality string claiming Q40 = 0.01%**, so the
`AvgQual >= 20` branch was reading a value wrong by three orders of magnitude, not a slightly
optimistic one.

**AND THE SECOND DEFECT: MaxRSS 34,071,012K against `Size.GB(32)` = 33,554,432K is 101.5%.** Under
the STANDARD transform's 32 GB declaration this production task would have been **OOM-killed,
independently of the preset**. The pinned `e2/flye.py`'s `Size.GB(64)` is what makes it survive, at
50.8% utilisation. So the pin repaired **two** defects and only one of them was the one it targeted.

The memory figure is stable across builds and thread counts: the A/B arm B measured 32.68 GiB at
8 cpus on Flye **2.9.6**; production is 32.49 GiB at 16 cpus on **2.9.5**. That is a property of the
workload, not a one-off — so `Size.GB(32)` is wrong for CAMI long reads generally, not just here.

    Not a controlled comparison, but for shape: arm B gave 3,186 contigs / 129,618,595 bp /
    N50 96,428 at 1h05m41s on plant nano with 2.9.6. Production gives 2,617 / 115,338,976 /
    N50 107,818 in 32m45s. Fewer, longer contigs and half the wall, on a different sample and
    a different build.

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

## B15 — E5 declares no GPU for CLEAN  ·  **CLOSED AT EXECUTION 2026-09-13.** `def-shallam_gpu` + a 3g.40gb MIG slice; CLEAN 3 of 3 exit 0 with real products

**RESOLVED, verified BY PRODUCT rather than by the run's state.** In `52mAOnXS` (e5_cami):

    p08__clean (1) (2) (3)   job 59642405_{0,1,2}   completed=3  errored=0
    .exitcode 0 in all three, and each wrote a real TSV:
      10/7997d289…  5,257,352 B      7d/58a73fcd…  4,042,725 B      8a/b719b39f…  5,932,333 B

**The account was the fix, and it was not the account this campaign uses.** `sacctmgr show assoc`
gives exactly `def-shallam_cpu`, `def-shallam_gpu`, `rpp-shallam_cpu`, `rrg-shallam-ab_cpu` — so
**there is no GPU RAC**, and `--account=rrg-shallam-ab` is refused outright ("You may not be a member
of the specified account"). GPU work goes to `def-shallam_gpu`, which is in better shape than the CPU
RAC anyway (fairshare 0.2746 at EffectvUsage 0.6839 against 1.000).

**A `3g.40gb` MIG slice is both sufficient and cheaper**: CLEAN asks 16 GB, the slice offers 40, and
its band carries 60 nodes against a full H100's 30 — `--test-only` put the MIG request **~15 minutes
earlier** in the queue on otherwise identical requests.

**Still in flight, and NOT this blocker:** `YzCrdOoF` (e5_metagem) and `8Z7x3L7z` (e5_pratama) each
mention `__clean` but show 0 completed / **0 errored** — queued or running, not failing. The
declaration defect is fixed; those are progress.

    CAUTION my first query for CLEAN's state read `<run>/nxf.log` and returned NOTHING for all
    three lanes, which reads exactly like "CLEAN never ran". **There is no `nxf.log` in a run
    directory** -- the positive control (`ls .../runs/*/nxf.log | wc -l`) returned 0 files. It
    lives at `_metasmith/logs.latest/nxf.log`, beside `agent.log`, `main.log`, `lineage.csv` and
    `nxf_trace.tsv`. Seventh empty-grep false reading of this campaign, caught by the control.

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

## B18 — metaSPAdes needs attempt 2 (384 GB) on large Pratama samples · a DEVIATION from the paper's `-m 190`, NOT a lost sample

**RETRACTED AND REWRITTEN 2026-09-13. My first version of this row said "no retry ladder, sample is
silently lost". That was wrong on both counts and the experimenter corrected it. See the withdrawal
note at the bottom — it is the fifth instance of one error shape and the most instructive.**

**What is real.** The first metaSPAdes task to reach the hammer stage's peak exceeded its 192 GB
grant exactly:

    59636440_5  FAILED  01:28:44  MaxRSS 201,318,144K = 191.99 GiB  ReqMem 192G  fc30411
    SPAdes' own log: "Memory limit set to 182 Gb"
    == Error == system call for '/usr/local/bin/spades-hammer' finished abnormally,
                OS return value: 21

**100.0% of the cgroup.** SPAdes reserved its own headroom — an internal limit of 182 Gb out of the
192 GB grant — and **`spades-hammer` exceeded the cgroup regardless, so the `-m` flag does not
govern that stage's peak.** That part stands.

**THE LADDER EXISTS AND IT WORKED.** `workflow.resources.nf:38-41` carries
`memory = { (2**(task.attempt-1)) * 192 GB }`, generated from the transform's own `Resources`. The
retry was submitted at 05:00:05 at `--mem 393216M -t 48:00:00` and is running. So the sample is not
lost; it costs a second attempt.

**THE REAL CONSEQUENCE IS A CORPUS-WIDE PARITY DEVIATION, NOT A LARGE-SAMPLE ONE — measured
2026-09-13, and this replaces the original "the largest samples" framing.**

**Input size separates the failures with NO OVERLAP.** Every terminal spades task, with its bbduk
`clean_short_reads` input:

    bd/bf206af8…  exit 0  196608M   6,552,067,603   <- the ONLY success
    87/80bf6d66…  exit 1  196608M   8,448,412,814
    dc/54ff8199…  exit 1  196608M   9,776,002,681
    20/5933972d…  exit 1  196608M   9,808,533,270
    01/e65e5331…  exit 1  196608M  11,321,984,114
    2d/26c210e4…  exit 1  196608M  11,341,404,142
    44/95031177…  exit 1  196608M  11,428,582,958

So the 192 GB ceiling lies between **6.55 and 8.45 GB** of input. And the corpus:

    n=70  total 705.2 GB  mean 10.07 GB
    min 6.55 · p25 9.06 · p50 10.31 · p75 11.22 · max 12.86 GB
    >= 8.0 GB  (at or past the observed threshold):  68 of 70 = 97.1%
    >  6.55 GB (larger than the success):            70 of 70 = 100%
    >= 13.1 GB (2x the success, at risk at 384 GB):   0 of 70

**The one sample that succeeded is the single SMALLEST input in the corpus, and 68 of 70 sit at or
beyond the failure threshold.** So essentially the whole Pratama corpus goes to attempt 2 — ~65 tasks
at 384 GB on a 48 h wall.

**Deviations row, reworded:** not *"a sample that needs attempt 2 deviates from the paper's
`-m 190`"* but **"~97% of samples run at 384 GB, so the arm departs from the paper's `-m 190`
corpus-wide."** That is a materially different claim about the reproduction. Declaring 384 GB as
attempt 1 would make the deviation explicit and cost one wall per sample instead of two.

    TWO CAVEATS, because the useful half is a projection. The 8 GB threshold is interpolated from
    SEVEN points and could lie anywhere in 6.55-8.45 GB. And "0 at risk at 384 GB" assumes memory
    scales roughly linearly with input, which for SPAdes is not guaranteed -- its peak is driven by
    k-mer graph complexity, so a high-diversity sample can cost more than its byte count suggests.
    Treat it as a projection to be falsified by the 393216M attempts, not a guarantee.

**Owner:** the experimenter. **Action:** deviations-table row, not a fix.

---

**WHY PRATAMA'S `-m 190` SUFFICED AND OURS DOES NOT — researched 2026-09-13. IT IS NOT A
TABLE-FIDELITY DIFFERENCE. Every scientific parameter matches; what is left is operational, and the
strongest candidate is REDACTED in their methods.**

Their own workflow doc, verbatim
(`data/docs/pratama2026/Groundwater_virome/Workflows/MetaG_and_MAGs_bioinformatics.md:43-44`):

    module load SPAdes/3.15.2
    spades.py --meta -o ${sample} -1 ${sample}_R1.fastq.gz -2 ${sample}_R2.fastq.gz \
              -t INTEGER -k 21,33,55,77 -m 190

Ours, the rendered line from the task that succeeded, plus the tool's own banner:

    spades.py --meta -k 21,33,55,77 -t 48 -m 182 --12 <interleaved.fq.gz> -o spades_ws
    SPAdes version: 3.15.5

| | ours | theirs |
|---|---|---|
| `--meta` | yes | yes |
| k list | **21,33,55,77** | **21,33,55,77** — identical |
| `--only-assembler` | **no** | **no** — so `spades-hammer` runs on BOTH sides |
| pre-normalisation / subsampling / separate error correction | none | **none** |
| bbduk QC | `ktrim=r qtrim=rl trimq=20 minlen=50 k=23 mink=11 hdist=1` | **the same, character for character** |
| `-t` | **48** | **`INTEGER` — redacted** |
| `-m` | 182 | 190 |
| input packaging | `--12` one interleaved file | `-1`/`-2` split pairs |
| SPAdes | 3.15.5 | 3.15.2 |

**THE HYPOTHESIS THIS KILLS.** Reproduction-map row A1 records our bbduk as `qtrim=r trimq=0` with
a derived `minlen`, against Pratama's `qtrim=rl trimq=20 minlen=50` — which would have meant our
reads carry more low-quality bases, a larger erroneous k-mer space, and therefore a bigger
`spades-hammer` peak. **That row describes the STANDARD `assembly/bbduk.py`, not the
`bbduk_pratama` variant E3 actually runs**, and the rendered command proves `bbduk_pratama` is
Pratama-faithful. We quality-trim identically. The noisier-reads explanation is dead.

**`-t 48` IS THE LEVER, AND THEIR SOURCE IS SILENT ON IT.** `spades-hammer`'s k-mer counting
allocates per-thread buffers, so 48 threads costs materially more resident memory than 16 or 24 on
the same input. Their doc writes the literal token `INTEGER`.

**And `-m` does not bound the hammer stage on either side.** We proved it: SPAdes reported
`Memory limit set to 182 Gb` and hammer exceeded the 192 GiB cgroup regardless. So their run fitting
is NOT explained by `-m 190`; something reduced the actual peak, and thread count is the candidate
that does so without touching the science.

**CHEAP DISCRIMINATING TEST, not yet run:** one known-failing sample at `-m 182 -t 16`, otherwise
unchanged. Fits => the deviation collapses to a thread-count difference, an operational note rather
than a fidelity claim, and the corpus can run at 192 GB as the paper did. Still OOMs => the cause is
version or input packaging and the 384 GB rung is the honest answer.

**HOW I GOT IT WRONG, because the shape is the reusable part.**

1. **I grepped the wrong generated file and read absence as absence.**
   `grep "withName: '.*spades'" workflow.config.nf` returns nothing — **correctly**, because a
   transform's own declared `Resources` are emitted to **`workflow.resources.nf`**, a different
   file. A driver-side `withName` selector is only one of two places a ladder can live, and it is
   the one a `withName` grep can see. *Checking one of two generated config files is not checking
   for a selector.*

2. **I mapped a Slurm array index to a nextflow task index by assuming they correspond.** They do
   not. `59636440_5` is `p05__spades_pratama (6)`. The dir I read as "the retry of (1), identical
   memory" was **task (1)'s first attempt** — a different sample entirely, so its 196608M was
   correct rather than a failure to double. **NEW RULE: map a Slurm array index to a nextflow task
   index through `nxf.log`, never by arithmetic on the suffix.**

**Fifth instance of one error shape in two days** — after B12's live exposure, the megahit scratch
defect, 22 megahit "retries", and B16's cache-unreachable. In every one the measurement was correct
and the sentence built on it was not. The MaxRSS, the ReqMem, the SPAdes log line and the missing
`withName` selector are all still true as measured.

## B19 — the 97% depth-identity floor DESTROYS METABAT2 ON BOTH ARMS and SEMIBIN2 SURVIVES IT ON BOTH · `jgi_summarize_bam_contig_depths` defaults to `--percentIdentity 97` against 15%-error ONT reads

**CORRECTED 2026-09-13 — MY OWN HEADLINE WAS WRONG. I wrote "E2 long's binning produces ZERO bins".
It is metabat2 that produces zero; semibin2 produces real bins on the same depth data, on BOTH
arms. The floor is symmetric; the two binners' RESPONSES to it are not, and that is what makes the
long-read half of criterion 12 still reportable.**

    OUR ARM, 33hlLu8Q                        REFERENCE ARM, E1 long
    metabat2  41 indices FAILED, 0 bins      MetaBAT2  82 non-unbinned files, and the three
              exit 1, the transform refuses            largest are `*.lowDepth.fa.gz` at
                                                       7,195 / 3,809 / 4,771 contigs, 50-58 MB
                                                       -- the lowDepth dump, NOT bins. Reports OK.
    semibin2  41/41 exit 0, 0 failures        SemiBin2  1,335 bin files, 0 named `unbinned`,
              19-44 bin files per task,                 1,198 over 100 KB, largest 4,075 contigs
              task dirs 100-180 MB

**THE FAILURE-MODE ASYMMETRY IS ITSELF A FINDING, and it favours our arm.** Given the same ~0.02%
well-mapped rate, **our metabat2 fails loudly and truthfully with exit 1 and no product, while
nf-core's writes a `lowDepth.fa.gz` holding thousands of contigs and reports success.** A reader
counting files in `GenomeBinning/MetaBAT2/` on the reference side would count 164 and conclude it
binned; 82 are named `unbinned` and the rest are dominated by the lowDepth dump. That is the
campaign's own signature failure — a green step with no real product — appearing on the reference
side of the comparison.

**So: report the long-read comparison on SemiBin2, and record MetaBAT2 as lost on both arms for one
shared cause.** Do NOT report nf-core's MetaBAT2 file count as bins.

    CAUTION my first read of the reference side said "1,413 bins written". The first three files
    a `find` returned were **20-byte gzips with ZERO contigs** -- `unbinned.remaining.fa.gz` and
    `unbinned.pooled.fa.gz` placeholders, the same 20-byte-gzip signature as B14's fastp defect.
    Counting files named `*.fa.gz` counts placeholders. Exclude `unbinned` by name, then check
    the size distribution, then count contigs in the largest.


**Live in E2 long (`33hlLu8Q`) 2026-09-13. Diagnosed by the experimenter. EVERY metabat2 dir in
the run exits 1.**

    CORRECTION: my "20 distinct indices, about half the samples" was a SNAPSHOT count taken
    mid-array. By directory it is every metabat2 task in the run. Counting distinct indices is
    the right discipline against retry inflation, and it still gives a floor rather than a
    total while an array is live.

    ReqMem 16G · MaxRSS 833,620K-1,379,880K (0.8-1.3 GiB, ~8% of grant) · Elapsed 00:00:59-00:01:28
    ExitCode 1:0 · array 59652946
    distinct failing indices: (1)(3)(4)(5)(8)(10)(12)(14)(15)(16)(17)(19)(23)(24)(29)(31)(32)(33)(39)(40)

**The mechanism, read from the 2.15 image's own `--help` rather than inferred.**
`jgi_summarize_bam_contig_depths` defaults to **`--percentIdentity 97`**. The lane's BAM is
**minimap2's**, over ONT reads that this campaign has already measured at a **15.4% real error rate**
(Flye's own `Alignment error rate: 0.154408`, against a NanoSim quality string claiming Q40). Almost
no alignment clears a 97% identity floor:

    jgi 2.15 ran and reported "Finished"
    "with 1826529 reads and 231 readsWellMapped"
    well-mapped counts across the run: 147-753 out of 1.1-2.5 MILLION reads

    CONFIRMED SYMMETRIC ON THE REFERENCE ARM, measured independently on both sides:
      E1 long, my read of 41 jgi task dirs:  180-290 well mapped of 1.10-1.11 M
      E1 long, the experimenter's read of
        METABAT2_JGISUMMARIZEBAMCONTIGDEPTHS_LONGREAD:  490-797 of 1.7-2.4 M
      33hlLu8Q:                              231 of 1,826,529
      `--percentIdentity` is passed NOWHERE in E1 long's .command.sh -- the tool's own 97
      default, because nf-core sets it only when `longread_percentidentity` is given and that
      defaults to null. Every figure lands at ~0.02% well mapped.

So every contig gets ~0 depth, the whole assembly lands in `lowDepth.fa` (86 MB), metabat2 yields
**0 bins**, and the e2 transform correctly returns `success=False` — which is why this is a loud
failure rather than a silent empty bin set. **The tool is behaving exactly as configured; the
configuration is wrong for long reads.**

**NOT a resource failure, and NOT the image.** The image hypothesis was refuted by a control inside
the campaign rather than by argument: `WfOlaqLT` (E2 short) declares the **identical two tags** —
`metabat2:2.15--h986a166_1` and `2.17--hd498684_0` — and its metabat2 ran **208/208** in the same
agent home. The agent home's `container_images/` holds only megahit, spades and bbtools, and
`APPTAINER_CACHEDIR` appears nowhere in the task's `.command.sh` or `.command.run`; both facts are
true and neither is the cause.

**AND IT IS SYMMETRIC, WHICH MAKES IT A PARITY FINDING RATHER THAN ONLY A DEFECT.** nf-core/mag
5.5.0 passes `--percentIdentity` **only if `longread_percentidentity` is set, and it defaults to
`null`** — so E1 long inherits the same 97% floor from the same tool. Both arms of the long-read
comparison are hit identically, exactly as phiX removal turned out to be a measured no-op on both
sides. So the comparison stays sound; what is at risk is whether either arm recovers any long-read
MAGs at all.

**THE THRESHOLD, RESEARCHED 2026-09-13. The cited value is 85 and IT IS STILL TOO HIGH HERE.**

`nf-core/mag`'s own `docs/usage.md:468-470` describes our exact symptom and prescribes a number:

> "If you are having trouble with the coverage estimation steps (for example, **the output depths
> for each bin are all at or near zero**)... By default, alignments are filtered to retain those
> with 97% percentage identity. This value is good for short read Illumina data, however for
> certain long read technologies error rates can be much higher. For example, older Oxford
> Nanopore chemistries can have error rates approaching **15% - 20%**... **For older ONT data, you
> may wish to look at values of around 85%** to improve coverage estimation."

Corroborated three ways inside the pipeline: `conf/test_full.config:35` sets
`longread_percentidentity = 85` for their own full-size release test; `nextflow.config:61` has the
`null` default; `CHANGELOG.md:244` is PR **#873** by @prototaxites, which both documented the
parameters and set 85 in test_full.

**And MetaBAT2's docs explain why 97 exists**, which is how the change gets justified rather than
merely made: *"Reads that map imperfectly are excluded when the %ID of the mapping drops below a
threshold (--percentIdentity=97). MetaBAT is designed to resolve strain variation and mapping reads
with low %ID indicate that the read actually came from a different strain/species."* It is a
**strain-discrimination** filter that assumes short-read accuracy. On 20%-error ONT every
legitimate self-mapping read looks like another species — the filter works exactly as designed, on
data it was not designed for.

**OUR MEASUREMENT — from the BAMs, per read. RECOMMENDED FLOOR: 80.**

Job 59655322, 4 samples, 2% seeded sample, primary mapped only (`-F 0x904`),
identity = 100 x (1 - NM/aligned_length) with aligned_length = the sum of CIGAR M/I/D/=/X, i.e. **the
aligned block with soft clips excluded**. Zero reads missing an NM tag in any sample.

    dataset            reads    mean     p5      p10     p25     p50     p75     p95
    plant_associated   13,787   85.79   83.36   84.21   85.16   85.96   86.76   88.22
    plant_associated   13,982   85.81   83.14   84.09   85.12   85.96   86.84   88.45
    toy_humangut       20,482   88.04   86.19   86.77   87.48   88.17   88.87   89.98
    toy_humangut       20,195   88.04   86.21   86.75   87.48   88.17   88.86   89.98

    fraction at or above     50       60       70       75       80       85
    plant (a)             1.0000   1.0000   0.9985   0.9916   0.9822   0.7852
    plant (b)             1.0000   0.9999   0.9980   0.9891   0.9792   0.7773
    gut   (a)             1.0000   0.9999   0.9987   0.9959   0.9911   0.9798
    gut   (b)             1.0000   0.9999   0.9987   0.9954   0.9915   0.9810

**THE MECHANISM IS NOW QUANTITATIVELY CLOSED.** p95 is 88.2-90.0%, so essentially no read reaches
97 — which is exactly the observed **231 of 1,826,529 = 0.013%**. The floor sits ~9 points above the
top of the distribution.

**80 retains 97.9-98.2% of plant reads and 99.1-99.2% of gut reads**, and sits ~3 points below
plant's p5, so it is near no edge of the distribution.

**85 — the value nf-core documents — is NOT symmetric on this corpus**: it keeps 98.0% of gut reads
and only **77.7-78.5% of plant** ones. A ~22% read loss on one dataset and ~2% on the other, inside
one lane, is a dataset-dependent coverage bias introduced by the parameter itself. 75 also works
(98.9-99.6%) but buys nothing over 80 and drifts further from the documented value for no measured
reason.

Deviations row: *nf-core/mag documents ~85 for older ONT data; measured per-read identity on this
corpus is 85.8-88.0% (p5 83.1-86.2), where 85 would discard 22% of plant_associated reads and 2% of
toy_humangut, so the floor was set to 80, retaining >=97.9% in both.*

    RETRACTED, AND IT WAS THE BASIS OF THE WHOLE EARLIER RECOMMENDATION. I first reported identity
    **76.8-80.1%**, derived from Flye's own `Alignment error rate` (0.228-0.232 plant,
    0.199-0.203 gut) read as 1 - identity, and recommended 75 on it. **Flye's figure is not that
    statistic** -- it is computed during Flye's own consensus over raw reads and evidently charges
    unaligned or clipped portions that an aligned-block identity does not. The gap is large and
    one-directional: ~6 points on plant, ~8 on gut. The consequence was a recommendation reasoned
    from the wrong instrument, and it could not have surfaced the plant-versus-gut asymmetry at 85,
    which is the finding that actually decides the value.

    **A proxy already sitting in a log you are reading is not evidence that it measures what you
    need.** The BAM was always the right instrument and cost one 50-minute job. Same family as the
    Q40 quality string that misled both the flye preset and this filter -- one level up, because
    this time the misleading number was a TOOL'S OWN SUMMARY STATISTIC rather than the data's.

**Disabling the filter is the other defensible option and is arguably cleaner for a benchmark** — it
removes a parameter from the comparison rather than adding a chosen one. The cost is that genuine
cross-strain mappings inflate coverage, which a strain-madness-style community would punish; neither
of these two datasets is that set.

    AND ONE OBSERVATION THAT LINKS THIS TO B12: NanoSim wrote these reads with a FLAT Q40 quality
    string while their real error is ~20%. That is the same root cause as B12, where the flye
    preset heuristic read Q40 and chose --pacbio-hifi. **Two independent tools have now been misled
    by the same synthetic quality string** -- one through a preset, one through an identity filter.
    Any other tool in these lanes that keys off base quality is suspect for the same reason.

**Owner:** the experimenter, as a T19 row. **Fix direction:** set the floor to 75 (or disable it) on
the long-read lane, with parity checked against nf-core/mag's own long-read depth arguments —
and if it is changed on one side it must be changed on both, or the arms diverge on the parameter
that decides whether a contig is binnable. **No cancellation:** downstream `das_tool` waits on
comebin regardless, and each retry costs about a minute.

    CAUTION — THE TRAP THAT DEFEATED MY OWN INSTRUMENT TWICE. An ARRAY JOB'S PARENT DIRECTORY
    CARRIES A `.command.err` THAT BELONGS TO NO TASK. I read `ce/d1811d65…`, found a 60-byte
    `.command.err` holding only `grep: write error: Broken pipe` / `tr: write error: Broken pipe`,
    and a 29 KB `.command.out` that is the NODE RELAY's aggregate log -- several `apptainer exec`
    blocks with different `.bounce.*` files, two different metabat2 tags probed ten seconds apart,
    and an `'active' file was deleted`. None of it is one task's output, and all of it looked like
    evidence.

    Then I mapped indices to dirs using nxf.log's `submitted process` lines and got two dirs with
    NO `.exitcode` and NO `.command.sh` at all, whose only "errors" were
    `Error: Failed to append to file: $trace_file` -- literal UNEXPANDED shell template text, not
    a failure.

    **Map a task index to its work directory through nxf.log's `Task completed > … workDir=` line,
    never through `submitted process`, and never by reading the array parent.** The real dir here
    is `d6/197a49…`, and it holds jgi's actual output.

## B20 — E2 LONG WILL HANG, NOT COMPLETE: `das_tool` has three hard requirements and metabat2's will never exist · and that closes criterion 12's long-read half IN-PLAN

**Downstream of B19, and a different disposal decision: this run does not fail, it WAITS.**

    p12__das_tool   submitted = 0   completed = 0

The pinned `e2/das_tool.py` (`_metasmith/task/transforms/FVWXPAYrmdVA/das_tool.py`) takes all three
binner tables as **plain requirements** — no default, no optional marker — grouped by assembly:

    11  mb = AddRequirement(e2::metabat2_contig_to_bin, parents={asm})
    12  sb = AddRequirement(e2::semibin2_contig_to_bin, parents={asm})
    13  cb = AddRequirement(e2::comebin_contig_to_bin,  parents={asm})
    61  group_by=asm

B19 means metabat2's table will never be produced for **any** of the 41 assemblies. So Nextflow
never satisfies that input channel, the process never submits, **never errors, and never appears in
a FAILED row.** `p13__checkm2` and `p14__amber` sit behind it. The driver holds its job's wall clock
indefinitely.

`das_tool` is *also* waiting on comebin — `p05__comebin` is absent from the trace entirely — but
comebin landing would not release it. metabat2's absence blocks it permanently.

**AND IT CLOSES THE IN-PLAN ROUTE TO A LONG-READ AMBER NUMBER.** The pinned `e2/amber.py`:

     8  table = AddRequirement(e2::das_tool_contig_to_bin, parents={asm})
    46  group_by=table

and the plan's only amber process is `p14__amber`. **There is no per-binner amber step in this run
at all** — correct per the principal's correction that the consolidated set is the reported one, but
it means the consolidated table is the *only* path to a score on that lane.

**Two ways forward, and they are NOT substitutes:**

1. **Fix B19's identity floor.** metabat2 produces a table, das_tool consolidates, `p14__amber`
   gives a consolidated long-read row in-plan, and the long-read half stays structurally identical
   to the short-read half. This is what criterion 12 actually asks for.
2. **Score SemiBin2 post-hoc, out of plan**, with `score_reference_amber.py --contig-to-bin` (the
   two-column path, verified against both column layouts and byte-identical to the in-plan scorer on
   the same assignment). Both sides exist today — our semibin2 is 41/41 exit 0 with 19-44 bin files
   per task; the reference arm has 1,335. But it is a **raw-binner** comparison, an intermediate on
   both sides, not the MAG set.

**Owner:** the experimenter. **Next action:** decide between the two; (2) is available immediately
and should be labelled SemiBin2-vs-SemiBin2 on raw bins rather than as criterion 12's answer.

    CAUTION a step with 0 submitted and 0 completed is invisible to every failure-shaped check --
    no FAILED row, no non-zero exitcode, no ignored-step line, nothing in `.command.err` because
    there is no task dir. The only signal is the ABSENCE of a submit line for a step that is in
    `workflow.nf`. Diff the plan's process list against the trace's step names to find one.

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

