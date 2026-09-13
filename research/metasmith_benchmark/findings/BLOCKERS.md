# ACTIVE BLOCKERS — CAMI/Pratama/metaGEM benchmark campaign

Live tracker. **Rewrite rows in place; do not append a history** — the plan file's run log is
the append-only record, and a tracker that grows becomes a changelog nobody trusts.

Columns: what is blocked · why · who owns it · the exact next action. A row leaves this file
only when its next action is done AND verified by product.

Last updated 2026-09-12 19:40 PDT (pivot: proving capabilities, not launching).

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

## B5 — criterion 9 unmeasured: COMEBin's peak RSS at 48 cpus

CAMI rung 1's COMEBin (job 59548383, 48 cpus) has been **PENDING (Resources) since 16:35** —
queued, not stalled. A Pratama-arm COMEBin IS running healthily at 48 cpus, which gives a
48-cpu data point but not criterion 9's, since that names a CAMI sample. das_tool and the
MAG-level AMBER row both wait behind it.

**Owner:** L4 (`a6a51efe509e7c8f9`). **Next action:** `sacct MaxRSS` + elapsed once it
finishes. The reference arm's COMEBin rerun is running under a 16 h limit, which also
CLOSED a standing unknown: a task does auto-route past fir's 3 h band. For calibration nf-core's COMEBin on the same sample ran 36+ min healthily, so a
long COMEBin is not a stall; the real deadlock signature is zero `cluster_res` files.

## B6 — storage gate: the binning inode term is the last unmeasured one

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

## B11 — `carveme_from_orfs_cplex` times out on 5% of bins  ·  affects criterion 6

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

## B12 — `flye_raw.py`'s preset heuristic is misled by CAMI's synthetic quality strings

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

**AND THE SAME RUN EXPOSED A SECOND DEFECT IN THE SAME TRANSFORM: its declared memory would have
OOM-KILLED IT.** `flye_raw.py` declares `Size.GB(32)`, which renders `'32.00 GB'`, and both
Nextflow and Slurm read `G` as 1024-based — Slurm says so in its own submit note. So the ceiling is
**33,554,432 KiB**, and arm B's MaxRSS was **34,269,012 KiB = 102.1% of it.** It survived only
because I allocated 128 GB deliberately so a kill could not destroy the measurement. That is ONE
sample, and not the largest of the 172 in the long arm. Recommend **96-128 GB**.

**And the heuristic is not merely reading a synthetic value — it is reading one wrong by three
orders of magnitude.** Arm B's log reports `Alignment error rate: 0.154408`, a **15.4% real error
rate, about Q8**, against a quality string claiming Q40 = 0.01%. Off by a factor of ~1500.

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

