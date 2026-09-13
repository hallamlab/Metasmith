# ACTIVE BLOCKERS — CAMI/Pratama/metaGEM benchmark campaign

Live tracker. **Rewrite rows in place; do not append a history** — the plan file's run log is
the append-only record, and a tracker that grows becomes a changelog nobody trusts.

Columns: what is blocked · why · who owns it · the exact next action. A row leaves this file
only when its next action is done AND verified by product.

Last updated 2026-09-12 19:40 PDT (pivot: proving capabilities, not launching).

---

## B1 — VirSorter2 cannot run on a compute node  ·  BLOCKS criterion 2's viral lane

**Nothing in this campaign runs snakemake as a workflow engine — metasmith drives Nextflow.**
VirSorter2 2.2.4 *is* a snakemake workflow internally: `virsorter run` shells out to
`snakemake --snakefile .../virsorter/Snakefile ... --use-conda --conda-prefix <db>/conda_envs`,
with snakemake 5.26.0 bundled in its own biocontainer. So this is a defect in one tool's
internals, not in our stack.

snakemake 5.26 names a conda env `md5(realpath(conda_prefix) + yaml)[:8]`, so **the env name
depends on where the DB is mounted**. Proven: `md5("/db/conda_envs" + vs2.yaml) = 671930f2`, exactly what the run
demanded; the staged DB holds `91185d68`, built in a per-run work dir. `downloadVirsorter2DB`
and `virsorter2.py` can therefore never agree. The run then tries to CREATE the env, which
needs network, on a node with 0.14 MB/s egress. `--use-conda-off` is not an escape hatch —
the image's own env has no sklearn/pandas/numpy/screed/prodigal/hmmsearch.

**Scope, measured rather than assumed:** only two of the campaign's cached images embed
snakemake at all — `virsorter 2.2.4` (5.26.0) and `pathofact 2.0` (7.25.0). PathoFact is in
no launched arm. So the blast radius is exactly one tool, and PathoFact is the only other
candidate should it ever consume a staged database — on a different snakemake major, so
re-derive its hash rather than assuming 5.26's scheme. (metaGEM's *published* pipeline is
also snakemake, but we read its source for parameters and never run it.)

**Owner:** orchestrator. **Gate:** DRAM staging must finish first — login3 is the only node
that can fetch and is at 10.5 GB of its 16 GiB cgroup carrying L3's driver.
**Next action:** `bash /scratch/phyberos/stage_virsorter2.sh` detached on login3, then
register `annotation::virsorter2_db` in `STAGED_REFS_PRATAMA`, re-measure keys, relaunch.
Afterwards apply the same `/db` bind to `downloadVirsorter2DB` itself — the staging run is
the test of that mechanism.

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

Now in the **mmseqs profile build over Pfam-A.full** — compute, not transfer, and unbounded in
duration. Memory measured rather than feared: mmseqs oscillates **2.34–3.07 GB** (cycling, not
climbing), the co-resident nf-core driver is 0.88 GB, so non-reclaimable peaks near 5 GB of the
16 GiB cap. The page-cache guard has fired **12 times** and held (15 GiB → 12 GiB) with
**`oom_kill 0`** throughout. login3 stays usable and the 3 h COMEBin on it is not at risk.

**Owner:** orchestrator · watch `bof1hlf81`. **Next action:** verify BY CONTENT — the five
`*_form`/`*_database` sheets non-null — then release the VirSorter2 staging (B1), serialized
behind this on login3.

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

    asis    (--pacbio-hifi --read-error 0.000501)  FAILED 9m43s, exit 1, no assembly.fasta
            ERROR: No disjointigs were assembled - please check if the read type
                   and genome size parameters are correct
    nanoraw (--nano-raw)                           RUNNING healthily past the overlap stage

Flye names the cause itself. So on CAMI's NanoSim reads the CAMI long arm would have failed EVERY
Flye step, and under retry-then-ignore reported complete with no long-read assembly whatever.

**CAVEAT the A/B does not separate two flags.** The `asis` arm varied preset AND `--read-error`
together, so "the transform's HiFi branch fails, `--nano-raw` works" is what is proven. Arm C
(job 59601518, `--pacbio-hifi` with no `--read-error`) separates them. It only matters if the HiFi
branch is kept; taking the preset from the declared platform removes both at once.

**Owner / gate.** The experimenter's driver work. **Next action:** take the preset from
`read_metadata`'s platform / `length_class`, never from a quality score — a simulator's quality
string carries no error-model information.

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
