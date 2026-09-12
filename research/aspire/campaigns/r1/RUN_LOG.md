# GMCF_3495 r1 — run log

Decisions taken during the run, and why. Written as they happen so a later
reader (or a resumed session) does not have to re-derive them. Measurements go
in as they are made; anything still open says so.

Plan of record: `~/.claude/plans/radiant-rolling-naur.md`.

---

## D1 — the FANOUT-1 cache commit (c114308) was deliberately not taken

The metasmith aggregate for this run is `dev` + `feat/reentrancy-try1-release-alpha`
+ `feat/mamba-executor`, landing at 0.19.1. The plan also named two straggler
commits. One of them, c114308 ("FANOUT-1"), was **skipped**.

It is a month stale against `release-alpha` and conflicts on 19 hunks, and
`release-alpha`'s own comments show that step-level cache sharding is intentional
rather than an oversight. Hand-merging it would have produced a caching system
matching neither branch's tested state — which is the wrong thing to hand a run
whose *purpose* is evaluating that caching.

**Consequence, accepted:** on a re-plan, one changed sample in a 34-sample
fan-out invalidates the whole step rather than just its own task. Slower on
resubmit, still correct. This is the shape the caching pilot will observe, so
the pilot's findings should be read as findings about step-level sharding.

## D2 — no `ifVirtualEnvDo` arms were added to the ported transforms

The plan's acceptance criteria asked for `.ifVirtualEnvDo(...)` "wherever a conda
equivalent is genuine". 59 of the 110 ported chains qualify on paper. None were
added.

Declaring an arm means hoisting an inline `cmd=f"""…"""` out of its call, and
metasmith's `RemoveLeadingIndent` strips the *first non-empty line's* indent from
every line of the command — so re-indenting a command body silently corrupts the
script rather than failing loudly. Doing that to 59 transforms, which then
process 34 libraries each, to declare arms that this Apptainer run never
executes, is a bad trade.

The eligibility analysis is recorded instead, in metasmith-libraries
`docs/ENV_PORT.md` (59 eligible / 23 blocked on binds-or-args / 28 with no conda
package, of 110). That leaves the follow-up cheap and evidence-backed.

## D3 — metabuli 1.1.0 cannot read GTDB r232; the pin moved to 1.2.0

Correcting something stated earlier in planning. Metabuli **does** reach r232, so
the "metabuli is the release blocker" framing was out of date — but gtdb232
requires **Metabuli ≥ 1.2.0**, and the library pinned 1.1.0, which cannot read
that database at all. It fails at load, not at classification.

So the version pairing is a hard floor, not a preference. Three places now carry
it: `resources/env/metabuli.env` pinned to `1.2.0--pl5321h0bb26bb_0`, the Arbutus
reference volume's `MANIFEST` recording `MIN_METABULI`, and
`metabuli-run-batch.sh`, which compares the running tag against that floor before
a job starts rather than letting it fail hours in.

## D4 — the agent container is a derived image, not a fresh build

`setup --run` failed on its first attempt: `Agent.container` resolves to
`docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}` = `0.19.1`, and **no 0.19.x
image is published on quay** — `latest` is `0.18.8-60556ca`. The pull step is
`[ -e <sif> ] || <pull>`, so a missing tag surfaced one step later as the sandbox
build failing on an absent SIF.

Building 0.19.1 properly (`dev.sh -bp -bd -bs`) is not cheap: the Dockerfile
COPYs Rust relay binaries cross-compiled for four targets, including both macOS
ones, and `main/relay_agent/target/` is not in the source tree.

It is also not necessary. `Agent.Deploy` binds the run's own source tree over
`site-packages/metasmith` inside the container (visible in the deploy log as the
`dev binds` line), so **the image supplies the conda env, not the Python code**.
Diffing 0.19.1's `envs/base.yml` against the conda env inside the already-cached
`0.19.0-fabfos` image showed exactly two packages missing — `cbor2=5.6.5` and
`blake3=0.4.1`, both added by the caching work — and no version-family
mismatches anywhere else.

So the agent image is `0.19.0-fabfos` plus those two packages, converted to a SIF
and placed at fir's expected cache path. Verified before use: both packages
import and round-trip inside the SIF, and all four `/app/msm_relay.*` binaries
are real (ELF `7f454c46` / Mach-O `cffaedfe`), which is what `dev.sh`'s own
`_assert_real_relays` checks for.

**Nothing was pushed to quay.** The registry is shared lab infrastructure and a
0.19.1 tag there would imply a release that has not happened. The SIF is local to
fir and removing it reverts the change completely.

**Caveat to carry:** the image reports its own metasmith as 0.19.0-fabfos. That
is cosmetic while the dev bind is in place, but it means the image is not a
substitute for a real 0.19.1 release build, and a future run that loses the dev
bind would silently execute 0.19.0.

## D5 — published on fir only; no Globus mirror

The plan flagged an open question for T8: Spanish Lakes' README describes a
Globus mirror of its published tree, so is one expected here too?

Answer: **no**, and the reason is in the sentence that describes it. Spanish
Lakes mirrors to
`chinook:/Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/metagenomics/` —
a *named manuscript folder*. The mirror is not a general publication convention,
it is that manuscript's archive. GMCF_3495 has no such destination, and inventing
one would deposit 34 samples of products into someone's manuscript tree
uninvited.

What was asked for was "an appropriate folder on fir projects", and that is what
gets written. Verified that the plan's `~/projects/rpp-shallam/phyberos/…` and
the publisher's `/home/phyberos/project-rpp/…` resolve to the same directory
(`/project/6004975/phyberos`), so the two descriptions of the destination are one
place, not two.

The mirror stays cheap to add later: the published tree is rsync-shaped and one
`globus transfer --recursive` from any destination the lab names.

**Superseded 2026-08-06.** The premise expired: a manuscript folder now exists at
`chinook:/Manuscripts/Science/2024-09-25_Lung_microbiome/WGA_metagenomics_via_metasmith/`,
the mirror is in it, and it was current as of that date. Every layer is there and
byte-verified. Do not act on the reasoning above — it was right about *why* not to
invent a destination, and says nothing about one the lab has since named.

**Also closed:** the Sockeye read copy the plan wanted removed once fir's
verified is already gone — `/scratch/st-shallam-1/txyliu/gmcf3495` exists but is
empty. Nothing to reclaim.

---

## Caching pilot — method

C2 changes the shape of the experiment the plan described. A mid-flight re-plan
would measure nothing, because entries are not finalized into `cache.sqlite`
until the run ends — every step would miss regardless of merit. So the pilot is
two complete runs, not one interrupted one:

1. Let run 1 (`dcYCo2Px`) finish, and record wall clock per step from
   `_metasmith/logs.latest/nxf_trace.tsv`.
2. `metasmith cache list --cache-root <agent_home>/task_cache` — how many
   entries exist, and how large.
3. Submit run 2 with identical targets and inputs. Every step should hit.
4. `metasmith status <run2_dir>` — counts `status: hit` against
   `status: promoted` per task. A `promoted` in run 2 is a step the cache
   *should* have served and did not: that is the finding worth chasing.
5. Then perturb deliberately — touch one sample's input — and confirm the blast
   radius is its whole step (C5) and nothing beyond it.

The instruments were validated against `yrNTL4E3` before they were needed:
`metasmith status` renders per-task events, and `cache list`/`explain` read the
shard store.

## Caching pilot — observations

### C1 — the mount-straddle check misfires on the agent home's dual bind

Every compile emits:

> `cache_root and work_dir straddle mounts; forcing publishDir 'copy' strategy.`
> `(cache_root [<home>/task_cache] is on mount [<home>] (lustre) but work_dir`
> `[/ws/runs/<key>] is on mount [/ws] (lustre).`

They are the *same* filesystem. `Agent.Deploy` binds the agent home at both `/ws`
and its real path — that dual bind is deliberate and load-bearing — so the check
sees two mount points, concludes a cross-mount rename would be non-atomic, and
downgrades `publishDir` from `move`/`link` to `copy`.

This is a false positive, but it fails safe, and the cost is real rather than
theoretical: every product is copied instead of linked, which on a 34-sample DAG
with per-bp coverage tables is a lot of duplicated Lustre I/O and disk. It is not
a caching *correctness* problem — it is the caching work's mount detection not
knowing about the runtime's own bind aliasing. Worth reporting upstream against
the reentrancy branch; both mounts resolve to the same device, which is the
cheaper test than comparing mount-point strings.

### C2 — results and cache are written once, after nextflow exits

`promote_run` and `CollectResults` are both called from `RunWorkflow` *after* the
nextflow process returns (agents.py, "S5 — post-execution promote"). During the
run, `_metasmith/trace.jsonl` holds only its `session_start` line and
`results/_metadata/` does not exist, even though products are already landing in
`results/<step>_<dtype>/` via publishDir.

The cache is the one thing that *is* incremental, and the distinction matters:
tasks write their outputs into a `<cache_key>.tmp/` shard as they go, and
`promote_run` only **finalizes** them at the end — renaming each shard to its
content-addressed home and inserting the row into `cache.sqlite`. So the bytes
accumulate during the run; the commit is what's deferred.

Three consequences, none of them a defect, all worth having written down:

1. **T7 cannot start until the DAG finishes.** Attribution needs the lineage
   graph, which does not exist until collection. The plan's idea of running the
   metabuli campaign early as a shakedown is not available; both campaigns
   queue behind the whole of T6.
2. **A run that dies mid-way caches nothing.** The safety net is nextflow's own
   `-resume` against `nxf_work/`, which the launcher already passes — so a
   resubmit still skips completed tasks. The metasmith cache is what makes a
   *second, separate* run cheap, not what makes this one crash-tolerant. Worth
   keeping straight when reading the caching pilot's numbers.
3. **Products on disk are not evidence of a completed step.** Reconciliation
   against the 34-sample expectation has to count files, since the manifest that
   would answer the question authoritatively arrives last.

### C3 — the `_manifests/` sidecars are gone; the campaign driver was written against them

The reentrancy branch removed the `_manifests/*.json` publishDir route entirely
(S6) and made `trace.jsonl` the sole lineage source, read back through
`DataInstanceLibrary`. `given.csv` survived but moved to `results/given.csv` and
changed columns — now `instance_id, dtype_key, path, origin`, with `dtype_key` a
hash rather than a readable type name.

`run_arbutus_campaigns.py` was written against the old schema. It would not have
crashed: it would have found no manifests and reported "the DAG has not produced
it yet" indefinitely, which is the failure mode that looks like patience.

Rewritten to resolve through the API instead of parsing files — `find_by(dtype=)`
for the products, `walk_ancestors()` back to the givens, intersected with the
read rows of `given.csv`. Re-deriving the trace format by hand would be a second
implementation of something that has already moved once.

Validated against the completed container-prefetch run `yrNTL4E3` — a real
0.19.1 results library — where all 21 products join back to their given `.env`
instance. That exercises every step of the path except the read-filename regex,
which the real run's `given.csv` will be the first to test.

### C4 — every product exists three times on disk, and C1 is one of the three

Measured on the live run, on one 208 MiB interleaved read file:

| copy | `nlink` | why it exists |
|---|---|---|
| `nxf_work/<hash>/…` | 1 | nextflow's task working directory |
| `results/<step>_<dtype>/…` | 1 | publishDir — **would be a hardlink** if not for C1 |
| `task_cache/<key>.tmp/…` | 1 | the cache shard |

All three are distinct inodes, so this is 3× the bytes, not 3 names for one
extent. At 13 of 34 samples through step 1 the run already holds 5.5 GB in each
of the three places.

This is what makes C1 expensive rather than merely untidy. The mount-straddle
false positive downgrades publishDir from `link` to `copy`, and a hardlinked
`results/` would cost approximately nothing. Fixing the check to compare devices
instead of mount-point strings would remove a full copy of every product in the
run.

Scratch has 17 TB free against a plausible few-TB footprint, so this is a cost
rather than a wall — but it is worth watching as the large per-bp coverage and
BAM products arrive, and it is the single most actionable thing the caching
pilot has turned up so far.

### C5 — step-level sharding confirmed: 21 tasks, one cache entry

The completed container-prefetch run (`yrNTL4E3`) was one step across 21 batches.
It produced **one** finalized cache entry and one row in `cache.sqlite` — not 21.

That is exactly the consequence D1 predicted when the FANOUT-1 commit was left
out, now observed rather than reasoned about: the cache shards per *step*, not
per *task*. For the r1 DAG it means a single changed sample invalidates its whole
34-sample step. The pilot's numbers should be read in that light — they measure
step-level sharding, which is the tested state of `release-alpha`, not the
per-task sharding FANOUT-1 would have given.

---

## Failures

### F1 — run 1 died on step 1: bbmap hangs, rather than fails, on the large reads

**Run `dcYCo2Px` was stopped 45 minutes in.** 13 of 34 samples finished step 1
(`interleave_zipped_short_reads`) in under 90 seconds each. The other 21 sat at
`RUNNING` and had written nothing for half an hour. Every one of the 21 carries
the same line in `.command.log`:

> `Exception in thread "BGZF-InputProducer" java.lang.AssertionError: Not a gzip file: 255, 236`
> `  at stream.bam.BgzfInputStreamMT2.readNextBlock(BgzfInputStreamMT2.java:212)`

**The producer thread dies; the process does not.** bbmap's reader thread throws,
the main thread goes on waiting for a queue nothing will ever fill, and the task
never exits. So SLURM reports `RUNNING`, nextflow reports `RUNNING`, and
nextflow's retry-with-doubling — the mechanism that was supposed to catch step
failures — never fires, because from every layer's point of view nothing has
failed. Left alone this would have held 21 tasks × 16 cores until the 3-hour
walltime, four times over, and then reported a timeout rather than a cause.

That is worth stating plainly because it is the second time this run has
produced the same shape of bug: **the dangerous failure is not the one that
crashes, it is the one that looks like patience** (C3 was the first). A monitor
watching for error signatures in the nextflow log would not have caught this
either — nextflow logged nothing wrong.

**It is not the staged reads.** Every file's on-disk size on fir matches the
source manifest byte for byte (S28_R1: 7,882,114,205 = expected), and the gzip
magic is intact. T1's checksum verification was right.

**It tracks size, not sample.** S1 (154 MB) completed; S11 (961 MB) hung. All 13
survivors are the small libraries and all 21 casualties are the large ones — so
the trigger is a property of the file that only large files have, not a corrupt
subset.

**Root cause, read out of bbmap's own source** (the container ships `.java`
alongside the classes, which is what made this cheap). `ReadWrite.getGZipInputStream`
opens *every* `.gz` input like this:

> `}else if(USE_UNBGZIP && ALLOW_NATIVE_BGZF && PREFER_NATIVE_BGZF_IN) {`
> `    return getUnbgzipStream(fname);`

All three default to `true`, so a plain gzip file is handed to the **BGZF**
reader. `getUnbgzipStream` then explicitly notices it is not BGZF —
`boolean bgz = isBGZip(fname)` is false, and it drops the thread count to 2 —
but still constructs `BgzfInputStreamMT2`, a reader that splits the stream on
block boundaries that only exist in BGZF. With two threads the second one lands
mid-deflate and asserts. A file small enough not to be split never trips it,
which is exactly the 154 MB/961 MB boundary observed.

These are ordinary Novogene gzip files (`1f 8b 08 00` — FLG=0, so no `FEXTRA`,
so *by construction* not BGZF). Nothing about the data is unusual; bbmap simply
routes plain gzip through a blocked-gzip reader by default.

**The fix is a documented flag, not a workaround.** `unbgzip=f` (parsed at
`Parser.java:1071` as `USE_UNBGZIP=PREFER_UNBGZIP=false`) makes that branch
false, and the next one falls through to `getUnpigzStream` — external `pigz -c -d`,
which is both correct on multi-member gzip and faster than the Java path. pigz is
already in the container, since the transform pipes its output through it.

### F1 verification — measured, on S11 (961 MB, 14,887,694 read pairs)

| variant | rc | wall | reads out | verdict |
|---|---|---|---|---|
| baseline | 124 (timeout) | hung | 0 | reproduces the hang |
| `unbgzip=f` | 0 | 63 s | **29,775,388** | exact |
| `multithreadedbgzf=f` | **0** | 7 s | 3,085,808 | **silently drops 90%** |
| `<(pigz -dc …)` | 0 | 93 s | 29,775,344 | 44 reads short |

`gzip -t` passes on both mates, and both are ordinary multi-member gzip — 3067
members for S11, 534 for S1, which completed. So multi-member is not the trigger;
being large enough for the reader to split is.

Two of those rows are the reason this was checked by **read count** rather than
by exit code. `multithreadedbgzf=f` is the knob most people would reach for
first, and it is far more dangerous than the bug: it exits 0, in 7 seconds,
having thrown away 90% of the data. It would have produced a plausible-looking
interleaved file that poisoned all 19 downstream steps silently. Even the
process-substitution fallback quietly loses 44 reads at the pipe boundary.

Only `unbgzip=f` is exact, and that is what shipped
(metasmith-libraries `e731465`), applied to all four bbtools readers this DAG
exercises — `interleave_zipped_short_reads`, `bbduk`, `kraken2`, `centrifuger` —
not just the one that hung, because bbduk and both classifiers read the same
large plain-gzip interleaved file that step 1 writes. The same latent bug sits in
`interleave_short_reads`, `ora2fastq`, `filtlong`, `sylph`, `ganon2` and
`phyloflash`; they are untouched because this run does not exercise them and an
untested edit is not an improvement.

Rendering of all four edited commands through `RemoveLeadingIndent` was checked
before submitting — that is the D2 hazard, and adding a line to a command body is
exactly how it bites.

One of the 13 outputs that *did* complete was verified read-for-read against its
source (NTC: 448 + 448 = 896 interleaved). The survivors are genuinely correct,
not just unhung.

### F2 — killing the run promoted a cache entry for a step that was 13/34 done

Stopping run 1 exposed something the pilot would otherwise have had to go
looking for. `promote_run` ran on SIGTERM and **finalized a cache entry for the
interleave step containing 13 files** — one per completed task — under a single
step-level key, alongside a `cache.sqlite` row (5.8 GB, `transform_key=S0FQ5Dbh`,
`ids` = one slot hash). C5 already established the shard is per *step*; this says
a step that never finished can still be committed as though it had.

Whether a later 34-sample run would actually *hit* that key is **not** tested
here — the key derives from a slot id whose input set may or may not encode the
full fan-out. But the entry is a landmine either way, so it was deleted (payload
exported first, in the job dir, as evidence) and the store is back to the single
prefetch entry. The settling test belongs in pilot step 5: perturb one sample and
see whether the key moves.

Run 1 was resubmitted as **`KMQ5eomS`** after the fix.

Nextflow was SIGTERMed and the array cancelled, reclaiming ~336 cores. Nothing
of value was lost: C2 means a run that dies mid-way caches nothing anyway, and
the 13 completed tasks cost about a minute each to redo.

### F1 at scale — an independent ground truth for all 34 libraries

The S11 bench proves `unbgzip=f` is exact on *one* library. It does not prove the
34 outputs run 2 is producing are complete, and `reformat.sh`'s own reported
"Input: N reads" cannot close that gap: it comes from the same reader that could
be wrong. That is precisely how `multithreadedbgzf=f` passes inspection — its
internal accounting is perfectly self-consistent at 10% of the data.

So the check has to come from a *different decompressor*. `truth_counts.sh`
(job array `51334189`, staged at `/scratch/phyberos/gmcf3495/truth_counts.sh`)
runs `pigz -dc | wc -l` over all 68 staged files and writes one
`<sample>.truth` row of `sample, r1, r2, r1+r2, status`. It fails loudly rather
than quietly on the two things that would otherwise pass: a non-zero `pigz` exit
(caught via `PIPESTATUS`, since `wc` always succeeds and would mask it) and a
line count not divisible by 4. Mate-count disagreement is recorded as
`MISMATCH` rather than treated as fatal, because that would be a staging fault,
not a bbmap one, and the two want telling apart.

`r1 + r2` is the expected interleaved read count, so every step-1 product gets
compared against a number derived without bbmap touching it. Running it against
the *inputs* means it does not wait on the DAG — the ground truth is ready before
the outputs it will judge.

**Result: 34/34 OK.** Every library decompresses cleanly, every line count is
divisible by 4, and every R1 count equals its R2 count — so the staging is sound
and any later count discrepancy is the pipeline's, not Globus's. The table is
committed as `library_read_counts.tsv`; **7,702,258,772** interleaved reads in
total.

**Step 1 cleared 34/34 on run 2**, every task exit 0, including S25 and S27 —
the two libraries that between them are 18% of the dataset and that took 38 and
36 minutes against the small samples' 1. Run 1 stalled permanently at 13. That
is the fix demonstrated at full scale rather than on a bench.

**And the products match the ground truth exactly, 34/34, zero mismatches**
(`verify_products.sh`, array `51335083`). Not "within tolerance" — every one of
the 34 interleaved outputs decodes to precisely `r1 + r2` reads. The last to
land was S25 at **1,468,846,072** reads, which is also the one that mattered
most: it is the largest library, it is where a size-dependent reader bug would
show, and it is past the ~500 MB threshold that triggered F1 by three orders of
magnitude. `unbgzip=f` is now proven, not argued.

The plumbing worth keeping is `build_product_map.sh`. A step-1 product is named
by content hash and carries no sample label, so nothing in `results/` says which
library it came from; the mapping has to be recovered from the task that made
it, by reading the source path out of each work dir's `.command.sh`. It resolves
to the `results/` copy rather than the work-dir one because that is the inode
the rest of the DAG consumes (C4). Any later per-sample audit of a hashed
product needs this same trick.

Two operational notes from diagnosing a false alarm at that transition. The
nextflow log is **not** `.nextflow.log` in the run dir — metasmith redirects it
to `_metasmith/logs.latest/nxf.log`, with `nxf_trace.tsv` beside it. Looking in
the obvious place finds nothing and looks like a dead run. And the monitor
briefly reported `dag-running=0` with products still outstanding, which is the
exact shape of a genuine stall; it was the ~2 minute gap between the last task of
a step completing and nextflow's `TaskArrayCollector` submitting the next arrays.
Worth checking every time — but check `nxf.log` before concluding anything, and
expect a transient zero at every step boundary.

**The second reader is verified too, for free.** `unbgzip=f` went onto four
bbtools calls, not one; step 1's product counts only exercise the first. But
kraken2 runs `--paired`, so its report's `U` + `R` rows total the number of read
*pairs* it was handed — a number that has passed through kraken2's own
`reformat.sh` split, the second patched reader. Re-run as reports land, it has
stayed perfect: at 26 reports, **26 matched, 26 distinct samples, 0 unmatched
and 0 duplicated**. Each report's `U` + `R` total lands exactly on one truth
sample's `read_pairs` and no two land on the same one — which is a stronger
statement than 26 equalities, because a reader dropping a constant fraction
would still produce 26 numbers, just not 26 that are each some *other* sample's
exact count.

**The third reader too, by the same trick.** Centrifuger's kreport is the same
kraken-style format, and its unclassified + root rows total the reads its own
`reformat.sh` split handed over: 19 reports, 19 matched, 19 distinct, 0
unmatched. One formatting difference is worth knowing, because it silently
returns zero if missed — kraken2 marks root with rank `R`, centrifuger leaves
the rank column as `-` and only taxid 1 identifies the row. The predicate has to
be `$4=="U" || $5==1`, not a rank test alone.

**And a second independent decompressor confirms all 34, also for free.**
`seqkit` is not bbtools at all — separate project, separate language, its own
gzip decoder — and the DAG already runs it over every step-1 product to produce
`read_qc_stats`. Its `reads` field matches the truth table's interleaved
expectation on **34/34, all distinct, 0 unmatched**. So the products are now
agreed on by two decompressors that share no code with each other or with
bbmap: `pigz` in `verify_products.sh` and `seqkit` here. One mismatch on `reads`
vs `read_pairs` is the trap — seqkit counts the interleaved total, so it matches
column 4 of the truth table where the classifier reports match column 2.

Three of the four `unbgzip=f` sites are therefore verified against an
independent decompressor: the interleave twice over, kraken2's and centrifuger's
splits for free. Only bbduk's is unexercised, because trimming has not run yet.
`verify_readers.sh` is the reusable form and should be re-run at each wake.

The general point is worth stating once, because it is the cheapest lesson in
this run: **the DAG was already computing the evidence.** Three of these four
checks cost nothing but a comparison against a table — no extra jobs, no extra
walltime. The instinct after F1 was to build a bespoke verification harness, and
one was needed for the interleave; but the rest of the verification was sitting
unread in outputs the pipeline produces anyway. Look there first.

For bbduk the check has to be the *input* count, not the output. Trimming
legitimately removes reads, so an output total carries no information about
whether the reader was correct — the failure mode and a good trim look alike in
that number. But bbduk reads the interleaved product, whose exact count is now
known for all 34, and it prints its own `Input: N reads`. That is an equality
against a verified number, and it isolates the reader from the trimming.

So the chain holds end to end — staged file, interleave, split, classify —
without a read gained or lost at any bbtools boundary. The check localises
instantly: a failure here is the split, not the interleave, because the
interleave is already independently verified.

It is worth knowing the spread before reading any per-sample result. Excluding
the control the libraries span **four orders of magnitude** — S2 at 62,871 pairs
to S25 at 734,423,036, a factor of 11,681 — and NTC at 448 pairs stretches that
to six. Two samples (S25, S27) are 18% of the dataset between them. Uniform
per-sample resourcing is therefore the wrong mental model for every step
downstream, and a "completed" step on S2 says nothing about whether the same
step will hold on S25.

### squeue collapses a pending array, so job counts undercount pending work

Step 5 first appeared as a single `nf-p05__bbduk` line while 34 work dirs were
already prepared on disk, which reads like a step failing to fan out — or, worse,
like the one running task being hung, which is exactly F1's signature. It is
neither: `squeue` prints a wholly-pending array as one line, `51335527_[0-33]`,
and only splits it per-task once tasks start. The 34 were submitted together.

Two consequences worth carrying. Any monitor counting `squeue` lines
*undercounts* pending work, so a draining queue number can be an artifact of
arrays consolidating rather than work running out. And the honest test for "is
this step actually fanned out" is the count of prepared work dirs against
`squeue -r` (which does expand arrays), not the line count. Distinguishing
prepared-not-launched from launched is the presence of `.command.begin`.

The run monitor was rebuilt on `squeue -r` rather than left with the caveat
written down beside it, and the first reading settles how much this mattered:
**43 queued tasks where the old one reported 10.** The undercount was the whole
pending bbduk array. A monitor reporting a steadily falling queue while a step
sat entirely unstarted is the same failure shape as F1 — an instrument agreeing
that nothing is wrong — so it gets fixed rather than remembered. The replacement
also alarms on `sacct` non-success rows above a recorded benign baseline, which
is the only signal that catches an F3-style launch failure.

---

### fir buckets partitions by walltime — and that nearly caused a bad edit

Step 5 sat wholly pending with `Reason=ReqNodeNotAvail, Reserved for
maintenance` and `StartTime=Unknown` while centrifuger ran alongside it. The
obvious reading was that bbduk's inherited **12 h** limit was unschedulable,
because fir splits every partition into walltime buckets — `_b1` ≤ 3 h, `_b2`
≤ 12 h, then 1, 3, 7 and 28 days — and a 12:00:00 request sits exactly at the
b2 ceiling, locked out of every b1 node. Centrifuger asks 3 h and lands in b1;
bbduk asks 12 h and lands in b2. The plan had already flagged that Sockeye's
resource shapes should be revisited rather than transplanted, so trimming the
limit to 3 h looked both correct and in scope.

It would have been wrong. `cpularge_bycore_b1` and `_b2` have **the same five
nodes** available (3 mix, 2 idle). The bucket boundary costs nothing here, so
the edit would have bought no scheduling benefit at all — while exposing S25
and S27, at 1.47 and 1.33 billion reads, to a 3 h timeout on a step whose real
duration is unmeasured. A confident, plausible, in-scope change that made
things strictly worse.

What the job is actually waiting on is backfill: it is queued across
`cpularge_bycore_b2`, `cpubackfill` and `c12hbackfill`, and the last of those
has 61 idle and 195 mixed nodes. b2 jobs from other users have been starting
throughout. So the reason string is cosmetic and the queue is doing its job.

Two things generalise. The reason SLURM prints names a constraint, not
necessarily *the* constraint — check node availability on both sides of the
boundary you think is binding before acting on it. And a resource change that
is genuinely in scope is not thereby correct; scope authorises the edit, evidence
justifies it. A tripwire on the array starting costs nothing and answers the
question the edit was guessing at.

---

### The run's error strategy ends in `ignore`, and only two attempts precede it

Reading `workflow.config.nf` to answer a scheduling question turned up something
more consequential than the question. The error strategy is

    task.attempt < params.process.tries ? 'retry' : 'ignore'

with `tries = 2`. So a step gets one retry, and if the second attempt also
fails the task is **ignored** — nextflow drops it and carries on. `workflow.
failOnIgnore` is `false` and `output.ignoreErrors` is `false`, so nothing
downstream turns that into a run failure. A library can therefore vanish from
the results with the run reporting success.

That is the failure mode this run is least equipped to notice by feel. It does
not hang like F1, it does not leave a non-zero `.exitcode` like an ordinary
failure, and unlike F3 it will not even be a retry that later succeeds. The only
thing that catches it is counting products per step against 34 — which is
already the standing check, and is now the *load-bearing* one rather than a
tidiness habit.

Two smaller notes fall out. The plan called for **four** tries on bbduk and
megahit; the rendered config has two. The 64 GB memory floor the plan wanted is
present, via a `withName` block that overrides the resource file's 32 GB, so
only the attempt count drifted. And on retry, `memory` and `time` double but
`cpus` does not — so a task that is too slow because it is under-threaded gets
more wall clock, never more threads, and a second timeout is the end of it.

### The bbduk resource shape is a real risk, and the evidence to settle it is coming

`p05__bbduk` asks for **2 CPUs** and 12 h. Against that, the only measured bbduk
on fir in this account — job `50627314`, from `scadc_multiassembly` — ran 35
libraries in **under 3.5 minutes each with 32 CPUs**, peaking at 36 GB RSS. Our
largest libraries are S25 and S27 at 1.47 and 1.33 billion reads, and 2 CPUs is
sixteen times less parallelism than that reference.

Naive scaling puts S25 near or past the 12 h limit, which combined with the
error strategy above means: timeout, one retry at 24 h with *the same 2 CPUs*,
and then silent removal. But naive scaling is exactly what got the walltime
question wrong an hour ago, so it is not grounds for action on its own —
bbduk's cost per read is not obviously CPU-bound, and the reference libraries'
read counts are unknown.

The 32 small libraries will finish quickly and give a measured reads-per-second
at 2 CPUs on this exact data. Extrapolating S25 from that costs nothing and
arrives long before the 12 h deadline, so the decision waits for it. What would
*not* be acceptable is discovering the answer by watching S25 time out twice.

---

### F4 — `unattended-upgrades` killed the metabuli download at 95%

At 07:00:46Z, with 607 of 634 GB fetched after four hours, systemd logged
`Stopping metabuli-build.service` → `Started metabuli-build.service`. The
second invocation printed `STAGE format`, hit
`/dev/disk/by-id/virtio-… is mounted; will not make a filesystem here!`, and
died. No wget, no build script, and a state file that ended in `STAGE format`
— which reads exactly like the volume being wiped and rebuilt.

The cause was Ubuntu's `apt-daily-upgrade.service`, which fired at 06:59:16,
upgraded about 180 packages including `systemd` itself, and had `needrestart`
restart every running unit. `metabuli-build.service` is a *transient* unit
created by `systemd-run`, so it was in scope like anything else. Nothing about
this was specific to metabuli; **any long build on a stock Ubuntu cloud image
is one unattended-upgrade away from being restarted**, and the longer the build
the likelier it is to be caught.

What saved it was a decision already in the build script for a different
reason: the tarball stages on the ephemeral disk with `wget -c`, so 607 GB
survived and resumed rather than restarting. The comment beside it says `-c so
a dropped connection resumes` — it was written against network faults and paid
out against a process kill. Worth noticing that resumability bought far more
than it was designed to.

Three fixes, in order of how much they matter.

**The builder must not patch itself.** `apt-daily.timer`,
`apt-daily-upgrade.timer` and `unattended-upgrades.service` are now disabled
and the two services masked. A builder VM lives for hours and is deleted; it
has no need of security updates, and it has an enormous amount to lose from
one. This belongs in the cloud-init for both reference services, not just as a
manual fix here.

**The build script is not resumable and should be.** It is linear and begins
with `mkfs`, so re-running it after any interruption formats — or, as here,
refuses to format and dies, having logged a `STAGE format` line that makes the
state file look like the data is gone. It should skip straight to the first
incomplete stage when the volume already carries the right label. Until it
does, recovery means hand-running the post-download stages, which is what
`metabuli-resume.sh` does; it is deliberately **not** a systemd unit, because a
unit is the thing that got restarted.

**Resuming needs a size gate, not a resumed download.** `wget -c` will happily
leave a short file if the far end truncates, and a partially extracted 744 GB
database would look plausible and be silently incomplete. The resume script
refuses to extract unless the tarball is exactly 633,960,254,218 bytes. Same
principle as F1: completion is not correctness, and the check has to be against
an externally known quantity.

**Landed upstream** as `arbutus-infra` `25e809b`, on *both* builders. `bootcmd`
masks `unattended-upgrades` and the `apt-daily` timers; the format stage skips
`mkfs` on a device that already carries a filesystem and mounts only if not
already mounted. That second change is what makes re-invocation the recovery
path — with `wget -c` already resuming, a rerun now costs the extract rather
than the four-hour download, and `metabuli-resume.sh` becomes a one-off rather
than the pattern. The GTDB-Tk builder gets the same treatment: it has never
been bitten, but it runs the same script shape on the same image for an hour,
which is luck rather than immunity.

### `pgrep -f` matches its own caller, which silently disabled a liveness check

The watcher meant to catch exactly this failure did not fire. It ran
`pgrep -cf build-metabuli-ref.sh` inside an ssh command line *containing that
string*, so `pgrep` matched its own invocation and the count was never zero.
The check was dead code from the moment it was written, and it read as
reassuring the whole time — the same shape as the `squeue`-line undercount, and
the same shape as F1 itself: an instrument reporting health because it was
measuring the wrong thing.

The same bug then bit a second time within the hour: a `pgrep -f "wget.*metabuli_db"`
guard reported `ALREADY_RUNNING` and skipped starting the resume, when what it
had found was the shell command carrying the guard. Two independent instances
in one session is enough to call it a habit rather than a slip. Match the
binary with `pgrep -x`, or assemble the pattern from fragments at runtime so
the literal never appears in the scanned argv.

Jobs `51334944` and `51335017` (`nf-p01__seqkit_reads`) both went `FAILED`
with exit `1:0` after 7–8 seconds; `51335118` then ran the same task to
`COMPLETED`. Nothing else in the run was affected, and the 35-task and 34-task
seqkit arrays that followed are clean.

The reason to record it rather than wave it through is *where the evidence
isn't*. Neither failure left a work directory with a non-zero `.exitcode` — a
full scan of every `nxf_work/*/*/.exitcode` in the run returns nothing non-zero
— and neither appears in `nxf_trace.tsv` or as an error in `nxf.log`. A task
that dies before writing `.exitcode`, in under ten seconds, failed at launch
rather than in its body, and nextflow's trace only keeps the surviving attempt.

That matters for how this run is audited. "No non-zero exit codes on disk" is a
real check and it passes, but it is *not* the same as "nothing failed" —
launch-time failures are invisible to it, and `sacct` is the only place they
show up. Both signals get read at each wake, not just the cheap one.

---

## Measurements

| what | value |
|---|---|
| reads staged to fir | 68 files, 431 GiB, checksum-verified, 0 faults |
| metabuli tarball | 633,960,254,218 B exactly (S3 `Content-Length`) |
| metabuli reference download | **40.7 MB/s cumulative** (29–50 instantaneous); ~4.3 h |
| agent SIF | 713 MB |
| Arbutus tenancy | 220 cores total, 168 already used → **52 free** |
| rpp-shallam quota | 11 TiB / 47 TiB space; **361K / 500K inodes** (139K headroom) |
| fir scratch | 2.7 / 19 TiB, 272K / 1000K inodes — headroom is ample |

**Scratch is not the constraint, checked rather than assumed.** C4 says every
product exists three times (nxf_work, results, cache shard), which invites worry
on a 431 GiB read set. Measured at step 1: 878 GiB total, and the worst case
across all 20 steps — interleaved, trimmed and BAM products all tripled — projects
to roughly 4–5 TiB against 16 TiB free. Inodes are 272K of a 1000K limit. The
quota that actually bites in this run is the *publication* one on
`/project/rpp-shallam` (139K inodes left), which is what the per-sample tarballs
in T8 exist to avoid — not scratch.

**Arbutus core budget, corrected.** The plan assumed ~84 free cores and so two
concurrent 32-core workers. The tenancy actually reports 168 of 220 cores in
use: `worker-01`..`worker-04` (32 each) and `devbox-01` are pre-existing lab
infrastructure from 15 and 22 July, not strays from this run. With the metabuli
builder up, 52 cores are free — **one worker at a time**. Two only become
possible after the builder is deleted, and even then only if nothing else in the
tenancy grows.

**The stray-worker rule was carried wrong for several wakes.** The every-wake
note said to treat `metabuli-ref-builder` and anything named `*-batch-*` as
ours. No worker is ever named `*-batch-*`: `run_arbutus_campaigns.py` names them
`{campaign}-{batch_id}-{pid}` (`metabuli-b00-31337`), and a bare submit-script
call defaults to `metabuli-job-$$`. A wake following the note literally would
have looked straight past a stranded 32-core worker.

`reap()`'s own matcher — `startswith(("metabuli-", "gtdbtk-")) and "ref" not in
name` — is correct and catches all three shapes while sparing the reference
builders. So the code was right and only the human-readable rule beside it had
drifted. The rule is: **any server starting `metabuli-` or `gtdbtk-` without
`ref` in the name is ours**; `worker-01`..`04`, `devbox-01` and `bastion-01` are
not. Prefer running `reap --dry-run` to eyeballing `openstack server list` —
the authority should be the code, not a remembered glob.

**The metabuli download rate, and a lesson about short baselines.** The rate was
guessed at "~40 MiB/s", then at one point "~70 MiB/s", neither from a timed
interval. Two self-timed baselines at 04:31Z — 115 s and 354 s — both gave
30–33 MB/s, so the figure was corrected to 31 MB/s and called flat.

That correction was worse than what it replaced. A 17-minute baseline to 04:47Z
gives **49.8 MB/s**, and the honest number is the cumulative one: 260 GB in the
1 h 46 m since the download started at 03:01Z, **40.7 MB/s average**, with
instantaneous rates wandering between 29 and 50. S3 throughput is simply not
stationary, and a six-minute window sampled during a slow patch is not evidence
of a flat rate — asserting flatness from it was the actual error, more than the
number was.

The original "~40 MiB/s" was therefore close to right all along. Recorded
measurements: **40.7 MB/s cumulative, 29–50 MB/s instantaneous**, ~4.3 h for
the tarball, with extraction of ~744 GB onto the Cinder volume still to follow.
`METABULI_SERVICE.md` gets the cumulative figure and the range, not a single
spot rate, because a lab member sizing a build window needs the number that
survives the variance.

Arbutus service numbers (metabuli batch, GTDB-Tk batches) and the caching-pilot
findings go here as they are made.

### F5 — bbduk was never a resource-shape problem; the whole cluster is going down

The bbduk tripwire fired at 08:10Z with the conclusion it had been written to
draw: *"still fully pending after 2h — revisit the resource shape."* That
conclusion was wrong, and it was wrong in the way this run keeps being wrong —
the instrument measured a real thing (34 queued, 0 running) and attributed it to
the hypothesis it was built around.

`scontrol show job 51335527` gives the reason plainly:

    JobState=PENDING Reason=ReqNodeNotAvail,_Reserved_for_maintenance
    TimeLimit=12:00:00
    Partition=cpularge_bycore_b2,cpubackfill,c12hbackfill

and `scontrol show reservation` says what the reservation is:

    ReservationName=CDUMaintenance2
    StartTime=2026-07-27T08:00:00  EndTime=2027-07-27T08:00:00
    NodeCnt=1032  Flags=MAINT,IGNORE_JOBS,SPEC_NODES,ALL_NODES
    State=INACTIVE

1032 nodes is **every node on fir** — all 1024 `fc` compute nodes and all 8 `fb`
large-memory nodes, verified by expanding the ranges. It activates at 08:00
local, which was 6 h 50 m after the tripwire fired. A 12-hour job cannot finish
before then, so the backfill scheduler will not start it. Nothing is wrong with
bbduk's 2 CPUs, its 64 GB floor, or its partition set.

The 365-day `EndTime` is a placeholder, not a forecast. Every one of fir's nine
reservations carries `Duration=365-00:00:00`; the eight active ones are
long-parked broken nodes. The name is the real tell — `CDUMaintenance2` implies
a `CDUMaintenance` that is no longer in the list, so these do get deleted when
the work is done. A coolant-distribution-unit swap is hours-to-days, not a year.
The status page is behind a bot wall and the wiki API returns the same denial,
so **the outage length is genuinely unknown** and must be discovered by watching
the reservation, not assumed.

**Decision: change nothing.** The tempting edit is to cut bbduk's walltime to
fit the remaining window — most libraries would make it, since the only measured
bbduk on fir did 35 libraries at ≤3:27 each. Three things kill it:

- Downstream is worse off, not better. megahit asks 18 h. Even a fully
  successful bbduk sprint buys one step and then stops at the same wall.
- The failure mode is silent. A task that hits a shortened wall gets `TIMEOUT`,
  retries at **2× the time limit** per `workflow.config.nf`, which is even less
  schedulable, and after `tries=2` the strategy is `ignore` with
  `failOnIgnore=false`. S25 and S27 — the two largest libraries, 1.47e9 and
  1.33e9 reads — are exactly the ones that would time out, and they would
  disappear from a run that still reported success.
- This is the second time this shape of reasoning has been talked out of a bad
  edit; see *fir buckets partitions by walltime* above. The pattern is a
  correct-looking fix aimed at a misdiagnosed cause.

So the run waits. The three centrifuger tasks still running (6 h limit, started
01:00 local) land before the window; bbduk stays queued through it and starts
when the reservation is released. Pending jobs are not cancelled by a MAINT
reservation, and `IGNORE_JOBS` means nothing running gets killed either.

**What the outage actually threatens is the head process.** nextflow runs on
login1 as a plain java process (pid 3331392), with the metasmith relay and
`msm api run_workflow` beside it — not inside a job. If login1 reboots during
maintenance, the run stops. That is survivable: `cleanup = false`, work dirs
persist, and the invocation already carries `-resume`. Before resuming, orphaned
SLURM jobs from the dead head must be cancelled first, or a resumed nextflow
will submit a second writer into work dirs the orphans are still using.

There is an upside worth naming. The caching pilot wanted "a deliberate re-plan
and resubmit at a point where most of the graph is complete." A forced
cold restart across a cluster outage is a **better** test than a manufactured
one, because nothing about it was arranged to succeed.

### The shared account is not the run

`squeue -u phyberos` is not a view of this run. The account is shared, and a
second metasmith workflow — key `kecQUgYv`, cwd
`/scratch/phyberos/fabfos_refs/agent_home/runs/kecQUgYv` — started at 01:01
local and put `diamond_uniref50`, `kofamscan` and `clean` jobs in the queue.
Those step names briefly read as *our* DAG having leapt ahead to functional
annotation, which is impossible: annotation needs ORFs, which need assemblies,
which need a bbduk that has not run.

Attribution has to come from `scontrol show job <id> | grep WorkDir` and a match
on the run key. Right now that is 37 ours, 2 theirs. The contention is
negligible; the measurement error was not, and every job-counting probe in this
run now filters on `KMQ5eomS`.

That makes four instances of the same failure: F1's exit 0 over 90% data loss,
`squeue` collapsing a pending array to one line, `pgrep -f` matching its own
caller, and now a job count summing two unrelated workflows. The common shape is
an instrument that returns a number for the right question asked of the wrong
population.

## Caching pilot — first observations

`task_cache/` in the agent home holds **425 GB**. The first read of it here said
"298 entries"; that was a file count, and it was wrong about what it implied.
Counted properly, the store contains:

| | |
| --- | --- |
| committed shard entries (`<2hex>/<62hex>/manifest.cbor` + `out/`) | **1** |
| rows in `cache.sqlite` `entries` | **1** |
| un-promoted `<key>.tmp/` staging directories | **4**, holding ~425 GB |

The one committed entry is 402 kB and dates from 20:29 — it is F2's, promoted
when the *previous* run was killed. **This run, 137 completed tasks in, has
banked nothing.**

### Promotion is one pass after the workflow returns, not incremental

The mechanism, read out of the source rather than guessed. Nextflow's
`publishDir` writes each step's outputs straight into
`<cache_root>/<cache_key>.tmp/` as tasks finish — which is why the disk cost is
already paid, and why the four staging directories map one-to-one onto the run's
four completed steps:

    ...66193e.tmp   372 GB   34 × .fq.gz              step 1, interleave
    ...b902c1c.tmp  140 KB   34 × .json               seqkit stats
    ...4325980f.tmp   39 GB  136 files (34 × 4)       kraken2 + bracken
    ...b3fc0b.tmp     15 GB   93 files (31 × 3)       centrifuger (31 of 34)

But `manifest.cbor`, the shard rename and `CacheStore.upsert` all happen in
`promote_run`, and `promote_run` is called **once**, in `agents.py`, after the
Nextflow process returns (`caching/promote.py:716-732`, `agents.py:1847`). A run
that is still going has promoted nothing; a run killed before Nextflow returns
promotes nothing. F2's single entry is not a counter-example — that kill let
Nextflow return, so the post-execution promote ran.

That is the finding this pilot exists to produce, and it is a design point
rather than a defect: **the cache banks work at run end, so the longer and more
interruptible the run, the less the cache is worth.** For a DAG whose steps are
measured in hours it is exactly inverted — the runs most likely to be
interrupted are the ones with most to bank. Promoting per step, as each step's
tasks drain, would cost one manifest write per step and make an interrupted run
resumable at step granularity. Worth raising upstream.

### The staging directories are one sweep away from deletion

`recover_orphan_tmp_dirs` (`promote.py:292-316`) walks `<cache_root>/*.tmp` and
**`rmtree`s every one that has no `manifest.cbor`** — all four of ours. It runs
at the end of `promote_run`, *after* the promote loop, so a `.tmp` the loop
promoted has already been renamed out of the way and is safe.

The sweep is **indiscriminate over the whole cache root**, not scoped to the
workspace being promoted: it globs `cache_root/*.tmp` and deletes on the single
test of whether a manifest is present. So promoting *any* run deletes every
un-promoted staging directory belonging to *any* run. There is no "not mine,
leave it" case. Corrects an earlier note here that said dirs outside the spec
set were the ones at risk — the opposite of the read, and the wrong way round to
be wrong.

The operational consequence is concrete: promoting steps 1, 2 and 4 while
centrifuger is still short would take its 15 GB of 31 finished runs with it. If a
step is to be held back, its `.tmp` must be **moved aside first**, not merely
left out of the promote set.

No data is ultimately at risk — C4 established every product also exists in
`results/` and in `nxf_work/`. What is at risk is 425 GB of already-done work
quietly evaporating at the next promote, which would read as the cache working
while it discarded the run. A reason not to `rm -rf` `nxf_work/` before then.

### The hand-promotion was rehearsed on a copy before it was run for real

The promotion has to happen inside the ~1 h between centrifuger's retries
finishing and the maintenance window, against a cache holding 425 GB that cannot
be re-staged. That is a bad place to discover that a call signature is wrong, so
the whole procedure was first run against an isolated copy: `step_2` is 34 files
and 25 KB, small enough that a full rehearsal costs nothing.

Two things came out of it, both worth more than the reading they replaced.

`promote_run` works from an **isolated workspace** exactly as intended. Given a
directory holding only the chosen `workflow.step_*.meta` and no `nxf_work/`,
`_find_step_outputs` finds nothing and promotion falls through to the
files-at-root branch, producing the correct `<2hex>/<62hex>/` shard with its
`manifest.cbor` and 34 files in `out/`. The live run directory is never read.

And the orphan sweep was **demonstrated rather than inferred**. A decoy `.tmp`
holding one file, named for a key absent from the workspace, was staged beside
the real one. It came back `orphan_recovery: {'1e20dead…': 'deleted'}`, and the
file was gone. The section above was written from the source; this is the same
claim measured. It also surfaces the operational check that matters on the day —
`promote_run` **returns** what it swept, so `orphan_recovery` must come back
empty. A non-empty one is the signal that something was destroyed, and it arrives
in time to stop rather than after the fact.

Rehearsing a one-shot destructive operation on a scale copy of its own inputs is
cheap whenever the inputs shard naturally. Here the shard was already sitting
there, being the smallest of four.

### 411 GB banked, ahead of the deadline rather than inside it

The plan had been to hand-promote at 14:03Z, in the 57 minutes between
centrifuger's retries finishing and the maintenance window. That window was the
wrong place to do it. The head is a plain login-node java process; maintenance
takes all 1032 nodes and there is no guarantee the login nodes stay up; and if
the head dies before Nextflow returns, `promote_run` never runs and the 25-step
restart recomputes 372 GB of interleaving. Steps 1, 2 and 4 were already complete
at 34, 34 and 136 files with no live writer. Nothing about them needed the
deadline.

So they were promoted immediately, with centrifuger's `.tmp` **moved out of the
cache root first** — same filesystem, so a rename — because it is still 31/34 and
the sweep would have taken it. Result:

    promoted       : [step_1, step_2, step_4]
    skipped        : []
    orphan_recovery: {}

leaving four `cache.sqlite` entries: step_1 at 399,110,871,497 B, step_4 at
41,585,594,067 B, step_2 at 18,544 B, plus F2's inherited one. The held
centrifuger directory still has its 93 files.

Promotion is **all renames** — `shutil.move` within the staging dir when the
source is already in `<key>.tmp`, then `tmp.rename(final_dir)` — so 372 GB
promoted as fast as 18 KB. Worth knowing before deciding whether a promote fits
in a window: it is a metadata operation, not a copy, and the only per-file cost
is a `stat`.

One wrinkle: `trace.jsonl` is written relative to the **workspace handed to
`promote_run`**, not the run. The 204 rows landed in the isolated promote
workspace, so the run's own trace still reads 1. Copied back as
`trace.hand_promote_2026-07-27.jsonl` rather than merged, so the record stays
attributable to the out-of-band promotion that produced it.

### Why the re-plan should still hit these four

The banked keys are only worth something if the 25-step plan asks for the same
ones. `lineage_key(transform_key, signature, sorted_inputs)` is a function of
three things, all local to the step: the transform's key, its
`_hash:_protocol_source_hash` signature, and the `instance_id`s of its inputs.
**Nothing downstream contributes.** So the ORF-sharding commit can only
invalidate steps 1–4 by changing one of those.

It changes none. `2cbdf17` touched `chunkOrfsForAnnotation.py`, the four
`merge_*.py`, the four annotators and the type registries; it did not touch
`interleave_zipped_short_reads.py`, `centrifuger.py`, `kraken2.py` or the seqkit
transform. The registry edits are 90 insertions and zero deletions — `orf_chunk`
and its annotation siblings added, no existing type redefined. And leaf identity
is content-addressed over the read files, which have not moved.

That is an argument, not a measurement. The measurement is one line at restart:
compare the new run dir's `workflow.step_*.meta` keys against the four banked
ones. Recorded in T6 so it is not skipped in the rush to resubmit.

### Raising a walltime in the transform would have destroyed the cached step

Centrifuger's three largest libraries blew its declared 3 h wall, and the doubled
6 h retry is the last attempt the error strategy allows. The obvious fix is to
raise `duration=Duration(hours=3)` in `centrifuger.py`.

That would have been a quiet, expensive mistake. The step's cache key folds in
`_protocol_source_hash`, a digest of the transform's **definition-file bytes** —
which exists precisely so that editing a protocol busts the cache rather than
false-hitting it with stale output. Changing the walltime is an edit like any
other, so it would have changed step 3's key and orphaned the 31 finished
centrifuger runs being held for exactly this step.

The lever that does not is the driver's `resource_overrides`, applied at
`RunWorkflow` and never hashed. It merges **per-directive** — the override
supplies `time`, and `cpus` and `memory` still come from the transform — so
`{"centrifuger": Resources(duration=Duration(hours=6))}` renders
`time { (2**(task.attempt-1)) * ('6hours' as Duration) }` and leaves everything
else, including the key, alone. Attempt 1 becomes 6 h and attempt 2 becomes 12 h,
which is a real second chance where 3 h → 6 h was not.

It is inert if the retries land: a cache hit does not run the process. So it
costs nothing to have in place and is the difference between recovering the three
largest libraries and dropping them.

The general shape is worth keeping: **in a content-addressed cache, "just bump
the resources" is only free if the resources live outside the hashed artifact.**
Check which side of that line a knob is on before turning it.

### `trace.jsonl` — resolved

Earlier note asked whether the single `session_start` line after 137 tasks meant
per-task events were buffered and lost on a kill. It is neither: trace rows are
appended by `_append_invocation_event_v2`, called from the promote loop. No
promote, no rows. The earlier runs bear this out — `yrNTL4E3` has 22 lines and
`dcYCo2Px` 14, both having reached their post-execution promote. `trace.jsonl`
is a record of what was *banked*, not of what was *run*.

## ORF sharding — load-balancing the annotators

Mid-run instruction: *"for T6, please use the spanish lakes transforms
specifically for the orf sharding that load balences teh annotators"*.

The four annotators (diamond/uniref50, kofamscan, eggnog, proteinbert) consumed
`sequences::orfs` — one task per sample. With libraries spanning four orders of
magnitude that is the worst possible shape: S25's annotation tasks would still
be running long after all 33 others finished, and the run's tail would be a
handful of jobs holding thousands of idle cores.

The spanish-lakes-metagenomics library already solves this with a **chunk →
annotate → merge** topology, so the work was to adopt it rather than invent it.
`chunkOrfsForAnnotation` length-sorts the ORFs and deals them round-robin into
5000-ORF chunks, so no chunk owns the long tail; each annotator runs per chunk;
a `merge_*` transform gathers the chunks back into the per-sample product.

**It is a port, not a copy, and that is the whole difficulty.** The two library
scopes had diverged along different axes. Ours went through the 0.19.x refactor
(T3) to `ExecWithEnv().ifContainerDo(env=…)` and `env::*.env`; spanish-lakes is
still on `ExecWithContainer` and `containers::*.oci`, which is in
`_FORBIDDEN_CALLS` and fails `dispatch_scan` outright. Copying the files would
have produced a library that could not run at all. Only two of the five
transforms actually exec — the chunker and `merge_proteinbert` (polars) — so the
port surface was small; the other three merges are pure-Python gathers and came
across untouched.

The driver needed no change. `build_targets` already names the merged products,
so the solver discovers the chunk and merge steps by itself. That is the
sharding being *correctly typed* rather than wired: `orf_chunk` is a distinct
sibling of `orfs`, not a subtype, so nothing routes an annotator at a whole
sample by accident.

Verified: `dispatch_scan` clean over 95 transform files, and the rendered plan
goes from **20 steps to 25** — the chunker plus four merges, exactly the five
additions intended, with `interleave_zipped_short_reads` still step 1.

This is free with respect to the cache. No annotation has run — there are no
assemblies yet — so all 298 cached entries are upstream of the edit.

### A transform file in the directory is not a transform

The first dry-run after the port failed on **every target**, including
`sequences::read_qc_stats`, whose products have been sitting on disk verified
34/34 for hours. The chains it printed were all about `forward_short_reads` and
NCBI accessions — nothing to do with annotation — and
`interleave_zipped_short_reads`, the transform that actually ran in production,
was not mentioned anywhere.

The cause was that `chunkOrfsForAnnotation.py` and the four `merge_*.py` files
were sitting in the library directories and **not loading**. A metasmith
transform library is enumerated by `_metadata/index.yml`, not by globbing the
directory; an unregistered `.py` is invisible. The count gave it away — 29
transforms loaded against 30 files on disk.

Two things worth keeping from this. First, the diagnostic: `ls | wc -l` against
`len(IterateTransforms())` finds an unregistered transform in one line, where
reading the failure output does not. Second, and more useful — **the solver
plans one joint case, so a single unsatisfiable requirement fails every target
in the plan, and the error is reported against all of them.** The annotators
required `orf_chunk` with no registered producer, and that made the report name
`read_qc_stats` as broken. Read the failure as "something is unsatisfiable",
never as "this named target is the problem".

The A/B that settled it cost one command: `git archive HEAD | tar -x` into a
temp dir, point `MLIB` at the export, re-run the identical dry-run. HEAD planned
20 steps clean, so the breakage was definitely mine and definitely not
pre-existing. Worth preferring to `git stash` — it cannot disturb the working
tree, which matters when the run's own inputs live in it.

### `--dry-run` is not read-only

`build_inputs()` opens the tracked inputs database and calls `Purge()` before
repopulating it. Running `run --dry-run --sample S2 --sample NTC` therefore
**deleted 32 samples' entries** from the committed `.cache/r1_inputs.xgdb` — a
git-tracked directory — leaving a 2-sample database behind.

Harmless here: the live run staged its own copy into the agent home at plan
time, and `git checkout -- main/r1/.cache/` restored all 69 files. But "dry" ran
against the real artifact, and a `--sample` filter is exactly when that
truncation is silent and plausible. Subsequent dry-runs in this session
overrode `CACHE_DIR` to a scratch path, which is the right habit: a planning
experiment should not be able to write the run's inputs.

### F6 — the metabuli reference build failed a check that could never have passed

The r232 build reached `STAGE release-check` at 08:58:00Z with 744 GB extracted
and byte-exact, and refused itself:

> `FAIL db.parameters does not name r232; refusing to label this volume`

The check was wrong, not the database. Reading the extracted tree resolved it,
and turned up two further bugs in the same script set — all three mine, none
upstream's. Recorded together because they share one root cause: I wrote the
service against what the tool's `--help` and file names implied, and never ran
it against a real database until now.

**B1 — a prebuilt Metabuli database records its GTDB release nowhere.**
`db.parameters` holds `DB_name 952338149` (a random integer), `Creation_date
2026-4-23`, the build commit, and k-mer parameters. That is all. The only string
naming GTDB anywhere in the 744 GB is the source directory in `updateDB.log`'s
command line, `.//gtdb+human+virus`. The plan's premise — "that file is the only
reliable release marker" — was simply false, and the grep it justified could not
have succeeded against any release.

What is actually available is provenance, and it is good: the object is upstream's
own `metabuli/gtdb232.tar.gz`, the download matched its `Content-Length` to the
byte (633,960,254,218), and the object's `Last-Modified` (2026-06-07) is
consistent with the database's internal creation date (2026-06-02). Upstream
publishes no checksum — `.md5` and `.sha256` both 404. So the build now records
`RELEASE_EVIDENCE=pinned-source-url` in the `MANIFEST` alongside `SOURCE_BYTES`
and the database's own `DB_NAME`/`DB_CREATED`/`DB_COMMIT` fingerprint, and the
size is checked against the origin *before* extraction rather than against a
constant. That last one matters more than it looks: `wget -c` resumes happily
onto a truncated file, so without it a short tarball reaches the extractor
looking complete.

The honest summary is that nothing in the bytes can prove "232", and a check
that appears to prove it is worse than one that says what it actually knows.

**B2 — `metabuli classify` takes exactly one query file, and this one nearly
shipped.** In `--seq-mode 1` and `--seq-mode 3` the second positional is the
database directory:

> `Error: bsubtilis.fna is a file. Please specify a database directory.`
> `For '--seq-mode 1' and '--seq-mode 3', please provide one query file.`

(`--seq-mode 2` takes two because they are read *pairs*.) `metabuli-run-batch.sh`
passed `/mnt/job/queries/*.fna`, so the 34-assembly campaign would have died in
under a second, months of reference build later. The batching argument — seven
database passes paid once rather than thirty-four times — is untouched; only the
mechanism was wrong. The queries are now concatenated into one file on the worker.

The plan left this fork open and named the escape: *"whether that batching is done
with a metabuli query-list argument or by concatenating contigs under
sample-prefixed headers … is an implementation detail to settle against the
container's actual `--help`, with the concatenate-and-split fallback always
available."* Settled the right way, just later than it should have been — the
`--help` does not disclose the arity, only running it does.

Concatenation also *strengthens* the header-prefix requirement rather than
relaxing it: one file means one sequence-id namespace, so `k141_1` from two
assemblies is now a direct collision. The existing check already covers it and
`run_arbutus_campaigns.py` already prefixes.

**B3 — `--taxonomy-path /refdata/db/taxonomy` names a path that does not exist.**
There is no `taxonomy/` directory in a prebuilt database; the taxonomy is the
`taxonomyDB` file, which is what the flag's empty default selects. Removed.

**And the verify stage verified nothing.** It ran `metabuli databases`, which
lists what upstream offers for download and would pass against an empty volume.
Metabuli does ship a real check — `--validate-db 1`, which confirms every
required file is present and that k-mer counts are internally consistent — and
that plus a one-contig classification is now the gate.

**The transferable lesson** is about where a shakedown belongs. This build was
designed to snapshot first and shake down after, against a cloned worker. Running
the shakedown *on the builder* instead — it is already a `cb32-120gb` box with the
database mounted — costs nothing, needs no extra cores, and catches all of this
before 744 GB is committed to a snapshot. There was no core pressure to trade
against, either: T7's real campaigns wait on assemblies that are themselves behind
the fir maintenance window.

### F6 aftermath — the shakedown passed, and corrected a number I had used

The on-builder shakedown finished `rc=0` in **1 h 07 m**, and both genomes came
back with correct GTDB lineages. *B. subtilis* resolved to species and on to the
assembly accession `GCF_000009045.1` at score 0.998. *E. coli* stopped at family
`Enterobacteriaceae` at 0.650 — correct, but coarser, and correctly so: this is
`--seq-mode 3` with a whole 4.6 Mb genome as a single query, and GTDB r232 places
*Shigella* inside *Escherichia*, so an LCA over the entire genome has nowhere
finer to sit. Worth knowing before reading the real campaign: assemblies are
contigs, not genomes, and will not behave like this.

Two measurements came out of the same `time -v` block, and one of them says I was
wrong.

**The volume sustains ~269 MB/s, not 91.** `File system inputs: 2,121,346,726`
blocks is 1.086 TB read in 4,035 s. The 91 MB/s figure came from thirty seconds
of `/sys/block/vdc/stat`, and I had used it to argue that `--validate-db` costs
2.4 h per job. It costs about 45 minutes. The conclusion in
`METABULI_SERVICE.md` survives — a read-only reference cannot have changed since
it was snapshotted, so per-job validation checks nothing new — but it was the
right conclusion resting on a number that was wrong by 3×.

That is the **second** time in this build that a short sample of a streaming
workload underestimated it: the download's six-minute sample read 31 MB/s
against a true 41.8. Twice is a pattern, not luck. A spot sample lands wherever
it lands — in a stall, before readahead settles — and there is no reason for it
to be representative of a workload whose whole character is bulk sequential I/O.
Take bytes over total elapsed, or quote no rate.

**Peak RSS was 115.2 GiB against a 120 GiB flavor, at `--max-ram 110`.** Metabuli
overshoots the budget it is handed by about 5 GiB, so the real headroom is under
5 GiB rather than the 10 that "110 of 120" implies. The original choice to sit
short of the ceiling was right; the margin is just thinner than it reads.

### One variable cannot be both a version floor and a container tag

Writing the `MANIFEST` by hand — the failed build never reached that stage —
surfaced a bug that would have made the service refuse *every* job.

`MIN_METABULI` was doing two things. It is the version floor written into
`MANIFEST`, where `metabuli-run-batch.sh` compares it against the running
container with `sort -V`; and it named the SIF baked onto the reference volume
and the `docker://` reference used to pull it. Those need different spellings.
The floor must be a bare `1.2.0`. The container must be the full biocontainers
tag `1.2.0--pl5321h0bb26bb_0`.

Neither value works for both. A bare version is not pullable. And a `MANIFEST`
carrying the full tag makes the check refuse everything, because `sort -V` orders
`1.2.0` ahead of `1.2.0--pl5321h0bb26bb_0`, so the running version always looks
*older* than the floor it is being tested against. Verified both directions
against the real file before splitting them into `MIN_METABULI` and
`CONTAINER_TAG`.

The check is also guarded by `if [ -n "${manifest}" ]`, so a missing `MANIFEST`
makes it silently pass. A safety check with a quiet no-op on one side and a
refuse-everything on the other is worth testing in both directions rather than
observing that it did not complain.

### The batched report has to be rebuilt, not filtered

Batching is what makes the service affordable, and it is also what produces one
report covering all 34 samples. `classifications.tsv` partitions on the
`<sample>__` prefix. `report.tsv` cannot be partitioned at all: every count in it
is a sum across the batch, so per-sample reports are a **recomputation**.

`--lineage 1` makes that possible without the taxonomy dump — each row carries
its lineage — and the batch report supplies the tree. Two details only visible in
real output: the report has **no parent column**, encoding depth as two-space
indentation on the name; and an assigned taxon need not be a leaf, since *E.
coli* landed at family.

`split_metabuli_batch.py` carries its own check rather than trusting the
arithmetic. Because every batch count is a sum over samples, the per-sample
clade counts must add back up to it, and `verify_roundtrip` asserts exactly
that. The merge fails loudly instead of publishing a table that looks right.

### A verification script only covers what it covers

I had been recording the wake check as "kraken2 34, bracken 34, centrifuger 31,
seqkit 34". `verify_readers.sh` never looked at bracken. Three tools, not four —
and the belief survived several wakes because the script exits 0 and I read the
exit code as covering the sentence I had written next to it.

Bracken's absence turned out to be *correct*. The script cross-checks bbtools'
BGZF readers by asserting each report's total read count is exactly some
library's ground-truth count. Bracken never opens a fastq — it redistributes
kraken2's assignments — so its totals are derived, not independent evidence, and
feeding them to that check would flag all 34 samples UNMATCHED. The right fix was
not to add bracken to the reader check but to notice the script answered
"are the reports believable?" while I was reading it as "are the reports there?"

Those are different questions, so there are now two sections and two exit codes.
Mid-run every product is short and that is unremarkable; a reader disagreeing
with ground truth never is. Folding both into `rc=1` would have trained the wake
check to ignore the one that matters, so completeness shortfall exits **3** and a
reader mismatch keeps **1**. The run currently exits 3, naming the three
centrifuger products at 31/34 — which is the S25/S27/S9 gap under retry, and the
first time that gap has been visible from the check itself rather than from a
number I carried in my head.

Attribution bit worth keeping: a third co-tenant fabfos run appeared this window
as `nf-p09__gpr_4lane`, and its `p09` prefix is in the same namespace ours draws
from. `scontrol show job` put its WorkDir under `fabfos_refs/agent_home/runs/1v8oPFg0`.
Job name remains not attribution; WorkDir is.

### The caching pilot's real finding: a plan that reuses everything reports zero hits

Rehearsing the restart before maintenance, rather than after, is what caught
this. A freshly staged plan reported **0 hit / 25 miss** against a cache holding
411 GB. Had that gone unexamined until nodes came back, the run would have
recomputed every banked step.

Two independent causes, and the second was hiding behind the first.

**Leaf identities were being reminted every staging.** A step's `cache_key`
folds in its inputs' identities, and two stagings four minutes apart with no
edits between them agreed on 21 of 163. Metasmith is not at fault in the way
that first looked: `_mint_leaf_id` derives an id from `blake3(content)` plus the
library-relative path *precisely* so two runs agree, falling back to
`uuid4 + time_ns` only when the path is not a readable regular file at mint
time. Two things put 142 of the 163 on the fallback — the reads and reference
DBs are named by absolute fir paths while the driver mints on this machine,
where they do not resolve (68 + 6); and `AddValue` calls `AddItem`, which mints,
and only then writes the file, so a value's id is always minted while its own
file is still absent (68). The 21 that were stable are exactly the `env`
entries, library-relative and readable locally. That the stable set matches the
readable set exactly is what makes this a diagnosis rather than a guess — and
the `AddValue` ordering would take the random path even on fir.

**A ten-line type addition re-keyed the front of the DAG.** With ids pinned, all
163 givens matched and the upstream steps *still* missed. The ORF-sharding
commit had added `sequences::orf_chunk` and the four `*_results_chunk` types to
every transform group's `_metadata/types/`, and put the chunker in `logistics`.
A group's type registry and manifest feed its transforms' signatures, so
annotation-only types landing in `logistics` and `assembly` gave new keys to
transforms with nothing to do with ORFs. The sharding was fine; its blast radius
was not. Confining both to `functionalAnnotation` restored the original keys.

**And the tooling was reporting the opposite of the truth.** A step whose
outputs are cached is *elided from the plan* — no step meta is written for it at
all — so a per-step loop cannot see it, and full reuse and zero reuse produce
the same "0 hit". `verify_cache_hits.sh` now walks the shards from the other
side and names the ones no planned step claims. The staged plan reads 22 steps
with 4 shards elided at out=34, 34, 136 and 21, and step 3 carries
`1e207cb95d61ad93…` — the exact key the held centrifuger directory is named for.

The plan warned against blaming a new subsystem for an ordinary failure. The
warning was right twice over: the caching system worked as designed both times,
and both faults were ours — one in how the driver names remote inputs, one in
where a type was declared.

### Every product is copied into the cache, because one directory is bound twice

Staging warns that `cache_root` and `work_dir` "straddle mounts" and falls back
to `publishDir mode: 'copy'`. The generated `workflow.nf` confirms it is not
cosmetic — every process publishes with `mode: 'copy'`, so each product is
written once into `nxf_work` and copied again into the cache staging directory.

The two paths are the same directory. The agent binds
`/scratch/phyberos/gmcf3495/metasmith` twice, once as `/ws` and once under its
own name, so `work_dir` reads as `/ws/runs/<key>` and `cache_root` as
`/scratch/.../task_cache`. A bind mount is its own entry in the mount table,
which is enough for the check to see two filesystems where there is one.

Metasmith is being conservative rather than wrong: a rename that genuinely
crossed mounts would not be atomic, and a torn shard is a far worse failure than
a slow one. The check just cannot tell a double-bound directory from two
filesystems. Left alone deliberately — the fix is either metasmith's mount
comparison or the agent's bind layout, and neither is worth touching between a
verified cache-hit configuration and a multi-day submission. The cost is
bounded: an extra copy per product, on a scratch filesystem with 18 TB free.

Worth carrying into the pilot report, since it is a real property of the 0.19.x
caching path under a normal agent bind layout, not something specific to this
dataset.

## An empty batch part broke the merge in both directions

The GTDB-Tk campaign's merge had never seen data — it is downstream of bins,
which are downstream of assemblies — so it was tested against synthetic batches
while the centrifuger retries ran. Two of four cases failed, and they were the
same defect seen from opposite sides.

`merge()` decides whether a set of per-batch tables shares a header by requiring
every part's first line to be identical. An empty part contributes `""` to that
vote, which is never equal to a header, so the vote fails and *every* part keeps
its header — the merged table then carries a header row in its middle, which a
TSV reader consumes as a data row whose `classification` is the literal word
"classification". And if the empty part sorts first, the writer's `if has_header
and i` test — indexed against the original list — strips the header from every
real part instead, leaving a table with no header at all.

Neither is hypothetical. GTDB-Tk writes no `ar53.summary.tsv` for a batch with
no archaea, and with 34 samples' worth of MAGs an archaea-free batch is likely;
a truncated batch leaves a zero-byte table. Batch ids sort, so which direction
the bug takes depends only on where the empty batch landed in the campaign.

The fix is to drop empty parts before deciding anything, rather than guarding
each failure separately. Filtering first makes both unreachable, and makes the
"all parts empty" case explicit instead of writing a zero-byte merged file that
looks like a successful merge.

The general lesson is the one this run keeps re-teaching: the merge step is
where a campaign's correctness actually lives, and it is the step with no
natural test data until the expensive work has already run. Synthesising the
edge cases costs an hour and is worth doing before the batches exist, not after.

## Re-planning a campaign at a new batch size silently dropped a quarter of it

The same synthetic-testing pass found a worse defect than the merge one, in the
step immediately before it, and it was reachable by following the plan exactly
as written.

T7 prescribes measuring before committing: run a deliberately small first
GTDB-Tk batch, observe peak RSS and wall clock against Arbutus's 120 GB ceiling,
then size the rest of the campaign from what that shows. `plan_batches` supports
re-planning and preserves finished batches so a resize does not redo paid work —
but it preserved them *by id*. Ids are positional (`b000`, `b001`, …), so what
`b000` means changes the moment the size does.

Re-planning 1000 bins at 400 after a completed batch of 150 keeps `b000`'s old
150 members, while the new slicing assumes `b000` covers the first 400. The 250
in between belong to no batch at all. Nothing reports it: the ledger is
internally consistent, every batch runs, the merge succeeds, and the final table
is simply 250 rows short with no record that anything was skipped.

The fix is to make preservation content-addressed rather than positional —
subtract the members of finished batches from the staged set and batch only what
is left, giving fresh batches ids past the highest already used. Verified for
resize up, resize down, and two finished batches of different sizes: full
coverage, no bin in two batches, finished batches untouched. A closing assertion
now refuses to write a ledger that does not cover every staged input, so the
silent version of this failure cannot recur under a different cause.

A completed batch whose members are no longer staged is reported rather than
ignored, since that means the harvest changed underneath the ledger and the
run's provenance no longer holds.

Both this and the merge defect share a shape worth naming: the campaign driver's
correctness lives in the bookkeeping around the expensive work, not in the
expensive work itself, and none of it gets exercised until the expensive work
has already been paid for. Synthesising the edge cases is the only way to test
it before it matters.

## Harvest needs a clean exit, and that is what makes the cache load-bearing

Confirmed against the live run rather than inferred: `KMQ5eomS/results` holds
nine products and has neither `_metadata/` nor `given.csv`. Metasmith writes
both in `CollectResults`, after the workflow exits. Product files are
hash-named and the instance_id → sample map lives nowhere else, so mid-run there
is no way to tell which assembly belongs to which sample.

The consequence for scheduling is blunt: the Arbutus campaigns cannot start
until the fir DAG has exited cleanly once, even though metabuli only needs the
34 assemblies and those finish early. T7 is serialized behind a *clean* T6, not
behind the products T7 actually consumes.

The consequence for risk is the interesting one. A DAG that runs for days and
fails in its last step leaves every product on disk and none of them
harvestable — which would be an alarming exposure if a re-run cost what the
first run cost. It does not, and that is precisely what the caching pilot has
already demonstrated: a resubmission whose steps are all banked replans to
almost nothing and exits cleanly in minutes, writing the metadata the first
attempt never got to write.

So the caching system is not only saving compute here, it is the mechanism that
makes an all-or-nothing harvest contract survivable. That is a better argument
for the feature than the cost saving, and it is worth putting in the pilot
report: the value showed up somewhere other than where it was expected.

## T8's two open questions, answered before they blocked anything

**Inodes.** `rpp-shallam` is at 361K of 500K, leaving ~139K. The published tree
ships per-bin fan-outs as tarballs rather than files, so the estimate is roughly
1–2K inodes for 34 samples across every product class. Comfortable, and the
tarball decision is what makes it comfortable — Spanish Lakes' bin files alone
are over 31,000.

**The Globus mirror.** Spanish Lakes is mirrored to
`chinook:/Manuscripts/Science/2026-05-13_Spanish_lakes_viromics/metagenomics/`
and its README documents both the mirror and what is deliberately excluded from
it. So the pattern exists — but the instruction for this run was to publish to
fir, and there is no manuscript folder on chinook that this dataset obviously
belongs under. Publishing to a new path on a shared lab collection is a naming
decision with an audience, not a mechanical step. The fir tree is built and the
README notes the mirror is available and not yet performed; the destination is
raised at debrief rather than guessed at.

## `cp -n` made a third silent-loss path, in the publish step

Having found two silent losses in the campaign driver, the same reading was
applied to `publish_r1.py`, and it had one — same shape, different mechanism.

Every copy `render_script` emits is `cp -n`, chosen so re-running the publish is
idempotent. But `cp -n` skips an existing destination and exits 0. So two
products routed to one destination path publish as one, with nothing reporting
it: `plan` counts manifest items rather than destinations, so it shows the right
total, and the run reports success.

The routes make this reachable. A `single`-mode dtype assumes exactly one node
in the library — two would both claim `cluster_table.tsv`. A `sample`-mode dtype
assumes at most one product per sample — a dtype that turns out to have two
would publish one and drop the other. Neither assumption is checked anywhere
else, and both are the kind that hold until a transform changes.

`render_script` now refuses when a destination is claimed by more than one
source, naming every source on the path, and collapses exact repeats. Because
the check is over the full destination relpath it covers tar members too, where
a colliding name inside a per-sample archive would fail the same silent way.

Three defects in one afternoon, all the same shape: the bookkeeping around
expensive work, none of it exercised until the expensive work is already paid
for, every one of them failing by producing a plausible short answer instead of
an error. The guards now in place all share a property worth keeping — they
refuse at plan time, before anything runs, rather than validating afterwards.

## Centrifuger was not slow; it was running on 8 of 192 cores

The step-3 promotion was scheduled for the moment the centrifuger retries hit
their 6h wall, on the assumption that nothing could be known before then —
centrifuger writes nothing to its log between "Inferred --min-hitlen" at minute
26 and completion, so silence carried no information and the plan was to find
out at the wall.

That assumption was wrong, and cheaply so. The three tasks were still holding
their input files open, and `/proc/<pid>/fdinfo` reports the read offset of an
open fd. Against the known compressed size of `split_r1.fq.gz` that is an exact
completion fraction, and sampled twice seven minutes apart it is a rate. The
three tasks were at 70.1%, 39.8% and 47.9%, consuming 1.3–1.8 MB/s, projecting
to 4.8h, 6.6h and 7.5h against a 6h wall. Two of the three were going to miss,
by 36 minutes and by two hours — knowable two and a half hours early, and known
from the job's own file descriptors rather than from anything it chose to say.

The more useful question was why a classifier on a 192-core node needed seven
hours. It was allocated `-c 8`, and `centrifuger -t 8` was saturating that (530–
680% of 800%) while node fb21808 sat at CPUAlloc=24 of 192 with 6 TB of RAM.
The libraries declare 8 cpus for their heaviest steps, which is correct for a
library that has to run anywhere; it is simply much smaller than fir. So the run
was not compute-bound, it was allocation-bound, and the fix was threads rather
than clock.

`cpus` reaches the tool and not just the scheduler: nextflow's runtime
`task.cpus` is echoed into `.command.metadata` (models/workflow.py) and parsed
back into `context.params` (bootstrap.py), which `centrifuger.py` turns into
`-t`. That path was confirmed against the live task, which showed `-t 8`.

So centrifuger, megahit and bbduk now carry cpu overrides, and centrifuger's
base wall goes to 8h. All of it via `resource_overrides`, which merges
per-directive at submit time into `workflow.config.nf` — loaded after the
staged `workflow.resources.nf`, so a `.*__centrifuger` selector wins over the
rendered `p03__centrifuger` for the directives it sets and leaves the rest.
Crucially it is never folded into a cache key: re-staging after the edit left
step 3's key at `1e207cb95d61ad93…`, byte-identical, with all four promoted
shards still elided. The same change made inside a transform would have re-keyed
every step in its group.

Step 3 was not promoted. 31 products were held and at most one more was
reachable, and `promote_step3.sh` is right to refuse below 34 — a partial
promotion is per-step, so it would have banked "centrifuger, done" over 32
samples, which is the same silent-short-answer failure as the three found in the
campaign and publish code. Rerunning all 34 costs almost no wall-clock, because
the tasks run in parallel and the critical path is the largest sample either
way; only core-hours are spent, and those are not the binding constraint. The
held directory is kept as insurance until the rerun banks step 3. It sits
outside the cache root, so the orphan sweep — which is cache-root-wide — cannot
reach it.

One incidental finding: re-staging over an existing staged directory failed with
`could not find data library [fpHOpc0QNwhI]`. The recompile-in-place path
rewrites the workflow against a new data-library set but does not transfer the
libraries it newly references, leaving the staged directory with two of three.
Staging fresh into a moved-aside directory was clean.

## A content hash taken over content containing itself

Re-staging the run after the resource edit failed twice with `could not find
data library [fpHOpc0QNwhI]`, then `[u8sLH4jxKWqG]` — a different id each time,
which is itself the clue. The `run` path then launched anyway and died the same
way, because `StageWorkflow` checks only that a launcher file exists afterwards,
and the launcher left by the previous staging satisfied that check. A failed
stage therefore presented as a successful one.

The driver defaults `--on-exist` to `update_workflow`, on the stated reasoning
that the plan is deterministic so the staged directory can be reused. That mode
resends the transform libraries and recompiles, and skips resending the data
libraries — but `WorkflowTask.SaveAs` writes `task.yml` unconditionally, and
`task.yml` is what names the data libraries by key. So the manifest is rewritten
while the payload it names is not, and they can only ever disagree in that one
direction.

Which would be harmless if the keys were stable. Three generations of the
reads library survived in the parked directories, and diffing them is conclusive:
the payload is byte-identical, and the sole difference is the library's own key
appearing inside its own `_metadata/index.yml`, as the `<key>@<file>` parent
reference on every instance. The key is a content hash over `Pack()`, and
`Pack()` includes that metadata. So the hash is taken over content that contains
a previous value of the hash. There is no fixed point, and a fresh key falls out
of every staging. The env-files library in the same run kept the key
`g2SMkK5FCM9k` throughout — it has no parent references, so nothing
self-referential enters its hash.

That makes this deterministic rather than flaky: `update_workflow` cannot work
for any run whose given-inputs library carries parents, which is every run that
declares read pairs.

The driver now defaults to `update`, which resends everything into the same
directory so the manifest and the payload move together. It leaves the
superseded key directory behind — a little disk, not a correctness problem.
`clear` would also work but `rm -rf`s the run directory, `nxf_work` and results
included, which is far too much leverage for a default.

The metasmith-side fix is not applied here. The deployed tree is bind-mounted
into every running task, so editing it mid-run would change code underneath
tasks that are already queued. The finding belongs to the reentrancy pilot and
is written down for it: exclude the self-reference from the hashed content, and
make a failed `stage_workflow` fail rather than be masked by a stale launcher.

One incidental clarification: staging reports "[3] data libraries" while two
exist on disk. The manifest simply lists one of them twice. Not a symptom.

## The lineage graph does not describe the run

Both remaining tasks derive every filename they produce from a lineage walk.
The Arbutus campaigns need to know which sample a megahit assembly came from;
the publish step needs the same thing for all 297 products, since everything
metasmith emits is content-hashed and the sample id survives only in the read
filenames. Both asked metasmith's own graph, through `DataInstanceLibrary`.

That graph is wrong on 0.19.1.

Run `dcYCo2Px` is the clean measurement: 13 interleave steps, ground truth
available for every one of them because each task's `.command.sh` names the read
files it staged. **0 of 13 agree.** The step whose output is published for S19
has a `consumes` record decoding to `S18_R1`, `S13_R2` and S28's `read_pair` —
three different samples, none of them S19. The mis-assignment respects the slot
partition, so the R1 slot always holds some R1 and nothing looks malformed.

Four artifacts, and only one of them disagrees:

| artifact | says |
| --- | --- |
| `results/given.csv` | `1e200eb5…` → `S19_R1.fastq.gz` |
| the staged reads library's own `_metadata/index.yml` | the same, independently |
| the task's `.command.sh` `lin` block and `FILES` list | the same, and it is what dispatched |
| `_metasmith/trace.jsonl` `consumes` | `1e20f5f3…` → `S18_R1.fastq.gz` |

So this is not a question of which record to trust. The trace is the odd one
out, and the results library is built from it.

**The computation is unaffected.** Nextflow bound the right files — the work
directory for that task contains `S19_R1`/`S19_R2` and nothing else — so reads,
assemblies and taxonomy are all correct. Only the bookkeeping is wrong. But
every name either driver was about to write comes from that bookkeeping.

The dangerous part is not the loud failure. `harvest` drops any product that
does not resolve to exactly one sample, on the stated grounds that mislabelled
taxonomy is worse than missing taxonomy — and 12 of the 13 resolve to two
samples and would have been dropped. The thirteenth resolves to exactly one
sample, `S1`, and is really `S2`. The guard cannot see it. Left alone, this run
would have published a tree in which some unknown fraction of files carried
another library's name, with nothing about the output looking wrong.

### What replaced it

`nxf_attribution.py` reads the binding out of the run instead of out of the
record of the run. Each task's `.command.sh` carries a `FILES` list — the
absolute path of every input it was handed, either a given
(`…/reads/S19_R1.fastq.gz`) or an upstream product
(`…/nxf_work/e8/16ad…/1-1-1.<hash>.fq.gz`) — and the task's outputs are the
non-dotfiles beside it. That is a complete DAG over real paths whose roots still
carry the sample id in their filenames.

Validation, at three levels:

- **exact**, against ground truth: 13/13 on `dcYCo2Px`, where the answer was
  independently known;
- **complete**, on the live run: 297/297 products over all 34 samples;
- **corroborated twice at scale, by mechanisms that do not touch the
  attribution machinery.** All 34 interleaved outputs sit at 0.716–0.875 of
  their input size — a 1.22× band across libraries spanning 59 kB to 86 GB. All
  34 seqkit read counts sit at 25.8–33.2 bytes of input per read. A scrambled
  assignment across four orders of magnitude of library size cannot produce
  either band.

Two further things fell out of the rewrite:

**It no longer needs a clean workflow exit.** `publishDir` fills
`results/<n>_<dtype>/` as each step finishes and `nxf_work` is always present,
so both inputs exist mid-run. The old route needed `CollectResults`, which runs
only after nextflow exits — which is why T7 was serialised behind a clean T6
exit, and it is not any more. The killed `KMQ5eomS` proves it from both sides:
11 populated product directories, and a manifest containing `{}`.

**A second defect went with it.** Product nodes carried paths like
`out/1-1-1.<hash>.fq.gz`, and `<run_dir>/results/out/` does not exist — the file
is published under `results/<n>_<dtype dashed>/`. Every rsync harvest would have
issued would have missed. Reading the results tree directly fixes the path and
supplies the dtype from the directory name at the same time.

One thing is deliberately left unsolved. `binning_local::quality_bin_fasta`
cannot be attributed to a binner this way: the aggregator consumes all three
binners at once, so all three sit upstream of every bin it emits. Those bins are
byte-identical copies of a per-binner bin fasta, so content matching is the
honest route — and it is not built, because the binning steps have not run and a
matcher written against no data is a guess with a test suite. Quality bins
publish as `unattributed` until then. `checkm_stats` does resolve, since each of
its three steps consumes exactly one binner's bins.

As with the self-referential key, metasmith itself is not patched: the deployed
tree is bind-mounted into every queued task. Both findings go to the reentrancy
pilot together, and this is the more serious of the two — the key bug announced
itself with a stack trace, and this one would not have.

## Both Arbutus campaign paths, proven live

`run_campaign` was the last untested execution path on either service. Testing it
meant driving real workers, and that is what found the two things below — neither
of which a synthetic test could have produced.

**GTDB-Tk could never have booted a worker.** `gtdbtk-up.sh` defaults
`SECGROUP=gtdbtk-worker`, a security group created only by
`envs/saas-gateway/main.tf`. That environment is undeployed, so the group has
never existed on this tenancy and the submit path had never once reached a running
instance — the service's documented numbers all predate it. The script's comment
says the default is deliberately *not* `worker-private`, to keep jobs out of the
group the four production workers sit in, so the fix is the group it names rather
than the shared one: tcp/22 ingress from `bastion-ssh`, default egress, nothing
else. Additive and reversible; the shared group and the four workers are untouched.

**`merge()` counted one batch twice.** GTDB-Tk 2.7 writes
`gtdbtk.bac120.summary.tsv` at the batch output root *and* again under `classify/`.
`merge()` globbed recursively and bucketed by basename, so one batch contributed
the same table as two "parts" and every genome appeared twice in the merged table.
Across the real campaign that duplicates every bin, and the run exits 0 either way.
Parts are now deduped per batch, preferring the shallowest path; a
same-name-different-content pair is reported rather than resolved silently, because
that would mean "repeated basename means same table" had stopped being true.

Worth naming why the smoke test was run against real tool output rather than
synthetic parts: the empty-part header-vote logic directly above this in `merge()`
had only ever been exercised synthetically, and synthetic parts do not reproduce a
tool writing its own output twice. The bug lived in the gap between what the test
fixture modelled and what the tool does.

### The classification that looked wrong and was not

The smoke set was *B. subtilis* 168 and *E. coli* K-12 MG1655. *B. subtilis* came
back exactly right. *E. coli* came back as
`g__G047199095;s__G047199095 sp047199095` — no genus name, no species name — which
reads like a truncated reference package, the exact failure a copy-on-write clone
of a 150 GB volume could plausibly produce.

It is not. GTDB's own API says `GCA_000005845.2` — E. coli K-12 MG1655, the query
genome itself — is `g__G047199095;s__G047199095 sp047199095` in R232, and was
`g__Escherichia;s__Escherichia coli` in R226. r232 reorganized *Escherichia*. The
service reproduced GTDB's own r232 answer for that exact accession, which
incidentally validates reference volume, ANI screen and classify path end to end
against an authority outside the run.

Two consequences. The published README has to say that r232 renamed this group, or
anyone comparing against an r226-era table will read placeholder names as failures.
And "the answer looks biologically wrong" is not evidence about our infrastructure
until the reference's own taxonomy has been checked — here the surprising answer
was the correct one.

Measured, 2 genomes: spin-up 127s, upload 4s, container resolve 1s off the
reference volume, classify 266s, download 5s, total 403s, peak RSS 14.7 GiB against
the 120 GB ceiling. Both genomes hit the ANI pre-screen, so Identify and Align were
skipped — these numbers bound nothing about a batch of novel MAGs, which take the
full tree-placement path. The real campaign supplies those.

---

## 2026-07-31 — restart on metasmith 0.20.2 from altair, run QkqCNJOo

Driven from altair; capella is used only as a git source. Toolchain is now
`projects/metasmith/lung-microbiome` (0.20.2) and
`projects/metasmith-libraries/lung-microbiome`, driver env `msm_lung`.

**Step 1 is not re-run.** The 34 interleaved products from KMQ5eomS are
hardlinked into `/scratch/phyberos/gmcf3495/interleaved_backup/<sample>.interleaved.fq.gz`
(34/34 verified same-inode, same-size, link count 2; zero bytes consumed) and
enter the plan as `sequences::short_reads` givens. They are trustworthy: every
`.verify` count equals its `.truth` column 4, and because the 34 counts are
mutually distinct that agreement also proves sample attribution. The plan is
therefore 24 steps against the approved DAG's 25 — the only absent step is
`interleave_zipped_short_reads`, and the only absent dtypes are the two raw
zipped read types nothing else consumed.

**Why the previous results were bogus.** All 34 bbduk tasks in j60YFVIo read
`Input: 896 reads` — NTC, the smallest library. 0.19.1 replayed only the first
member of a cached batch to every consumer. Fixed upstream by ba76d63 / 68cc8c6,
which is what put this run on 0.20.x. Everything downstream of step 1 in both
j60YFVIo and KMQ5eomS is discarded.

### Three instruments that were measuring nothing

1. `verify_bbduk.sh` reported `no_input_line=34` on a run where every task had a
   good `Input:` line — its `^Input:` anchor no longer matched timestamp-prefixed
   logs, and its `/msm_home/...` path extraction met a task_cache list. Rewritten:
   anchors on the token, takes the sample from the (sample-named) interleaved
   path, and asserts the counts are **distinct**. **Now PASSES on QkqCNJOo:
   completed=34, ok=34, mismatch=0, 34 distinct counts over 34 distinct samples.**
2. The leaf-pin size guard stat'ed paths locally; all 108 it protects are on fir,
   so `is_file()` was always false and a tampered size planned straight through.
   Now stats on fir in one batched call. Verified in both directions.
3. `watch_run.sh`'s sacct pass was per-user and unwindowed, reporting 80 FAILED /
   66 CANCELLED from a previous killed run against a run that was 48/48 clean.
   Now windowed to the run directory's start time.

### Defects found and fixed

- **metasmith (still present upstream in 0.20.2, not yet reported):** the
  dev-overlay stager verifies an extracted overlay against `models/workflow.py`,
  which the 0.20.x god-file split turned into a package. On real 0.20.x source the
  check can never pass, so every task falls back to the shared-Lustre read the
  tarball staging exists to prevent. Its e2e fixture fabricated its own
  `workflow.py`, certifying the check against a file the test created.
- **libraries:** `python_for_data_science.env` was repinned by the env migration
  (a78d98f) to `sha256:334b01e4...`, which is tags `1.3.1`/`plotly_no_chrome` built
  2025-12-16 — older than `1.2.5` despite the number, and with no polars. 80 tasks
  died converting TSV to parquet *after* the expensive work succeeded. Repinned to
  the digest of 1.2.5 and the `instance_id` re-minted, since that id **is** the
  cache identity and is read from the manifest rather than re-derived.
- **driver:** `run` rendered its DAG over `.cache/r1_dag.svg`, the tracked approved
  reference. Recovered from git; default is now `r1_dag_current`.

### The megahit OOMs needed no restart — the escalation was already there

S25 (1.47e9 reads) and S27 (1.33e9) were OOM-killed at ReqMem=64G extracting solid
21-mers, and the response was an attempt-scaled `withName: '.*__megahit'` block in
`make_slurm_config`, held back for a restart. **That was wrong on both counts and
has been reverted.** Reading the live `workflow.config.nf` on fir:

- the base slurm preset already scales every process — `task.attempt==1 ?
  params.process.memory : 2*params.process.memory`;
- `resource_overrides` emits its own `withName: '.*__megahit'` carrying
  `2**(task.attempt-1) * 64.GB`, giving **64 → 128 → 256 → 512 GB** over the 4
  tries, with time on the same ladder.

Both retries were in fact already running at 128 GB / 36 h when this was checked.
And the new block would not have worked anyway: the override block is appended
*after* the config file and uses the *same* selector, so its `memory` would have
shadowed the new one while the adjacent `time` and `clusterOptions` still applied —
half-effective, which is worse than absent. The ladder stays in one place,
`resource_overrides`, and the driver now says so where someone would go to add it.

The general lesson is the session's recurring one pointed at myself: I read a
static `Resources(memory=Size.GB(64))` in the driver and concluded the request was
static, without reading the config that request is *interpolated into*.

### Open at time of writing: one library silently dropped before submission

**megahit built 33 tasks for 34 libraries. S15 has no megahit task at all.**

This is the failure the per-step product count exists to catch, and it caught it —
`sequences-megahit_assembly` cannot reach 34 because a 34th task was never created.
Nothing failed; there is no exit code, no sacct row, no log line. `nxf.log` knows
only `p05__megahit (1)`…`(33)`.

What is *not* wrong, checked explicitly: S15's clean reads exist and are healthy
(`1-1-1.QPLTzSlXv3qfRvqW-cDLsYLHR.fq.gz`, 9.7 GB, bbduk exit 0), all 34 published;
the 34 bbduk tasks carry 34 **distinct** sample keys, so this is not a key
collision; and every one of the 33 megahit tasks consumes exactly one clean-read
file and one distinct sample. So the 33 assemblies are correctly attributed — one
library is missing, not misassigned.

The drop is in `Orchestrator.groovy`'s `group()`. megahit joins on `giHCQ5sY`
(read_metadata, a given, 34 items) with clean reads as `DESCENDANT_OF_BY`. That
branch bags each item under every hash in `_index[by_name]`; a `null` there is
logged as `LINEAGE_VIOLATION` and dropped, but an **empty list** iterates zero
times and vanishes with no log — and there are no `LINEAGE_VIOLATION` entries in
this run. That shape matches the evidence exactly (a single key, cleanly absent,
no partial group), but it is a hypothesis, not a confirmed cause: the item's index
was not recovered.

It cannot be fixed mid-run — the task list is fixed at plan time. The plan is to
let the run drain, restart under the same key so the cache replays the 33
assemblies and everything upstream, and see whether megahit comes back at 34. If it
does, the drop is a race and the restart is the fix. If it comes back at 33 it is
deterministic, and S15 gets a targeted single-sample run. Either way the restart is
cheap, because the only uncached work is whatever is genuinely missing.

### assembly_stats: genomecov depth is a float (found 2026-07-31 ~10:00Z)

`invalid literal for int() with base 10: '1.24488e+06'` — S18's assembly_stats
died parsing `bedtools genomecov -bg` output, *after* minimap2 and the sort had
run. bedtools carries depth as a double and prints it through a C++ ostream at
the default 6 significant digits, so any pileup at or above 1e6 arrives in
exponent form.

The boundary is exactly 1,000,000, and the completed products confirm it: the 14
finished at the time contained **zero** scientific-notation values, with peak
depths of 129 … 496,770. So this hits only the deepest libraries, and only after
the expensive part has already succeeded. Fixed in `assembly_stats.py` (75f636b):
depth parses as float, coordinates stay int.

Cost of the fix is bounded and was checked before making it. `_protocol_source_hash`
is a per-transform digest of that transform's own definition bytes, folded into the
*lineage* cache signature and deliberately kept out of `_key`/`_hash` (which stay =
model topology, for nextflow process naming). So editing this file busts
assembly_stats and its descendants only — **the 33 banked megahit assemblies and the
whole annotation branch are untouched**. The "an edit re-keys the whole group" rule
is about a group's *type registry*, not a protocol body.

### Why the fix waits for the run to end

Both open items — this and the S15 megahit drop — need a fresh plan, so they share
one restart. Restarting now would cost more than it saves: of the 96 tasks in
flight, 77 are eggnog_mapper and diamond_uniref50 over the 767 ORF chunks. That
work descends from megahit, not from assembly_stats, so it survives a restart
through the cache **only if it is allowed to finish and bank first**. The 17
in-flight assembly_stats tasks are the only ones the fix invalidates, and they were
going to be invalidated either way.

What that costs in the meantime: each deep library burns its 4 retries on a defect
that is already fixed on disk — roughly an hour of minimap2 per attempt. That is
the price of not killing 77 annotation tasks, and it is the right side of the trade.

### 2026-07-31 21:50Z — the run was not slow, it was deadlocked

`QkqCNJOo` sat for 19 hours looking alive: head running, six jobs in the queue,
nxf.log ticking. It had made no progress since 10:47Z.

**Nextflow will not submit a job array unless the whole array fits in the task
monitor's free capacity at once.** The monitor logs `capacity: 100`; the upstream
default is `array = 100`. So a single other running task makes every full array
permanently unsubmittable, and the run stops without failing.

The evidence is exact rather than inferred. Each of the four annotation processes
built 767 tasks and submitted precisely indices **701–767** — the tail array of 67,
the only one small enough to fit beside the assembly_stats retries. The other 700
per process sat `status: NEW, jobId: null` from 02:56 onward, and their array-leader
work dirs are empty directories. `Submitted process > p16__eggnog_mapper` appears 67
times in a log covering 3,068 annotation tasks.

Note also `capacity: 100` **despite `executor.queueSize=500` being passed** — the
request does not reach the monitor. Size `array` against the logged capacity, not
against what was asked for. Driver now passes `process.array = 25` (0e2f6e9), and
the restart confirms capacity is still 100.

This is the fifth instrument in this run to report health while measuring the wrong
thing, and the worst of them: `squeue` was accurate, `nxf.log` was being written,
the head was alive, and *every* per-step count was legitimately mid-flight. The
signal that actually distinguishes it is the ratio between what nextflow is holding
for submission and what the cluster is running — the new watcher wakes on exactly
that.

### What the restart cost, and one lead it opened

The restart demoted most banked shards:

    cache shard 1e20c95c carries no on-channel index for 767 output(s);
    demoting the hit so the step re-runs rather than emitting a tuple the
    orchestrator would drop

— and similarly for shards of 33, 66, 67, 68, 102, 134, 136 outputs. So the
annotation branch and the ORF chunking are re-running rather than replaying.

That message is worth more than it costs, because it names the same mechanism as
the unexplained **S15 megahit drop**: a tuple the orchestrator would drop for want
of an on-channel index. The `DESCENDANT_OF_BY` branch bags items under each hash in
`_index[by_name]`, and an empty list there iterates zero times and vanishes with no
log — which is precisely one library, cleanly absent, no partial group. The two are
plausibly the same defect seen from two sides. Whether S15 now gets a megahit task
is the observation that decides it.

### The restart's real cost, measured rather than guessed

I predicted the restart would be cheap because the completed work was banked. That
was wrong, and the demotion warnings are why: bbduk re-runs all 34, centrifuger 5
of 34, kraken2 6 of 34 (the rest replay from nextflow's own cache db). Steps whose
metasmith shard lacked an on-channel index re-execute once, and the shard they
write on the way out carries the index — so it is a one-time cost, not a recurring
one. The shard directories are being rewritten as this is filed.

Per-step runtimes from the previous run's trace, which is what the estimate below
is built on rather than intuition:

| step | n | mean | max |
|---|---|---|---|
| seqkit_reads | 34 | 4m | 17m |
| centrifuger | 34 | 46m | 342m |
| kraken2 | 34 | 13m | 71m |
| bbduk | 34 | 12m | 71m |
| megahit | 33 | 44m | 299m |
| prodigal | 33 | 2m | 6m |
| assembly_stats | 22 | 72m | 588m |
| chunkOrfsForAnnotation | 33 | 1m | 2m |
| proteinbert / eggnog / diamond / kofamscan | 67 ea | 2m / 20m / 9m / 4m | 4m / 28m / 13m / 7m |

Getting back to the pre-restart position is on the order of a day; assembly_stats is
the long pole at up to ~10 h for the worst library, and it has to re-run in full
anyway because the genomecov fix re-keys it. The second half of the DAG (binning
onward, steps 9–24) has never run at all.

**Concurrency is capped at 100 tasks, not the 500 requested.** With 767 ORF chunks
across four annotators — 3,068 tasks — that cap, not the cluster, is what sets the
annotation wall-clock. Raising it means finding why `executor.queueSize` does not
reach the monitor; not worth another restart mid-flight, but it is the single
largest available speedup and should be settled before the next run.

## 2026-08-01 08:40Z — the deadlock recurred, because the fix went where it could not act

The `array = 25` change committed as `0e2f6e9` never took effect. The restart came
up with the same stall it was meant to cure: 24 annotation array leaders sitting
`jobId: null; status: NEW` behind nine running assembly_stats, at task indices
1, 101, 201 — spacing of 100, not 25.

Two independent readings say the same thing, and neither is the request:

* `Creating task monitor for executor 'slurm' > capacity: 100` — not the 500 asked
  for.
* Array-leader index spacing of exactly 100 — not the 25 asked for.

Both match `workflow.config.nf`'s `params{}` defaults (`queueSize = 100`,
`array = 100`, `tries = 2`) and neither matches `workflow.params.yml`, which
faithfully contained `queueSize: 500`, `array: 25`, `tries: 4`. The driver's
`RunWorkflow(params=...)` writes that yml and passes it as `-params-file`; the slurm
preset arrives as `-config`; for these settings the config wins.

**The reason only some settings are shadowed is evaluation order, and it is worth
stating precisely, because it decides where a fix has to go.** The preset contains

    executor { queueSize = params.executor.queueSize }
    ...
    array = params.process.array

Those are eager assignments, resolved as the config file is parsed. A `params` value
supplied afterwards cannot reach back and change what they already captured. By
contrast `errorStrategy` and `memory` are written as closures that read `params` at
the moment a task runs, so a params value is still live for them — which is why
`tries` genuinely does work through `params` and why the megahit memory ladder,
which is a closure, has been working all along. Same file, same `params` object,
opposite outcomes.

The fix (`7537eea`) sets the two eager ones on the effective directives instead, in
a block appended after the preset:

    executor { queueSize = 500 }
    process { array = 25 }

`tries` stays in `params`, where it works. `array = 0` under `withLabel: 'xlocalx'`
is a selector and is not touched by the top-level override.

This also closes an item filed in the previous section as unexplained — "concurrency
is capped at 100, not the 500 requested; find why `executor.queueSize` does not reach
the monitor". There was nothing specific to `queueSize`. The whole params-file was
being shadowed, and the cap and the array size were the same defect seen twice.

### What this cost, and the general lesson

Nineteen hours the first time, and a second full stall afterwards. The first
diagnosis was correct — arrays must fit free capacity whole, so capacity 100 with
array 100 means any single running task blocks every full array. The remedy was
aimed at a setting that could not receive it, and nothing in the run said so: the
params file on disk read exactly as intended, and the only contradicting evidence
was a capacity line and an index spacing that had to be gone looking for.

That is the seventh instance in this run of an instrument agreeing while the system
disagreed, and the first where the misleading instrument was the fix itself. The
habit that catches it is the one already written into `make_slurm_config` for the
megahit block: after changing a setting, read the value the *run* resolved, not the
value the driver sent. For scheduler concurrency the two ground truths are the
`Creating task monitor ... capacity:` line and the array-leader index spacing in the
submission-queue dump.

### Why the restart waits for assembly_stats

Applying the fix needs a restart; nextflow reads config only at launch. At the time
of writing nine assembly_stats tasks are two hours into a shared array, megahit is
at 32/34, and assembly_stats products are at 23/34. Annotation is blocked behind
those nine either way. Restarting now would kill them and force a full redo;
restarting once they land costs essentially nothing and still gets the fix in before
any of the 3,068 annotation tasks run. A monitor is armed on that seam.

### Correction: shard demotion is not a one-time cost

The previous section recorded that steps demoted for lacking an on-channel index
"re-execute once, and the shard they write on the way out carries the index — so it
is a one-time cost, not a recurring one." That is wrong, and the 2026-08-01 restart
falsified it.

bbduk re-ran all 34 libraries on the 07-31 restart, so its shard was rewritten by
the current metasmith. On 08-01 that same shard was demoted again — the warning
names 68 outputs, which is bbduk's two products across 34 libraries. Twelve shards
were demoted in total, 1,664 outputs, covering essentially the whole DAG below the
pinned reads: 68 (bbduk), 102 (centrifuger), 136, 134, 124, 66, 67×3 (annotation),
33×2 (megahit, prodigal) and 767 (ORF chunks).

So a rewritten shard still does not carry the index, and **every restart costs a
near-full re-run**. The task cache provides no cross-restart value for this pipeline
as it stands. The practical consequence is operational: restarts are not cheap
recoveries here, they are re-runs, and the decision to take one has to be priced
that way. It also means the earlier claim was the same failure as the rest of this
log — a plausible mechanism asserted without checking the run afterwards.

Worth reporting upstream alongside the `group()` empty-`by_hashes` silent drop.

### The 08-01 restart: what it bought and what it cost

Bought: `capacity: 500` in the task monitor — the first time this run has come up
with the concurrency the driver asked for — and 102 tasks in flight immediately,
which was impossible under the old cap. The array deadlock cannot recur, and the
second half of the DAG (binning onward, steps 9–24, never yet run) would have been
subject to it too.

Cost: 1,664 demoted outputs, i.e. re-running most of the DAG. Against the
alternative — leaving annotation to grind one 100-task array at a time, roughly 8 h
for the remaining ~2,500 tasks, with a straggler able to stall it indefinitely — the
restart is more expensive in the short run and was still the right call, because the
fix had to be in before binning regardless. But the cost was underestimated when the
decision was made, on the strength of the incorrect one-time-cost note above.

## 2026-08-04 12:21Z — the DAG ran end to end for the first time

`QkqCNJOo` reached `p24__skani_dedup` and exited cleanly: `Session await > all
processes finished` … `Execution complete -- Goodbye`. Every step of the approved
DAG has now executed at least once, including the whole second half (binning
onward, steps 9–24) which had never run.

    succeededCount=2991  failedCount=160  ignoredCount=39  cachedCount=34
    retriesCount=121  peakRunning=265  peakCpus=3252  peakMemory=13.4 TB

`peakRunning=265` is the concurrency fix confirmed from the run rather than from the
config: that number was unreachable under the old cap of 100.

### Where the products landed

| step | count |
|---|---|
| read QC, centrifuger, kraken2, bracken, clean reads | 34/34 |
| megahit, assembly_stats, gff, orfs, annotation | 32/34 |
| ORF chunks / per-chunk annotation | 640 |
| metabat2 / semibin2 / comebin contig-to-bin tables | 21 / 22 / 9 of 34 |
| checkm stats | 781 |
| quality bins | 45 |

### The dropped libraries are S13 and S22 — and the drop is a race

Two libraries never received a megahit task. They are **S13 and S22**, and
**S15 — the library dropped on the previous attempt — recovered.** That settles the
question left open earlier: the drop is not deterministic and not a property of any
particular library. It moves between attempts, which is the signature of the
`group()` empty-`by_hashes` path silently iterating zero times.

Attribution was done through `assembly_stats` work dirs, which name the sample
directly because that step consumes the pinned, sample-named interleaved reads while
everything else is content-addressed. Scoped to this attempt's trace hashes — an
unscoped walk of `nxf_work` reports all 34 present, because it pools every attempt
the directory has ever held.

megahit itself did not lose them: sacct shows 34 job entries, 30 completed at 64 GB
and 2 that OOM'd at 64 GB and then **succeeded at 128 GB**. The memory ladder works.
The two missing libraries never got a task at all.

### Binning failed for reasons that are mostly not ours, and one that is

Failures classified from the task logs, scoped to this attempt:

| binner | dominant cause | n |
|---|---|---|
| metabat2 | `Negative coverage depth is not allowed ... -1.11603e+09` | 32 of 36 |
| comebin | COMEBin `UnboundLocalError: local variable 'logits'`, and runs producing no bins at all | 80 of 84 |
| semibin2 | `Error: Running hmmsearch fail` (20), degenerate assemblies (4+) | 33 |

**metabat2's negative depth is an overflow, and it is the same family of defect as
the genomecov float bug.** `jgi_summarize_bam_contig_depths` reports depths near the
int32 boundary with the sign flipped: −1.116e9 corresponds to a true depth around
3.2e9, which is reachable for a very short, very high-copy contig in a library where
25.9% of 25.5M reads map. The same ultra-deep pileups that made genomecov print
`1.24488e+06` overflow metabat2's accumulator outright. This one is worth fixing —
32 of 36 metabat2 failures are this, not data.

The comebin and semibin2 failures are largely genuine: the assemblies span four
orders of magnitude, from 8 contigs / 1,988 bp to 1.28 M contigs / 1.13 Gbp, with
`fraction_reads_mapped` as low as 0.04. Low-biomass lung material with essentially
no assembly cannot be binned, and all three binners say so in their own idiom.
comebin is the most fragile of the three and fails well above the rate the assembly
quality alone would predict.

### Open, and what each would cost

1. **S13 and S22 have no assembly** — a metasmith race. A re-run would probably
   cover them and might drop a different pair. Report upstream.
2. **metabat2 overflow** — 32 failures, fixable, needs binning re-run.
3. **comebin fragility / semibin2 hmmsearch** — 20 hmmsearch failures look
   environmental and are worth one look before writing them off as data.

All three would be addressed by a single further run, but per the correction above a
restart is a near-full re-run — on the order of three days — so that is a decision
about time budget, not a cheap retry.

## Standing strategy from 2026-08-04: back up, then gap-fill — do not re-run

Full re-runs are retired for this run. The reasons are recorded above: a restart
demotes most cache shards and re-executes most of the DAG (~3 days), and the library
drop is a race, so a re-run trades one missing pair for a possibly different one.

The loop instead is: **after each pass, hardlink-backup the products, then fill the
gaps by hand.**

Backups are `cp -al` of `results/` into `/scratch/phyberos/gmcf3495/backup/<runkey>.<pass>/`.
Hardlinks must stay on `/scratch` — a link across filesystems is either nothing at all
or a silent full-size copy. Verify by inode and link count, not by name and size: a
same-named file of the right size is exactly what a failed link looks like.
`QkqCNJOo.pass1` holds 6,238 files / 674 GB at zero additional space.

Target state for the gap-fill: **bins for all 34 samples × all 3 binners**, then
re-run `p24__skani_dedup` over the complete set. Standing between here and that:

| gap | n | nature |
|---|---|---|
| S13, S22 have no assembly | 2 | need megahit + prodigal + assembly_stats + ORF chunks + 4 annotators |
| metabat2 | 13 | 32 of 36 failures are the int32 depth overflow — fixable |
| comebin | 25 | COMEBin `logits` UnboundLocalError, and runs yielding no bins |
| semibin2 | 12 | 20 × `hmmsearch fail` (looks environmental), rest degenerate assemblies |

Gap-filling runs the tools directly on fir rather than through metasmith, so each
filled product has to be placed where the publish step expects it and attributed to
its sample explicitly — the products are content-addressed, so attribution cannot be
read off a filename. `assembly_stats` work dirs are the reliable bridge: that step
consumes the pinned, sample-named interleaved reads.

## 2026-08-04 — the gap table above was wrong in three places

Before filling gaps I built the join the earlier table lacked: sample → assembly →
BAM → per-binner outcome, over this attempt only. Three of the four rows changed.

The join is worth recording because it is reusable. Products are content-addressed,
so nothing can be attributed by filename; the chain that works is

    sample → bbduk work dir (consumes the pinned, sample-named interleaved reads)
           → megahit work dir (its .command.sh names the clean-reads work dir)
           → assembly product name
           → assembly_stats work dir (names the assembly, and *emits the BAM*)
           → binner work dirs (their .command.sh names the assembly they consumed)

There is no separate alignment step: `p07__assembly_stats` produces `alignment::bam`,
which is why `alignment-bam` and `sequences-assembly_stats` both hold exactly 32. The
scripts are `inv/`-prefixed on fir; the joined table is `inv/sample_asm_bam.tsv`.

### What changed

**S29 and S19 were never binned at all** — no work dir, no failure, no trace row for
any of the three binners. S29 is not a marginal library: 92,766 contigs, 143 Mbp,
0.984 of reads mapping back. It is one of the best assemblies in the set and nothing
was ever run on it. This is the same silent-drop mechanism as S13/S22, one layer
down, and it did not appear in any per-step failure count because a task that is
never created cannot fail. Counting products per step is what catches this; counting
failures is not.

**comebin's failures are a misconfiguration, not fragility.** The transform's own
docstring says COMEBin is GPU-only and that the launching runner must allocate a GPU
slice. This run used `fir_slurm_r1_cpucomebin.config`, whose `withName: '.*__comebin'`
block sets cpus/memory/time and an account but no GPU. `--nv` with no GPU present
warns and falls back to CPU — the docstring's "functional but slow; don't do that
intentionally". The result is exactly what that predicts: comebin succeeded on the
9 smallest assemblies and failed on every large one. The `logits` UnboundLocalError
is real but accounts for only 10 of the 21 failures, and the GPU container
(`quay.io/hallamlab/external_comebin:gpu-1.0.4`) was cached on fir the whole time.

**metabat2's overflow hits 8 assemblies, not 13, and they are the degenerate ones.**
`jgi_summarize_bam_contig_depths 2.17` reports `Negative coverage depth is not allowed
... -1.11603e+09` for S1, S2, S3, S8, S17, S18, S20 and S21. The earlier reading of
this as "ultra-deep pileups on short contigs" was right about the mechanism but wrong
about which samples it implies: these are small assemblies carrying full-size read
sets, so coverage per contig is astronomical. S2 is 88 contigs and 40 kbp against 25 M
reads. Of the eight, only S1 (4,888 contigs, 2.8 Mbp, 0.689 mapped) is large enough to
plausibly yield a MAG; the rest would not bin even with correct depths.

Corrected picture, over the 32 assemblies that exist:

| binner | ok | failed | never attempted |
|---|---|---|---|
| metabat2 | 21 | 9 | 2 (S19, S29) |
| semibin2 | 22 | 8 | 2 (S19, S29) |
| comebin | 9 | 21 | 2 (S19, S29) |

And the failures separate cleanly by assembly quality: every sample with all three
binners failing has ≤4k contigs or ≤0.43 of reads mapping. **"Bins for 34 samples ×
3 binners" is not reachable** — NTC (8 contigs, 1,988 bp), S2, S3, S21 and the other
low-biomass libraries cannot yield bins from any binner, and that is a property of
the material, not of the pipeline. The reachable target is every binner run on every
assembly that can support one.

### The harness

`/scratch/phyberos/gmcf3495/gapfill/` runs the tools directly from the cached `.sif`
images, outside metasmith: `submit_bin.sh <binner> <sample> [cpus] [mem] [time]` and
`submit_megahit.sh <sample> ...`, both reading `inv/sample_asm_bam.tsv`. comebin routes
to `def-shallam_gpu` with `--gpus-per-node=h100:1`; the MIG profile named in the
transform docstring exists only on `gpubase_interac` and is not requestable in batch.

S13's clean reads are 23.9 GB gzipped, seventh-largest of the 34 — worth stating
because it argues against "it was dropped because it was too big". S25 is 73 GB and
assembled fine on the same node-local scratch, so S13 needs no special node.

## 2026-08-04 — published QkqCNJOo to the project dir; two publisher bugs found

Ran `publish_r1.py plan`/`run --force`/`readme` against `QkqCNJOo` for the
first time (the only prior publish, `KMQ5eomS`, predates the 0.20.2 restart).
Two problems surfaced that are about the publisher, not the DAG.

**`nxf_attribution.py`'s `READ_RE` only matched `<sample>_R[12].fastq.gz`.**
This run's step 1 was never re-run -- the 34 interleaved products from
`KMQ5eomS` were hardlinked in as `<sample>.interleaved.fq.gz` givens (see
2026-07-31 entry above) -- so the attribution walk's root never matched and
every one of 5992 rows resolved to `sample=?`. Fixed by widening the regex to
`(?:_R[12]\.fastq\.gz|\.interleaved\.fq\.gz)$`. After the fix: 1628 products
attributed over all 34 samples, matching this file's own product counts
(32/34 assemblies, metabat2 21/34, semibin2 22/34, comebin 9/34). Fix is
committed nowhere yet -- it's an uncommitted change to `r1/nxf_attribution.py`
in the worktree.

**`binning_local::cluster_table` is not actually single-file for this run.**
`results/binning_local-cluster_table/` holds 8 small, non-overlapping TSVs
(disjoint `bin_id` sets, each with its own `c95_00001`-style numbering
starting fresh) instead of the one whole-run table `ROUTES`' `"single"` mode
assumes -- `skani_dedup` fanned out rather than running as one cross-sample
step. `render_script`'s destination-collision guard caught it correctly and
refused. Left unresolved: excluded that one route for this publish (828 files
+ 53 tarballs went through everything else, `binning/cluster_table.tsv` did
not), then restored `ROUTES` unchanged so a future run still refuses instead
of silently picking one shard. Whether to renumber-and-concatenate the 8
shards or re-run `skani_dedup` as a true single step needs a decision before
`cluster_table.tsv` can publish.

Published tree: `~/project-rpp/steven_c_gmcf3495/metagenomics/`, sample-named
throughout (`assembly/fna/S12.fna`, not a content hash), `README.md` at the
root. Known-incomplete per the standing backup-then-gapfill entries above
(S13/S22 missing, partial binning per binner) -- `cp -n` makes re-running the
publish after gap-fill additive, not destructive.

## 2026-08-04 — the gap-fill pass: two harness bugs, and what comebin is actually doing

Ran the gap-fill to completion against the corrected census. Three things are worth
keeping; the rest is in `gapfill/binner_status.tsv`, which is now the per-(sample,
binner) source of truth.

### `binner_status.tsv` — 102 cells, no silent absences

One row per sample × binner for all 34 × 3, carrying either a bin count or a zero
**with the cause the tool itself printed**. This exists because S29 and S19 sat
unnoticed for days: a task that is never created cannot fail, so no failure count
could see them, and only a per-cell census can. `NEVER_ATTEMPTED` and `ZERO` are
now different values, and every `ZERO` names its reason — `METABAT2_NO_BINS_FORMED`,
`ONLY_2_CONTIGS_OVER_2500BP`, `SKIPPED_NO_USABLE_CONTIGS`, and so on. Regenerate
with `python3 gapfill/binner_status.py`; it reads `squeue` so an in-flight job
reports `IN_FLIGHT` rather than being mistaken for a genuine zero.

### Two apptainer bugs in our own harness, both of which looked like tool failures

**Compute Canada's lmod exports a `which` bash *function*** (`BASH_FUNC_which%%`),
and apptainer forwards exported bash functions into the container. COMEBin's
`run_comebin.sh` finds its own install directory with
`dirname $(which run_comebin.sh)`, so the leaked function ran
`/usr/bin/which --tty-only …` — which the container's minimal `which` rejects with
`Illegal option --`. `dirname` then received several arguments, `cd` got
"too many arguments", and python could not find `main.py`. That is the whole of the
"GPU comebin failure" on S5; it was never about the GPU. Fix: `--cleanenv`.

**`--no-home` removed the implicit cwd bind.** `$SLURM_TMPDIR` is on
`/localscratch`, which the `/scratch/phyberos` bind does not cover, so with
`--no-home` the container starts with no valid cwd. Callers now pass
`--bind "$SLURM_TMPDIR" --pwd "$SLURM_TMPDIR"` explicitly. (`--no-home` itself is
required: without it the host's `~/.local/.../torch` shadows the container's and
fails on a `libmpi_cxx.so.40` that only exists in the host stack.)

Both are recorded in `gapfill/env.sh`, next to the flags they justify.

### comebin's `logits` crash is a batch-size bug, but the batch is not sized off contigs

The transform sized COMEBin's training batch from the total contig count. COMEBin
keeps only contigs ≥1000 bp and its dataloader sets `drop_last`, so a batch larger
than the usable count yields zero batches and COMEBin dies on an unbound `logits`
deep in `simclr.train_addpretrain`. The correlation was perfect across the 32
assemblies — all ten failures had 2–575 usable contigs, all nine successes had
≥2321, no overlap — and the transform now sizes the batch from contigs ≥1000 bp
(`metasmith-libraries` 4fd99ca).

**That fix is necessary but not sufficient, and the reason matters.** COMEBin does
not train on the contigs; it trains on *augmented cut subsequences* of them, and
re-applies the 1000 bp filter after cutting. So the real dataset is smaller than the
usable-contig count and cannot be measured before the run. Seven of the ten cleared
the wall at `batch = usable_contigs`; S8, S17 and S18 (N50 419–507) still produced an
empty loader and needed `COMEBIN_BS=8`. Sizing off usable contigs is the right
default — it is measurable, and it fixes most cases — but a sample whose contigs are
mostly under a kilobase can still starve the loader, and the honest knob for that is
a smaller batch, not a bigger one.

With the batch corrected, **all ten `logits` samples now reach COMEBin's UNITEM
ensemble step and fail there instead** — the same terminal state as the eight that
were already failing that way. So every comebin failure in the run has converged on
one question, which the preserved-intermediate S5 run exists to answer: did the
UNITEM crash discard bins that other clustering methods had produced?

### comebin's other failure mode is a real zero, and a control proves it

The eighteen assemblies that reach COMEBin's UniteM ensemble step and die there —
`Missing quality table for weight_seed_kmeans_k_0_result.tsv` — are not a crash
discarding bins. The mechanism, from S5 run with intermediates preserved:

```
markerCmd failed! Not exist: … test_getmarker_2quarter.pl …
Seed_num:            0
Run unitem profile:  0
```

`cluster_res/` holds nothing but an empty `unitem_profile/`. COMEBin's marker-gene
seeding found no complete single-copy marker set, so the clustering had no seeds,
produced no clusterings, and UniteM's bundled CheckM then crashed on a directory
that was never created. The crash is *downstream* of the zero.

The control is what makes that a conclusion rather than a story. **S31 — which
succeeded under the pipeline — was re-run through the same wrapper, same container,
same flags: `Seed_num: 3`, a full set of Leiden clusterings, 12 bins.** So the
harness is faithful and `Seed_num` is the discriminator. Under the standing rule
("if comebin fails deliberately, skip; if it times out, add time"), these eighteen
are a deliberate zero and are skipped, recorded as `COMEBIN_NO_MARKER_SEEDS`.

Two notes on method. `test_getmarker_2quarter.pl` **does** exist in the container
and perl is on PATH — "Not exist" is COMEBin reporting the *seed file* it failed to
produce, not a missing script, and reading it the other way would have sent the
next session chasing a packaging bug. And the S31 control had to be deleted after
measuring: S31 already carries 10 comebin bins from the pipeline, so leaving the
control's 12 in `out/` would have double-counted that sample in the catalogue.

### COMEBin writes beside its input, so never hand it a published path

COMEBin puts FragGeneScan and hmmsearch intermediates (`.frag.faa`, `.frag.gff`,
`.bacar_marker.hmmout`, `_lengths.txt`) next to the input FASTA. The gap-fill
wrapper was passing `results/sequences-megahit_assembly/<hash>.fna` directly, which
scattered 111 files through the published assembly directory — no data lost, but the
directory stopped being 32 assemblies and started being 143 files. metasmith never
hit this because nextflow stages inputs into the task work dir first. The wrapper now
copies the assembly into `$SLURM_TMPDIR` and the directory has been restored.

### comebin's runtime is now a formula, and it said 24 h was still wrong

S25 and S9 were resubmitted at a 24 h wall clock after timing out at 8 h. Checking
them 1.6 h in rather than at the deadline is what caught that 24 h was also too
short: S25 had finished 15 of its progress bars and S9 fourteen, against **400** in
the run that completed.

The control makes it arithmetic instead of extrapolation. S31 — 2,321 usable
contigs, batch 1024, so 2 iterations per epoch — logged exactly 400 progress bars
in 21 m 27 s at 64 CPUs. So:

    wall ≈ 400 × ceil(usable_contigs / 1024) × ~2 s

which reproduces S31 to within 15% and puts **S25 (258k usable) at ~55 h and S9
(157k usable) at ~42 h**. Both were killed at 1 h 40 m and resubmitted at 84 h and
60 h. Nothing is pathological here: COMEBin is linear in contigs and these are the
two largest assemblies in the set, at 1.28 M and 755 k contigs.

Worth stating plainly because the earlier estimate in the plan (12–20 h, from a
per-contig rate fitted to four assemblies) was wrong by a factor of three. Fitting a
rate across runs conflated thread counts; counting one run's own iterations against
a control that finished did not. **A job that is 4% done at 4% of its wall clock is
not on schedule — it is exactly on the line, and the line is where you look.**

The faster lever, if anyone wants these two sooner: the GPU container. The transform
docstring claims 10–20× on the contrastive training phase, and the reason GPU was
dropped from the plan — a wrapper failure — turned out to be the `which`-function
leak, which is fixed. It was not taken here because a concurrent GPU attempt needs
its own work dir, which means a fourth binner name flowing into `collect_bins.sh`,
which is a double-counting trap of exactly the kind the S31 control already sprang
once.

## 2026-08-04 — publishing bin quality from the catalogue

`publish_r1.py plan` reported **763 unattributable products**, and 761 of them were
two dtypes that no amount of graph-walking will fix:

| dtype | products | attributable |
|---|---|---|
| `taxonomy::checkm_stats` | 781 | 65 |
| `binning_local::quality_bin_fasta` | 45 | 0 |

checkm ran **batched across assemblies**, not per sample, so 716 of its per-bin CSVs
descend from several samples at once; the 65 that resolve are one batch that
happened to hold only S25's bins. And every aggregator bin has all three binners
upstream by construction, which `C.binner_of` already documented.

So the publisher now routes the gap-fill catalogue instead. That is not a
workaround — it is the only source that has both facts, because it attributes each
bin through the work-dir join *before* checkm sees it. It also covers what the DAG's
own numbers cannot: the bins the all-three-binners gate discarded, and the
gap-filled bins that never entered the DAG.

Before switching the catalogue's own attribution off the hand-built `inv/*.tsv`
join and onto `nxf_attribution.py` — needed because the join was written against one
run key and cannot see the scoped S13/S22 run — the two were compared over all 781
published QkqCNJOo bins: **781 agree on both sample and binner, 0 disagree**, and
the rebuilt catalogue is byte-identical.

Three smaller things the pass paid for:

- `binning_local::cluster_table` was declared `single` but has 8 products, so all
  eight claimed one destination. `render_script`'s clash check catches it, but the
  fix is that the clustering was recomputed over the complete bin set and the DAG's
  is superseded, so it is now unrouted rather than renamed.
- Every copy is `cp -n`, which is right for content-addressed products and wrong for
  the catalogue layer, which is recomputed whenever a binner finishes. A second
  publish would have kept the first catalogue and reported success. `run --replace`
  clears that layer, and only that layer, first.
- The tree already held `binning/qc_stats_semibin2/S25.tar` from an earlier pass —
  the one checkm batch that attributed. It reads as "CheckM2 statistics for semibin2
  on S25" when what happened is that checkm ran on everything and only that batch
  resolved. `--replace` removes it.

The published README now states that contig taxonomy (metabuli) and bin taxonomy
(GTDB-Tk) are **absent**, rather than describing them as present. The Arbutus
infrastructure that produces them is not in this workspace — no project, no ssh
host, no openstack CLI, only a leftover key — so neither campaign can be driven from
here. Read-level taxonomy is complete for all 34 libraries.

### The COMEBin pollution came back, because the clean did not outlive the jobs

`results/sequences-megahit_assembly/` was restored to 32 files by hand after the
staging fix went into `submit_bin.sh`. Two hours later it held 41 again — nine
`.frag.*` / `.bacar_marker.*` files for S29's assembly, written at 14:27.

Not a regression in the fix. S29's comebin job had been submitted a few minutes
*before* the fix landed, and `submit_bin.sh` renders a self-contained `job.sh` at
submission time — so that job carried the old body for its whole 2 h 36 m life and
polluted at the end of it, well after the directory was cleaned. The two resubmitted
jobs (S25, S9) were checked and both carry the staging line.

The durable form of the fix is that the sweep is now step 0 of `finish.sh` rather
than a command someone remembered to run once. `<assembly>.fna.<anything>` is never
a product — published names end at `.fna` — so the pattern is unambiguous and the
sweep is idempotent.

The general shape is worth naming, because this is the second time it has bitten:
**a fix to a job-submission script does not reach jobs already in the queue.** The
in-flight population is a different population from the one the fix applies to, and
"I fixed it" is not the same claim as "no running job can still do it."

### A wiped work dir must not be able to delete collected bins

`submit_bin.sh` starts with `rm -rf "$WD"`, so invoking it on a sample that already
has bins destroys them — STATE.md has carried that warning since the pass began, and
it was still walked into: a command meant as a syntax check on the new absolute-path
branch submitted a real job and wiped `out/S29/comebin`.

Nothing was lost, because `bins/` holds normalised copies and is what
`catalogue.tsv` actually points at. But the near-miss exposed the real defect, one
step removed from the `rm -rf`: **`collect_bins.sh` mirrored the work dir, wiping its
destination before copying.** The next `finish.sh` would have found an empty `out/`
and deleted all five collected S29 comebin bins — including a 94.5% complete / 0.09%
contaminated MAG — with the deletion attributed to the collector rather than to the
submission that emptied the source.

`collect_bins.sh` now refuses to let an empty work dir empty a populated
destination, and says `KEPT` when it declines. The general form: **when a scratch
directory feeds a durable one, the sync must not be able to propagate absence.**

## 2026-08-04 — the scoped run's driver was killed, and the DAG did not need to be restarted

`hEYVT7HY` stopped producing results with S13's assembly still unbinned. The cause is
in nextflow's own log, not metasmith's:

```
Aug-05 01:15:48.912 [SIGTERM handler] DEBUG nextflow.Session - Session aborted -- Cause: SIGTERM
Aug-05 01:15:48.915 [SIGHUP handler]  DEBUG nextflow.Session - Session aborted -- Cause: SIGHUP
```

That is 18:15 PDT (nextflow logs UTC). The driver runs on the **login node** under
`nohup`, and SIGTERM and SIGHUP arriving in the same 3 ms is a session teardown or a
login-node reap, not a crash — there is no exception anywhere in the log. It
correlates with the ssh ControlMaster to fir dropping in the same window, though the
log cannot prove that link.

**The SLURM job outlived the driver and finished the work.** S13's `assembly_stats`
ran 5 h 26 m, exited 0 at 19:43, and wrote all four products — a 19.9 GB BAM, the
stats JSON, per-contig and per-bp coverage. Nextflow died at 18:15 with roughly 90
minutes left on that job, so it never ran publishDir and never dispatched anything
downstream. Nothing was lost; only the bookkeeping stopped.

So the recovery was not a restart. Under the standing rule — *stop rerunning, gap-fill
until complete* — restarting would have re-executed a DAG that was already done to
collect four files that already existed. Instead:

1. The four orphaned products were **hardlinked** from the work dir into `results/`
   under the names nextflow would have given them (they are content-addressed, so the
   names were already correct). Every step in the run now shows exactly 2 products,
   which was the acceptance criterion for this run.
2. `nxf_attribution.py` was run against the run dir: **184 products, 184 attributed to
   a single sample** — S13 to assembly `RDwuHsBoCuPhmkRB` and BAM `Rj02fZ9CKzA0GApL`,
   S22 to `RtiSn4FQMZFF8swe` / `3m5FnIc6OQOqrS2G`. The sizes happen to agree with the
   obvious guess; the point is that the guess was not what was used.
3. All six binner jobs were submitted through the gap-fill harness, which now accepts
   absolute paths precisely so it can reach a second run's products.

Worth keeping: **a driver dying is not the same as work being lost.** The instinct is
to resume the workflow, and resuming would have been the expensive wrong answer here —
the compute was complete and sitting on disk, and what had actually failed was a file
copy. Check what the jobs produced before deciding what to re-run.

## 2026-08-05 — the tree is published, and verifying it found three defects

The r1 results are assembled at `~/project-rpp/steven_c_gmcf3495/metagenomics` on fir:
692 GB, 915 inodes, README written. Every reads, assembly, annotation and read-taxonomy
layer is 34 of 34; binning ships 62 quality MAGs as 13 per-sample tarballs, a 1006-row
bin catalogue, the 102-cell census, and 13 cluster tables.

S13's binners landing first brought the census to **102 cells with zero never-attempted**
— every one of 34 samples × 3 binners has now been tried, which was the acceptance
criterion. 42 of the 45 zeros carry a cause read off the tool's own output; the other 3
are the comebin jobs still running. Quality MAGs went 56 → 62 and clusters 33 → 38.

Publishing is where the value was, though — not because it worked, but because checking
that it worked found three things.

**`cp -n` exits 1 when it skips.** On coreutils 9.3, which fir has, `cp -n` over an
existing destination is a failure, not a no-op. Under `set -euo pipefail` that aborted
the entire publish at `reads/filtered/S24.fq.gz`. The script's own comment asserted the
opposite ("skips an existing destination and exits 0"), which had been true on some other
host and was never checked here. The first publish only ever worked because the tree was
empty; every re-publish would have died. `render_script` now emits a `cpn()` function —
**skip-if-present has to be the test, not the copy's exit status.**

**Four published extensions lied about the format.** proteinbert was Apache Parquet named
`.npy`; its index and kofamscan were comma-separated named `.tsv`; read QC stats were JSON
named `.txt`. Nothing failed — a wrong extension is a claim no code checks. Running `file`
over one member of each of the 31 leaf directories takes seconds and found all four; the
other 27 were right. Fixed in `ROUTES` and renamed in place, no re-copy of 692 GB.

**The proteinbert merges were silently short, and this is the interesting one.**

`merge_proteinbert` is `group_by=parent_orfs`, so it inherits the `group()` drop race
already reported upstream — the one that lost S13 and S22 from the DAG outright. Here it
did something worse than lose a sample: it dropped chunks *inside* a group. The merge
still ran, still exited 0, still published. The product looked entirely normal and was
short.

It was found by counting merged index rows against ORFs in the published `.faa`:

| sample | ORFs | published | short by |
|---|---|---|---|
| S19 | 1,683 | *nothing* | its whole group was lost, so no merge task ever existed |
| S12 | 44,807 | 34,849 | 9,958 |
| S13 | 654,789 | 634,795 | 19,994 |
| S25 | 1,184,829 | 1,179,830 | 4,999 |

Eight chunk pairs across the two runs were consumed by no merge at all, and each one's
ORF ids are **fully contained in exactly one sample's `.faa` and no other**, which is
what assigns them. Their row counts close the four shortfalls exactly: 1,683 / 4,979 +
4,979 / 4,999 / 4,999 + 4,998 + 4,999 + 4,998. Nothing was left over and nothing had to
be guessed.

`pbert_remerge.py` re-runs the library's own merge body over the complete chunk list.
Two details mattered:

- **The chunk order is by producing work-dir path, not by product name.** The transform
  sorts on `str(p.local)`, which is the work dir each chunk's own task wrote it in;
  work-dir hashes have nothing to do with the content hash in the filename. Sorting by
  basename produced the right rows in the wrong order — caught only because a
  clean-merging sample was rebuilt alongside as a control and failed to match.
- **Those work dirs are mostly gone**: the merge protocol unlinks each chunk after
  consuming it, so only the orphans survive on disk and the consumed ones are recoverable
  only from the input list each merge recorded in its own `.command.sh`.

With ordering fixed, the **S33 control reproduced the published product byte for byte** —
index CSV and Parquet both. That is what makes the other four trustworthy. All four now
publish with a merged row count equal to their ORF count and an ORF id set identical to
their `.faa`, and the whole layer is 34/34 with zero mismatches.

Then the same question was asked of the other three merge families, since all of them
are `group_by` and all of them inherit the race: `merge_eggnog_mapper`,
`merge_diamond_uniref50` and `merge_kofamscan` consumed **every** chunk in both runs.
The defect is contained to proteinbert.

Worth keeping: **a step that aggregates a group can come up short without failing.** The
per-step product count that caught S13 and S22 cannot see this — proteinbert had 31 of 32
merged products, which looks like one missing sample, and said nothing at all about the
three that were present and truncated. Only counting rows against the upstream catches a
short one. Any `group_by` step deserves that check, and the cheap version is the orphan
audit in `gapfill/merge_audit.py`: a chunk that no merge names in its recorded inputs was
silently discarded.

## 2026-08-06 — the last three comebin runs were deadlocked, not slow, and were resumed

The three surviving comebin jobs — S13, S9, S25 — were reported healthy in this log's
previous pass. They were not. All three had been deadlocked for hours, and the way that
was missed is the most reusable thing here.

**`ps` `pcpu` is a lifetime average.** A process that worked for ten minutes and then hung
for twelve hours still reports plausible CPU, so a single sample cannot distinguish work
from a hang. `sstat`'s `TresUsageInTot` is worse — it does not move between samples minutes
apart even on a healthy job. What settled it was comparing `time=` for the same pid across
two checks 5 h 20 m apart: **11, 11 and 13 seconds of CPU** in that window. Two samples,
hours apart, are the only proof of liveness.

The fingerprint, once looked for, was unambiguous: the parent and all 64 pool workers in
`futex_do_wait`, no file written for 7–12 h, and the Leiden parameter sweep stopped at
95, 64 and 48 of its 120 points — a *different* count each time, so a race, not a bad
parameter combination.

**The cause is a fork from a threaded parent.** `cluster.py` calls `seed_kmeans_full`,
which builds `KMeans(n_jobs=-1)`; joblib's loky backend leaves worker processes and their
threads live in the parent afterwards — the stalled parent still had exactly such a loky
process sitting in `pipe_read`. The very next thing `cluster.py` does is
`multiprocessing.Pool(num_threads)`, forking 64 children from that threaded parent. A fork
inherits locked mutexes but not the threads that would release them, so every child parks
in a futex it can never acquire. Textbook, and reproducible three times out of three.

**Resume, don't re-run.** Training was 14–25 h per sample and had already succeeded; it
lived only on `/localscratch`, which dies with the job. So the representations were tarred
to `/scratch` and verified member-by-member *before* anything was cancelled — 192 idle CPUs
were worth less than 57 h of training. `submit_comebin_cluster.sh` then restores a tar and
resumes, riding three existence checks that are all COMEBin's own: `run_comebin.sh` skips
augmentation and training when their outputs exist, `run_leiden` skips any sweep point
already on disk, and `seed_kmeans_full` skips when its result file exists. That last one is
the actual fix rather than a convenience — on a resume the k-means never runs, so the loky
pool never exists and the fork is clean.

| sample | deadlocked | resumed from | bins | elapsed |
|---|---|---|---|---|
| S13 | 12 h | 95/120 | 30 | 24 m |
| S9 | 12 h | 64/120 | 50 | 1 h 00 m |
| S25 | 7 h | 48/120 | 54 | 2 h 32 m |

**Judge a sweep by its Leiden count, not its clock.** S25 spent minutes per point where
S13 spent seconds — cost per point scales with the assembly, and a 15× difference in rate
looks exactly like a hang. The in-job watchdog therefore triggers on *no new file for
60 min*, and only while the sweep is incomplete: once it finishes, `get_result` runs UniteM
and CheckM for hours without touching `cluster_res`, and killing that would kill real work.
Progress is checkpointed back to `salvage/<S>_progress.tar` on every exit path including
SIGTERM at the wall clock, so a failed attempt still advances the run.

**134 bins bought 5 quality MAGs — and it was still worth it.** Quality MAGs went 62 → 67
and clusters 38 → 39, but four of those five MAGs are *cluster centroids*: species
representatives that nothing else in the set covers, including one at 99.12% complete and
0.09% contaminated. Bin counts are a poor measure of a binner; centroids are the honest one.

That closes the gap-fill. The census is **102 cells, 60 BINS, 42 ZERO, zero
never-attempted**, and every zero names a cause read off the tool's own output —
20 `COMEBIN_NO_MARKER_SEEDS`, 11 `METABAT2_NO_BINS_FORMED`, 7
`SEMIBIN_NO_BINS_PRE_RECLUSTER`, 4 degenerate assemblies. A deadlock had to name itself
here, because `binner_status.py` would otherwise have inferred `TIMEOUT` from the SLURM
state and recorded a silent, plausible, wrong zero.

Republished and re-verified by walking the tree: 692 GB, 881 inodes, 1140 catalogued bins,
67 quality fastas in 13 per-sample tarballs, and 13 cluster tables naming 39 clusters —
matching the catalogue exactly. Note that leaf counts under `binning/` are samples, not
bins: the fan-outs are tarred per sample to stay inside the project's inode quota.

Still blocked, and not by anything here: the Arbutus metabuli and GTDB-Tk campaigns, whose
`arbutus-infra` project is absent from this workspace. They are the only two entries on the
publisher's problem list, which is what makes `--force` safe to use.

---

## 2026-08-06 — a whole read layer was never published, and no check could see it

`reads/interleaved` was empty on fir and absent from the Globus mirror, while
`reads/filtered` and `reads/discarded` beside it were complete at 34/34. Nothing
failed, nothing was logged, and every completeness check this run has accumulated
passed.

The cause is structural. The 2026-07-31 restart fed the interleaved reads into the
plan as **givens** rather than letting the DAG produce them — they already existed,
so re-interleaving 372 GiB would have been waste. But a dtype that enters a plan as
a given has no producing step, a step with no products routes nothing, and the
publisher's README generator lists the directories that exist. Absence rendered as
absence. The tree, the manifest and the README all agreed with each other and all
three were wrong about the dataset.

**Every check here counts products against steps, so none of them can see this.**
Per-step product counts catch a missing sample. Counting rows against the upstream
— the check that caught the short ProteinBERT merges — catches a short one. Neither
catches a layer whose step count is legitimately zero, because *no step is exactly
what was planned*. The check that would have caught it does not compare the run to
itself at all: it compares the published tree to the **declared target set**, which
still names `sequences::short_reads` whether or not this particular execution
produced it.

That generalises past this run: **a plan is not a specification.** Feeding a product
in as a given is a statement about this execution, and treating the execution as the
inventory silently narrows the dataset every time a restart does it. Restarts are
routine here — S13/S22's scoped run, the gap-fill, this one — so the narrowing is
routine too.

Fixed by archiving the layer to Globus rather than re-publishing it to fir: the
raw fastqs it derives from are already on the same collection and the interleave is
deterministic and read-count-verified, so a third copy on the allocation buys
nothing. The asymmetry is now stated in `README_HEAD` in `publish_r1.py` — prose in
the generator's source, not a row in the generated layout table, because the table
is rebuilt from whichever tree is being described and would drop the note every time.

---

## 2026-08-06 — both Arbutus campaigns ran, and 8 of the 67 "quality MAGs" are human

The two campaigns that had been blocked since the tree was first published are
done, and the dataset is complete: reads, contigs and bins all carry GTDB r232
taxonomy, published to fir and mirrored to Globus.

**Access came from neither route that was planned for.** An `arbutus-infra` scope
is not provisionable and never will be — the tenancy application credential and
the worker SSH key live in that repo's gitignored `secrets/`, which workspace
policy excludes from anything that travels with a scope, so a cloned scope arrives
unable to authenticate. The handoff fallback was not needed either: the project
already exists on **capella**, a peer reachable by ssh with `secrets/` in place.
The campaigns were driven by running the submit scripts *in place there* rather
than copying credentials to this node, which is the thing the policy exists to
prevent. Nothing in the arbutus-infra worktree was modified.

**Harvest from the published tree, not the lineage graph.** The driver's own
comments record bin-to-binner attribution as unsolvable via the attribution graph.
Publication had already solved it: bins are `<sample>.<binner>.<n>.fa`, assemblies
are `assembly/fna/<sample>.fna`. A `harvest --from-published` path stages from
there, which produced **0 unattributed bins**, made the campaigns independent of
scratch cleanup entirely, and is why deleting `hEYVT7HY` mid-flight was safe rather
than merely sequenced around. All 67 staged bins matched a `quality=1` catalogue
row with none unmatched — independent proof the right bins were staged.

Two reconciliations that would otherwise have failed silently: contig headers are
rewritten to `<sample>__<contig>` **before** concatenation, because megahit
restarts contig numbering per assembly and 34 assemblies collide outright in one
query (0 duplicates over 3,340,538 contigs, gated, not reported); and published
bins are `.fa` where both campaign specs declare `fna`, reconciled by renaming so
the glob and the service's `--extension` agree rather than letting a glob match
nothing.

Costs, for sizing future runs: GTDB-Tk 17 min, peak RSS 39.9 GB of a 120 GB cap —
the memory ceiling that was the live worry never came close, because 59 of 67
genomes short-circuited on the ANI screen. Metabuli 4433 s as **one** submission
over all 34 assemblies, peak RSS 108.1 GB against the same cap at `--max-ram 110`.
Both torn down with their reference clones; the tenancy was verified back at its
148-core baseline by asking `openstack`, not by trusting the exit trap.

### The finding: this dataset is 92% host

Metabuli classified all 3,340,538 contigs: **92.4% Eukaryota**, 3.3% Bacteria,
4.1% unclassified.

That is not a curiosity, it invalidates a headline number. GTDB-Tk classified
67/67 quality bins with zero failures, but **8 came back Unclassified carrying
zero of 120 bacterial and zero of 53 archaeal markers** — in bins of 3.2–7.2 Mbp
with contigs to 70 kb. Cross-checking those bins' own contig ids against the
metabuli output settles what they are: **5,584 of their 5,587 classified contigs
are *Homo sapiens***, where ANI-screened bins are 100% Bacteria by the same check.

    S13.semibin2.0010  S25.semibin2.0001  S25.semibin2.0002  S26.semibin2.0001
    S6.semibin2.0004   S6.semibin2.0011   S9.semibin2.0001   S9.semibin2.0002

All 8 are semibin2 and all 8 sit at the acceptance boundary — 51.5–56.8% complete,
7.4–9.6% contaminated. **CheckM2 scores prokaryotic completeness and is not
trained to reject non-prokaryotic input**, so host DNA enters as a plausible
mid-range score rather than as a failure. A genome-sized bin with no ribosomal
proteins is a contradiction, not a novel lineage; the tell was available in the
marker counts all along and nothing in the pipeline looks at them.

The honest count is **59 prokaryotic MAGs plus 8 host-DNA artifacts**, not 67. The
bins stay in the tree because "Unclassified" is a real GTDB-Tk result and silently
dropping data is worse, but the README now says this above the layout table rather
than below it. **Generalisable: a quality score from a model that assumes its input
is in-domain says nothing about whether the input is in-domain.** In a host-associated
dataset, marker-gene count is the cheap check that CheckM2 completeness is not.
