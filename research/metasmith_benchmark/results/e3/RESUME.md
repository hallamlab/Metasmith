# Resuming E3 from the chinook archive

## Purpose & Contents

E3's agent home left fir on 2026-09-22 and lives on the chinook Globus collection. This file tells
the next agent what the archive contains, what state E3 stopped in, and the five things that turn a
restore into a silent 2 TB recomputation.

The restore procedure itself is not here. Run `drivers/restore_e3.sh`, which carries the endpoint
ids, the external-dependency preflight and the verification numbers. Run
`drivers/verify_e3_archive.sh` to check the archive on chinook against the source-side manifest.
This file holds only what the scripts cannot: why each constraint exists, and what the archive is
worth.

## Where it is

    chinook collection 2602486c-1e0f-47a0-be15-eec1b0ff0f96
    /Workspace_backups/Tony_Liu/fir_bench_e3/
        pratama2026/   the agent home
        meta/          manifests, index fingerprint, dependency audit, step logs, cache census
        rescue/        p38's kofam annotations, recovered before the run was stopped
        deps/          the 0.22.1 apptainer image, which lived outside the home

**CAUTION** The archive sits beside `altair/`, not inside it. The nightly `workspace_backup` job
mirrors into `altair/workspace/` with deletion enabled. An archive placed under that tree is erased
on the next tick for the crime of not existing locally.

## What E3 delivered

E3 ran as `Qt0rbV1R`, wave 8, from checkout `61c0eebc` on image
`docker://quay.io/hallamlab/metasmith:0.22.1`. The driver stopped on a gated USR1 and exited
`COMPLETED 0:0`. Nextflow's shutdown hook cancelled the grid jobs, so nothing was orphaned and every
finished task kept its products.

**36 of 39 steps produced products.** Steps `p01` through `p36` are complete: 3,642 tasks, 0
unignored errors. The whole DRAM-v arm is missing, and it is one coherent gap rather than three
unrelated ones.

| step | state | why |
|---|---|---|
| `p37 dramv_pfam` | failed, ignored | needs ~57 h against a 24 h cap |
| `p38 dramv_kofam` | failed, ignored | same, and its kofam table was rescued |
| `p39 dramv_distill` | never submitted | would crash on a column its database cannot produce |

`DRAM-v.py annotate` feeds all 58,593 contigs to one call. Its final act,
`make_gbk_from_gff_and_fasta` at `mag_annotator/annotate_bins.py:576-632`, is O(N^1.96) in contigs:
line 596 builds the whole GFF plus the whole FASTA as one 2.1 GB Python string, line 603 re-copies it
through `io.StringIO` once per contig, and scikit-bio's `_construct_seq` has no `break`, so it drains
the entire GFF on every call. Measured in the container at 200/400/800/1600 scaffolds:
1.814 / 10.031 / 38.712 / 152.015 s, exponent 1.95 to 1.97. Extrapolated to 58,593 contigs that is
~57 h, of which ~2 h are parallel.

Plain DRAM escapes the same code because `annotate_fastas` calls `annotate_fasta` once per input
FASTA. DRAM gets 4,105 bin FASTAs of ~100 contigs each, DRAM-v gets one FASTA of 58,593 in one call.
The quadratic term differs by a factor of ~5,400. This is a data-shape problem, not a worse
algorithm.

`p39` would fail regardless. `summarize_vgfs.py` reads `frame['vogdb_categories']` unconditionally,
and the staged `/scratch/phyberos/refs/dram_1.5.0/DRAM.config` has `"vogdb": null`. The rescued
annotations table in `rescue/` confirms it from the other end: its header carries no
`vogdb_categories` column.

**To finish 39/39, chunk the contig FASTA and the VIRSorter2 affi table.** The idiom already exists
in `split_viral_contigs_pratama.py` and `checkv_batch_pratama.py`. At k=16 every task lands under
2 h. Stage VOGDB as well. A walltime exception is not a route: it asks ~960 core-hours with 15 of 16
cores idle and still crashes at the end.

## The five things that cool the cache

1. **Restore to `/scratch/phyberos/pratama2026` and nowhere else.** `given_name(base, declared)`
   returns `f"{base}@{sha256(str(declared))[:12]}"`, folding the absolute path string of every
   declared input. A different path renames every pool entry. Nothing raises. The next run simply
   recomputes.
2. **`cache.sqlite` cannot be rebuilt.** No code anywhere reconstructs the index from the shards.
   `msm cache` offers list, tag, gc, explain and status, and `index_shard` is never called in a loop
   over the store. Lose the file and `EnsurePoolEntries` re-imports every given, `mint_import_id`
   mints fresh identities from a fresh `os.urandom(32)` nonce, and 2 TB of shards become
   unaddressable with no error raised.
3. **Pin the engine version.** The archived index reads `schema_version 2`,
   `lineage_payload_version 6`, `shard_layout_version 3`. An engine with a higher key version
   mass-tombstones every non-imported row on first open, and a later `gc --delete` then reclaims the
   lot.
4. **Run the archived image, not the tag.** `_common.py` names
   `docker://quay.io/hallamlab/metasmith:0.22.1`, a mutable remote tag, and apptainer resolved it
   through `APPTAINER_CACHEDIR` to `/scratch/phyberos/cache/apptainer/`, outside the home. So no
   image was inside the archive set. The 978 MB SIF is archived separately as
   `deps/docker..quay.io_hallamlab_metasmith..0.22.1.sif`. Restore that file and point
   `APPTAINER_CACHEDIR` at it. The engine inside it reports version 0.23.0 -- the image tag and the
   engine version do not agree, so pin the SIF rather than either number.
5. **Keep the 15 external paths alive.** They are listed in `restore_e3.sh` and audited in
   `meta/external_dependencies.txt`. `ref::vibrant_db` was itself copied out of this cache, and the
   GTDB skani squashfs's source genome tree is already deleted. The driver declares three of them
   through `/home/phyberos/project-rpp`, a symlink to `/project/6004975/phyberos`. `given_name`
   folds the literal string from the source, so a cache hit survives the symlink going missing.
   A cache miss does not.

## Why the archive is smaller than the home

Three subtrees stayed behind, and each is either reconstructible or meaningless off fir.

- `runs/Qt0rbV1R/nxf_work/`, 84,416 inodes and 1.86 TB. Nextflow scratch. The cache hit decision
  never consults it: `invocation.probe` checks for a tombstone, a readable `manifest.cbor` and the
  presence of each manifest relpath, and never opens sqlite. Most of its bytes are hardlinks to
  task_cache content that Globus would have written out a second time.
- `runs/Qt0rbV1R/results/`, 5,332 inodes and 554 GB. All but 10 files are hardlinks into task_cache.
  Globus does not preserve hardlinks, so archiving this meant a second copy of the same bytes. The
  10 exceptions are `results/_metadata/`, which is archived separately and lands back in place.
  A restore does not need `results/` anyway: `runner.py` deletes and recreates it before nextflow
  starts, so nothing ever reads the archived copy.
- `metasmith/relay/`, 25 unix sockets pointing into each compute node's `/tmp`. **CAUTION** The 26th
  entry is `msm_relay`, a 1,799,672-byte executable that `SETUP_COMMANDS` starts before every run.
  The exclusion dropped it. It is archived separately, back in place at
  `pratama2026/metasmith/relay/msm_relay`. Without it a restored `./msm` prints
  `relay on <host>: MISSING` rather than failing outright.

`.staging/` also stayed behind. It holds 20 truncated partial downloads, every one of which has a
larger completed counterpart under `reads_2019/` or `reads_2022/`. It is 55,992,517,500 bytes of
abandoned bytes, not data.

**CAUTION** The manifest is not the archive, in two ways, and comparing a restored tree against it
raw reports thousands of false absences. `meta/manifest_full.tsv.gz` lists `.staging`, and it lists
3,511 symlinks that the transfer ran with `recursive_symlinks=ignore` and therefore never wrote at
all -- neither followed nor recreated. Subtract both. Then add the two subtrees that arrived outside
the manifest, `results/_metadata/` and `relay/msm_relay`.

| | entries | bytes |
|---|---|---|
| manifest total | 145,220 | 3,832,855,343,605 |
| minus `.staging` | −21 | −55,992,517,500 |
| minus symlinks | −3,511 | 0 |
| plus `results/_metadata` | +12 | +230,472,594 |
| plus `relay/msm_relay` | +1 | +1,799,672 |
| **archive** | **141,701** | **3,777,095,098,371** |

`drivers/verify_e3_archive.sh diff` applies exactly that arithmetic, and reports extra paths as
well as absent ones. An unexplained extra means the manifest and the archive describe different
trees.

**CAUTION** The diff cannot cover `task_cache`. A non-recursive `globus ls` of its single `1e/`
directory, 11,583 children, does not return within two minutes, and the tree below holds 108,004
files. So the diff is scoped to the other 2,767 files, and the whole set is verified by
`verify_e3_archive.sh resync` instead: resubmitting the identical batch with
`--sync-level checksum` makes Globus checksum both ends of every file and send only what differs.
Zero bytes transferred proves the archive matches the source file for file. That check is stronger
than a path listing, and it repairs as it verifies. `drivers/e3_archive_batch.sh` generates the
batch, and is the authoritative statement of what the archive contains.

Losing the symlinks costs nothing. 3,510 of them pointed into the excluded `nxf_work/` and would
have arrived dangling, and their content is dereferenced into `meta/step_logs_Qt0rbV1R.tar.gz`,
3,529 members and no symlinks -- now the only copy of the per-step logs. The one survivor is
`_metasmith/logs.latest`, a convenience pointer. No symlink exists anywhere under `task_cache/` or
`imports/`.

## What the restore is worth

The cache holds **11,326 entries: 11,083 lineage products and 243 imported givens, 0 tombstoned**,
across 8 runs rather than just this one. `sum(size_bytes)` is 3.12 TB, of which the imported rows
account for 963 GB that live outside the home as pointers.

**Zero of the 11,083 lineage rows reference an absolute path.** Every product sits under its own
shard by relative path, and `output_root` is stored relative and re-joined on read. That property is
what makes this archive a round trip rather than a museum piece. It was measured, not assumed, by
regexing all 11,326 payload blobs as raw bytes: only the 243 imported rows name absolute paths, and
those 15 distinct paths are exactly the external-dependency list.

**The cache is a shared store, and E3's own run contributed 132 shards to it.** Grouping the 11,083
lineage rows by their `run` tag gives 4,398 untagged, 2,284 `JtWdzRCY`, 1,581 `Son2YJiI`, 1,494
`bqyYO0Ip`, 758 `AvPNgFtP`, 380 `F3KJbPJK`, 299 `qcMKf68s` and 132 `Qt0rbV1R`. That reconciles with
the finalizer, which promoted 132 members and served 3,510. So E3 wave 8 was itself mostly a replay,
and this archive protects seven earlier runs' work as well as E3's.

`meta/cache_census_named.tsv` is the per-transform census: shard count, bytes, step name and
transform key for all 69 transforms. Step names come from `trace.jsonl`, which only covers this run
and its hits, so 37 transforms holding 7,413 shards show `?` -- those belong to the runs above,
whose run directories no longer exist. The largest named lanes are `genomad_pratama` and
`vibrant_pratama` at 854 shards each, `virsorter2_pratama` at 847, `splitContigsForAmr` at 133, and
`prodigal`, `megahit` and `seqkit_reads` at 68 each. The two largest lanes overall are unnamed:
2,261 shards under `632daaHl` and 1,880 under `hWSjG4p7`, both tiny per-shard.

**CAUTION** Do not quote shard counts off the per-run rows of `meta/cache_transform_census.tsv`. It
is grouped by transform *and run*, so a transform's total is the sum of several rows. Reading a
single row is how `genomad_pratama` and `vibrant_pratama` were previously recorded as 847.

## What a resume actually does

A resume re-stages a fresh run directory. It does not reuse `runs/Qt0rbV1R/`, which is archived as
the record of what ran rather than as something to restart. `results/` is recompiled from the cache
during finalisation, so it does not need to be restored.

The declared givens are not the reads. `metasmith/imports/e3/<accession>/read_pair@<hash>` holds the
accession string in 10 bytes, and the whole `imports/` tree is 22 KB. The reads are resolved by the
transform at task run time. Because every `p01` shard is cached, a replay never opens a FASTQ. The
744 GB of `interleaved/` and the 823 GB of raw per-mate FASTQs are archived for a re-run from
scratch, not for a replay.
