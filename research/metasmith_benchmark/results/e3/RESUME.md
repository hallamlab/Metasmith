# Resuming E3 from the chinook archive

## Purpose & Contents

E3's agent home left fir on 2026-09-22 and lives on the chinook Globus collection. This file tells
the next agent what the archive contains, what state E3 stopped in, and the four things that turn a
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
        meta/          manifests, index fingerprint, dependency audit, step logs
        rescue/        p38's kofam annotations, recovered before the run was stopped

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

## The four things that cool the cache

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
4. **Keep the 15 external paths alive.** They are listed in `restore_e3.sh` and audited in
   `meta/external_dependencies.txt`. Note that `ref::vibrant_db` was itself copied out of this cache,
   and that the GTDB skani squashfs's source genome tree is already deleted.

## Why the archive is smaller than the home

Three subtrees stayed behind, and each is either reconstructible or meaningless off fir.

- `runs/Qt0rbV1R/nxf_work/`, 84,416 inodes and 1.86 TB. Nextflow scratch. The cache hit decision
  never consults it: `invocation.probe` checks for a tombstone, a readable `manifest.cbor` and the
  presence of each manifest relpath, and never opens sqlite. Most of its bytes are hardlinks to
  task_cache content that Globus would have written out a second time.
- `runs/Qt0rbV1R/results/`, 5,332 inodes and 554 GB. All but 11 files are hardlinks into task_cache.
  Globus does not preserve hardlinks, so archiving this meant a second copy of the same bytes. The
  11 exceptions are `results/_metadata/`, which is archived separately.
- `metasmith/relay/`, 26 unix sockets pointing into each compute node's `/tmp`.

`.staging/` also stayed behind. It holds 20 truncated partial downloads, every one of which has a
larger completed counterpart under `reads_2019/` or `reads_2022/`. It is 55,992,517,500 bytes of
abandoned bytes, not data.

**CAUTION** `meta/manifest_full.tsv.gz` lists `.staging` even though the archive omits it. The
manifest totals 145,220 entries and 3,832,855,343,605 bytes. The archive holds 145,199 entries and
3,776,862,826,105 bytes. Compare a restored tree against the manifest minus `.staging`, which is
what `drivers/verify_e3_archive.sh diff` does.

The run's 3,510 per-step logs were symlinks into `nxf_work/` and would have arrived dangling. They
are dereferenced into `meta/step_logs_Qt0rbV1R.tar.gz`, 3,529 members and no symlinks.

## What the restore is worth

The cache holds **11,326 entries: 11,083 lineage products and 243 imported givens, 0 tombstoned**,
across 8 runs rather than just this one. `sum(size_bytes)` is 3.12 TB, of which the imported rows
account for 963 GB that live outside the home as pointers.

**Zero of the 11,083 lineage rows reference an absolute path.** Every product sits under its own
shard by relative path, and `output_root` is stored relative and re-joined on read. That property is
what makes this archive a round trip rather than a museum piece. It was measured, not assumed, by
regexing all 11,326 payload blobs as raw bytes: only the 243 imported rows name absolute paths, and
those 15 distinct paths are exactly the external-dependency list.

The expensive work that replays from cache:

| transform | shards | | transform | shards |
|---|---|---|---|---|
| checkm2 | 2,261 | | genomad / virsorter2 / vibrant | 847 each |
| memote_score | 680 | | diamond_uniref50 / kofamscan / proteinbert | 562 each |
| prodigal_from_bin / carveme_from_orfs | 381 each | | megahit | 68 |
| spades_pratama, each metawrap_* | 65 each | | | |

## What a resume actually does

A resume re-stages a fresh run directory. It does not reuse `runs/Qt0rbV1R/`, which is archived as
the record of what ran rather than as something to restart. `results/` is recompiled from the cache
during finalisation, so it does not need to be restored.

The declared givens are not the reads. `metasmith/imports/e3/<accession>/read_pair@<hash>` holds the
accession string in 10 bytes, and the whole `imports/` tree is 22 KB. The reads are resolved by the
transform at task run time. Because every `p01` shard is cached, a replay never opens a FASTQ. The
744 GB of `interleaved/` and the 823 GB of raw per-mate FASTQs are archived for a re-run from
scratch, not for a replay.
