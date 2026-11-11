# Mechanism: where the eggnog.db-journal / -wal stats come from

> no need for fix, we just need to confirm and locate issue for now.

## Result (strace on fir, 2026-11-10)

The calls are confirmed. SQLite inside emapper checks for a hot journal and a WAL file at the start of every read on `eggnog.db`. It runs `stat` on `eggnog.db-journal` and then on `eggnog.db-wal`, and both fail with ENOENT. Nothing else contributes.

- **Setup:** emapper 2.1.12 (biocontainers SIF from the port's `container_images/`), annotation phase only (`-m no_search --annotate_hits_table`), 200 hits taken from production task `00/b8331947…`, `--cpu 4`, DB in place on Lustre `/scratch/phyberos/databases/eggnog`. Job 3361515 on fc30669, now cancelled.
- **Counts:** in 104 s, `stat` hit `eggnog.db-journal` 39,043 times and `eggnog.db-wal` 39,043 times, all failing. Total ENOENT file calls were 79,656. `eggnog.db` itself was stat'ed 29 times.
- **Rate:** about 390 failed lookups per protein annotated, about 750 per second for one 4-thread task. The about 200 production tasks ran 16 threads each for 10–12 h, so the metadata server took on the order of 10⁵ failed lookups per second or more.

## Result after the fix (fir, 2026-11-10)

The fix is on `feat/engine/local-staging` (b29cc953). It has two parts: each task copies its inputs into node-local scratch, and `eggnog_mapper` opens its SQLite files with `immutable=1`.

- **immutable=1:** the same 200-hit annotation run against the database in place on Lustre made 0 `-journal` and 0 `-wal` lookups, against 39,043 of each before. Total ENOENT file calls fell from 79,656 to 1,682. The 200 annotation rows are identical to the pre-fix run. The emapper image has no `sitecustomize.py` of its own, so the shim loads. Job 3382163, summary `S.summary`.
- **Local copy:** a one-chunk metasmith run (200 proteins, queue size 1) copied the 49 GB `eggnog` directory into `$SLURM_TMPDIR` in about 2 minutes. The step log's `uses:` line reads `/ws/eggnog`, the scratch copy. No container bind names `/scratch/phyberos/databases`.
- **Page cache:** the copy fills the job's memory cgroup with page cache up to `memory.max`. The kernel reclaims it, with 0 OOM kills. Slurm's MaxRSS reports it, so a copying task shows MaxRSS near its memory request.
- **Not finished:** no run reached the end of emapper on the copy. One ran 15 minutes of the DIAMOND search on fc30562 before that node's CVMFS dropped, with a bus error and "Transport endpoint is not connected". Three more attempts landed on fc30556, fc30629 and fc30568, and each had broken CVMFS or `/home` at job start. The status page showed no fir incident.

 `/scratch/phyberos/beth_mp/tmp/emapper_strace/`, which holds `L.summary`, `L/strace.log.gz`, `hits200.tsv`, `annot.sh` and `run.sh`.

Sample lines:

    stat("/scratch/phyberos/databases/eggnog/eggnog.db-journal", ...) = -1 ENOENT (No such file or directory)
    stat("/scratch/phyberos/databases/eggnog/eggnog.db-wal", ...) = -1 ENOENT (No such file or directory)
