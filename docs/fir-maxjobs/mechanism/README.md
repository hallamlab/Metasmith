# Mechanism: where the eggnog.db-journal / -wal stats come from

> no need for fix, we just need to confirm and locate issue for now.

## Result (strace on fir, 2026-11-10)

The calls are confirmed. SQLite inside emapper checks for a hot journal and a WAL file at the start of every read on `eggnog.db`. It runs `stat` on `eggnog.db-journal` and then on `eggnog.db-wal`, and both fail with ENOENT. Nothing else contributes.

- **Setup:** emapper 2.1.12 (biocontainers SIF from the port's `container_images/`), annotation phase only (`-m no_search --annotate_hits_table`), 200 hits taken from production task `00/b8331947…`, `--cpu 4`, DB in place on Lustre `/scratch/phyberos/databases/eggnog`. Job 3361515 on fc30669, now cancelled.
- **Counts:** in 104 s, `stat` hit `eggnog.db-journal` 39,043 times and `eggnog.db-wal` 39,043 times, all failing. Total ENOENT file calls were 79,656. `eggnog.db` itself was stat'ed 29 times.
- **Rate:** about 390 failed lookups per protein annotated, about 750 per second for one 4-thread task. The about 200 production tasks ran 16 threads each for 10–12 h, so the metadata server took on the order of 10⁵ failed lookups per second or more.
- **Untested:** the local-copy run and the `--dbmem` run were skipped as low priority. The fix is expected to move these calls off Lustre, but that is not measured.

Evidence on fir: `/scratch/phyberos/beth_mp/tmp/emapper_strace/`, which holds `L.summary`, `L/strace.log.gz`, `hits200.tsv`, `annot.sh` and `run.sh`.

Sample lines:

    stat("/scratch/phyberos/databases/eggnog/eggnog.db-journal", ...) = -1 ENOENT (No such file or directory)
    stat("/scratch/phyberos/databases/eggnog/eggnog.db-wal", ...) = -1 ENOENT (No such file or directory)
