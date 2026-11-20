# Why 3,100 cores: the job specs

> why 3100 cores? what were the job specs?

## Answer

3,100 ≈ 2 drivers × 100 queued tasks × 16 CPUs.

- **Per task:** `p05__eggnog_mapper` asked for 16 CPUs, 32 GB and 1 day. These are the `Resources` in `eggnog_mapper.py` before `71cd5453`. Each task is one chunk of about 5,000 proteins, set by `CHUNK_SIZE = 5000` in `transforms/logistics/chunkOrfsForAnnotation.py`. A chunk took 10–12 h (median 11.6 h for `sTKo4WlC`, 9.6 h for `C9htVb7I`).
- **Per driver:** each parent assembly yields about 1M proteins, which makes about 200 chunks: 207 for `operams`, 190 for `shortspades`. Nextflow's Slurm executor in each run has `queueSize = 100` and `submitRateLimit = '1/5sec'` (`workflow.config.nf`). So each driver holds up to 100 Slurm jobs at once. Peak concurrency was 107 and 100.
- **Two at once:** the two parent `bgc_func` drivers started 3 minutes apart (11:19 and 11:22 on Nov 5). eggnog was the slowest step, so both queues filled with eggnog chunks: 2 × 100 × 16 = 3,200 cores. Each of the about 200 processes runs SQLite lookups for its whole 10–12 h life.
- **Nothing capped it across drivers.** `queueSize` limits each Nextflow run, not the account. Eight other parent drivers (mp, noncoding, mge and others) ran at the same time with their own queues, which is how the account total reached 4,357 cores.
- **The outage ended it.** 95 + 61 tasks show `CANCELLED by 0`, and 3 show `NODE_FAIL`.
