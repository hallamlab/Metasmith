# nibi exposure

> check nibi, cancel jobs if causing issues

## Result (2026-11-10)

Nothing is causing issues on nibi, so I cancelled nothing.

- `squeue` shows 2 jobs, `beth_tables_shortspades_parent` and `beth_tables_operams_parent`, at 2 CPUs each.
- The nibi eggnog work is finished. Since Nov 9 there were 394 completed `nf-p05__eggnog_mapper` tasks at 16 CPUs, and 156 that phyberos cancelled, which was the restart onto `--dbmem` in `71cd5453`. The last task ended at 13:03 and `merge_eggnog_mapper` at 13:09 on Nov 10 (nibi clock).
- The nibi associations (`def-shallam_cpu`, `def-shallam_gpu`) have no MaxJobs limit, only MaxSubmit=1000.
