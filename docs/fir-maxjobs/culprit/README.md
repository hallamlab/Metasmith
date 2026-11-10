# Culprit: what ran the ~3100 cores of eggnog.db stats on fir

> may not be Beth's issue, could be some other task, track down what was running the 3k jobs?

## Result

The load came from the eggNOG-mapper step of the `bgc_func` drivers for Beth's two CPE3 parent assemblies. Those drivers belong to the T9 MetaPathways port orchestrator. No other phyberos work on fir touched eggNOG.

| Run | Parent | Driver job | Started (fir clock) |
|---|---|---|---|
| `sTKo4WlC` | `operams_cpe3_parent` | 2671247 | 2026-11-05 11:19 |
| `C9htVb7I` | `shortspades_cpe3_parent` | 2671479 | 2026-11-05 11:22 |

Both runs live under `/scratch/phyberos/beth_mp/port/metasmith/runs/`. The orchestrator log `logs/t9/orch.log`, lines 83 and 87, records each launch.

## Evidence

The evidence comes from `sacct -u phyberos` for 2026-11-04 to 2026-11-06, which lists 4,849 jobs.

- **Core-hours by tree:** 71,366 under `/scratch/phyberos/beth_mp/port`. Everything else totals under 1,100: `/home/phyberos` has 749 and `beth_mp` outside the port has 311.
- **eggNOG jobs:** all 550 `nf-p05__eggnog_mapper` tasks ran under the port. The two parent runs account for 397 of them, and each task used 16 CPUs.
- **eggNOG cores in flight:** about 1,700 at 11:00 on Nov 5, 2,800 at 13:00 and 3,024 at 23:00. They held at about 2,700 until the cap at 12:25 on Nov 6. This matches Wade's "over 3100 cores". Jobs that the outage killed may show inflated End times, so treat the hourly tail as an upper bound.
- **Total peak:** 4,357 phyberos cores at 11:21 on Nov 5. Of these, eggnog_mapper held 1,712, diamond_tcdb 1,080 and diamond_uniref50 976. All of them were port tasks.
- **Shared DB:** every task read the same DB, `--data_dir /scratch/phyberos/databases/eggnog`, on Lustre `/scratch`. The command was `emapper.py ... -m diamond --cpu 16`, with **no `--dbmem`**. The transform `src/metasmith_libraries/transforms/functionalAnnotation/eggnog_mapper.py` produced it.

## Related

- Commit `71cd5453` (2026-11-10 06:38, Opal, on this branch) adds `--dbmem` and raises the step's memory to 80 GB. Its reason was slowness on nibi's NFS scratch, not the fir ticket. The fir runs used the code before that commit.
- The earlier record of the cap is journal `c2e7dc77` (seat beth-davenport): root set max_jobs=1 on all 4 phyberos associations at 2026-11-06 12:25.
