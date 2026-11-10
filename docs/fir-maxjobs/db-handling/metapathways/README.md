# MetaPathways

> what about metapathways?

## Survey (fir, 2026-11-10)

There are two MetaPathways paths.

1. **The metasmith port** (`research/beth_metapathways/port/run_port.py`, `mp` lane and the others). These are metasmith transforms, so the scratch-contracts pass covers them. Its reference DBs are Lustre paths in `GLOBAL_REFS` and `EXTRA_REFS`: eggnog, uniref50 dmnd, tcdb/vfdb/bacmet/megares dmnd, kofam, bakta, antismash, genomad, virsorter2, pathofact and others.
2. **The original MP4 tool** (T8, finished for all 17 assemblies). Its wrappers are `$B/jobs/mp_analysis{,_one,_parent}.sbatch` and `.sh`, and `B=/scratch/phyberos/beth_mp`.
   - One job per sample, 32 CPUs each, so at most about 544 cores. It was not the cause of the cap.
   - Scratch is local: `METAPATHWAYS_FAST_SCRATCH=$SLURM_TMPDIR`. The parent wrapper also writes output to `$SLURM_TMPDIR/mprun` and copies it back. The small-sample wrappers write output straight to `$B/mp4/out` on Lustre.
   - The DB `-d $B/MPDB` is read in place on Lustre. It is 617 GB, mostly diamond-formatted swissprot, uniref50/90, cazy, eggnog and metacyc, with symlinks into Juan's `/scratch/jcsm2010/metapathways_dbs`.
