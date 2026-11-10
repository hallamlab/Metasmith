# How reference databases are handled

> we should talk about how reference databases are handled. we'd want to do read-only and local copy if possible.

## Survey (fir, 2026-11-10)

- Metasmith passes a reference DB to a task as a path on Lustre. In the task folder, Nextflow symlinks `eggnog -> /scratch/phyberos/databases/eggnog`, so every task reads the DB in place.
- Task scratch is already local. `.command.run` changes into `NXF_SCRATCH` under `$SLURM_TMPDIR` and copies the outputs back to `nxf_work` once at the end (`nxf_fs_copy`). The DB is the exception, because it reaches the task only as a symlink.
- The DB is not read-only. `/scratch/phyberos/databases/eggnog/` is `drwxr-xr-x` and its files are `-rw-r--r--`, so the owner, which is every task, can write.
- The eggnog files: `eggnog.db` 41 GB (random access), `eggnog_proteins.dmnd` 9.3 GB (streamed), `eggnog.taxa.db` 278 MB.

Children: `lustre-reads/`, `dbmem/`.
