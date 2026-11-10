# How reference databases are handled

> we should talk about how reference databases are handled. we'd want to do read-only and local copy if possible.

## Survey (fir, 2026-11-10)

- Metasmith passes a reference DB to a task as a path on Lustre. In the task folder, Nextflow symlinks `eggnog -> /scratch/phyberos/databases/eggnog`, so every task reads the DB in place.
- Task scratch is already local. `.command.run` changes into `NXF_SCRATCH` under `$SLURM_TMPDIR` and copies the outputs back to `nxf_work` once at the end (`nxf_fs_copy`). The DB is the exception, because it reaches the task only as a symlink.
- The DB is not read-only. `/scratch/phyberos/databases/eggnog/` is `drwxr-xr-x` and its files are `-rw-r--r--`, so the owner, which is every task, can write.
- The eggnog files: `eggnog.db` 41 GB (random access), `eggnog_proteins.dmnd` 9.3 GB (streamed), `eggnog.taxa.db` 278 MB.

Children: `lustre-reads/`, `dbmem/`.

## Decision (2026-11-10)

> lets go for local copy and make a similar change to other tools when we make a pass. I think diamond calls in general should be local copy.

> I reject all but 3 as being too eager. local copy and the pass should fix it. lustre technicalities dont belong in metasmith, but we can make a note in dev notes

- Copy reference DBs to node-local disk at task start, starting with eggnog. The scratch-contracts pass applies the same change to the other tools.
- Copy every diamond DB locally.
- Drop `--dbmem`. This reverts that part of `71cd5453`; the memory goes back down with it.
- Not adopted: chmod read-only, per-transform access labels, and batching more chunks per job.
- Keep Lustre detail out of metasmith code. Write it in the dev notes.
