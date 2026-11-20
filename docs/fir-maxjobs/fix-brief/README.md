# Fix brief (draft)

> we should create journal brief as deliverable for next task to fix all of this.

This is the draft of the journal post that hands the fix to the next task. The full record is `docs/fir-maxjobs/` on `feat/lab/beth-mp-port`.

---

**Next task: metasmith tasks read inputs once from local scratch and write once at the end.**

**Why.** Alliance capped phyberos on fir at max_jobs=1 on 2026-11-06 (Ticket#0331813). Beth's two CPE3 parent `bgc_func` runs (`sTKo4WlC`, `C9htVb7I`) held about 200 `eggnog_mapper` chunks at once, 16 CPUs each, about 3,100 cores. Each chunk read the shared `eggnog.db` (41 GB SQLite) in place on Lustre. SQLite's per-read probes for `eggnog.db-journal` and `-wal` flooded the Lustre metadata server. The root cause is `stageInMode = 'symlink'` in `src/metasmith/nextflow_config/slurm.nf`: every input, references included, is a link back to Lustre for the life of the task.

**Decided (Tony, 2026-11-10)**
1. Every task input copies into node-local scratch by default, reference DBs included.
2. A data type opts out with `stage: link` for files too big to copy per task, such as uniref90 or MPDB-scale DBs. There is no size check and no launch failure.
3. Copy eggnog locally and drop `--dbmem` from `eggnog_mapper.py`. This reverts that part of `71cd5453`, and its memory request goes back down with it.
4. Copy every diamond DB locally.
5. Pass over the transforms and contracts: confirm scratch work is local, and find writes outside `$PWD` (absolute paths, `--tmpdir`, outputs beside a DB). Check the three `noscratch` builds (`buildDramDB`, `buildPhyloflashDB`, `buildGanon2DB`) against write-once.
6. Docs:
   - `src/metasmith_libraries/AGENTS.md`: the authoring rule (inputs copy, so set `stage: link` on too-big types).
   - `docs/metasmith/architecture.md` § Execution: the staging mechanism, beside the `noscratch` paragraph.
   - `docs/metasmith/source/usage/nextflow.rst`: the Lustre dev note, with the fir cap as the example.
   Keep Lustre detail out of code.

**Not adopted:** chmod read-only DBs, per-transform access labels, more chunks per job, an account-wide concurrency cap, a size-threshold failure.

**Out of scope:** the original MetaPathways (MP4) wrappers in `$B/jobs/`. They were not the cause. If MP4 is rerun, its wrapper copies the MPDB subsets it uses to `$SLURM_TMPDIR` and writes output locally.

**Mechanism evidence:** see `docs/fir-maxjobs/mechanism/` (strace on fir, pending).

**Also owed:** the reply to Wade (Alliance support) asking to lift the limit, once the fix lands. See `docs/fir-maxjobs/reply-to-wade/`.

## Posted (2026-11-10)

> post where the work will happen.

Created scope `metasmith/engine/local-staging` (branch `feat/engine/local-staging` from `feat/dev`) and set its brief. The brief above is posted as its journal `a6851a3c`. When the strace evidence lands, it gets an addendum there.
