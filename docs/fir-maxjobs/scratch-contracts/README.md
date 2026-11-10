# Scratch contracts: local work, read once, write once

> as part of the next task, we shuold also make a pass at metasmith contracts so that scratch work is done locally. this is implied by design, but some may still be writing to lustre scratch. jobs should only ideally write once at the end, and read once in the beginning.

## Survey (2026-11-10, `src/metasmith/nextflow_config/slurm.nf`)

- **Local work holds by default.** `process.scratch` is `${SLURM_TMPDIR:-...}`, so each task runs in node-local scratch and Nextflow copies the outputs back once at the end. The results step then hard-links them into `results/`.
- **Inputs are not read once.** `stageInMode = 'symlink'` applies to every input. Its comment reads "some inputs are large reference databases and must not be copied". As a result, every input, references included, is read in place on Lustre for the whole task. This one setting is the root of the eggnog problem.
- **Three exceptions write straight to Lustre.** The `noscratch` label (`models/libraries/resources.py` `NO_SCRATCH_LABEL`) turns off scratch for `buildDramDB`, `buildPhyloflashDB` and `buildGanon2DB`. These are reference builds whose products stay as large files in the work dir.
- **Every task also runs from Lustre.** It runs the bootstrap `$DIRECT/lib/msm_bootstrap` from the Lustre run dir, and the task cache (`task_cache`) lives on Lustre. Both are small, once-per-task operations.

## For the pass

- Copy the reference-DB inputs into scratch, starting with eggnog and every diamond DB, as decided in `db-handling`.
- Audit each transform for writes outside `$PWD`, such as absolute paths, `--tmpdir` or `-o` flags pointing at a reference dir, or tools that write next to their DB.
- Check the three `noscratch` builds against the write-once rule.
- Put Lustre detail in the dev notes, not in code (see `PREFERENCES.md`).
