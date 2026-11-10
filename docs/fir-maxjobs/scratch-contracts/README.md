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

## Copy-by-default safety (2026-11-10)

> copy by default would make naive metasmith contract copy 600GB... is this safe? how can we give a warning to agents building new contracts?

It is not safe. Copying 600 GB into each task takes hours and can fill `$SLURM_TMPDIR`: on fir, 12 tasks on one node would fill its 7.2 TB. At 100 tasks it reads 60 TB from Lustre, which is its own way to get capped.

Proposed instead:
- **Task products** (chunks and intermediate files between steps) copy into scratch by default. They are chunk-sized.
- **Reference DBs** take their staging from the data type. The types in `data_types/ref.yml` all extend `reference_data`. Give that base type a property `stage: link`, so a new reference type inherits the safe choice and never copies 600 GB by accident. eggnog and the diamond DB types override it with `stage: copy`.
- **A guard instead of a warning.** At launch the driver stats each copy-staged input once. Above a cap, the launch fails with an error that names the type and the `stage: link` override. An agent cannot skip a failed launch the way it can skip a warning.
- **Docs.** Add one paragraph in the transform and type authoring guide (when to choose `copy` or `link`) and the Lustre detail in the dev notes.

## Decision (2026-11-10)

> just copy and opt out without failure on size threshold. where in docs?

- Every input copies into scratch by default, reference DBs included. This replaces the global `stageInMode = 'symlink'`.
- An input opts out with `stage: link` on its data type. The pass marks the types too big to copy, such as uniref90 and MPDB-scale DBs.
- No size check and no launch failure. This supersedes the proposal above.

Docs (proposed homes):
- `src/metasmith_libraries/AGENTS.md` carries the warning to agents. It resolves by directory, so an agent editing a transform or type reads it. Add one rule: inputs copy into local scratch; set `stage: link` on a type whose files are too big to copy per task.
- `docs/metasmith/architecture.md` § Execution: how staging works and why copy is the default. It sits beside the existing `noscratch` paragraph.
- `docs/metasmith/source/usage/nextflow.rst`, which already has "Network filesystem detected": the Lustre detail, as the dev notes. Metadata lookups and failed lookups go to one server, and the fir cap (Ticket#0331813) is the example.
