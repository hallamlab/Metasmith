# Re-unit the task cache: one group member's invocation, decided before submission

## Context

The trio investigation left the cache correct for an unchanged input set and useless for a
changed one: adding one sample re-runs every sample, because the cache unit is the plan step
and a step's key covers every input across every sample. Two prior attempts kept that unit and
tried to predict the run-time fan-out at compile time. This plan changes the unit to what
actually runs, mints its key where its inputs are known, and keeps every hit off the scheduler.

## What you said

> the caching is still broken. understand what the caching is supposed to do on first principles and strategize a fix. I think the cached unit was choosen poorly. Do not accept patch solutions.

> use tests (existing or new) to isolate the error and verify sucess

> inside the task is probably a must. I'd like to not do a round trip through the slurm scheduler though.

> can we get groovy to call an external library to do the hashing? 2 implementations is inviting drift and churn. And how come we cant use the isntanced lineage to make the cache hit even if the batch order changes?

> what if we defer to a local executor that just copies or hardlinks the products over? from nextflow's perspective, the task would have actually run and produced results and from our perspective, we would have skipped the compute time.

> Yes, pay it once (the cache epoch bump)

> run 1: samples A, B, C -> through some workflow, complex, done -> results cached. run 2: samples X, A, Y -> through some modified workflow, but A is produced by exact lineage including types and data -> *A should be cache hit*

## Issues

- I1. The cache unit is the plan step, so a per-sample step's key moves whenever the sample set moves, and every step downstream moves with it (`test_sample_addition.py`: 5 of 5 per-sample steps re-key).
- I2. A produced file's on-channel identity hashes the absolute staged paths of its task (`payload.output_name_hash` folds `FILES`), so it differs on every run and cannot anchor a key.
- I3. The hit decision is made at compile time from a predicted fan-out (`cache_decisions.batches`), which cannot exist downstream of the solver's fold.
- I4. A hit is served by splicing shard paths into the channel, so Nextflow refuses to publish them and the driver places them by hand afterwards (`cache_publish.json`, `PublishCachedProducts`), and a hit has no row in the run's own trace.
- I5. Promotion is a post-run scan of the work directory that re-derives what each task read, instead of a record the task wrote.
- I6. A step with `batch_size > 1` is cached as a whole batch, so a change of batch membership discards every member's work.

## High-level goals

- G1. Re-running a workflow after adding or changing one sample computes only that sample's chain and the steps that genuinely consume every sample.
- G2. A cache hit never reaches the cluster scheduler and costs no compute reservation.
- G3. The cache key has one implementation, and every side that needs a key calls it.
- G4. A hit looks like a task that ran: it publishes, it staged its outputs in a work directory, and it appears in the run trace.
- G5. Batch order and batch composition do not decide whether a member hits.
- G6. A sample that reaches the same step through the same lineage hits, whatever else the workflow around it does.

## Acceptance criteria

- `tests/metasmith/cache/test_sample_addition.py::test_a_per_sample_step_keeps_the_keys_it_already_minted` passes, rewritten against member keys; its two siblings stay green.
- The trio at 2 samples, then at 3 in the same agent home, executes only the third sample's per-sample chain plus the three merges; the databases and the first two samples hit.
- `tests/metasmith/audit/test_trio_cache_reuse.py` stays green: a second identical run and a re-solve execute no transform.
- Run 1 is `[A, B, C]` through the trio; run 2 is `[X, A, Y]` through a modified plan (a different target set, so step orders shift and steps are added or dropped). Every per-sample step on A's chain hits; X, Y and the merges miss. A second variant modifies a transform on A's chain and asserts that step and everything below it misses while everything above it still hits.
- A `batch_size=3` transform run on samples `[a,b,c]`, then on `[c,d,a,b]`, submits one task carrying only `d` and serves `a,b,c` from their shards.
- The key the Python entry point computes for a fixture equals the key the generated Groovy obtains through it, checked by a test that runs Nextflow in the dev image.
- `tests/metasmith/e2e/docker/test_cache_real_nextflow.py` passes: the second run's hits are served by `*_cached` processes on the local executor, their products land in `results/` through `publishDir`, and `nxf_tasks.csv` shows them.
- `PublishCachedProducts`, `AgentPaths.CACHE_PUBLISH_MANIFEST`, `parse_cache_hits`/`CACHED-METASMITH`, `cache_decisions.batches`, `output_name_hash`, and the synthetic `Channel.of` hit path are deleted.
- `CACHE_KEY_VERSION` is 5. `LIN_PAYLOAD_VERSION` is 5. `SHARD_LAYOUT_VERSION` is 3.
- `tests/metasmith/cache`, `flow`, `audit` and `unit` are green apart from the four solver-engine failures and the relay-reclaim failure recorded in `.awm/context.md`.

## Tasks

- T1. Write the contract tests and observe them red (G1, G5, G6)
- T2. Write the member key, the probe, and the structural slot id in one module (G3, G5)
- T3. Make a produced file's identity position-independent and key-carrying (G1, G5, G6)
- T4. Route each member on the channel before submission (G2, G5)
- T5. Emit the local-executor twin process per step (G2, G4)
- T6. Promote per member inside the task (G1, G4)
- T7. Strip the compile-time cache pass to stamping and step meta (G1)
- T8. Record hits and promotions after the run (G4)
- T9. Mirror the member cache in the virtual runtime (G1, G3)
- T10. Rewrite the cache lane's remaining tests to the new unit (G1)
- T11. Run the contract tests and the lanes, and observe them green (G1–G6)
- T12. Commit a checkpoint
- T13. Compact
- T14. Validate the Groovy under real Nextflow in the dev image (G2, G4)
- T15. Draft the doc updates
- T16. Debrief

## Approach by task

### T1. Write the contract tests and observe them red

Write every test named in the acceptance criteria before touching `src/`. Put the
`[A,B,C] → [X,A,Y]` test (both variants), the trio 2→3 test and the batch-membership test in
`tests/metasmith/audit`, driven through `capture_run` on the virtual runtime. Put the
member-key unit tests in `tests/metasmith/cache/test_member_key.py`: a member whose `PROV`
lacks its own id is uncacheable, two members with the same consumed ids share a key, and a
`mint_file_id` fixture name at batch position 3 and at position 1 mint one id. Rewrite
`test_sample_addition.py` to feed `build_entry` members into the new key function. Write the
Python↔Groovy agreement test and the twin-process assertions in the docker lane. Run the
set. Record the failure line of each in the plan file. Every test must fail on the current
tree, and the reason must be the defect it names, not an import error.

Gotchas: import the new symbols lazily inside the tests so collection succeeds while the
module does not yet exist. `_cache_harness.executed_steps` counts `bootstrap_call` events;
the batch test asserts on the members a call carried, which needs `batch_start`/`batch_end`
from the trace.

### T2. Write the member key, the probe, and the structural slot id in one module

Create `src/metasmith/caching/invocation.py`. Import only `caching.keys` and stdlib. Write
`consumed_of(entry, slot_channels)`: read the member's `PROV`, return per slot the own-id of
each staged item under the slot's channel name, return `None` when `PROV` is absent or any
item lacks its own id. Write `member_key(transform_key, signature, consumed)` as
`lineage_key` over `{slot: sorted ids}` under `CACHE_KEY_VERSION = 5`. Write
`structural_slot_id(transform_key, signature, slot_key, branch, upstream_slot_ids)`. Write
`probe(cache_root, key)`: a hit needs `manifest.cbor`, every listed file present, and no
`tombstone` marker. Write a `__main__` that reads `{tk, sig, slk, cache_root, members}` JSON
on stdin and writes one line per member: `<key>|-`, `<key>|hit|<shard>` or `<key>|miss`. Bump
`CACHE_KEY_VERSION` in `keys.py`. Keep blake3 and CBOR.

Gotchas: `caching/__init__.py` is empty and must stay so, or the helper's start-up cost grows
to the store's sqlite import. Do not add `step.order` to the slot id: run 1 with a given
database and run 2 that downloads it shift every order, and A's chain must still hit.

### T3. Make a produced file's identity position-independent and key-carrying

Change `payload.output_file_name` to take the member key. Delete `output_name_hash`. Change
`LinPayload.mint_file_id` and `Orchestrator._post` (`Orchestrator.groovy:121`) to hash the
name with its leading `<pos>-` replaced by `1-`. Add the reserved key `KEY` to
`LinPayload.RESERVED_KEYS`, `Orchestrator.stripReserved`, `combineIndexes`'s exclusion,
`payload.output_index` and `_RESERVED_KEYS_GROOVY`. Bump `LIN_PAYLOAD_VERSION` to 5. Make
`ExecuteStep._get_output_paths` raise when an entry carries no `KEY`. Record the as-published
name in the miss event's `path` and index the position-stripped basename in
`collect._published_index`.

Gotchas: `mint_file_id` and `_post` are the two ends of one identity. The stub lane's
`test$hash` names must keep the `<pos>-<item>-<branch>.` shape `_debatch` parses.

### T4. Route each member on the channel before submission

Rewrite `Orchestrator._batch`. Collate as today. Per batch, run the helper once
(`["python","-m","metasmith.caching.invocation"].execute()`, JSON on stdin), stamp each
member's `KEY`, tag each member hit or miss, and append one JSON line per hit to
`<workspace>/_metasmith/cache_hits.jsonl`. Split with two `filter` consumers: misses through
`_collateBatch` to the real process, hits through `_collateBatch` to the twin with the shard's
`out/` files as values in task-local coordinates. Extend `group()` with one map argument
(`transform_key`, `signature`, `slot_channels`, `cache_root`, `cacheable`, `helper`) and
return `[misses, hits]`. Send everything to the real process when the step is uncacheable,
when `METASMITH_CACHE=0`, or when the helper fails, and log the failure. Render the helper
command and the local cache root from codegen through `PathMap`.

Gotchas: `branch` is a script AST transform and is not available in a `-lib` class. A helper
failure is a miss, never a hit. Measure the helper's cost on a 200-batch fixture and record
it in the followups doc.

### T5. Emit the local-executor twin process per step

Extend `prepare_step` to emit `<name>_cached`: `executor 'local'`, `cpus 1`, `memory '256
MB'`, no container, the same `output:` block, `input: tuple val(index), val(sources)`, a
script that links (`ln`) or copies (`cp -r`) each source into the work dir under its stored
name re-prefixed with the member's position, and a `stub:` that touches the same names. Write
the twin's resources into `workflow.resources.nf`. Emit the main block as `def (__miss_N,
__hit_N) = o.group(...)`, call both processes, mix their outputs per branch, and hand the mix
to `o.post`. Delete the cache-staging `publishDir`. Delete `AgentPaths.CACHE_PUBLISH_MANIFEST`,
its writer, `PublishCachedProducts`, `parse_cache_hits` and `_merge_cache_hits`.

Gotchas: `ceiling.py` reads `workflow.resources.nf` for every process. The twin name must
pass `NextflowProcessName`'s character rules.

### T6. Promote per member inside the task

Write `caching/promote.py::promote_members(...)`. Call it from `ExecuteStep` after the
protocol. For each successful member: match outputs by `<pos>-` prefix, rename them to `1-`
in the shard, link when same filesystem and copy otherwise, write `manifest.cbor` with files,
`slot_id`, `dtype_key`, `parents` from that member's `PROV`, `consumes` and `size`. Skip a
member whose non-optional branch produced nothing. Stage under `<key>.<host>.<pid>.tmp` and
rename onto the shard. Let an existing shard win. Write one `.command.cache` record per task
beside `.command.metadata` and copy it to the real work dir the same way. Read the cache root
as `default_cache_root(AgentPaths.HOME_ROOT)`. Never open sqlite from a task. Delete
`promote_run`'s work-dir scan, `_find_step_outputs`, `_collect_output_provenance`,
`recover_orphan_tmp_dirs` and the lock file.

Gotchas: `ExecuteStep` reports success when any member succeeded. Under `scratch` only
declared outputs and `.command.*` survive the node.

### T7. Strip the compile-time cache pass to stamping and step meta

Reduce `compute_cache_decisions` to: stamp structural slot ids onto produce and require
instances, write `transform_key`, `signature`, `slot_files`, `cacheable` and `slk` into the
step meta, rotate `trace.jsonl` with its `SessionStart` sentinel. Delete the store probe,
`cache_key`, `sorted_inputs`, `batches`, `out_indexes`, the demotion checks and the
compile-time `hit` events. Keep `restat_leaf_ids`.

Gotchas: `virtual_runtime._read_slot_ids` and `_read_hit_decisions` read this meta; T9
follows.

### T8. Record hits and promotions after the run

Rename `promote_run` to `record_run(workspace, cache_root)`. Read every `.command.cache` and
`cache_hits.jsonl`. Append one `InvocationEvent` per member with `task_hash` = member key,
`status` `promoted` or `hit`, `produces` from the manifest and `consumes` from the record,
through one emitter. Upsert and touch one sqlite row per member shard. Copy the producing
task's `.command.{out,err,sh,log}` into each member's `logs/`. Bump `SHARD_LAYOUT_VERSION`
to 3. Make `gc_cache --delete` remove the shard directory and make a tombstone without
delete write a `tombstone` marker. Reorder `runner.py`: `record_run`, then copy the trace.

Gotchas: `telemetry.resolve_log_bundle` reads `logs/`. `TraceIndex.by_task` is last-write-wins
on `task_hash`, so the key must be per member.

### T9. Mirror the member cache in the virtual runtime

Rewrite `virtual_runtime.cli_nextflow`'s cache path. Per batch: build the member entries,
call `consumed_of`, `member_key` and `probe`, stage hit members from their shards, run
bootstrap for the miss members only, name synthesised outputs with the member key, call
`promote_members`, and write `cache_hits.jsonl` and `.command.cache` in the shapes T8 reads.
Delete `_read_hit_decisions` and make `_populate_hit_outputs` per member.

Gotchas: the runtime must call the same functions, not copy them; `tests/metasmith/AGENTS.md`
records the opposite today and T15 corrects it.

### T10. Rewrite the cache lane's remaining tests to the new unit

Rewrite `test_incomplete_shard.py` (a member with an empty required branch mints no shard and
probes as a miss), `test_codegen.py` (the twin process and the two-stream `group` call are
emitted, no `Channel.of`), `test_hit_lineage.py`, `test_trace.py`, `test_identity*.py`,
`test_structural_identity.py`, `test_empty_index.py`, `test_promote.py`, `test_kill_switch.py`
and `test_cross_*.py`. Update every test in `flow` and `unit` that names `output_name_hash`,
`cache_publish` or `batches`. Delete `tests/metasmith/e2e/virtual/test_cache_baseline.py` if it
pins the step unit.

Gotchas: rewrite the expectation, not the assertion; a test that pins the old unit is a
test of a shape that no longer exists.

### T11. Run the contract tests and the lanes, and observe them green

Run the T1 set first and record each result against the failure line recorded in T1. Then
run `tests/metasmith/cache`, `flow`, `audit` and `unit` by file. Report the counts.

Gotchas: the `fast` marker is stamped by directory and these lanes take hours. Deselect
`test_the_engine_reads_the_shipped_templates`, which never terminates.

### T12. Commit a checkpoint

Commit T1 as the red commit with the observed failures in its message, then one commit per
landed shape. Exclude docs.

### T13. Compact

Compact here, before the docker lane.

### T14. Validate the Groovy under real Nextflow in the dev image

Rebuild the dev image (`dev/metasmith.sh --help`). Run
`tests/metasmith/e2e/docker/test_cache_real_nextflow.py`, the agreement test and
`test_resource_cap_groovy.py`. Run the lane alone on a quiet box. Fix what it finds and commit.

Gotchas: the agent container carries its own metasmith build, so a stale image tests the old
promote path and says nothing.

### T15. Draft the doc updates

Load `write-docs`. Rewrite the unit, identity, hit and promote paragraphs of
`docs/metasmith/architecture.md` § *Task cache and lineage*. Delete the publish-outside-workDir
paragraph and the compile-time short-circuit paragraph. Add `KEY` to the version-constants
paragraph. Move *Adding one sample re-runs every sample* and FANOUT-1 in
`consolidation-followups.md` to resolved and record the helper cost measured in T4. Drop the
red-on-purpose, per-batch `consumes`, `_find_step_outputs` and `gc --delete` threads from
`.awm/context.md`. Correct the virtual-runtime trap in `tests/metasmith/AGENTS.md`. Commit.

### T16. Debrief

Run the `debrief` skill.

## Callouts

- The first run of every workflow after this change re-executes once; `msm cache gc --delete` reclaims the old shards.
- A hit costs one local task per hit batch, seconds each, on the head node.
- A sample hits only at the same path with the same mtime; a copy at a new path is a new leaf. That is the stat-addressing trade already documented.
- `slurm.nf` is untested on a cluster; the twin's `executor 'local'` under slurm runs on the submit host, which is the intended place.

## Autopilot

Guardrails: never bare `git stash`; never `rm -rf` a variable-led path; run tests as
`PYTHONPATH="$PWD/src" mamba run -n msm python -m pytest <files> -q -p no:cacheprovider`;
deselect `test_the_engine_reads_the_shipped_templates`; the docker lane needs a rebuilt dev
image (T14). Worktree `engine/dev`, branch `feat/dev`, base `ea8ebd2`.

### Live state

T1–T14 done; HEAD baa0d80 on feat/dev (clean). Docker lane green in the dev image: 9/9
(`test_cache_real_nextflow` ×4, `test_member_key_agreement`, `test_resource_cap_groovy`,
`test_scratch_metadata` ×2). Next: T15 docs (architecture.md § Task cache and lineage,
consolidation-followups.md, .awm/context.md, tests/metasmith/AGENTS.md), commit; T16 debrief.

### Run log

- 2026-08-26 T1 observed red (all on `ea8ebd2` + tests only):
  - `cache/test_member_key.py` — 6 tests: `Failed: metasmith.caching.invocation does not exist`;
    `test_a_file_id_does_not_depend_on_its_batch_position`: `a member's product changes identity
    with its position in the batch` (md5 differs between `3-1-1.` and `1-1-1.`);
    `test_key_is_a_reserved_payload_key`: `assert 'KEY' in frozenset({'FILES', 'PROV'})`.
  - `cache/test_sample_addition.py` — 3 ERROR in the fixture: the trio's second run at three
    samples dies in `CollectResults`: `KeyError: parent instance [...] is neither a file this run
    produced nor an input it was given` — a hit's produces carry run-1 ancestry (I2/I5).
  - `audit/test_sample_membership.py::TestTrioGainsASample`, `::TestASampleReturnsInADifferentRun`
    (both variants) — same `KeyError` in the second run.
  - `::TestBatchMembership::test_a_reshuffled_batch_submits_only_the_new_member` —
    `expected one task carrying only d, got batch widths [3, 1]` (I6: the whole batch re-ran).
  - `e2e/docker/test_member_key_agreement.py`, `test_cache_real_nextflow.py::TestAHitIsALocalTask`
    — not run on the host (no nextflow); run in T14. Both name symbols that do not exist yet
    (`caching.invocation`, `Orchestrator.probeMembers`, `*_cached` twins).
- Deviation: `test_sample_addition.py` reads member keys from the run's `trace.jsonl` instead
  of feeding `build_entry` members into the key function — the per-member PROV only exists
  once the runtime has staged the fan-out, which is the whole point of I3.
- 2026-08-26 T2–T9 landed. Deviations: `output_file_name` keeps its entry-based signature (the entry
  carries `KEY`; an uncacheable member names by a lineage token so two uncacheable members cannot
  collide). `compute_cache_decisions` keeps its name (task.py/__init__ callers) but only stamps.
  Session ids are `prev + 1` from the trace, no sqlite at compile time.
- Finding: the trio's "merges" are per sample (`group_by=parent_orfs`, one task per sample's chunks);
  no trio step consumes every sample. Tests restated; `[X,A,Y]` expects merges ×2.
- Finding: the virtual runtime's `StagedFiles.of` gave the by-slot every member sharing any id
  (the env leaf poisoned the join), so every downstream step ran once per member with the whole
  slot and the step key hid it. Fixed: by-slot = key item; other slots exclude ids common to all
  members. Real Orchestrator semantics unchanged.
- record_run reads every `.command.cache` under `nxf_work`/`work`, resumed dirs included — same
  exposure the old scan had.
- Known pre-existing reds to expect in T11: flow `repro_16_group_buffering` (wall-clock), unit/solver
  reds named in `.awm/context.md`.
- 2026-08-26 T14: lane first run 8 passed, 1 failed — `TestDirectoryProducts`: the twin's `stub:`
  block `touch`ed each source under `-stub`, so a cached directory arrived as an empty file. Fix:
  the twin has no stub block (its script *is* the copy). Rerun 9/9 green. Commit baa0d80.
- Helper cost (T4 gotcha): `python -m metasmith.caching.invocation` takes ~0.07 s per batch call
  in the msm env, flat from 1 to 200 members — interpreter start-up, not hashing.
- 2026-08-26 T15 landed (docs commit after baa0d80). Deviation: the `gc --delete` thread stays in
  `.awm/context.md` — `gc_cache` still deletes only tombstoned rows past the grace, and nothing
  tombstones pre-epoch shards, so the plan's callout that `gc --delete` reclaims them is wrong as
  stated. Dropped: red-on-purpose, per-batch consumes, `_find_step_outputs`. Cache lane 116 passed
  after the change.
