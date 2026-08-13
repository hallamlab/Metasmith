# Plan: Single Point of Provenance (data-model harmonization)

## Context

Metasmith compiles a typed dataflow into a Nextflow pipeline and tracks the
provenance of every data product so it can trace results back to inputs and
(separately) address a lineage cache. Over several sessions the provenance
machinery accreted **multiple parallel representations of the same fact** —
"this data instance has this identity, and descends from these ancestors" — and
correctness now depends on keeping all of them in agreement by hand. Nothing
structurally forces agreement, so every past caching effort turned into a bridge
between two representations and stalled (the last one, `sharded-nest`, literally
"paused on iid-bridge"), and a standing audit (`plans/lineage-quadrant-audit.md`)
catalogues ten latent divergences (Bugs A–J) between the representations.

The five representations, today:

1. **On-channel identity** — the Nextflow `index` Map, keyed by
   `Long.parseLong(md5(filename)[0..14], 16)`.
2. **Off-channel canonical identity** — `instance_id` (blake3 multihash), in
   `trace.jsonl`, `manifest.cbor`, `workflow.step_N.meta`, and the `input_ids`
   sidecar.
3. **Compile-time archetype view** — intermediate steps collapsed to one
   representative instance per slot.
4. **Runtime fan-out arity** — the real per-file instances, which only exist
   while Nextflow runs.
5. **Two emission routes** — the compile-time cache-hit path and the post-exec
   promote path, required to emit byte-identical event shapes.

This session unifies these into **one provenance data model**: a single identity
scheme carried on *and* off the channel, and a single authored record that is the
sole source of truth, usable uniformly at every level (leaf, static sample
fan-out, dynamic intra-slot fan-out). This is **architectural cleanup only** —
observable pipeline behaviour (results, cache hit/miss decisions, telemetry query
answers) must stay unchanged. Caching (cross-run reentrancy, the compile-time
cache-walk, per-batch keying) is built on top of this model in the **next**
session.

## What you said

> "The objective of this session will be single point of provenance."

> "describe the new data model and how it can be used at all levels and fanouts."

> "We will build caching on top in the next session."

> "From the perspective of tests, we expect overall behaviour to remain
> unchanged, this is just architectural cleanup."

> "pull in changes from dev first." (done — merge `d4904f6`)

## High-level goals

- **G1.** There is one identity for a data instance, and it is the same value
  wherever that instance appears — on the Nextflow channel and off it.
- **G2.** There is one authored provenance record per transform run; every other
  view (channel lineage, telemetry, the cache manifest) reads that record or
  carries its identities, rather than re-deriving provenance independently.
- **G3.** The model represents provenance correctly at every fan-out level,
  including a transform that emits a runtime-variable number of files into one
  output slot.
- **G4.** The pipeline behaves exactly as before — same results, same cache
  hit/miss decisions, same answers from the telemetry API.

## The new data model (what "single point of provenance" means)

This is the substance the rest of the plan implements. Two value types and one
record, all keyed on one identity scheme.

### One identity scheme

Every data instance — a user-provided leaf, one of N statically-fanned samples,
or one of N files a tool emits dynamically into a single slot — is identified by
a single `instance_id`: a blake3 multihash hex string (the existing
`caching/keys.py` scheme). There are two *layers* of this one scheme, already
named in `models/lineage.py`, and they are kept:

- **`slot_id`** — the identity of a *production channel* for a
  `(transform, output slot, branch)` triple. Deterministic from the producer's
  cache_key. This is what rides the Nextflow channel and routes inter-task flow.
- **`file_instance_id`** — the identity of one *emitted file*, deterministic over
  `(slot_id, relative_path)` via `LinPayload.mint_file_id`. This is the identity
  the user-facing library and cache manifest address.

Both are the same hash family; `file_instance_id` is a refinement of `slot_id`
by relative path. Leaves are `slot_id`-level (they have no producing slot; their
id is minted at `AddItem`). The **leaf identity derivation is unchanged this
session** — leaves keep their current minting; making leaf ids deterministic
(content-addressing) is a caching-reentrancy change and belongs to the next
session. What changes now is only that this one id becomes the *sole* identity,
including on-channel.

### One authored record

The single source of truth is the **`InvocationEvent`** (already the canonical
`trace.jsonl` row), authored once per transform invocation, mirrored durably in
the per-shard `manifest.cbor`. It records:

- `transform_key` + `signature` — which transform ran.
- `consumes: {slot_key -> [instance_id, ...]}` — the concrete input identities.
- `produces: [ProducedFile{file_instance_id, slot_id, dtype_key, path}]` — the
  concrete output identities.
- **per-output ancestry** — for each produced file, which consumed input(s) it
  descends from. This is the piece **not durably recorded today**
  (`manifest.cbor` is written with `index_payload=[]` at `promote.py:705`, and
  intermediate `consumes` is step-aggregated), and it is the substrate the
  next-session cache-walk will read. Recording it now completes the model.

Everything else is a *view* of this record: the on-channel `index` Map carries
the same `instance_id`s (not a parallel md5), the telemetry API reads the record,
and the cache manifest stores the record's identities. The two emission routes
become **producer/consumer** — promote *writes* the record, the cache-hit path
*reads* it — instead of two predictors that must be cross-checked.

### How the one model serves every level and fan-out

- **Leaf**: `instance_id` authored at `AddItem`; flows onto the channel as the
  seed identity (via `postIn`), replacing `md5(path)`.
- **Static sample fan-out (N samples known at plan time)**: each sample is a
  leaf-level instance with its own `instance_id`; the N ids flow on-channel and
  the `group()` joins key on them. Arity comes from the given library
  (`AsSamples`), unchanged.
- **Dynamic intra-slot fan-out (N files from one slot, N unknown until runtime)**:
  at execution each emitted file is minted a `file_instance_id` from
  `(slot_id, relpath)`, and its ancestry is authored into the record. This is the
  only level where arity is genuinely runtime-discovered; the record is where
  that discovery is captured once and made durable.
- **On-channel**: the `index` Map value type changes from `Long(md5)` to the
  `instance_id` string; join/grouping semantics are identical, so routing
  outcomes (and therefore results) are unchanged.
- **Off-channel**: `trace.jsonl`, `manifest.cbor`, telemetry, and the cache all
  key on the same `instance_id`.

## Acceptance criteria

- The generated Nextflow `index` Map carries `instance_id` strings; there is no
  `Long.parseLong(md5(...))` on-channel identity and no Python/Groovy md5
  mirroring (`Orchestrator.groovy` `_post`/`postIn`/`group`/`combineIndexes`;
  `workflow.py` given-lineage seeding).
- The `input_ids` sidecar bridge is gone (its job — carrying `instance_id`
  alongside a divergent channel identity — is subsumed once the channel carries
  `instance_id` directly).
- `manifest.cbor` and `InvocationEvent` durably record per-output ancestry at all
  fan-out levels, including dynamic intra-slot fan-out (the `index_payload=[]`
  gap is closed; intermediate-step `consumes` is no longer merely aggregate).
- A single code path authors the provenance record; the cache-hit path consumes
  it rather than re-emitting a divergent shape. The Bug A–E field divergences
  (empty `path`, `slot_id == file_instance_id`, consumer-side `dtype_key`,
  missing `step_name`, double-encoded `consumes`) are resolved by construction.
- Cache hit/miss decisions and shard keys are byte-identical to pre-refactor for
  the same inputs (the cache key is already computed off-channel from
  `instance_id`; this refactor does not touch how it is computed).
- `PYTHONPATH=$(pwd)/src mamba run -n msm python -m pytest -m fast` is green with
  the same pass set as the pre-refactor baseline (378 pass / 7 skip / 3 xfail),
  except that provenance-shape tests currently `xfail`ing on the divergences may
  flip to pass — no test that pins observable pipeline behaviour changes.

## Tasks

- **T0 — Merge `dev`.** *(done — `d4904f6`)* Brings the multi-container group_by
  fix + 2-level regression tests and infra onto the 0.19.0 provenance line.
- **T1 — Author the provenance data-model spec + consolidate the value types.**
  Write the model down as the authority and tighten `models/lineage.py` so the
  identity layers and the record are the single declared shape.
- **T2 — Put the one identity on the channel.** Replace on-channel `Long(md5)`
  with `instance_id`; delete the md5 mirroring and the `input_ids` sidecar.
- **T3 — Make the record the single authored source of truth.** Populate
  per-output ancestry at all fan-out levels; unify the two emission routes into
  writer/reader.
- **T4 — Reconcile every consumer onto the record.** `CollectResults`,
  telemetry, bootstrap, and promote read identity/ancestry from the one record;
  remove duplicate reconstruction.
- **T5 — Test alignment + behaviour-parity proof.** Keep observable behaviour
  identical; update only tests that pinned an old *internal* representation;
  prove parity against the captured baseline.

## Approach by task

### T1 — Provenance data-model spec + value-type consolidation

Write the model above into the scope docs as the governing description, and make
`src/metasmith/models/lineage.py` its single code expression: the `instance_id`
layers (`slot_id`, `file_instance_id`), the `ProducedFile` shape, and the
`InvocationEvent` record with an explicit per-output-ancestry field. Where other
modules currently declare their own lineage-ish structures (the `index` payload
shape in `caching/store.py:encode_manifest`, the `lin` envelope in
`bootstrap.py`), point them at the `lineage.py` definitions so there is one
declaration.

**Gotchas:** don't change `InvocationEvent`'s on-disk field names/semantics that
telemetry already reads; extend, don't rename. Keep `from_jsonl` legacy-row
tolerance intact. This task is mostly consolidation + documentation and should be
behaviour-neutral on its own.

### T2 — One identity on the channel

In `src/metasmith/nextflow_config/Orchestrator.groovy`, rewrite `_post`
(and `post`/`postIn`) so `index[name]` holds the `instance_id` string instead of
`Long.parseLong(md5(...)[0..14], 16)`; delete the `fullHash` toggle and the
sign-avoidance dance. The `group()` joins (`PARENT_OF_BY` / `DESCENDANT_OF_BY` /
`SIBLING`) and `combineIndexes` keep their structure — only the key element type
changes from `Long` to `String`. In `src/metasmith/models/workflow.py`, the
given-lineage seeding (`_index[name] = [int(md5(path)[:15],16)]`, ~2006) emits the
leaf `instance_id` instead; the slot ids are already computed off-channel in
`_compute_cache_decisions` and can be threaded to the channel seed. Remove the
`input_ids/` sidecar (writer at ~1992-1995 and its reader in `agents.py`) once the
channel carries the id directly.

**Gotchas:** the on-channel identity feeds two things — the `group()` joins and
the output filename hash (`bootstrap.py` `KeyGenerator.FromStr(sorted(lin))`).
Output *filenames* will change (they embed a hash of the index); this is
tolerated because tests assert via telemetry, not filenames
(`tests/AGENTS.md`), and promote's file-matching keys on `dtype_key`/`branch`,
not the lin-hash. Groovy `String` equality must drive the joins exactly as
`Long` equality did — verify `combineIndexes` union and the intersection filter
behave identically with string keys. Keep the Python given-seed and the Groovy
`postIn` seed in lockstep (they must mint the same id for the same leaf). The
cache key is unaffected (already off-channel).

### T3 — The record as single authored source of truth

Populate per-output ancestry so the record is complete at every fan-out level.
Concretely: stop writing `index_payload=[]` in `promote.py`; author, per produced
file, the input `instance_id`(s) it descends from — sourced from the same
on-channel `index` the run already carries (now `instance_id`-valued after T2),
captured at promote time. Make intermediate-step `consumes` reflect true runtime
ancestry rather than the compile-time aggregate. Then collapse the two emission
routes: the promote path is the sole *writer* of the `InvocationEvent`/manifest;
the compile-time cache-hit path becomes a *reader* that replays the recorded
`ProducedFile`s verbatim instead of re-deriving `path`/`slot_id`/`dtype_key`
(which is what produced Bugs B/C/D). The `[[:], file]` empty-index special case in
the hit emission goes away — a replayed hit carries the recorded index.

**Gotchas:** this is the task most able to change telemetry *output*. The change
must be toward the already-correct promote-route shape (the cache-hit route was
the buggy one), so the net effect is the two routes *agreeing*, not new
behaviour. Dynamic intra-slot ancestry is only knowable at runtime — capture it
from the channel `index` at promote time, do not try to predict it at compile
time (that is exactly the archetype trap). Watch the `sibling`/co-emitted-file
case (see `[[lineage_walk_sibling_trap]]`): co-emitted outputs of one multi-slot
invocation are not each other's ancestors.

### T4 — Reconcile consumers onto the record

`CollectResults` (`agents.py`), the telemetry API (`telemetry.py` +
`DataInstanceLibrary` methods), `bootstrap.py`, and `promote.py` should each
obtain identity and ancestry from the one record. Remove paths that reconstruct
provenance independently — the dtype-key string parsing / positional-zip
reconstruction in `bootstrap.py`, the archetype-driven identity fixups that
duplicate what the record already states. Keep the archetype mechanism only where
it is still needed for *codegen* (emitting one representative process per slot);
it must no longer be a *provenance* authority.

**Gotchas:** the archetype collapse is load-bearing for codegen and cannot be
deleted; only its use as an identity source is removed. Ensure
`DataInstanceLibrary.Load(attach_trace=True)` and `walk_ancestors` /
`get_lineage_of` return identical answers before and after (they are the
behaviour contract for provenance). Preserve `legacy_key` resolution for old
serialized libraries.

### T5 — Test alignment + behaviour-parity proof

Capture the pre-refactor baseline first (fast suite + a virtual e2e run's
telemetry summary for a representative pipeline). After each task, re-run and
diff. Tests that assert on the *old internal representation* (the md5 `index`
shape in `tests/audit/`, the dual-route divergence xfails from the quadrant
audit) are updated to the unified model in the same commit that changes it — per
`tests/AGENTS.md`, no compat shims. Tests that assert *observable behaviour*
(results, cache hit/miss in `tests/cache/`, telemetry answers in
`tests/flow/` + `tests/e2e/virtual/`) must pass unchanged.

**Gotchas:** the correct invocation for this worktree is
`PYTHONPATH=$(pwd)/src mamba run -n msm python -m pytest` — the ambient
PYTHONPATH points `import metasmith` at the **dev** worktree via
`/home/tony/lib/locals/metasmith`, so a bare run does not test our code, and
`env -u PYTHONPATH` breaks the `python -m metasmith` subprocess CLI-smoke tests.
Run docker/e2e axes where feasible, but the identity change is fully exercised by
`e2e/virtual` + `flow` without Docker.

## Verification

- **Baseline (before):** `PYTHONPATH=$(pwd)/src mamba run -n msm python -m pytest
  -m fast -q` → record exact pass/skip/xfail counts (currently 378/7/3). Also run
  one `tests/e2e/virtual` pipeline and snapshot `DataInstanceLibrary.Load(...).
  summary()` + `walk_ancestors` output for a multi-step, fan-out pipeline.
- **After each task:** re-run `-m fast`; diff telemetry snapshot. Any diff must be
  explainable as "the two routes now agree" or "an internal-shape test updated,"
  never "a pipeline result or cache decision changed."
- **Identity-on-channel proof:** inspect a generated `workflow.nf` + a run's
  `trace.jsonl` — the strings appearing as `index[name]` values equal the
  `instance_id`s reported by `get_lineage_of`; no `md5`/`Long.parseLong` remains
  in `Orchestrator.groovy` or the given-seed.
- **Record-completeness proof:** for a pipeline with a dynamic intra-slot fan-out
  transform, the persisted `manifest.cbor` / `InvocationEvent` records per-output
  ancestry (non-empty), and `walk_ancestors` on a leaf output reaches the seed
  through the recorded ancestry alone (no `_manifests` fallback — already deleted).
- **Cache-parity proof:** run the same task twice pre- and post-refactor; the
  computed `cache_key`s and the resulting shard layout are byte-identical
  (behaviour of the *existing* cache is untouched this session).
- **Full gate before close:** `-m "fast or e2e_virtual"` green; docker axis
  (`-m e2e_docker`) run if Docker is available.

## Callouts

- **Scope boundary.** "Behaviour unchanged" means pipeline results + cache
  hit/miss decisions + telemetry answers are identical. It does **not** forbid
  flipping currently-`xfail`ed provenance-shape tests to green, nor changing
  internal output *filenames* — those are the cleanup. If a change would alter a
  cache decision or a pipeline result, it is out of scope for this session.
- **This unblocks the next session.** Populating per-output ancestry (T3) and
  unifying identity (T2) are exactly what the deferred compile-time cache-walk
  needs; doing them here as behaviour-neutral cleanup means the caching session
  is additive, not another bridge.
- **Sequencing risk.** T2 (identity on channel) and T3 (ancestry in the record)
  are coupled — T3's runtime ancestry capture reads the T2 channel `index`. Land
  T2 first and prove join-parity before T3.
