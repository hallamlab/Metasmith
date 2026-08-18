# tests/cache

Cache-axis tests against the lineage-addressed task cache (`src/metasmith/caching/`).

One file per axis — no overlap with `tests/e2e/virtual/cache_*` (those are integration smoke).

| File | Axis |
|---|---|
| `test_identity.py` | Cache key derivation: transform_key + sorted input instance_ids → canonical CBOR → blake3 multihash. Encoding stability across Python versions and dict orderings. |
| `test_hit_miss.py` | Probe → miss → execute → promote → probe → hit. Explicit miss-then-hit assertion on the same task across two runs. |
| `test_promote.py` | Lockfile + orphan recovery, atomic `.tmp/ → <key>/` rename, race loser cleanup. |
| `test_trace.py` | trace.jsonl rows for `status ∈ {hit, miss, promoted, fail}`, session_id monotonicity, SessionStart sentinel. |
| `test_cross_task.py` | Within-workflow reuse: two tasks in one plan share a cacheable step; expect single execution, two consumers. |
| `test_cross_workflow.py` | Cross-workflow reuse via `data import-library` (origin="imported") and via shared `<agent_home>/task_cache/` (origin="lineage"). |
| `test_kill_switch.py` | `cacheable=False` per-transform: no cache entry written. `METASMITH_CACHE=0` env: probe short-circuited even with hot cache. Both assert via trace.jsonl. |
| `test_store_meta.py` | `CacheStore.entries` schema, manifest.cbor shape, shard layout `<2>/<rest>`. |
| `test_codegen.py` | What the compiler writes into `workflow.nf`: no plugin block, no `process.cache`, and the coordinate system of a cache hit's synthetic channel (container-rooted) versus its `publishDir` (host-rooted). |
| `test_hit_lineage.py` | The on-channel lineage index across the cache boundary: promote captures the one each output travelled with, and a hit replays it instead of `[:]`. |
| `test_empty_index.py` | The degenerate index `{}` at both ends of that contract — never stored, never trusted. Below the harness: hand-built manifest into the real probe, hand-built work tree into the real promote. |

Default marker: `fast` — all axes run against the virtual runtime, no Docker.

Reuse: `src/metasmith/caching/{keys.py, store.py, promote.py, fs.py}`, `caching/layout.py` for anything that needs to name a shard, the `RunSnapshot` harness, and the linear_3step / parallel_then_group / mixed_cacheability fixtures.

The virtual runtime reimplements the cache-hit path from the store and never reads the generated `workflow.nf`, so anything about the *emitted text* has to be pinned at codegen level — `test_codegen.py` — not through a virtual run.
