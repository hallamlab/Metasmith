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

Default marker: `fast` — all axes run against the virtual runtime, no Docker.

Reuse: `src/metasmith/caching/{keys.py, store.py, promote.py, fs.py}`, the `RunSnapshot` harness, and the linear_3step / parallel_then_group / mixed_cacheability fixtures.
