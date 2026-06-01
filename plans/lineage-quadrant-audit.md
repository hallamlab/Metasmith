# Lineage Quadrant Audit (S0)

**Status:** initial pass — virtual_runtime quadrants observed empirically; docker quadrants observed via session #268 failure forensics. Re-run probe and update before any code commit (G7).

**Probe:** `tests/audit/test_quadrant_probe.py` (virtual_runtime, `linear_3step` n=1 cold then warm). Raw artifacts at `/tmp/quadrant_audit_data/`.

**Purpose:** ground every future commit in observed shape, not inferred shape. Plan `alright-implement-your-suggestions-clever-hummingbird.md` rests on this doc.

## Observed shapes — `InvocationEvent` per quadrant

Trace events from `_metasmith/trace.jsonl` (the canonical lineage trace, *not* `virtual_runtime.trace.jsonl` which is debug-only — virtual_runtime.py:75).

| Field | Q1 — virtual_runtime / cache-hit | Q2 — virtual_runtime / promote | Q3 — docker / cache-hit | Q4 — docker / promote |
|---|---|---|---|---|
| **emitter** | `workflow.py` `_compute_cache_decisions` ~line 1412-1498 | `promote.py` `_append_invocation_event_v2` line 298-383 | (same as Q1 in principle) | (same as Q2 in principle) |
| **observed?** | ✅ probe run | ✅ probe run | ❌ — sentinel only | ❌ — sentinel only |
| **count for `linear_3step` n=1** | 3 (1 per step) | 3 (1 per step) | 0 (only `SessionStart`) | 0 (only `SessionStart`) |
| **`status`** | `"hit"` | `"promoted"` | n/a | n/a |
| **`step_order`** | 1, 2, 3 | 1, 2, 3 | n/a | n/a |
| **`step_name`** | populated (`"trA"`, `"trB"`, `"trC"`) | **missing** | n/a | n/a |
| **`consumes` shape** | `dict[dep_key, list[hex_string]]` — same key shape as Q2 | `dict[dep_key, list[hex_string]]` | n/a | n/a |
| **`consumes` value encoding** | hex-encoded ASCII (double-encoded) — see "Bug A" | hex-encoded ASCII for step1 (130-hex), short-id for steps 2-3 (20-hex) — mixed | n/a | n/a |
| **`produces[].file_instance_id`** | `1e204d1ac0...` (matches Q2 cold-run output for same task) | `1e20fd2a00...` | n/a | n/a |
| **`produces[].slot_id`** | **equals `file_instance_id`** (degenerate; bug C) | distinct from `file_instance_id` (correct) | n/a | n/a |
| **`produces[].path`** | `""` (empty) — Bug B | `"out/1-1-1.dbxURmcWuJBboYMN-IA33yeXE"` (populated) | n/a | n/a |
| **`produces[].dtype_key`** | **wrong** — set to the downstream consumer's dep_key, not the producer's dtype.key (Bug D) | correct (`"IA33yeXE"`, etc.) | n/a | n/a |
| **single event per…** | step (aggregates batches; only 1 batch in this fixture so indistinguishable) | step (aggregates batches) | — | — |

Docker quadrants Q3/Q4 are empty per session #268: `run_stub_workflow` in `tests/integration/test_e2e_trace.py:65-160` shells out to `nextflow` directly, never calls `RunWorkflow`, so `_compute_cache_decisions` never runs and `promote_run` is never invoked. The `_metasmith/trace.jsonl` for a docker-stub run contains only the `SessionStart` sentinel. Docker-stub lineage today rides entirely on `_manifests/*.json` (the C2 deletion target).

## Raw samples

Cold-run promote event (Q2, step 1 of `linear_3step`):
```json
{"schema_version":2,"session_id":1,
 "task_hash":"1e204d99ae52a20fbcf03ec3e282c41c70b9f0b0216425489fc1f0958b1ae7432fef",
 "transform_key":"KnH9Y7Nt","status":"promoted",
 "consumes":{"X6Ha2P0i":["3165323035396234656664653262333239623565386431376433663032613735303361613165333666666534333437396230363262326562623632316564363566303263"]},
 "produces":[{"file_instance_id":"1e20fd2a00390ab26523b4442fe9500f27166fa67cdc5f48a52cdcda61fcf5f21077",
              "slot_id":"1e204d1ac07852cd0682d1bda83d66ef2a01a4f4ccd296a32986562868f0e338e42c",
              "path":"out/1-1-1.dbxURmcWuJBboYMN-IA33yeXE","dtype_key":"IA33yeXE"}],
 "step_order":1,"cache_key":"1e204d99ae52a20fbcf03ec3e282c41c70b9f0b0216425489fc1f0958b1ae7432fef"}
```

Warm-run hit event (Q1, step 1 of `linear_3step`, replay of same task):
```json
{"schema_version":2,"session_id":1,
 "task_hash":"1e204d99ae52a20fbcf03ec3e282c41c70b9f0b0216425489fc1f0958b1ae7432fef",
 "transform_key":"KnH9Y7Nt","status":"hit",
 "consumes":{"X6Ha2P0i":["3165323035396234656664653262333239623565386431376433663032613735303361613165333666666534333437396230363262326562623632316564363566303263"]},
 "produces":[{"file_instance_id":"1e204d1ac07852cd0682d1bda83d66ef2a01a4f4ccd296a32986562868f0e338e42c",
              "slot_id":"1e204d1ac07852cd0682d1bda83d66ef2a01a4f4ccd296a32986562868f0e338e42c",
              "path":"","dtype_key":"MbSRYjOi"}],
 "step_order":1,"step_name":"trA","cache_key":"1e204d99ae52a20fbcf03ec3e282c41c70b9f0b0216425489fc1f0958b1ae7432fef"}
```

Sample `_manifests/*.json` (Q1/Q2, end-of-pipeline target `step_c_target`):
```json
[["{\"X6Ha2P0i\":[989996440514987767],\"IA33yeXE\":[861278607962411109],
   \"iw83ywNQ\":[412990003688270277],\"XdZmyf4d\":[806627834803370650]}",
   "/abs/path/to/results/step_c_target/0001_1-1-1.oIXQp2l9WzsyTB3F-XdZmyf4d"]]
```
Manifest is a list of `[lin_string_json, path]` pairs. `lin_string` carries *transitively-accumulated* ancestry (every ancestor dtype_key → list of MD5-15-truncated path hashes). This is the load-bearing structure C2 deletes.

## Divergences from `ok-sketch-this-out-magical-koala.md` assumptions

The original plan was constructed against the cache-hit-route shape in workflow.py. The audit shows that shape diverges from observed reality in three places that the plan did not anticipate.

### Bug A — `consumes` is hex-of-ASCII, not just hex (**still latent post-C0-amend**)

Plan asserts (`ok-sketch-this-out-magical-koala.md:20-22`): "C0-amend split `sorted_inputs` into `list[tuple[str, list[str]]]`, derive the `+`-joined byte form locally only for `lineage_key`."

Observed: `consumes["X6Ha2P0i"][0]` decodes via `bytes.fromhex(v).decode()` to the ASCII string `"1e2059b4efde2b329b5e8d17d3f02a7503aa1e36ffe43479b062b2ebb621ed65f02c"` — which is itself an iid hex string. So the value is *hex-of-hex*: one extra encoding layer the plan didn't see. C0-amend (`f1aa831`) fixed the in-memory dataclass to be `list[str]`, but the actual strings stored are still the legacy `iid_bytes.hex()` form where `iid_bytes` was an ASCII-encoded hex string.

**Sites to inspect:** `src/metasmith/models/workflow.py:~1279-1290` (`+`-joined sorted_inputs construction) and `src/metasmith/caching/promote.py:128-131` (the parser fallback `v.split("+") if v else []` — still in code, still hit by step 1).

**Impact:** the BFS in `agents.py` (C1, `fa5af0c`) walks `consumes` looking for parent `file_instance_id`s. With the double-encoding, no lookup ever matches. The C1 fallback to `_manifests/*.json` is currently masking this.

### Bug B — cache-hit route emits `produces[].path = ""` (**C0.5 only fixed promote**)

Plan asserts (`ok-sketch-this-out-magical-koala.md:22-23`): "**C0.5** populates paths by … (iv) cache-hit emission reads `entry.payload` via `decode_manifest`".

Observed: Q1 warm trace has every `produces[].path == ""`. Q2 cold trace has populated paths. C0.5 (`72e1715`) shipped the promote-side enrichment; the cache-hit `decode_manifest` read is either missing or not propagating the path field.

**Site to inspect:** `src/metasmith/models/workflow.py:1412-1498` — the cache-hit emission. The plan said it would read from `entry.payload`; verify whether `decode_manifest` is called and whether `output_files[i]["relpath"]` is used to populate `ProducedFile.path`.

**Impact:** post-C2 (post-_manifests deletion), the BFS keyspace alignment in `agents.py` C1 depends on `pf.path` being non-empty. Cache-hit events silently won't be addressable.

### Bug C — cache-hit route emits `slot_id == file_instance_id` (**degenerate**)

Observed: Q1 every `produces[]` has `slot_id` byte-equal to `file_instance_id`. Q2 they're distinct (slot_id is the slot's identity, file_instance_id is `LinPayload.mint_file_id(slot_id, relpath)`).

**Site to inspect:** `src/metasmith/models/workflow.py:~1438-1477` — wherever cache-hit constructs `ProducedFile`. The mint should be `mint_file_id(slot_id, relpath)`; currently it appears to pass `slot_id` itself.

**Impact:** `TraceIndex.by_slot` works (slot_id distinct across steps); `TraceIndex.by_file` collapses. Per-file BFS edges break on warm runs.

### Bug D — cache-hit `produces[].dtype_key` is the downstream **consumer's** dep_key, not the **producer's** dtype.key

Observed: Q2 cold step 1 produces `dtype_key: "IA33yeXE"` (the actual step_a dtype). Q1 warm step 1 produces `dtype_key: "MbSRYjOi"` — which appears in step 2's *consumes*, not step 1's actual output type. Steps 2 and 3 show the same shift.

**Site to inspect:** same as Bug B — cache-hit emission. Whatever it reads to populate `dtype_key` is reading the consumer-side metadata.

**Impact:** any downstream walk that filters by `dtype_key` (e.g., `lib.Trace("step_c", "seed")`) misclassifies cache-hit events; the per-file output graph is wrong on warm runs.

### Bug E — `step_name` only on cache-hit, missing on promote

Observed: Q1 events carry `step_name` ("trA"); Q2 events don't. Cosmetic but symptomatic of route divergence — both should emit identical schema.

### Bug F — docker quadrants are empty (**known from session #268**)

Q3 and Q4 contain only the `SessionStart` sentinel. The C2 plan's S5 (docker stub invokes `promote_run`) covers this; the audit just confirms the empty state.

## Open invariants — pinned by red tests (G7 stop-work list)

The next commit in the chain may not land until the **next-blocking invariant** below has a red test in the suite. Tests live in `tests/integration/test_telemetry_e2e.py` unless noted.

| # | Invariant | Blocks | Test that pins it (S2) | Closes bug |
|---|---|---|---|---|
| I1 | Every `consumes[k][i]` parses as a 32-byte hex string (no double-encoding). | S4 emission | `test_consumes_values_parse_as_hex_lists` | Bug A |
| I2 | Every `produces[].path` is non-empty on both routes. | S4 emission | `test_produced_files_have_nonempty_path` | Bug B |
| I3 | On cache-hit, `produces[].slot_id != produces[].file_instance_id` for tasks whose mint-by-path applies. | S4 emission | `test_cache_hit_file_id_minted_from_path` | Bug C |
| I4 | `produces[].dtype_key` equals the producing slot's dtype.key (matches Q2). | S4 emission | `test_dtype_key_matches_producer_not_consumer` | Bug D |
| I5 | `step_name` is populated on every promote/hit event. | S4 emission | `test_step_name_populated_on_all_routes` | Bug E |
| I6 | For `linear_3step(n_samples=N)`, `len(non-sentinel events) == n_steps * N` (one per *batch*, not per step). | S4 emission | `test_invocation_event_is_one_per_batch_not_per_step` | C2 structural |
| I7 | For `linear_3step(n_samples=N)`, `lib.Trace("step_c", "seed")` yields exactly N pairs. | S4 emission | `test_linear_3step_n_samples_yields_exactly_n_parent_pairs` | C2 structural |
| I8 | Docker-stub `trace.jsonl` contains ≥ `n_steps * n_samples` non-sentinel events. | S5 plumbing | `test_docker_stub_trace_has_promote_events` (in `test_e2e_trace.py`) | Bug F |
| I9 | `_manifests/*.json` directory is empty post-run. | S6 deletion | `test_no_manifests_dir_after_run` | C2 deletion |
| I10 | `given.csv` is at `<output_path>/given.csv` with 4-col schema. | S6 deletion | `test_given_csv_new_location_and_schema` | C2 deletion |

**Originally five (G3). Audit expanded to ten.** Bugs A/B/C/D/E were not surfaced by previous sessions because no probe ever dumped Q1's full event shape and compared it field-by-field to Q2. The plan's S2 was scoped to "five tests"; revising upward to ten with the bugs uncovered here.

The plan's S2 should be expanded to cover I1–I8 as red tests before any S4 emission code is written. I9 and I10 are added at S6.

## How to refresh this audit

```bash
mamba run -n msm pytest tests/audit/test_quadrant_probe.py -s -p no:cacheprovider --tb=short
# then re-copy:
cp /tmp/claude-1000/pytest-of-tony/pytest-latest/test_dump_virtual_runtime_quad0/virtual_rt/agent_home/runs/*/_metasmith/trace.{,1.}jsonl /tmp/quadrant_audit_data/
```

For docker quadrants Q3/Q4: when S5 lands (promote_run invoked from docker stub), extend the probe to also drive `run_stub_workflow` and populate the Q3/Q4 columns. Until then they remain empty by design.

## Plan delta

The audit changes the C2-completion plan in three ways:

1. **S2 grows from 5 tests to ~10 tests.** Bugs A–E need their own red tests *before* S4 emission code can be modified. Currently they're masked by the `_manifests/*.json` fallback.
2. **S4 emission work is wider than "loop over batches".** It must also: (a) double-decode-fix `consumes` values, (b) populate `path` on cache-hit, (c) mint per-file IDs distinct from slot_id on cache-hit, (d) read `dtype_key` from the producer side, (e) populate `step_name` on promote.
3. **S0 (this doc) becomes a per-commit obligation.** Any commit that modifies emission MUST re-run the probe and update this table in the same commit.
