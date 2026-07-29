# tests/flow

Flow correctness — given a finalized `WorkflowPlan`, does the orchestrator/runtime correctly emit, fork, batch, group, and join data along declared lineage?

**This is NOT solver correctness.** Solver tests live separately (currently `tests/models/test_solver*.py`, slated for `tests/flow/test_solver_*.py` only because they probe planning — but they verify the *choice* of transforms, not the data motion).

## How to use this catalog

Each row is a test case ID. A new flow bug ships a new row in this catalog **before** the fix lands, then a test under `tests/flow/test_<axis>.py` references the ID in its docstring. Verification gate V5b enforces that every catalog ID resolves to at least one pytest node.

All flow tests run against `virtual_runtime` or `contract_runtime` — flow correctness is plan-level semantics, not protocol exec. Real-channel cases (RC*) that genuinely need Nextflow live under `tests/e2e/docker/test_orchestrator_exec.py` and are referenced by ID from here.

Assertions go through the telemetry API on `DataInstanceLibrary.Load(attach_trace=True)` — `get_lineage_of`, `walk_ancestors`, `find_invocations`, `get_siblings_of`. No parsing of `workflow.nf` text, no work-dir filename inspection.

Reuse: `src/metasmith/testing/{virtual_runtime.py, contract_runtime.py, plan_oracle.py, mock_transforms.py}`. Stimulus shapes live in `mock_transforms.py` — never write ad-hoc transforms per test.

---

## Axis 1 — Linear flow (oracle baselines)

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| L1 | Single step round-trips one instance with stable id | `Given → [T1] → Target` | `test_linear.py` |
| L2 | Two-step chain preserves instance_id through both hops | `Given → [T1] → [T2] → Target` | `test_linear.py` |
| L3 | Three-step chain: lineage_of(target) returns 3 ancestors in topological order | `Given → [T1] → [T2] → [T3] → Target` | `test_linear.py` |
| L4 | Single step with N input slots (no parents=); broadcast semantics — output carries all inputs as parents | `Given{A,B} → [T(A,B)] → Target` | `test_linear.py` |

## Axis 2 — Branching (one input → N transforms)

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| B1 | Two-way branch; both consumer outputs list T1's output as parent | `[T1] → {[T2], [T3]}` | `test_branching.py` |
| B2 | Three-way fan-out; lineage_of each consumer shows shared ancestor | `[T1] → {[T2], [T3], [T4]}` | `test_branching.py` |
| B3 | Sibling branches independent — failure in T2 does not abort T3 | `[T1] → {[T2 fail], [T3 ok]}`; assert get_invocation(T3) status=promoted | `test_branching.py` |
| B4 | Nested branching (multi-level DAG); lineage walk reaches root through any path | `[T1] → [T2] → {[T3], [T4]}` | `test_branching.py` |

## Axis 3 — Fan-out (one transform → N output slots)

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| F1 | Two-slot producer; consumers of out1 vs out2 receive distinct instance_ids; both list T1 as parent | `[T(A→{out1,out2})] → {[T2(out1)], [T3(out2)]}` | `test_fan_out.py` |
| F2 | Four-slot producer; per-slot derived_hex is stable across re-runs (cache identity) | `[T(A→{s1..s4})]` | `test_fan_out.py` |
| F3 | Downstream consuming slot N gets the slot-N instance, not slot-(N+1) — slot routing | parametrized over slot index | `test_fan_out.py` |
| F4 | Slot lineage isolation — modifying slot-A's downstream does not leak into slot-B's lineage walk | sibling consumers per slot | `test_fan_out.py` |

## Axis 4 — Batching & group_by

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| G1 | batch_size=1; single group; one execution; output parent=one input | `Given → [T(group_by=k)] → Target` | `test_batching.py` |
| G2 | batch_size=N, N inputs; one execution; output parents = union of N inputs | as G1 with N inputs | `test_batching.py` |
| G3 | Uneven batches (3 inputs, batch_size=2 → 2+1); two executions; per-batch parent unions correct | parametrized | `test_batching.py` |
| G4 | Empty input set; group() receives empty channel; plan compiles, step does not execute, telemetry has no row | `Given∅ → [T(group_by)]` | `test_boundary.py` |
| G5 | Large batch (100 items, batch_size=10); 10 executions; per-batch parents stable under ordering perturbation | parametrized | `test_batching.py` |
| G6 | Group-by under late arrival; out-of-order inputs land in correct batch by group key, not arrival order | parametrized | `test_batching.py` |
| G7 | Incremental emit on parent stream (inbox #16 fix) — non-parent stream buffering until parent closes | the 20-case matrix repro | `tests/e2e/docker/test_group_buffering.py` (relocate) |
| G8 | Duplicate group keys in input; both inputs land in one batch (no false split) | as G2 with collisions | `test_batching.py` |

## Axis 5 — Group then split (collect → unfold)

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| GS1 | Group then immediate AsSamples unfold; per-sample lineage carries the aggregate batch's parent set | `[T1] → [Group] → [AsSamples] → [T2]` | `test_group_to_split.py` |
| GS2 | Each unfolded sample's `parents={batch_member}` set is correct, not the full union | as GS1, assert per-sample parents | `test_group_to_split.py` |
| GS3 | Roundtrip — unfolded sample → downstream → DataInstanceLibrary.Load → get_lineage_of reaches batch ancestors | as GS1 with telemetry assert | `test_group_to_split.py` |

## Axis 6 — Lineage forks via parents=

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| LP1 | Two TargetBuilder.Add(t) calls with parents={A} vs parents={B} produce two distinct plan paths; both fire | pin (relocated repro_135) | `repro/repro_135_duplicate_producer.py` |
| LP2 | Duplicate-type targets with distinct parents (two `gtdbtk` targets, parents={mb_bins} vs {sb_bins}); both produce distinct instance_ids | parametrized | `test_lineage_parents.py` |
| LP3 | walk_ancestors(LP2 target A) excludes anything from LP2 target B's lineage | as LP2 with cross-check | `test_lineage_parents.py` |
| LP4 | Solver rejects impossible lineage (parents={missing_type}) with PlanHint, not silent drop | parametrized | `test_lineage_parents.py` |
| LP5 | WithDType retypes without changing instance_id (pinned in test_data_instance_identity.py); preserve | move from `tests/models/test_data_instance_identity.py` | `test_lineage_parents.py` |

## Axis 7 — Mixed cacheability

(Cache trace shape stays in `tests/cache/test_trace.py`. This axis verifies the FLOW IMPACT of a non-cacheable step in the chain.)

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| C1 | Non-cacheable step between two cacheable steps; downstream cache key correctly includes the re-derived parent id | `[cacheable] → [cacheable=False] → [cacheable]` | `test_cacheability_mix.py` |
| C2 | After re-run with no source change, only the cacheable steps show status=hit; the cacheable=False step shows status=promoted both times | as C1 | `test_cacheability_mix.py` |
| C4 | Batched cacheable=False step; per-batch derived_hex includes no cache-key collisions across re-derives | C1 shape with group_by | `test_cacheability_mix.py` |

## Axis 8 — Concurrency hygiene

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| RC1 | Many parallel upstream items arrive out-of-order; batch assembly is order-independent | virtual_runtime with shuffled emission | `test_concurrency.py` |
| RC2 | Concurrent index modifications during group(); no ConcurrentModificationException, no lost updates | extends RC1 with parallel mutators | `test_concurrency.py` |
| RC3 | Regression pin for inbox #16 (group buffering deadlock); already covered by the 20-case matrix — referenced by ID, not re-implemented | `tests/e2e/docker/test_group_buffering.py` |
| RC4 | Deadlock guard: upstream channel never emits terminal null; group() bounded by C13_DEADLOCK_TIMEOUT_S; already covered by 20-case matrix | `tests/e2e/docker/test_group_buffering.py` |

## Axis 9 — Empty & boundary

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| E1 | Empty input library; plan generation returns hints (no targets reachable) without exception | `Given∅` | `test_boundary.py` |
| E2 | Single-element input; full chain executes; lineage singleton roundtrips | as L2 with 1 input | `test_boundary.py` |
| E3 | 100 parallel branches; correctness + memory under scale (`--durations=10` sentinel under 5s) | parametrized | `test_boundary.py` |
| E4 | Zero outputs from a transform (dead code path); downstream channel is empty, plan does not hang | parametrized | `test_boundary.py` |

## Axis 10 — Lineage roundtrip & serialization

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| S1 | DataInstance.Pack→Unpack preserves instance_id (already covered in `tests/models/test_data_instance_identity.py`; relocate) | unit | `test_lineage_roundtrip.py` |
| S2 | WorkflowPlan.Pack→Unpack preserves step order + dependency_map + instance ids | unit | `test_lineage_roundtrip.py` |
| S3 | get_invocation via trace index returns event by instance_id or slot_id; covered by `test_telemetry_api.py` — relocate | unit | `test_lineage_roundtrip.py` |
| S4 | walk_ancestors traverses N-hop lineage; reaches roots; no cycle, no skip | parametrized with 5-hop DAG | `test_lineage_roundtrip.py` |
| S5 | merged_endpoints remapping during WorkflowPlan.Generate preserves identity through type unifications | pin against `workflow.py:826-834` | `test_lineage_roundtrip.py` |

## Axis 11 — Telemetry surface

Every method on `DataInstanceLibrary` gets one positive + one negative case.

| ID | Method | Positive | Negative |
|---|---|---|---|
| T1 | `find_invocations(transform_key=...)` | known key returns >=1 row | unknown key returns [] |
| T2 | `find_invocations(status="hit")` | hot-cache run returns >=1 row | cold run returns 0 hit rows |
| T3 | `find_failures()` | run with failing_at_slot_k returns the failed row | clean run returns [] |
| T4 | `find_failures(exit_code=>0)` | non-zero exits filtered correctly | exit_code=0 excluded |
| T5 | `get_siblings_of(scope="task")` | all tasks in same step_order returned | isolated task returns just itself |
| T6 | `get_logs_of(instance_id)` | post-promote log bundle has stdout/stderr | missing instance_id returns empty bundle |
| T7 | `summary()` aggregate | counts match sum of events; by_transform/by_status non-empty | empty library returns zeros, no exception |
| T8 | `summary()['by_dtype']` | counts per dtype match events | dtype with zero events absent or zero |

## Axis 12 — Sample splitting (`AsSamples`)

| ID | Invariant | DAG shape | File |
|---|---|---|---|
| AS1 | Parentless index items yield one view each, masking only that item's own subtree | N roots, one child each | `test_as_samples.py` |
| AS2 | Index items sharing any ancestor collapse to a single view over the library | one `meta` above N markers | `test_as_samples.py` |
| AS3 | The per-root descendant walk reaches grandchildren, not just direct children | root → reads → trimmed | `test_as_samples.py` |

---

## Trap cases (historical bug shapes — each gets a `repro/repro_*.py` pin)

| Trap | Source | Pin file |
|---|---|---|
| Duplicate producer via subtype | inbox #135 | `repro/repro_135_duplicate_producer.py` (relocated) |
| Batched lineage paths via container-view | inbox #139 | `repro/repro_139_batched_lineage.py` (relocated) |
| Non-parent stream buffering deadlock | inbox #16 | `repro/repro_16_group_buffering.py` (NEW — extracts the canonical case from `test_orchestrator_group_incremental.py`) |
| Instance ID double-encoding (hex-of-ASCII-hex) | lineage-robustness S4a | `repro/repro_s4a_instance_id_encoding.py` (NEW) |
| merged_endpoints remapping hang | inbox #135 regression class | covered by S5 — no separate pin |
| Slot ID isolation (multi-slot leak) | F4 catalog | covered by F4 — no separate pin |
