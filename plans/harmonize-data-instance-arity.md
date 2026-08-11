# Plan: Harmonize DataInstance/Endpoint/Dependency Relationships

## Full Object Lifecycle (Current State)

### The chain: Dependency → Endpoint → DataInstance

```
Dependency (solver concept — what a transform needs/produces, STABLE identity)
    ↓ maps to (via Application.used / Application.produced)
Endpoint (type concept — properties + parent lineage, MUTATES during solving/merging)
    ↓ instantiated as
DataInstance (runtime concept — a file on disk with a type, HASH CHANGES when dtype mutates)
```

### How it flows through the system:

**Stage 1 — User input**: `DataInstanceLibrary.Get()` creates DataInstances with Endpoints from the type library. Endpoint parents come from library parent metadata.

**Stage 2 — Planning**: `WorkflowPlan.Generate()` passes Endpoints to the solver, which produces Applications mapping `Dependency → Endpoint`. After solving, `Generate()` mutates DataInstance.dtype (line 317: `oe.dtype = e; oe.RecalculateKey()`) to match solver-remapped Endpoints. Then builds `WorkflowStep` with THREE redundant representations of the same Dependency→DataInstance mapping.

**Stage 3 — Nextflow codegen**: `PrepareNextflow()` uses `get_archetype()` to pick one representative DataInstance per Dependency (needed because branch merging creates multiple instances per dependency). Writes `dtype.key` strings into `.command.metadata`.

**Stage 4 — Bootstrap execution**: Loads the serialized task, then RECONSTRUCTS the Dependency→Endpoint→DataInstance mapping from scratch by parsing comma-separated `dtype.key` strings from metadata and filtering `step.uses` by key match. This bypasses `dependency_map` entirely.

**Stage 5 — Result compilation**: `agents.py` builds `k2inst: dict[str, DataInstance]` mapping `dtype.key → DataInstance` with last-write-wins semantics. Used to look up instances when building output library.

### Where relationships break down:

| Location | Representation | Problems |
|----------|---------------|----------|
| `WorkflowStep.uses` | `list[DataInstance]` (flat) | Loses Dependency association, ambiguous after merging |
| `WorkflowStep.produces` | `list[list[DataInstance]]` (nested by branch) | Loses Dependency within each branch |
| `WorkflowStep.dependency_map` | `dict[Dependency, list[DataInstance]]` | **The actual source of truth** — but ignored by bootstrap |
| `instance_map` in Generate() | `dict[Endpoint, set[DataInstance]]` | Sets cause ordering instability |
| `bootstrap input_map` | `dict[Endpoint, list[DataInstance]]` | Reconstructed from strings, positional zip against requires |
| `bootstrap dep2output` | `list[dict[Dependency, Endpoint]]` | Has bug at line 145 (uses wrong `e`) |
| `agents k2inst` | `dict[str, DataInstance]` | Last-write-wins when multiple instances share dtype.key |

## Problems (Summary)

1. **Three redundant representations** on `WorkflowStep`: `uses`, `produces`, and `dependency_map`
2. **Ad-hoc container type switching**: `set` in Generate(), `list` in dependency_map, `list[list]` in produces
3. **Brittle bootstrap reconstruction** (`bootstrap.py:129-150`): rebuilds Dependency→DataInstance from dtype.key strings, bypassing dependency_map
4. **Bug**: `bootstrap.py:145` uses `e` from wrong scope (DataInstance from comprehension, not Endpoint)
5. **Duplicated warn-and-take-first** for group_by at `bootstrap.py:122-126` and `workflow.py:927-931`
6. **Archetype pattern is load-bearing but undocumented** — correct behavior for solver branch merging, needs formalization
7. **`agents.py k2inst` last-write-wins** — when merged branches produce instances with same dtype.key, only one survives

## Harmonized Design

**Core principle: Dependency is the stable key.** Unlike Endpoint (mutates during solving/merging) or DataInstance (hash changes when dtype mutates), Dependency objects are created once during Transform definition and never change. The entire system should use Dependency as the primary lookup key.

### What changes

**`dependency_map` becomes the sole stored field** on WorkflowStep. `uses` and `produces` become derived properties that read from `dependency_map` using the ordering defined by `transform.model.requires` and `transform.model.produces`.

**Bootstrap reads dependency_map directly** instead of reconstructing it from string metadata. The metadata file emits Dependency keys alongside dtype keys, and bootstrap looks up `step.dependency_map[dep]` directly.

**`agents.py k2inst` uses Dependency key** instead of dtype.key to avoid last-write-wins collisions after branch merging.

## Implementation Plan

### Phase 0: Create comprehensive test suite (run before AND after refactor)

**Goal**: Capture current behavior of all data structures being refactored. Run before changes to confirm tests pass, then after each phase to verify no regressions.

**Test file**: `tests/models/test_workflow.py`

**Existing infrastructure** (all 51 tests pass as baseline):
- `tests/conftest.py` — `make_transform` and `make_endpoint` factory fixtures
- `tests/models/test_solver.py` — 13 solver tests including branching and lineage
- `tests/models/test_libraries.py` — 38 tests for DataInstanceLibrary save/load, trace, rename
- Run with: `conda run -n metasmith-dev python -m pytest tests/ -v`

**Fixtures needed** (extend existing conftest.py):

```python
@pytest.fixture
def temp_lib(tmp_path):
    """Creates a minimal DataInstanceLibrary for test DataInstances."""
    ...

@pytest.fixture
def make_workflow_step():
    """Factory to create WorkflowStep with proper dependency_map from a Transform and instances."""
    ...
```

**Test categories:**

1. **DataInstance identity and hashing**
   - `test_data_instance_hash_deterministic` — same path/dtype/name → same hash
   - `test_data_instance_hash_differs_by_path` — different path → different hash
   - `test_data_instance_hash_differs_by_dtype` — different endpoint → different hash
   - `test_data_instance_recalculate_key` — mutating dtype then recalculating updates hash
   - `test_data_instance_pack_unpack_roundtrip` — Pack() → Unpack() preserves fields

2. **WorkflowStep consistency invariants** (the core of what we're refactoring)
   - `test_uses_matches_dependency_map_requires` — `step.uses` contains exactly the instances from `dependency_map[d]` for each `d in transform.model.requires`
   - `test_produces_matches_dependency_map_produces` — `step.produces[i]` contains exactly the instances from `dependency_map[d]` for each `d in transform.model.produces[i]`
   - `test_dependency_map_is_superset` — every instance in uses+produces appears in dependency_map
   - `test_workflow_step_pack_unpack_roundtrip` — Pack() → Unpack() preserves all fields
   - `test_dependency_map_keys_cover_all_deps` — dependency_map has entries for all requires + all produces deps

3. **WorkflowPlan.Generate() end-to-end** (adapted from branching_test.py patterns)
   - `test_generate_simple_linear` — A→B→C pipeline, verify step structure
   - `test_generate_with_branching` — branch + merge, verify produces grouping
   - `test_generate_with_multiple_given` — multiple input samples, verify instance_map cardinality
   - `test_generate_pack_unpack_roundtrip` — Generate → Pack → Save → load → Unpack → compare

4. **Archetype behavior** (captures the solver branch-merge semantics)
   - `test_archetype_consistent_across_steps` — when solver merges branches, same archetype chosen for same instances across different steps
   - `test_archetype_memoized` — get_archetype returns same representative for same candidate set
   - `test_archetype_picks_previously_seen` — if instance A was memoized, and later [A, C] is queried, returns A

5. **Group-by extraction**
   - `test_group_by_single_dtype` — dependency_map[group_by] has single dtype → clean extraction
   - `test_group_by_archetype_selection` — first instance is selected as archetype

6. **Bootstrap reconstruction** (captures the fragile behavior we're replacing)
   - `test_input_map_reconstruction` — given step.uses and dtype.key strings, verify reconstruction matches dependency_map
   - `test_output_map_reconstruction` — same for outputs with grouping

**How to run:**
```bash
conda run -n metasmith-dev python -m pytest tests/ -v
```

**Files**: `tests/models/test_workflow.py`, `tests/conftest.py` (extend existing)

### Phase 1: Make `dependency_map` the sole stored field, derive `uses`/`produces`

**Goal**: Eliminate the three redundant representations. `dependency_map` is the source of truth; `uses` and `produces` become computed properties.

**Changes to `WorkflowStep`** (`workflow.py:25-70`):
- Remove `uses` and `produces` from `__init__` fields
- Add `@property` methods that derive from `dependency_map` + `transform.model.requires`/`transform.model.produces`:
  ```python
  @property
  def uses(self) -> list[DataInstance]:
      return [inst for d in self.transform.model.requires for inst in self.dependency_map.get(d, [])]

  @property
  def produces(self) -> list[list[DataInstance]]:
      return [[inst for d in group for inst in self.dependency_map.get(d, [])]
              for group in self.transform.model.produces]
  ```
- Add `group_by_instances` property: `self.dependency_map[self.transform.group_by]`
- Update `Pack()` to call the properties (serialization format unchanged)
- Update `Unpack()` to only populate `dependency_map` (invert `_resolve_dependency_map` — instead of building dep_map from uses/produces, build dep_map directly from packed dep keys, then uses/produces are derived)
- Simplify construction at `workflow.py:396-403` to only build `dependency_map`

**Changes to `WorkflowPlan.Unpack()`** (`workflow.py:199-210`):
- The current `_unpack_step` iterates over `step.uses` and `step.produces` to fix dtypes. Since these are now properties, the dtype fixup needs to happen on the DataInstances stored in `dependency_map` directly. Change to iterate `step.dependency_map.values()`.

**Files**: `src/metasmith/models/workflow.py`

### Phase 2: Normalize `instance_map` container type in Generate()

**Goal**: Eliminate set↔list conversions during workflow generation.

**Changes** (`workflow.py:356-400`):
- Change `instance_map` from `dict[Endpoint, set[DataInstance]]` to `dict[Endpoint, list[DataInstance]]`
- Replace set union at line 387 with list append + dedup check
- Remove `list()` conversions when building `dependency_map`

**Files**: `src/metasmith/models/workflow.py`

### Phase 3: Formalize the archetype pattern

**Goal**: The archetype pattern handles solver branch merging correctly — after merging, `dependency_map[d]` may contain multiple DataInstances from different branches, but Nextflow codegen needs one representative. The `get_archetype()` closure with memoization (`_archetypes` dict) ensures consistency across steps sharing merged instances. Formalize this rather than keeping it as an ad-hoc closure.

**Changes**:
- Create `ArchetypeRegistry` class that encapsulates the memoization dict and selection logic:
  ```python
  class ArchetypeRegistry:
      def __init__(self):
          self._memo: dict[DataInstance, DataInstance] = {}
      def get(self, candidates: list[DataInstance]) -> DataInstance:
          # Same logic as current get_archetype()
          ...
  ```
- `workflow.py:656-685`: Replace `get_archetype()` closure with `ArchetypeRegistry` instance
- `workflow.py:927-931`: Replace inline group_by warn-and-take-first with `step.group_by_instances[0]` (the property from Phase 1)
- `bootstrap.py:122-126`: Same replacement
- Add a centralized validation in `WorkflowStep.__post_init__` that warns if group_by dependency maps to instances with multiple distinct dtype.keys

**Files**: `src/metasmith/models/workflow.py`, `src/metasmith/bootstrap.py`

### Phase 4: Thread Dependency keys through metadata to eliminate bootstrap reconstruction

**Goal**: Bootstrap currently reconstructs `Dependency → Endpoint → DataInstance` from dtype.key strings by filtering `step.uses`. This is fragile and buggy (line 145). Instead, emit Dependency keys in metadata and look up `step.dependency_map` directly.

**Changes to metadata emission** (`workflow.py:756-757` in `prepare_step()`):
- Add a metadata line mapping Dependency keys to their position in the archetype lists:
  ```python
  f'echo "dep {json.dumps({d.key: e.dtype.key for d, e in zip(transform.model.requires, used_archetypes)})}" >>{METADATA_FILE}'
  ```
  (And similar for output deps)

**Changes to bootstrap consumption** (`bootstrap.py:129-150`):
- Replace the entire string-split-and-filter block with:
  ```python
  dep_meta = json.loads(raw_meta["dep"])
  dep_by_key = {d.key: d for d in step.dependency_map}
  input2dep = {dep_by_key[k]: dep_by_key[k] for k in dep_meta if dep_by_key[k] in step.transform.model.requires}
  # dep2output derived similarly
  ```
- The `alldep2output` dict becomes a simple read from `dependency_map` — each Dependency's Endpoint is `step.dependency_map[d][0].dtype`
- This fixes the bug at line 145 and eliminates the fragile positional zip at line 134

**Files**: `src/metasmith/models/workflow.py`, `src/metasmith/bootstrap.py`

### Phase 5: Fix `agents.py` last-write-wins collision

**Goal**: `k2inst: dict[str, DataInstance]` uses `dtype.key` as lookup key, but after branch merging, multiple instances may share the same `dtype.key`. Only the last one survives.

**Changes** (`agents.py:804-810`):
- Key by `(dtype.key, dependency.key)` tuple or by `DataInstance._key` (which is unique per instance)
- Or accumulate lists: `k2inst: dict[str, list[DataInstance]]` and handle the lookup appropriately
- Verify downstream usage at lines 850-860 handles the multi-instance case

**Files**: `src/metasmith/agents.py`

### Phase 6 (Optional, highest risk): Separate type identity from lineage on DataInstance

**Goal**: Make explicit that `DataInstance` identity should be stable across lineage changes.

Currently `DataInstance.RecalculateKey()` hashes `path + dtype.key + dtype_name`. Since `dtype.key` includes parent lineage (solver.py line 59), mutating `dtype` (as done at workflow.py:317) changes the instance's hash. This means any set/dict keyed by DataInstance before the mutation is stale.

**Proposed**:
- Add `type_key` property that hashes properties-only (ignoring parents)
- Use `type_key` in `RecalculateKey` instead of `dtype.key`
- Or: stop mutating `dtype` on existing instances — create new instances instead

**Risk**: This touches solver integration, serialization round-trips, and Nextflow caching. Must be thoroughly tested.

**Files**: `src/metasmith/models/libraries.py`

## Critical Files

| File | What Changes |
|------|-------------|
| `src/metasmith/models/workflow.py` | WorkflowStep restructuring (Phase 1), instance_map normalization (Phase 2), archetype formalization (Phase 3), metadata emission (Phase 4) |
| `src/metasmith/bootstrap.py` | Bootstrap reconstruction replacement (Phase 4), group_by cleanup (Phase 3) |
| `src/metasmith/agents.py` | k2inst collision fix (Phase 5) |
| `src/metasmith/models/libraries.py` | (Phase 6 only) DataInstance identity separation |

## Existing Code to Reuse

- `WorkflowStep._resolve_dependency_map()` (`workflow.py:65-70`) — already reconstructs dependency_map from uses/produces during Unpack; this logic validates the approach but needs INVERSION (build dep_map directly, derive uses/produces)
- `Transform.requires` and `Transform.produces` (`solver.py:173-177`) — define the canonical ordering for derived properties
- `KeyGenerator` (`hashing.py`) — existing hash infrastructure

## Commit Tracking

| Phase | Commit | Status | Notes |
|-------|--------|--------|-------|
| Phase 0 (tests) | `1589347` | done | `tests/models/test_workflow.py` baseline landed in C8 part 1 of `feat/lineage-robustness`; 6 originally-listed categories dropped with explicit justification (see `plans/ok-sketch-this-out-magical-koala.md` G7). |
| Phase 1 (dependency_map sole field) | `36c4a43` | done | A2 revised: `dependency_map` is now a property with a setter that always calls `RefreshViews()`. Drift is structurally impossible — there is no field to bypass. Pack()-time drift assertion dropped (was redundant under property model). |
| Phase 2 (instance_map normalization) | `ec1ece4` | done | Landed on `feat/arch-cleanup`. |
| Phase 3 (archetype formalization) | `36c4a43` | done | A3: closure at `workflow.py:1406-1415` preserved with rationale docstring. Extraction was net +lines for no behavioral gain. |
| Phase 4 (bootstrap dependency keys) | `ec1ece4` | done | Landed on `feat/arch-cleanup`. Further dep-keyed FILES + sar/par preflight landed in `dc8dd6c` (`feat/lineage-robustness` C5). |
| Phase 5 (agents k2inst fix) | `ec1ece4` | done | Landed on `feat/arch-cleanup`. |
| Phase 6 (DataInstance identity) | `ec1ece4`, `c7b60e3` | done | Two-source identity landed in `ec1ece4`; `file_instance_id` minting via `LinPayload.mint_file_id(slot_id, relative_path)` landed in `c7b60e3` (C6). |

### Lineage-robustness scope (this commit chain)

| Commit | Subject |
|--------|---------|
| C1 dataclasses | `LinPayload`, `InvocationEvent`, `SessionStart`, `LineageNode`, `LogBundle` in `src/metasmith/models/lineage.py`. |
| C2 cache versions | `LIN_PAYLOAD_VERSION`, `SHARD_LAYOUT_VERSION`, `trace_session_counter` rows in `caching/store.py`; `CacheStore.allocate_session_id()`. |
| C3 (`36c4a43`) | A2 dependency_map property+setter + A3 archetype docstring + `test_workflow.py` setter test. |
| C4 (`9f8f168`) | Slot-only lin payload v2 emit (`workflow.py:1573`). |
| C5 (`dc8dd6c`) | Bootstrap parses `LinPayload`, dep-keyed FILES, sar/par preflight, hard-raise on missing required input. |
| C6 (`c7b60e3`) | CollectResults mints `file_instance_id` at manifest-augmentation site. |
| C7 (`96c049d`) | trace.jsonl rotation + SessionStart sentinel + v2 hit/promoted emit; `promote.py` v2 emission; `test_cache_trace.py` v2 shape assertions. |
| C8 part 1 (`1589347`) | Phase 0 baseline tests + `_find_step_logs` + `.command.*` capture under `<shard>/logs/`. |
| C8c (`5fcd98a`) | `DataInstanceLibrary` telemetry API + `Load(attach_trace=True)` + `python_api` re-exports. |
| C8 status v2 (`cec6040`) | `status_for_run` tolerates v2 rows + SessionStart sentinel. |
| C8d (`e98d3f8`) | `tests/integration/test_telemetry_e2e.py` — G5/G6 acceptance gate. |

## Verification Strategy: Test Before, Test After

### Before any refactor (Phase 0):
1. Existing baseline: 51 tests pass (`conda run -n metasmith-dev python -m pytest tests/ -v` — confirmed passing)
2. Write new tests in `tests/models/test_workflow.py` targeting WorkflowStep/WorkflowPlan invariants
3. Run full suite — all tests (existing + new) must pass against current code
4. This baseline proves our new tests accurately capture current behavior

### After each refactor phase:
1. Run `conda run -n metasmith-dev python -m pytest tests/ -v` — all tests must still pass
2. For Phase 4 (metadata changes): also verify a WorkflowPlan.GenerateNextflow() output matches expected format

### Final integration:
1. Generate a workflow plan using the `local_mock` transforms and verify DAG renders identically
2. Pack → Save → Load → Unpack round-trip on the generated plan
