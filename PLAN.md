# Orchestrator Channel Deadlock Bug — Reproduction Plan

## Bug Summary

Metasmith's `Orchestrator.groovy` `group()` method deadlocks when combining streams with different cardinalities. Discovered in run `mHAnaWSi` (lib-transcriptomics):

- `stringtie_assemble` produces **9 items** (one per sample, grouped by `k0BWqyxf`)
- `braker3` produces **1 item** (one per experiment, grouped by `Rsoo8XE7`)
- `stringtie_merge` groups both outputs by `Rsoo8XE7` — **never receives inputs**

Nextflow reports "No more task to compute" while downstream processes remain ACTIVE with open ports. The bug is **deterministic** and reproduces on resume.

**Logs & artifacts:** `../lib-transcriptomics/data/mHAnaWSi-results/logs/`

## Root Cause Hypothesis

In the mHAnaWSi workflow, shared streams (`_Rsoo8XE7`, `_rRC4FxHn`) are passed to multiple `group()` calls. Inside `group()`, the stream's Nextflow channel is extracted from the `[name, channel]` tuple and consumed by operators (`.map()`, `.concat()`, `.flatMap()`).

Nextflow DSL2 auto-forks channels used by multiple operators at the DSL level, but this may not extend to channels extracted from data structures inside Groovy method bodies at runtime. The first `group()` call consumes the channel; subsequent calls receive an already-consumed (empty) channel.

For stringtie_merge's `group('Rsoo8XE7', [_Rsoo8XE7, _vyhx3Bb8, _mR9ozKye, _rRC4FxHn], ...)`:
- `by_stream` (`_Rsoo8XE7`'s channel) was consumed by braker3's `group()` → `by_parsed` is empty
- Since `by_parsed` seeds the `inject(by_parsed, combine)` chain, the entire chain produces nothing
- p03 gets an empty input channel → Nextflow completes it with 0 tasks

A secondary factor may be index key mismatch: `_vyhx3Bb8` items lack the `Rsoo8XE7` key in their index (they were produced by p01 which groups by `k0BWqyxf`), so `index["Rsoo8XE7"]` returns `null` in the non-parent buffer path.

## Reproduction Steps

### Step 1: Add test to `tests/integration/test_e2e_orchestrator.py`

Add two tests to `TestOrchestratorGroup`:

#### Test A — Canary: channel reuse across group() calls

Minimal test without processes. Verifies whether a stream passed to two `group()` calls produces output from both:

```groovy
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    ch = Channel.fromList([[[:], file("${projectDir}/a.txt")]])
    def (posted) = o.postIn([ch], ["x"])

    def g1 = o.group("x", [posted], ["t1"], 1)
    def g2 = o.group("x", [posted], ["t2"], 1)

    g1.view { "G1: ${it[0]}" }
    g2.view { "G2: ${it[0]}" }
}
```

**Expected (before fix):** `G1` prints, `G2` does not → confirms channel consumption.

#### Test B — Full DAG reproduction

Replicates the mHAnaWSi topology with 3 stub processes:

```
p01 (9x, groups by "sample") ──→ p03 (1x, groups by "exp")
p02 (1x, groups by "exp")   ──→ p03
```

- 9 sample files, 1 experiment, 1 container, 1 assembly
- `child2parent["sample"] = ["exp"]`
- `posted_exp` and `posted_container` passed to TWO group() calls
- Asserts p03 produces exactly 1 output

**Expected (before fix):** p03 produces 0 outputs (deadlock). Test fails.

### Step 2: Run the tests

```bash
cd /home/tony/workspace/msm/orchestrator-deadlock-fix
pytest tests/integration/test_e2e_orchestrator.py::TestOrchestratorGroup::test_channel_reuse_across_group_calls -v -s
pytest tests/integration/test_e2e_orchestrator.py::TestOrchestratorGroup::test_group_deadlocks_when_stream_reused_across_groups -v -s
```

### Step 3: Verify existing tests still pass

```bash
pytest tests/integration/test_e2e_orchestrator.py -v -s
```

## Critical Files

| File | Purpose |
|------|---------|
| `src/metasmith/nextflow_config/Orchestrator.groovy:147-260` | `group()` method — bug location |
| `tests/integration/test_e2e_orchestrator.py` | Add reproduction tests here |
| `tests/integration/conftest.py` | `nxf_runner` and `docker_image` fixtures |
| `../lib-transcriptomics/data/mHAnaWSi-results/logs/workflow.nf` | Real failing workflow (reference) |

## Fix Direction (after reproduction)

The fix likely needs to ensure channels are forked before being consumed inside `group()`. Options:
1. Call Nextflow's `.tap()` or explicitly fork channels before extracting from tuples
2. Restructure `group()` to not consume channels directly — instead collect all items first
3. Use Nextflow's `multiMap` to create independent copies of shared streams
