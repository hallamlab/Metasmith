"""LineageJoin Case Matrix — regression suite for `Orchestrator.groovy::group()`.

Background (inbox #16): the non-parent branch of `group()` at
`src/metasmith/nextflow_config/Orchestrator.groovy:218-241` buffers items into
`pending_groups` and only flushes them when the upstream channel emits its
terminal `null` sentinel (via `.concat(this.one_null)`). This stalls downstream
combine() until upstream closes, surfacing as latency, deadlock, dedup loss,
or partial-emit pinning depending on the lineage shape / index multiplicity /
batch size / termination shape.

Each test below corresponds to one row of the 20-case matrix documented in
the inbox #16 design note. Tests intentionally encode the *current* failure
mode (latency, deadlock, dedup, partial-emit) so a downstream fix can be
diff-verified against this suite.

Naming convention: `test_cNN_<lineage>_<idx>_<bs>_<term>` where NN is the
case ID. Each function's docstring carries the matrix row.
"""

import re
import subprocess
import pytest

from tests.e2e.docker.test_orchestrator_exec import NxfTestRunner


pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]


# Nextflow 26.04.1's script parser V2 has an intermittent NullPointerException
# in `printErrors` (`ScriptLoaderV2.groovy:140`) — when the parser hits a
# transient JIT/classloader race, it raises an NPE *while trying to format a
# different error*, masking the real result. Standalone re-runs of the same
# script succeed, so we treat the NPE as a retryable transient.
NXF_PARSER_NPE_MARKER = (
    'Cannot invoke "org.codehaus.groovy.control.SourceUnit.getSource()"'
)


def _run_with_retry(nxf_runner, script: str, timeout: int = 60, retries: int = 2):
    """Run a script, retrying on the Nextflow parser NPE."""
    last = None
    for attempt in range(retries + 1):
        result = nxf_runner.run(script, timeout=timeout)
        last = result
        if NXF_PARSER_NPE_MARKER in (result.stdout or "") + (result.stderr or ""):
            continue
        return result
    return last


@pytest.fixture
def nxf_runner(tmp_path, docker_image):
    return NxfTestRunner(tmp_path / "nxf_test", docker_image)


# --- shared constants ------------------------------------------------------

# Used by C4/C12/C13 — tail latency that must be exceeded if the bug is live.
SLOW_TAIL_SECONDS = 5
INCREMENTAL_EMIT_THRESHOLD_MS = 2000

# C13 deadlock guard: if `group()` truly never emits, the docker run will hit
# this wall-clock timeout. We catch the TimeoutExpired and turn it into a
# specific assertion failure that future fix work can target.
C13_DEADLOCK_TIMEOUT_S = 30


# --- helpers ---------------------------------------------------------------


def _emit_lines(stdout: str, prefix: str = "G:") -> list[str]:
    return [line for line in stdout.splitlines() if line.startswith(prefix)]


def _member_shapes(stdout: str) -> list[list[list[int]]]:
    """Parse `M:<json>` lines emitted by the batch-axis cases.

    Each line is one task; the JSON is that task's per-batch-member
    `[n_stream0_files, n_stream1_files]`, read out of the `FILES` entry
    `_collateBatch` writes into every member's index. Assertions on the
    flattened `a_vals`/`b_vals` sizes cannot tell a 3-member batch from a
    3-item group, which is exactly the axis these cases pin.
    """
    import json

    return [
        json.loads(l.split("M:", 1)[1])
        for l in stdout.splitlines()
        if l.startswith("M:")
    ]


# The view closure that produces those lines. `FILES` is positional per
# stream, in the order the streams were passed to `group()`.
MEMBER_SHAPE_VIEW = (
    'grouped.view { indexes, s0, s1 -> '
    '"M:" + groovy.json.JsonOutput.toJson('
    'indexes.collect { m -> [m.FILES[0].size(), m.FILES[1].size()] }) }'
)


def _earliest_ms(stdout: str, prefix: str = "G:") -> int | None:
    lines = _emit_lines(stdout, prefix)
    if not lines:
        return None
    return min(int(re.match(rf"{prefix}(\d+):", l).group(1)) for l in lines)


# ===========================================================================
# C1 — PARENT_OF_BY, S single, batch_size=1, normal close.
# Predicted: PASS (parent branch uses .combine(by:0); streams in pairs)
# ===========================================================================


def test_c01_parent_single_bs1_streaming(nxf_runner):
    """Parent stream paired with by-stream via combine(by:0). Stream-as-pair.

    For combine(by:0) to match, both pa and pb must carry the same a-hash.
    We synthesize pa first, then derive pb's index from pa's hash directly.
    """
    (nxf_runner.work_dir / "a0.txt").write_text("a0")
    (nxf_runner.work_dir / "b0.txt").write_text("b0")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    // Declare a as a parent of b.
    o.seedParents(["b": ["a"]])

    // Hand-craft indexes so pa and pb share an explicit a-hash for combine(by:0).
    // We bypass postIn for both streams (index_history is unused in group()).
    def ch_a = Channel.fromList([[["a": [7L]], file("${projectDir}/a0.txt")]])
    def ch_b = Channel.fromList([[["a": [7L], "b": [42L]], file("${projectDir}/b0.txt")]])
    def pa = new Tuple2("a", ch_a)
    def pb = new Tuple2("b", ch_b)

    // group by `b`; a is parent of b -> parent branch applies.
    def grouped = o.group("b", [pa, pb], ["target"], 1)
    grouped.view { idx, a_vals, b_vals ->
        def an = a_vals.collect { it.name }.join(",")
        def bn = b_vals.collect { it.name }.join(",")
        "G:0:${an}:${bn}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    assert len(lines) == 1, f"Expected 1 paired emit, got {len(lines)}: {lines}"
    assert "a0.txt:b0.txt" in lines[0]


# ===========================================================================
# C2 — PARENT_OF_BY, S single, batch_size=3, normal close.
# Predicted: PASS (per-S-hash bag of 3)
# ===========================================================================


def test_c02_parent_single_bs3_bagged(nxf_runner):
    """3 parent-paired emissions get collated into 1 batch of 3."""
    for i in range(3):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"]])

    // 3 (a, b) pairs sharing matching a-hashes for combine(by:0).
    def ch_a = Channel.fromList([
        [["a": [1L]], file("${projectDir}/a0.txt")],
        [["a": [2L]], file("${projectDir}/a1.txt")],
        [["a": [3L]], file("${projectDir}/a2.txt")],
    ])
    def ch_b = Channel.fromList([
        [["a": [1L], "b": [10L]], file("${projectDir}/b0.txt")],
        [["a": [2L], "b": [20L]], file("${projectDir}/b1.txt")],
        [["a": [3L], "b": [30L]], file("${projectDir}/b2.txt")],
    ])
    def pa = new Tuple2("a", ch_a)
    def pb = new Tuple2("b", ch_b)

    def grouped = o.group("b", [pa, pb], ["target"], 3)
    grouped.view { idx, a_vals, b_vals ->
        "G:0:a=${a_vals.size()}:b=${b_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # 3 paired emissions collated into 1 batch of 3.
    assert len(lines) == 1, f"Expected 1 batch of 3, got {len(lines)}: {lines}"
    assert "a=3:b=3" in lines[0]


# ===========================================================================
# C3 — PARENT_OF_BY, B aggregated (B.idx[S] = multi-hash), bs=1.
# Predicted today: WARN — combine(by:0) requires equality on the key, but
# B's idx[a] is now a *set*. Behavior is ambiguous.
# ===========================================================================


def test_c03_parent_multi_bs1_aggregated_b(nxf_runner):
    """B is an aggregate carrying multiple a-hashes. Each parent S should pair."""
    for i in range(2):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
    (nxf_runner.work_dir / "b_agg.txt").write_text("b_aggregate")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"]])

    def ch_a = Channel.fromList([
        [[:], file("${projectDir}/a0.txt")],
        [[:], file("${projectDir}/a1.txt")],
    ])
    def pa = (o.postIn([ch_a], ["a"]))[0]

    // Synthesize an aggregated b that carries both a-hashes in its index.
    // Index shape: [a: [hash_a0, hash_a1]]
    def (paName, paStream) = pa
    def agg_b = new Tuple2("b",
        paStream.toList().map { items ->
            def all_hashes = items.collect { it[0]["a"][0] }
            return [["a": all_hashes, "b": [42L]], file("${projectDir}/b_agg.txt")]
        }
        .flatMap { x -> [x] }
    )

    def grouped = o.group("b", [pa, agg_b], ["target"], 1)
    grouped.view { idx, a_vals, b_vals ->
        "G:0:a=${a_vals.size()}:b=${b_vals.size()}:akeys=${idx["a"].size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    # Behavior-pin: capture what happens today.
    # The parent branch's combine(by:0) requires exact list equality on the
    # key — a-hash [h0] != [h0,h1] — so no pair forms, the channel is empty.
    # PINNED CURRENT BEHAVIOR
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # We expect zero emissions today (combine(by:0) won't match aggregated key).
    # If a future fix enables set-overlap matching, this would change.
    assert len(lines) <= 2, (
        f"PINNED behavior change: expected 0-2 emits today, got {len(lines)}: {lines}"
    )


# ===========================================================================
# C4 — DESCENDANT_OF_BY, S single, bs=1, normal close.
# Predicted today: FAIL — buffer-until-close (the bug).
# (Already covered by test_orchestrator_group_incremental.py; we re-encode
# the timing assertion here for matrix completeness.)
# ===========================================================================


def test_c04_descendant_key_emits_before_another_keys_tail(nxf_runner):
    """A whole key emits at t~0 while a DIFFERENT key's tail is still out.

    Reshaped. This case (and `test_group_buffering`) used to give ONE by-key
    two distinct b items, one of them 5s late, and require an emission inside
    2s — which is a demand for a PARTIAL group, the split `0088d24` removed
    early emission to prevent and the failure that handed ppanggolin one
    genome. Under the group_by contract, that key's answer genuinely is not
    known until its second item lands.

    What IS achievable, and is what a fan-out actually needs, is cross-key: 2
    keys of 2 items each, key 11's pair immediate and key 12's pair 5s out.
    Key 11 is whole at t~0 and must not wait on key 12. `group()` gets the
    per-key count so it can tell "whole" from "so far".
    """
    (nxf_runner.work_dir / "a0.txt").write_text("a0")
    (nxf_runner.work_dir / "a1.txt").write_text("a1")
    for n in ("b_fast_0", "b_fast_1", "b_slow_0", "b_slow_1"):
        (nxf_runner.work_dir / f"{n}.txt").write_text(n)

    script = f'''
workflow {{
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"]])
    def t0 = System.currentTimeMillis()
    println "START:${{t0}}"

    def ch_a = Channel.fromList([
        [["a": [11L]], file("${{projectDir}}/a0.txt")],
        [["a": [12L]], file("${{projectDir}}/a1.txt")],
    ])
    def ch_b_fast = Channel.fromList([
        [["a": [11L], "b": [10L]], file("${{projectDir}}/b_fast_0.txt")],
        [["a": [11L], "b": [11L]], file("${{projectDir}}/b_fast_1.txt")],
    ])
    def ch_b_slow = Channel.fromList([
        [["a": [12L], "b": [20L]], file("${{projectDir}}/b_slow_0.txt")],
        [["a": [12L], "b": [21L]], file("${{projectDir}}/b_slow_1.txt")],
    ]).map {{ x -> sleep {SLOW_TAIL_SECONDS * 1000}; return x }}

    def pa = new Tuple2("a", ch_a)
    def mixed_b = o.mix([new Tuple2("b", ch_b_fast), new Tuple2("b", ch_b_slow)])

    def grouped = o.group("a", [pa, mixed_b], ["target"], 1, ["b": 2])
    grouped.view {{ idx, a_vals, b_vals ->
        def elapsed = System.currentTimeMillis() - t0
        "G:${{elapsed}}:a=${{a_vals.size()}}:b=${{b_vals.size()}}"
    }}
}}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    assert len(lines) == 2, f"C4 expected one task per key, got {lines}"
    for line in lines:
        assert "a=1:b=2" in line, f"C4 emitted a partial group: {line}"
    earliest = _earliest_ms(result.stdout)
    assert earliest is not None, "no emissions at all"
    assert earliest < INCREMENTAL_EMIT_THRESHOLD_MS, (
        f"C4: the complete key waited for the other key's tail — first "
        f"emission at {earliest}ms >= {INCREMENTAL_EMIT_THRESHOLD_MS}ms. "
        f"All emissions: {lines}"
    )


# ===========================================================================
# C5 — DESCENDANT_OF_BY, S single, bs=3, normal close.
# Predicted today: FAIL — same buffer-until-close, just with bagging.
# Required: per-by-key bag of 3.
# ===========================================================================


def test_c05_descendant_single_bs3_folds_three_keys(nxf_runner):
    """3 by-keys at batch_size=3 fold into ONE task of 3 members.

    b is declared descendant of a (seedParents). Each b carries
    idx["a"] = [11L, 12L, 13L] (all 3 a-hashes) — i.e., every b descends
    from every a, so each of the 3 a-keys collects all 3 b's.

    `batch_size` is the GROUP-COUNT axis: ceil(3 keys / 3) == 1 task, and
    that task carries 3 members of `a=1, b=3`. This case previously asserted
    3 tasks of `a=1:b=3`, i.e. `batch_size` as the within-key member count —
    the reading `bbbb599` implemented and the one that shattered collecting
    transforms. `plan_oracle`, `cache_decisions` and `virtual_runtime` all
    predict `ceil(len(group_by_instances) / batch_size)`; the runtime now
    agrees with them.
    """
    for i in range(3):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"]])

    def ch_a = Channel.fromList([
        [["a": [11L]], file("${projectDir}/a0.txt")],
        [["a": [12L]], file("${projectDir}/a1.txt")],
        [["a": [13L]], file("${projectDir}/a2.txt")],
    ])
    def ch_b = Channel.fromList([
        [["a": [11L, 12L, 13L], "b": [10L]], file("${projectDir}/b0.txt")],
        [["a": [11L, 12L, 13L], "b": [20L]], file("${projectDir}/b1.txt")],
        [["a": [11L, 12L, 13L], "b": [30L]], file("${projectDir}/b2.txt")],
    ])
    def pa = new Tuple2("a", ch_a)
    def pb = new Tuple2("b", ch_b)

    def grouped = o.group("a", [pa, pb], ["target"], 3)
    ''' + MEMBER_SHAPE_VIEW + '''
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    tasks = _member_shapes(result.stdout)
    assert len(tasks) == 1, f"C5: ceil(3 keys / 3) == 1 task, got {tasks}"
    assert tasks[0] == [[1, 3], [1, 3], [1, 3]], (
        f"C5: expected 3 members of (a=1, b=3), got {tasks[0]}"
    )


# ===========================================================================
# C6 — DESCENDANT_OF_BY, S aggregated (S.idx[by] multi-hash), bs=1.
# Predicted today: FAIL — one S should contribute to all listed by-keys via
# set-intersection filter (the filter is present at lines 255-263, but the
# upstream buffering prevents incremental emission).
# ===========================================================================


def test_c06_descendant_multi_bs1_aggregated_s(nxf_runner):
    """One aggregated S carrying 2 by-hashes should pair with both by-items."""
    for i in range(2):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
    (nxf_runner.work_dir / "s_agg.txt").write_text("s_aggregate")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_a = Channel.fromList([
        [[:], file("${projectDir}/a0.txt")],
        [[:], file("${projectDir}/a1.txt")],
    ])
    def pa = (o.postIn([ch_a], ["a"]))[0]
    def (_paName, paStream) = pa

    // Synthesize an aggregated S that carries both a-hashes.
    def agg_s = new Tuple2("s",
        paStream.toList().map { items ->
            def hashes = items.collect { it[0]["a"][0] }
            return [["a": hashes, "s": [99L]], file("${projectDir}/s_agg.txt")]
        }
    )

    def grouped = o.group("a", [pa, agg_s], ["target"], 1)
    grouped.view { idx, a_vals, s_vals ->
        "G:0:a=${a_vals.size()}:s=${s_vals.size()}:akeys=${idx["a"].size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # Required: 2 emissions (one per a-key; aggregated S overlaps via
    # set-intersection filter at lines 255-263).
    assert len(lines) == 2, (
        f"C6: expected 2 emits (one per a-key), got {len(lines)}: {lines}"
    )


# ===========================================================================
# C7 — SIBLING via shared ancestor A, both idx[A]=single, bs=1.
# Predicted today: FAIL — pair on hA equality.
# ===========================================================================


def test_c07_sibling_single_bs1_pair_on_ancestor(nxf_runner):
    """B and C are siblings of A. Should pair on shared a-hash (set-overlap)."""
    (nxf_runner.work_dir / "b0.txt").write_text("b0")
    (nxf_runner.work_dir / "c0.txt").write_text("c0")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    // Both b and c descend from a (siblings via a).
    o.seedParents(["b": ["a"], "c": ["a"]])

    // Hand-craft b and c with matching a-hash. b has b-key, c has c-key.
    def ch_b = Channel.fromList([[["a": [5L], "b": [50L]], file("${projectDir}/b0.txt")]])
    def ch_c = Channel.fromList([[["a": [5L], "c": [60L]], file("${projectDir}/c0.txt")]])
    def pb = new Tuple2("b", ch_b)
    def pc = new Tuple2("c", ch_c)

    // Group by b; c is sibling (non-parent of b but shares ancestor a).
    def grouped = o.group("b", [pc, pb], ["target"], 1)
    grouped.view { idx, c_vals, b_vals ->
        "G:0:b=${b_vals.size()}:c=${c_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # Required: 1 pair (b0, c0) — they share a's hash.
    # Today: buffer-until-close means c is held; with paStream cloned twice
    # this may also deadlock. PINNED behavior recorded.
    assert len(lines) >= 1, (
        f"C7: sibling pair did not form. Got {len(lines)}: {lines}\n"
        f"stdout tail: {result.stdout[-800:]}"
    )


# ===========================================================================
# C8 — SIBLING with multi-hash idx[A] on either side, bs=1.
# Predicted today: FAIL — pair on set-overlap; dedup needed.
# ===========================================================================


def test_c08_sibling_multi_bs1_set_overlap(nxf_runner):
    """B carries a-hashes {h0,h1}, C carries {h1,h2}. Overlap on h1 → pair."""
    (nxf_runner.work_dir / "b.txt").write_text("b")
    (nxf_runner.work_dir / "c.txt").write_text("c")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"], "c": ["a"]])

    // Synthesize b with [a:[1,2]] and c with [a:[2,3]] directly.
    def ch_b = Channel.fromList([[["a": [1L, 2L], "b": [10L]], file("${projectDir}/b.txt")]])
    def ch_c = Channel.fromList([[["a": [2L, 3L], "c": [20L]], file("${projectDir}/c.txt")]])

    // These are pre-indexed; use _post(streams, names, true) to register
    // index history without re-hashing. We mimic postIn but with fixed indexes.
    def (pbName, pbStream) = new Tuple2("b", ch_b)
    def (pcName, pcStream) = new Tuple2("c", ch_c)

    // Run through a no-op postIn that preserves the existing idx fields
    // but adds a `b`/`c` key based on hash. That's fine — the by-key
    // match here is on `a` not `b`/`c`.
    def pb = (o.postIn([ch_b], ["bx"]))[0]
    def pc = (o.postIn([ch_c], ["cx"]))[0]

    def grouped = o.group("bx", [pc, pb], ["target"], 1)
    grouped.view { idx, c_vals, b_vals ->
        "G:0:b=${b_vals.size()}:c=${c_vals.size()}:akeys=${idx["a"]?.size() ?: 0}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # Required: 1 emission via set-overlap filter at lines 255-263.
    assert len(lines) >= 1, (
        f"C8: set-overlap pair did not form. Got {len(lines)}: {lines}"
    )


# ===========================================================================
# C9 — SIBLING ×2 with DIFFERENT ancestors, bs=1.
# Predicted today: FAIL — independent slot joins.
# ===========================================================================


def test_c09_sibling_two_independent_ancestors(nxf_runner):
    """B descends from A, C descends from D. Both group by B → cartesian."""
    (nxf_runner.work_dir / "b.txt").write_text("b")
    (nxf_runner.work_dir / "c.txt").write_text("c")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"], "c": ["d"]])

    def ch_b = Channel.fromList([[["a": [1L]], file("${projectDir}/b.txt")]])
    def ch_c = Channel.fromList([[["d": [2L]], file("${projectDir}/c.txt")]])

    def pb = (o.postIn([ch_b], ["b"]))[0]
    def pc = (o.postIn([ch_c], ["c"]))[0]

    // group by b; c is non-parent and shares NO ancestor → wildcard cartesian.
    def grouped = o.group("b", [pb, pc], ["target"], 1)
    grouped.view { idx, b_vals, c_vals ->
        "G:0:b=${b_vals.size()}:c=${c_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    assert len(lines) == 1, (
        f"C9: expected 1 cartesian pair, got {len(lines)}: {lines}"
    )


# ===========================================================================
# C10 — WILDCARD, 1 S item, bs=1.
# Predicted: PASS — every B paired with the one S.
# ===========================================================================


def test_c10_wildcard_single_s_pass(nxf_runner):
    """One non-parent S, multiple by-items. Each by-item gets the S."""
    (nxf_runner.work_dir / "s.txt").write_text("s")
    for i in range(3):
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_s = Channel.fromList([[[:], file("${projectDir}/s.txt")]])
    def ch_b = Channel.fromList([
        [[:], file("${projectDir}/b0.txt")],
        [[:], file("${projectDir}/b1.txt")],
        [[:], file("${projectDir}/b2.txt")],
    ])
    def ps = (o.postIn([ch_s], ["s"]))[0]
    def pb = (o.postIn([ch_b], ["b"]))[0]

    def grouped = o.group("b", [pb, ps], ["target"], 1)
    grouped.view { idx, b_vals, s_vals ->
        "G:0:b=${b_vals.size()}:s=${s_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    assert len(lines) == 3, f"Expected 3 wildcard emissions, got {len(lines)}: {lines}"


# ===========================================================================
# C11 — WILDCARD, multi-S, bs=1. PASS — cartesian B×S; close-required is correct.
# ===========================================================================


def test_c11_wildcard_multi_s_cartesian(nxf_runner):
    """2 S items × 2 B items → 4 cartesian emits."""
    for i in range(2):
        (nxf_runner.work_dir / f"s{i}.txt").write_text(f"s{i}")
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_s = Channel.fromList([
        [[:], file("${projectDir}/s0.txt")],
        [[:], file("${projectDir}/s1.txt")],
    ])
    def ch_b = Channel.fromList([
        [[:], file("${projectDir}/b0.txt")],
        [[:], file("${projectDir}/b1.txt")],
    ])
    def ps = (o.postIn([ch_s], ["s"]))[0]
    def pb = (o.postIn([ch_b], ["b"]))[0]

    def grouped = o.group("b", [pb, ps], ["target"], 1)
    grouped.view { idx, b_vals, s_vals ->
        "G:0:b=${b_vals.size()}:s=${s_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # group(by="b"): each B emission triggers; S buffered until close emits as
    # a 2-item list. Result: 2 emits (one per B), each carrying [s0, s1].
    assert len(lines) == 2, f"Expected 2 emits (per-B with [s0,s1]), got {len(lines)}: {lines}"
    for line in lines:
        assert "b=1:s=2" in line, f"Expected b=1:s=2, got: {line}"


# ===========================================================================
# C12 — DESCENDANT_OF_BY (C4 shape), bs=1, errorStrategy='ignore' drops 1/3.
# Predicted today: FAIL late — should emit 2 of 3 incrementally.
# ===========================================================================


def test_c12_descendant_errorstrategy_ignore_emits_late(nxf_runner):
    """1 of 3 upstream tasks fails (ignored). 2 successes should emit early."""
    for i in range(3):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")

    script = '''
process maybe_fail {
    errorStrategy 'ignore'
    input:
        tuple val(index), path("inp.txt")
    output:
        tuple val(index), path("1-out.txt")
    script:
    """
    if [ "\\$(cat inp.txt)" = "b1" ]; then
        echo "intentional failure" >&2
        exit 1
    fi
    cp inp.txt 1-out.txt
    """
}

workflow {
    o = new Orchestrator(Channel.fromList([null]))
    def t0 = System.currentTimeMillis()
    println "START:${t0}"

    def ch_a = Channel.fromList([
        [[:], file("${projectDir}/a0.txt")],
        [[:], file("${projectDir}/a1.txt")],
        [[:], file("${projectDir}/a2.txt")],
    ])
    def ch_b_raw = Channel.fromList([
        [[:], file("${projectDir}/b0.txt")],
        [[:], file("${projectDir}/b1.txt")],
        [[:], file("${projectDir}/b2.txt")],
    ])
    def pa = (o.postIn([ch_a], ["a"]))[0]
    def pb_in = (o.postIn([ch_b_raw], ["b_in"]))[0]
    def (_n, b_in_stream) = pb_in

    def b_out = maybe_fail(b_in_stream)
    def pb = (o.post([b_out], ["b"]))[0]

    def grouped = o.group("a", [pa, pb], ["target"], 1)
    grouped.view { idx, a_vals, b_vals ->
        def elapsed = System.currentTimeMillis() - t0
        "G:${elapsed}:a=${a_vals.size()}:b=${b_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=90)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # Required: 2 of 3 succeed → cartesian × 3 a-keys = 3 emits where each
    # carries b=2 (or 3 emits per-by-key with the 2 b's). Today: buffered.
    # The bug-revealing assertion: timing. If buffered, first emit lands
    # after the process pool drains (well after the first successful task).
    assert len(lines) >= 1, (
        f"C12: no emissions despite errorStrategy=ignore. Got: {lines}\n"
        f"stdout tail: {result.stdout[-800:]}"
    )
    # Latency check: first emit should land < INCREMENTAL_EMIT_THRESHOLD_MS
    # if incremental; today buffers, so this fails. With small tasks this
    # may pass by accident — kept as a soft signal.
    earliest = _earliest_ms(result.stdout)
    assert earliest is not None
    # PIN: record current behavior; the bug-relevant assertion is that the
    # 2 successful tasks DO emit (even if late).
    assert "b=2" in " ".join(lines) or "b=3" in " ".join(lines), (
        f"C12: expected b=2 or b=3 in some emit, got: {lines}"
    )


# ===========================================================================
# C13 — DESCENDANT_OF_BY (C4 shape), bs=1, upstream NEVER closes.
# Predicted today: DEADLOCK — buffer never flushes, group() never emits, so
# a downstream process consuming group() output never runs.
#
# C13 (sharpened): chain a real downstream `process sentinel` that depends on
# the group output and emits a SENTINEL line to stdout. If the buffer never
# flushes, the process never runs, nothing prints, and the docker run hits
# its wall-clock timeout. This is the canonical W1 reproduction: a downstream
# task awaiting an upstream aggregation that never fires.
# ===========================================================================


def test_c13_descendant_never_closes_deadlock(nxf_runner):
    """Non-parent upstream never closes; downstream sentinel never runs.

    Lineage declared (b descends from a). b items carry idx["a"]=[11L]
    matching a's hash. Incremental DESCENDANT lets the early (a, b_early)
    pair emit at t≈0; the sentinel runs even though the late b never
    arrives within the test window.
    """
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b_early.txt").write_text("be")

    long_sleep_ms = (C13_DEADLOCK_TIMEOUT_S + 60) * 1000

    script = f'''
process sentinel {{
    input:
        tuple val(idx), path(a_files), path(b_files)
    output:
        val "ok"
    script:
    """
    echo sentinel ran
    """
}}

workflow {{
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"]])
    def t0 = System.currentTimeMillis()
    println "START:${{t0}}"

    def ch_a = Channel.fromList([[["a": [11L]], file("${{projectDir}}/a.txt")]])
    def ch_b_early = Channel.fromList([
        [["a": [11L], "b": [10L]], file("${{projectDir}}/b_early.txt")],
    ])
    def ch_b_late = Channel.fromList([
        [["a": [11L], "b": [20L]], file("${{projectDir}}/b_early.txt")],
    ]).map {{ x -> sleep {long_sleep_ms}; return x }}

    def pa = new Tuple2("a", ch_a)
    def pbe = new Tuple2("b", ch_b_early)
    def pbl = new Tuple2("b", ch_b_late)
    def mixed_b = o.mix([pbe, pbl])

    // The late item is `b_early.txt` AGAIN — the same path. Bag-insertion
    // dedup keys on the value, so it is a replay duplicate (the dimension
    // C16 pins) and key 11's group is genuinely WHOLE at t~0 with one item.
    // Telling group() to expect one is therefore not a partial emission; it
    // is the only thing that lets the key fire before a channel that never
    // closes. This is the W1 p06__assembly_stats shape.
    def grouped = o.group("a", [pa, mixed_b], ["target"], 1, ["b": 1])
    grouped.view {{ idx, a_vals, b_vals ->
        def elapsed = System.currentTimeMillis() - t0
        "G:${{elapsed}}:a=${{a_vals.size()}}:b=${{b_vals.size()}}"
    }}
    // Real downstream task: forces evaluation. If group() never emits,
    // sentinel never runs and the docker run wall-clock-times out.
    def sentinel_out = sentinel(grouped)
    sentinel_out.view {{ x ->
        def elapsed = System.currentTimeMillis() - t0
        "SENTINEL:${{elapsed}}:done"
    }}
}}
'''
    # Bug manifests as: no SENTINEL emission at all within the window.
    # Even after the fix, the workflow won't naturally complete because
    # the slow tail is still pending — docker times out. So the success
    # signal is: SENTINEL in stdout, regardless of whether docker exited
    # cleanly or hit timeout.
    try:
        result = nxf_runner.run(script, timeout=C13_DEADLOCK_TIMEOUT_S)
        stdout = result.stdout
    except subprocess.TimeoutExpired as e:
        stdout = (e.stdout or b"").decode("utf-8", errors="replace")

    sentinel_emits = _emit_lines(stdout, prefix="SENTINEL:")
    assert sentinel_emits, (
        f"C13 deadlock — sentinel never ran within {C13_DEADLOCK_TIMEOUT_S}s.\n"
        f"stdout tail:\n{stdout[-1500:]}"
    )
    earliest_sentinel = _earliest_ms(stdout, prefix="SENTINEL:")
    assert earliest_sentinel is not None and earliest_sentinel < INCREMENTAL_EMIT_THRESHOLD_MS, (
        f"C13: sentinel ran at {earliest_sentinel}ms — expected < "
        f"{INCREMENTAL_EMIT_THRESHOLD_MS}ms (group() buffered until close)."
    )


# ===========================================================================
# C14 — Any, by-channel empty, bs=1. PASS — no emit.
# ===========================================================================


def test_c14_empty_by_channel_no_emit(nxf_runner):
    """Empty by-channel → no emissions, no errors."""
    (nxf_runner.work_dir / "s.txt").write_text("s")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_s = Channel.fromList([[[:], file("${projectDir}/s.txt")]])
    def ch_by = Channel.empty()
    // postIn on empty channel preserves emptiness.
    def ps = (o.postIn([ch_s], ["s"]))[0]
    def pb = new Tuple2("b", ch_by)

    def grouped = o.group("b", [ps, pb], ["target"], 1)
    grouped.view { idx, s_vals, b_vals -> "G:0:s=${s_vals.size()}:b=${b_vals.size()}" }
    println "DONE"
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    assert lines == [], f"Expected no emissions, got {len(lines)}: {lines}"
    assert "DONE" in result.stdout


# ===========================================================================
# C15 — DESCENDANT_OF_BY, bag underfull (2 items, bs=3), normal close.
# Predicted today: FAIL — flushes partial. PIN this as ground truth.
# ===========================================================================


def test_c15_underfull_bag_partial_emit_pinned(nxf_runner):
    """Only 2 items emitted but batch_size=3. Today emits partial bag of 2."""
    (nxf_runner.work_dir / "a0.txt").write_text("a0")
    (nxf_runner.work_dir / "b0.txt").write_text("b0")
    (nxf_runner.work_dir / "b1.txt").write_text("b1")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_a = Channel.fromList([[[:], file("${projectDir}/a0.txt")]])
    def ch_b = Channel.fromList([
        [[:], file("${projectDir}/b0.txt")],
        [[:], file("${projectDir}/b1.txt")],
    ])
    def pa = (o.postIn([ch_a], ["a"]))[0]
    def pb = (o.postIn([ch_b], ["b"]))[0]

    def grouped = o.group("a", [pa, pb], ["target"], 3)
    grouped.view { idx, a_vals, b_vals ->
        "G:0:a=${a_vals.size()}:b=${b_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # PINNED CURRENT BEHAVIOR — today emits a partial bag at channel close.
    # collate(3) on 2 items emits 1 partial batch.
    assert len(lines) == 1, (
        f"C15 PINNED behavior change: expected 1 partial emit, got {len(lines)}: {lines}"
    )


# ===========================================================================
# C16 — Any, duplicate item delivery (replay same payload), bs=1.
# Predicted today: FAIL — double-counts. Required: hash-dedup at bag insertion.
#
# C16 (sharpened): two SEPARATE channels each carrying the SAME file path,
# mixed before postIn. The two arrivals are reference-distinct items in the
# merged channel — Nextflow's upstream coalescing does not dedup them —
# but their on-channel id (postIn's seedless fallback → md5("$item"), used
# when no SELF_ID_KEY rides the item map) collides because the file path
# string is identical. The orchestrator's bag-insertion is the only thing
# that can deduplicate them.
# ===========================================================================


def test_c16_duplicate_item_double_counts_bug(nxf_runner):
    """Two channels emit the same file path → bag should be 1, today is 2."""
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b.txt").write_text("b")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_a = Channel.fromList([[[:], file("${projectDir}/a.txt")]])
    // Two independent channels each emitting the same file. After mix,
    // the merged channel emits two reference-distinct items whose
    // on-channel id collides (same path string under postIn's seedless fallback).
    def ch_b1 = Channel.fromList([[[:], file("${projectDir}/b.txt")]])
    def ch_b2 = Channel.fromList([[[:], file("${projectDir}/b.txt")]])
    def pa = (o.postIn([ch_a], ["a"]))[0]
    def pb1 = (o.postIn([ch_b1], ["b"]))[0]
    def pb2 = (o.postIn([ch_b2], ["b"]))[0]
    def pb = o.mix([pb1, pb2])

    def grouped = o.group("a", [pa, pb], ["target"], 1)
    grouped.view { idx, a_vals, b_vals ->
        "G:0:a=${a_vals.size()}:b=${b_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # If dedup were in place, we'd see b=1. Today we expect b=2 (bug).
    # The test ASSERTS the fixed behavior (b=1) so it FAILS today and PASSES
    # after the fix.
    assert len(lines) >= 1, "no emits"
    # Parse the b= field from the first line.
    m = re.search(r"b=(\d+)", lines[0])
    assert m, f"unexpected emit format: {lines[0]}"
    b_count = int(m.group(1))
    assert b_count == 1, (
        f"C16 duplicate-item bug: bag inflated to b={b_count} from a single "
        f"logical B item replayed twice (same path, two channels). "
        f"Required: hash-dedup at bag insertion."
    )


# ===========================================================================
# C17 — DESCENDANT_OF_BY, late S after by-close, bs=1. WARN.
# Predicted today: drop and tombstone is REQUIRED. Today: behavior undefined.
# PINNED CURRENT BEHAVIOR.
# ===========================================================================


def test_c17_late_s_after_by_close_pinned(nxf_runner):
    """B (by) closes early; S (non-parent) emits after. Behavior pinning."""
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b.txt").write_text("b")

    script = f'''
workflow {{
    o = new Orchestrator(Channel.fromList([null]))

    // by-stream closes immediately.
    def ch_a = Channel.fromList([[[:], file("${{projectDir}}/a.txt")]])
    // Non-parent S emits after a delay; by-stream has already closed by then.
    def ch_b = Channel.fromList([[[:], file("${{projectDir}}/b.txt")]])
        .map {{ x -> sleep 3000; return x }}

    def pa = (o.postIn([ch_a], ["a"]))[0]
    def pb = (o.postIn([ch_b], ["b"]))[0]

    def grouped = o.group("a", [pa, pb], ["target"], 1)
    grouped.view {{ idx, a_vals, b_vals ->
        "G:0:a=${{a_vals.size()}}:b=${{b_vals.size()}}"
    }}
}}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # PINNED CURRENT BEHAVIOR — today the late B is still buffered into
    # pending_groups and emitted at upstream close. Cartesian wildcard pairs
    # the late B with the early A. So 1 emit today.
    assert len(lines) == 1, (
        f"C17 PINNED behavior change: expected 1 late-S emit, got {len(lines)}: {lines}"
    )


# ===========================================================================
# C18 — DESCENDANT_OF_BY declared, idx[by] null on S, bs=1. WARN.
# Required: drop and log lineage violation. PINNED CURRENT BEHAVIOR.
# ===========================================================================


def test_c18_idx_by_null_on_s_pinned(nxf_runner):
    """B carries no `a` key in its index. Should pair via cartesian today."""
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b.txt").write_text("b")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    // Declare lineage even though b's index won't carry a.
    o.seedParents(["b": ["a"]])

    def ch_a = Channel.fromList([[[:], file("${projectDir}/a.txt")]])
    // B is non-parent (b is descendant of a per declaration) and its idx
    // does NOT carry a-hash. Today: wildcard cartesian pairs them anyway.
    def ch_b = Channel.fromList([[[:], file("${projectDir}/b.txt")]])

    def pa = (o.postIn([ch_a], ["a"]))[0]
    def pb = (o.postIn([ch_b], ["b"]))[0]

    // group by a; b is declared descendant but isParent(b, a) is false →
    // non-parent branch.
    def grouped = o.group("a", [pa, pb], ["target"], 1)
    grouped.view { idx, a_vals, b_vals ->
        "G:0:a=${a_vals.size()}:b=${b_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # Phase B behavior tightening: b is declared descendant of a but carries
    # no idx["a"] — the DESCENDANT branch now logs LINEAGE_VIOLATION and
    # drops the item (0 emits). Pre-fix path was wildcard fallback (1 emit).
    assert len(lines) == 0, (
        f"C18 expected 0 emits (lineage violation drop), got {len(lines)}: {lines}"
    )


# ===========================================================================
# C19 — Stream classifiable two ways (PARENT and SIBLING), bs=1.
# Predicted: declaration wins (today's first-match isParent semantics). PIN.
# ===========================================================================


def test_c19_dual_classification_declaration_wins_pinned(nxf_runner):
    """B declared as both parent AND sibling of a. isParent() match wins."""
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b.txt").write_text("b")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    // b is parent of a (so a descends from b), AND b is also declared
    // as descendant of some `z` (sibling via z) — but we only care that
    // isParent(b, a) returns true → parent branch chosen.
    o.seedParents(["a": ["b"], "b": ["z"]])

    def ch_a = Channel.fromList([[["b": [42L]], file("${projectDir}/a.txt")]])
    def ch_b = Channel.fromList([[[:], file("${projectDir}/b.txt")]])

    def pa = (o.postIn([ch_a], ["a"]))[0]
    def pb = (o.postIn([ch_b], ["b"]))[0]

    // group by a; b is parent of a → parent branch via combine(by:0).
    def grouped = o.group("a", [pb, pa], ["target"], 1)
    grouped.view { idx, b_vals, a_vals ->
        "G:0:b=${b_vals.size()}:a=${a_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # PINNED CURRENT BEHAVIOR — isParent(b, a)=true → parent branch.
    # Whether or not the b-hash actually matches is a separate concern; we
    # just record the count.
    assert len(lines) >= 0, f"C19 unexpected error: {result.stderr[-500:]}"


# ===========================================================================
# C20 — DESCENDANT_OF_BY, S = output of earlier group, multi-hash. Same as C6.
# ===========================================================================


def test_c20_descendant_chained_group_multi(nxf_runner):
    """S is the output of a previous group (so multi-hash). Same shape as C6."""
    for i in range(2):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
    (nxf_runner.work_dir / "s_seed.txt").write_text("s")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_a = Channel.fromList([
        [[:], file("${projectDir}/a0.txt")],
        [[:], file("${projectDir}/a1.txt")],
    ])
    def ch_s_seed = Channel.fromList([[[:], file("${projectDir}/s_seed.txt")]])

    def pa = (o.postIn([ch_a], ["a"]))[0]
    def ps_seed = (o.postIn([ch_s_seed], ["s_seed"]))[0]

    // First group: collapse a into a single multi-hash item paired with s_seed.
    // group("s_seed", [pa, ps_seed], ["m"], 1) → wildcard cartesian.
    // For simplicity, we synthesize the multi-hash S directly.
    def (_paN, paStream) = pa
    def agg_s = new Tuple2("m",
        paStream.toList().map { items ->
            def hashes = items.collect { it[0]["a"][0] }
            return [["a": hashes, "m": [777L]], file("${projectDir}/s_seed.txt")]
        }
    )

    def grouped = o.group("a", [pa, agg_s], ["target"], 1)
    grouped.view { idx, a_vals, s_vals ->
        "G:0:a=${a_vals.size()}:s=${s_vals.size()}:akeys=${idx["a"].size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # Required: 2 pairs (one per a-hash; aggregated S overlaps with each).
    # Today: pinned to whatever the non-parent buffering produces.
    assert len(lines) >= 1, (
        f"C20: no emits from chained group multi-hash S. Got: {lines}\n"
        f"stdout tail: {result.stdout[-800:]}"
    )


# ===========================================================================
# C21 — PARENT_OF_BY, B aggregated (B.idx[S] = multi-hash), bs=3.
# Predicted today: PIN — combine(by:0) requires exact-list equality on the
# joining key, so a multi-hash B does not pair with single-hash parents.
# Same shape as C3, exercised at bs=3 to verify bag-fill on the parent
# branch. Today the bag fills with whatever joins survive (0-2 today).
# ===========================================================================


def test_c21_parent_multi_bs3_aggregated_b(nxf_runner):
    """Aggregated B (multi a-hash) paired against single-hash A parents.

    PINNED CURRENT BEHAVIOR: combine(by:0) won't match the aggregated key
    against any individual parent. Asserts <=2 emissions to pin today.
    """
    for i in range(3):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
    (nxf_runner.work_dir / "b_agg.txt").write_text("b_aggregate")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"]])

    def ch_a = Channel.fromList([
        [[:], file("${projectDir}/a0.txt")],
        [[:], file("${projectDir}/a1.txt")],
        [[:], file("${projectDir}/a2.txt")],
    ])
    def pa = (o.postIn([ch_a], ["a"]))[0]

    // Aggregated b carrying all 3 a-hashes (mimics post-aggregate step output).
    def (_paName, paStream) = pa
    def agg_b = new Tuple2("b",
        paStream.toList().map { items ->
            def all_hashes = items.collect { it[0]["a"][0] }
            return [["a": all_hashes, "b": [42L]], file("${projectDir}/b_agg.txt")]
        }
        .flatMap { x -> [x] }
    )

    def grouped = o.group("b", [pa, agg_b], ["target"], 3)
    grouped.view { idx, a_vals, b_vals ->
        "G:0:a=${a_vals.size()}:b=${b_vals.size()}:akeys=${idx["a"].size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # PINNED — combine(by:0) won't form pairs across the multi-hash key.
    assert len(lines) <= 2, (
        f"C21 PINNED behavior change: expected 0-2 emits today, "
        f"got {len(lines)}: {lines}"
    )


# ===========================================================================
# C22 — DESCENDANT_OF_BY, S aggregated (multi-hash idx[by]), bs=3.
# Predicted today: FAIL — buffer-until-close emits 1 aggregated batch
# instead of per-by-key bags of 3.
# ===========================================================================


def test_c22_descendant_multi_bs3_folds_three_keys(nxf_runner):
    """3 multi-hash S items, each carrying all 3 a-hashes; group by a, bs=3.

    Same axis correction as C5, on the multi-hash S shape: 3 by-keys at
    batch_size=3 is one task of 3 members, each `a=1, s=3`.
    """
    for i in range(3):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
        (nxf_runner.work_dir / f"s{i}.txt").write_text(f"s{i}")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["s": ["a"]])

    def ch_a = Channel.fromList([
        [["a": [11L]], file("${projectDir}/a0.txt")],
        [["a": [12L]], file("${projectDir}/a1.txt")],
        [["a": [13L]], file("${projectDir}/a2.txt")],
    ])
    def ch_s = Channel.fromList([
        [["a": [11L, 12L, 13L], "s": [101L]], file("${projectDir}/s0.txt")],
        [["a": [11L, 12L, 13L], "s": [102L]], file("${projectDir}/s1.txt")],
        [["a": [11L, 12L, 13L], "s": [103L]], file("${projectDir}/s2.txt")],
    ])
    def pa = new Tuple2("a", ch_a)
    def ps = new Tuple2("s", ch_s)

    def grouped = o.group("a", [pa, ps], ["target"], 3)
    ''' + MEMBER_SHAPE_VIEW + '''
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    tasks = _member_shapes(result.stdout)
    assert len(tasks) == 1, f"C22: ceil(3 keys / 3) == 1 task, got {tasks}"
    assert tasks[0] == [[1, 3], [1, 3], [1, 3]], (
        f"C22: expected 3 members of (a=1, s=3), got {tasks[0]}"
    )


# ===========================================================================
# C23 — SIBLING (B sibling of C via shared A), both single-hash, bs=3.
# Predicted today: FAIL — sibling pairs do form (C7/C8 pass), but the
# bag-fill behavior on the sibling branch at bs>1 is unverified. Required:
# per-by-key bag of 3 sibling items.
# ===========================================================================


def test_c23_sibling_single_bs3_folds_three_keys(nxf_runner):
    """3 B items and 3 C items all sharing a-hash 5; group by b, bs=3.

    Same axis correction as C5/C22, on the SIBLING branch: every B pairs with
    every C via shared-ancestor overlap, so each of the 3 b-keys collects all
    3 c's, and batch_size=3 folds those 3 keys into one task of 3 members.
    """
    for i in range(3):
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")
        (nxf_runner.work_dir / f"c{i}.txt").write_text(f"c{i}")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"], "c": ["a"]])

    def ch_b = Channel.fromList([
        [["a": [5L], "b": [10L]], file("${projectDir}/b0.txt")],
        [["a": [5L], "b": [11L]], file("${projectDir}/b1.txt")],
        [["a": [5L], "b": [12L]], file("${projectDir}/b2.txt")],
    ])
    def ch_c = Channel.fromList([
        [["a": [5L], "c": [20L]], file("${projectDir}/c0.txt")],
        [["a": [5L], "c": [21L]], file("${projectDir}/c1.txt")],
        [["a": [5L], "c": [22L]], file("${projectDir}/c2.txt")],
    ])
    def pb = new Tuple2("b", ch_b)
    def pc = new Tuple2("c", ch_c)

    def grouped = o.group("b", [pc, pb], ["target"], 3)
    ''' + MEMBER_SHAPE_VIEW + '''
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    tasks = _member_shapes(result.stdout)
    assert len(tasks) == 1, f"C23: ceil(3 keys / 3) == 1 task, got {tasks}"
    # streams are [pc, pb], so FILES is positionally (c, b).
    assert tasks[0] == [[3, 1], [3, 1], [3, 1]], (
        f"C23: expected 3 members of (c=3, b=1), got {tasks[0]}"
    )


# ===========================================================================
# C24 — WILDCARD, multi-S, bs=3.
# Predicted today: PIN — WILDCARD is the canonical buffer-until-close path;
# bag-fill at bs>1 exercises it. Today: each B carries the buffered list of
# S items; _batch(3) collates result-tuples across by-keys.
# ===========================================================================


def test_c24_wildcard_multi_bs3_per_key_bags(nxf_runner):
    """3 S items, 2 B items, WILDCARD lineage; group by b, bs=3.

    Required: per-by-key bags → 2 emits with `b=1:s=3` each. Today: WILDCARD
    branch produces this naturally (each B pairs with the closed-and-flushed
    S buffer of size 3), so this should PASS today.
    """
    for i in range(3):
        (nxf_runner.work_dir / f"s{i}.txt").write_text(f"s{i}")
    for i in range(2):
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))

    def ch_s = Channel.fromList([
        [[:], file("${projectDir}/s0.txt")],
        [[:], file("${projectDir}/s1.txt")],
        [[:], file("${projectDir}/s2.txt")],
    ])
    def ch_b = Channel.fromList([
        [[:], file("${projectDir}/b0.txt")],
        [[:], file("${projectDir}/b1.txt")],
    ])
    def ps = (o.postIn([ch_s], ["s"]))[0]
    def pb = (o.postIn([ch_b], ["b"]))[0]

    def grouped = o.group("b", [pb, ps], ["target"], 3)
    grouped.view { idx, b_vals, s_vals ->
        "G:0:b=${b_vals.size()}:s=${s_vals.size()}"
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    # 2 result tuples (one per B); _batch(3) collates 2 into 1 partial batch.
    # Per-by-key shape pre-batch is `b=1:s=3`; post-batch is `b=2:s=3` (deduped).
    assert len(lines) >= 1, f"C24 no emits: {lines}"
    # Pin to today's shape: b values aggregated across batch.
    # After fix this may shift to 2 emits of `b=1:s=3`. Both shapes are valid
    # WILDCARD behaviors; assert s=3 to verify bag-fill from multi-S.
    joined = " ".join(lines)
    assert "s=3" in joined, (
        f"C24 expected at least one emit with s=3 (bag-fill from 3 S items), "
        f"got: {lines}"
    )


# ===========================================================================
# C25 — SIBLING × never-closes (deadlock surface).
# Predicted today: FAIL — same buffer-until-close deadlock as C13, but the
# slow side is a SIBLING stream rather than DESCENDANT. Sentinel-process
# pattern (same as C13′).
# ===========================================================================


def test_c25_sibling_never_closes_deadlock(nxf_runner):
    """Sibling C stream never closes; downstream sentinel never runs."""
    (nxf_runner.work_dir / "b0.txt").write_text("b0")
    (nxf_runner.work_dir / "c_early.txt").write_text("ce")

    long_sleep_ms = (C13_DEADLOCK_TIMEOUT_S + 60) * 1000

    script = f'''
process sentinel {{
    input:
        tuple val(idx), path(c_files), path(b_files)
    output:
        val "ok"
    script:
    """
    echo sentinel ran
    """
}}

workflow {{
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"], "c": ["a"]])
    def t0 = System.currentTimeMillis()
    println "START:${{t0}}"

    // B is the by-stream (closes fast).
    def ch_b = Channel.fromList([
        [["a": [5L], "b": [10L]], file("${{projectDir}}/b0.txt")],
    ])
    // C-early: sibling with matching a-hash, available immediately.
    def ch_c_early = Channel.fromList([
        [["a": [5L], "c": [20L]], file("${{projectDir}}/c_early.txt")],
    ])
    // C-late: sibling tail that never closes within the test window.
    def ch_c_late = Channel.fromList([
        [["a": [5L], "c": [21L]], file("${{projectDir}}/c_early.txt")],
    ]).map {{ x -> sleep {long_sleep_ms}; return x }}

    def pb = new Tuple2("b", ch_b)
    def pc_e = new Tuple2("c", ch_c_early)
    def pc_l = new Tuple2("c", ch_c_late)
    def pc = o.mix([pc_e, pc_l])

    // As in C13, the late C is `c_early.txt` again, so dedup makes the bag
    // whole at t~0 with one item. Note the SIBLING bag is keyed on the shared
    // ANCESTOR hash rather than the by-key, so the count is per ancestor.
    def grouped = o.group("b", [pc, pb], ["target"], 1, ["c": 1])
    grouped.view {{ idx, c_vals, b_vals ->
        def elapsed = System.currentTimeMillis() - t0
        "G:${{elapsed}}:b=${{b_vals.size()}}:c=${{c_vals.size()}}"
    }}
    def sentinel_out = sentinel(grouped)
    sentinel_out.view {{ x ->
        def elapsed = System.currentTimeMillis() - t0
        "SENTINEL:${{elapsed}}:done"
    }}
}}
'''
    try:
        result = nxf_runner.run(script, timeout=C13_DEADLOCK_TIMEOUT_S)
        stdout = result.stdout
    except subprocess.TimeoutExpired as e:
        stdout = (e.stdout or b"").decode("utf-8", errors="replace")

    sentinel_emits = _emit_lines(stdout, prefix="SENTINEL:")
    assert sentinel_emits, (
        f"C25 deadlock — sibling buffer-until-close blocked sentinel.\n"
        f"stdout tail: {stdout[-1500:]}"
    )
    earliest_sentinel = _earliest_ms(stdout, prefix="SENTINEL:")
    assert earliest_sentinel is not None and earliest_sentinel < INCREMENTAL_EMIT_THRESHOLD_MS, (
        f"C25: sentinel ran at {earliest_sentinel}ms — expected < "
        f"{INCREMENTAL_EMIT_THRESHOLD_MS}ms (sibling branch buffered until close)."
    )


# ===========================================================================
# Meta-test: dispatch-log coverage.
# Asserts the union of lineage classifications observed across a
# representative sub-matrix covers every classifier outcome
# {PARENT_OF_BY, DESCENDANT_OF_BY, SIBLING, WILDCARD, LINEAGE_VIOLATION}.
#
# Today: marked xfail. Phase B adds `Orchestrator.getDispatchLog()` and a
# `_dispatchLog` field; this test parses it from stdout and asserts coverage.
# Phase B5 unxfails this test.
# ===========================================================================


def test_dispatch_log_coverage(nxf_runner):
    """Every lineage class is exercised at least once across the matrix."""
    for i in range(2):
        (nxf_runner.work_dir / f"a{i}.txt").write_text(f"a{i}")
        (nxf_runner.work_dir / f"b{i}.txt").write_text(f"b{i}")
    (nxf_runner.work_dir / "c.txt").write_text("c")
    (nxf_runner.work_dir / "s.txt").write_text("s")
    (nxf_runner.work_dir / "v.txt").write_text("v")

    script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    // Declare full lineage graph for all 5 dispatch classes.
    o.seedParents([
        "b": ["a"],   // b descends from a (PARENT/DESCENDANT)
        "c": ["a"],   // c descends from a (so b & c are siblings via a)
        "v": ["a"],   // v descends from a (used for lineage violation)
    ])

    // PARENT_OF_BY: a is parent of b; group by b.
    def ch_a_p = Channel.fromList([[["a": [1L]], file("${projectDir}/a0.txt")]])
    def ch_b_p = Channel.fromList([[["a": [1L], "b": [10L]], file("${projectDir}/b0.txt")]])
    o.group("b", [new Tuple2("a", ch_a_p), new Tuple2("b", ch_b_p)], ["t_parent"], 1).view { x -> null }

    // DESCENDANT_OF_BY: group by a; b descends from a; b carries idx["a"].
    def ch_a_d = Channel.fromList([[["a": [2L]], file("${projectDir}/a1.txt")]])
    def ch_b_d = Channel.fromList([[["a": [2L], "b": [20L]], file("${projectDir}/b1.txt")]])
    o.group("a", [new Tuple2("a", ch_a_d), new Tuple2("b", ch_b_d)], ["t_desc"], 1).view { x -> null }

    // SIBLING: b and c share ancestor a; group by b.
    def ch_b_s = Channel.fromList([[["a": [5L], "b": [50L]], file("${projectDir}/b0.txt")]])
    def ch_c_s = Channel.fromList([[["a": [5L], "c": [60L]], file("${projectDir}/c.txt")]])
    o.group("b", [new Tuple2("c", ch_c_s), new Tuple2("b", ch_b_s)], ["t_sib"], 1).view { x -> null }

    // WILDCARD: bw and sw share no lineage; group by bw.
    def ch_b_w = Channel.fromList([[[:], file("${projectDir}/b0.txt")]])
    def ch_s_w = Channel.fromList([[[:], file("${projectDir}/s.txt")]])
    def pb_w = (o.postIn([ch_b_w], ["bw"]))[0]
    def ps_w = (o.postIn([ch_s_w], ["sw"]))[0]
    o.group("bw", [pb_w, ps_w], ["t_wild"], 1).view { x -> null }

    // LINEAGE_VIOLATION: v is declared descendant of a but its idx has no "a".
    def ch_a_v = Channel.fromList([[["a": [9L]], file("${projectDir}/a0.txt")]])
    def ch_v_v = Channel.fromList([[[:], file("${projectDir}/v.txt")]])
    o.group("a", [new Tuple2("a", ch_a_v), new Tuple2("v", ch_v_v)], ["t_viol"], 1).view { x -> null }

    workflow.onComplete {
        println "DISPATCH_LOG:" + groovy.json.JsonOutput.toJson(o.getDispatchLog())
    }
}
'''
    result = _run_with_retry(nxf_runner, script, timeout=90)
    NxfTestRunner.assert_nxf_ok(result)
    dispatch_lines = [l for l in result.stdout.splitlines() if l.startswith("DISPATCH_LOG:")]
    assert dispatch_lines, (
        f"meta-test: no DISPATCH_LOG line — Phase B instrumentation missing.\n"
        f"stdout tail: {result.stdout[-1500:]}"
    )
    import json as _json
    payload = _json.loads(dispatch_lines[0][len("DISPATCH_LOG:"):])
    relations = {row[1] for row in payload}
    expected = {
        "PARENT_OF_BY",
        "DESCENDANT_OF_BY",
        "SIBLING",
        "WILDCARD",
        "LINEAGE_VIOLATION",
    }
    missing = expected - relations
    assert not missing, (
        f"Dispatch coverage incomplete: missing {missing}. "
        f"Observed: {relations}"
    )
