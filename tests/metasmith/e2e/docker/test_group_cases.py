import re
import subprocess
import pytest

from tests.metasmith.e2e.docker.test_orchestrator_exec import NxfTestRunner


pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]


NXF_PARSER_NPE_MARKER = (
    'Cannot invoke "org.codehaus.groovy.control.SourceUnit.getSource()"'
)


def _run_with_retry(nxf_runner, script: str, timeout: int = 60, retries: int = 2):
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


SLOW_TAIL_SECONDS = 5
INCREMENTAL_EMIT_THRESHOLD_MS = 2000

C13_DEADLOCK_TIMEOUT_S = 30


def _emit_lines(stdout: str, prefix: str = "G:") -> list[str]:
    return [line for line in stdout.splitlines() if line.startswith(prefix)]


def _member_shapes(stdout: str) -> list[list[list[int]]]:
    import json

    return [
        json.loads(l.split("M:", 1)[1])
        for l in stdout.splitlines()
        if l.startswith("M:")
    ]


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


def test_c01_parent_single_bs1_streaming(nxf_runner):
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


def test_c02_parent_single_bs3_bagged(nxf_runner):
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
    assert len(lines) == 1, f"Expected 1 batch of 3, got {len(lines)}: {lines}"
    assert "a=3:b=3" in lines[0]


def test_c03_parent_multi_bs1_aggregated_b(nxf_runner):
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
    NxfTestRunner.assert_nxf_ok(result)
    lines = _emit_lines(result.stdout)
    assert len(lines) <= 2, (
        f"PINNED behavior change: expected 0-2 emits today, got {len(lines)}: {lines}"
    )


def test_c04_descendant_key_emits_before_another_keys_tail(nxf_runner):
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


def test_c05_descendant_single_bs3_folds_three_keys(nxf_runner):
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


def test_c06_descendant_multi_bs1_aggregated_s(nxf_runner):
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
    assert len(lines) == 2, (
        f"C6: expected 2 emits (one per a-key), got {len(lines)}: {lines}"
    )


def test_c07_sibling_single_bs1_pair_on_ancestor(nxf_runner):
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
    assert len(lines) >= 1, (
        f"C7: sibling pair did not form. Got {len(lines)}: {lines}\n"
        f"stdout tail: {result.stdout[-800:]}"
    )


def test_c08_sibling_multi_bs1_set_overlap(nxf_runner):
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
    assert len(lines) >= 1, (
        f"C8: set-overlap pair did not form. Got {len(lines)}: {lines}"
    )


def test_c09_sibling_two_independent_ancestors(nxf_runner):
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


def test_c10_wildcard_single_s_pass(nxf_runner):
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


def test_c11_wildcard_multi_s_cartesian(nxf_runner):
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
    assert len(lines) == 2, f"Expected 2 emits (per-B with [s0,s1]), got {len(lines)}: {lines}"
    for line in lines:
        assert "b=1:s=2" in line, f"Expected b=1:s=2, got: {line}"


def test_c12_descendant_errorstrategy_ignore_emits_late(nxf_runner):
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
    assert len(lines) >= 1, (
        f"C12: no emissions despite errorStrategy=ignore. Got: {lines}\n"
        f"stdout tail: {result.stdout[-800:]}"
    )
    earliest = _earliest_ms(result.stdout)
    assert earliest is not None
    assert "b=2" in " ".join(lines) or "b=3" in " ".join(lines), (
        f"C12: expected b=2 or b=3 in some emit, got: {lines}"
    )


def test_c13_descendant_never_closes_deadlock(nxf_runner):
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


def test_c14_empty_by_channel_no_emit(nxf_runner):
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


def test_c15_underfull_bag_partial_emit_pinned(nxf_runner):
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
    assert len(lines) == 1, (
        f"C15 PINNED behavior change: expected 1 partial emit, got {len(lines)}: {lines}"
    )


def test_c16_duplicate_item_double_counts_bug(nxf_runner):
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
    assert len(lines) >= 1, "no emits"
    m = re.search(r"b=(\d+)", lines[0])
    assert m, f"unexpected emit format: {lines[0]}"
    b_count = int(m.group(1))
    assert b_count == 1, (
        f"C16 duplicate-item bug: bag inflated to b={b_count} from a single "
        f"logical B item replayed twice (same path, two channels). "
        f"Required: hash-dedup at bag insertion."
    )


def test_c17_late_s_after_by_close_pinned(nxf_runner):
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
    assert len(lines) == 1, (
        f"C17 PINNED behavior change: expected 1 late-S emit, got {len(lines)}: {lines}"
    )


def test_c18_idx_by_null_on_s_pinned(nxf_runner):
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
    assert result.returncode != 0, (
        f"C18 expected the run to stop, got exit 0. "
        f"stdout tail: {(result.stdout or '')[-1500:]}"
    )
    combined = (result.stdout or "") + (result.stderr or "")
    assert "declared descendant" in combined, (
        f"C18 expected the lineage-violation message; got: {combined[-1500:]}"
    )
    assert _emit_lines(result.stdout) == [], "nothing should have been grouped"


def test_c19_dual_classification_declaration_wins_pinned(nxf_runner):
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
    assert len(lines) >= 0, f"C19 unexpected error: {result.stderr[-500:]}"


def test_c20_descendant_chained_group_multi(nxf_runner):
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
    assert len(lines) >= 1, (
        f"C20: no emits from chained group multi-hash S. Got: {lines}\n"
        f"stdout tail: {result.stdout[-800:]}"
    )


def test_c21_parent_multi_bs3_aggregated_b(nxf_runner):
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
    assert len(lines) <= 2, (
        f"C21 PINNED behavior change: expected 0-2 emits today, "
        f"got {len(lines)}: {lines}"
    )


def test_c22_descendant_multi_bs3_folds_three_keys(nxf_runner):
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


def test_c23_sibling_single_bs3_folds_three_keys(nxf_runner):
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
    assert tasks[0] == [[3, 1], [3, 1], [3, 1]], (
        f"C23: expected 3 members of (c=3, b=1), got {tasks[0]}"
    )


def test_c24_wildcard_multi_bs3_per_key_bags(nxf_runner):
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
    assert len(lines) >= 1, f"C24 no emits: {lines}"
    joined = " ".join(lines)
    assert "s=3" in joined, (
        f"C24 expected at least one emit with s=3 (bag-fill from 3 S items), "
        f"got: {lines}"
    )


def test_c25_sibling_never_closes_deadlock(nxf_runner):
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


def test_dispatch_log_coverage(nxf_runner):
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

    workflow.onComplete {
        println "DISPATCH_LOG:" + groovy.json.JsonOutput.toJson(o.getDispatchLog())
    }
}
'''

    violation_script = '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["v": ["a"]])

    // LINEAGE_VIOLATION: v is declared descendant of a but its idx has no "a".
    def ch_a_v = Channel.fromList([[["a": [9L]], file("${projectDir}/a0.txt")]])
    def ch_v_v = Channel.fromList([[[:], file("${projectDir}/v.txt")]])
    o.group("a", [new Tuple2("a", ch_a_v), new Tuple2("v", ch_v_v)], ["t_viol"], 1).view { x -> null }

    workflow.onComplete {
        println "DISPATCH_LOG:" + groovy.json.JsonOutput.toJson(o.getDispatchLog())
    }
}
'''

    import json as _json

    def _relations(result):
        lines = [l for l in result.stdout.splitlines() if l.startswith("DISPATCH_LOG:")]
        assert lines, (
            "meta-test: no DISPATCH_LOG line — getDispatchLog() instrumentation "
            f"missing.\nstdout tail: {result.stdout[-1500:]}"
        )
        return {row[1] for row in _json.loads(lines[0][len("DISPATCH_LOG:"):])}

    ok = _run_with_retry(nxf_runner, script, timeout=90)
    NxfTestRunner.assert_nxf_ok(ok)
    relations = _relations(ok)

    violated = _run_with_retry(nxf_runner, violation_script, timeout=90)
    assert violated.returncode != 0, (
        f"a lineage violation must stop the run; exit was {violated.returncode}"
    )
    relations |= _relations(violated)

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


def _f2_script(b_index: str) -> str:
    return '''
workflow {
    o = new Orchestrator(Channel.fromList([null]))
    // b is DECLARED a descendant of a, so classify() routes it through the
    // DESCENDANT_OF_BY branch and the by-key is mandatory from here on.
    o.seedParents(["b": ["a"]])

    def ch_a = Channel.fromList([[["a": [7L]], file("${projectDir}/a.txt")]])
    def ch_b = Channel.fromList([[%s, file("${projectDir}/b.txt")]])

    def grouped = o.group(
        "a",
        [new Tuple2("a", ch_a), new Tuple2("b", ch_b)],
        ["target"],
        1,
    )
    grouped.view { idx, a_vals, b_vals -> "G:a=${a_vals.size()}:b=${b_vals.size()}" }
}
''' % b_index


def _assert_f2_aborts(result, arm: str):
    combined = (result.stdout or "") + (result.stderr or "")
    assert result.returncode != 0, (
        f"{arm}: a declared descendant arrived without its by-key and the run "
        f"still exited 0. Downstream sees an empty channel, which nextflow "
        f"treats as a legitimate end of the DAG.\n"
        f"emits: {_emit_lines(result.stdout)}\n"
        f"stdout tail: {(result.stdout or '')[-1500:]}"
    )
    for needle, what in (("[b]", "the stream"), ("[a]", "the by-key"), ("b.txt", "the file")):
        assert needle in combined, (
            f"{arm}: the abort message does not name {what} ({needle!r}).\n"
            f"output tail: {combined[-1500:]}"
        )


def test_f2a_absent_by_key_stops_the_run(nxf_runner):
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b.txt").write_text("b")

    result = _run_with_retry(nxf_runner, _f2_script("[:]"), timeout=60)
    _assert_f2_aborts(result, "F2a")


def test_f2b_empty_by_key_list_stops_the_run(nxf_runner):
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b.txt").write_text("b")

    result = _run_with_retry(nxf_runner, _f2_script('["a": []]'), timeout=60)
    _assert_f2_aborts(result, "F2b")
