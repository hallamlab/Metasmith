import re
import pytest

from tests.metasmith.e2e.docker.test_orchestrator_exec import NxfTestRunner


pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]


@pytest.fixture
def nxf_runner(tmp_path, docker_image):
    return NxfTestRunner(tmp_path / "nxf_test", docker_image)


SLOW_TAIL_SECONDS = 5
INCREMENTAL_EMIT_THRESHOLD_MS = 2000


def test_group_emits_a_whole_key_before_another_keys_stream_closes(nxf_runner):
    for n in ("a0", "a1", "b_fast_0", "b_fast_1", "b_slow_0", "b_slow_1"):
        (nxf_runner.work_dir / f"{n}.txt").write_text(n)

    script = f'''
workflow {{
    o = new Orchestrator(Channel.fromList([null]))
    o.seedParents(["b": ["a"]])
    def t0 = System.currentTimeMillis()
    println "START:${{t0}}"

    ch_a_raw = Channel.fromList([
        [["a": [11L]], file("${{projectDir}}/a0.txt")],
        [["a": [12L]], file("${{projectDir}}/a1.txt")],
    ])
    ch_b_fast = Channel.fromList([
        [["a": [11L], "b": [10L]], file("${{projectDir}}/b_fast_0.txt")],
        [["a": [11L], "b": [11L]], file("${{projectDir}}/b_fast_1.txt")],
    ])
    ch_b_slow = Channel.fromList([
        [["a": [12L], "b": [20L]], file("${{projectDir}}/b_slow_0.txt")],
        [["a": [12L], "b": [21L]], file("${{projectDir}}/b_slow_1.txt")],
    ]).map {{ x ->
        sleep {SLOW_TAIL_SECONDS * 1000}
        return x
    }}

    def posted_a = new Tuple2("a", ch_a_raw)
    def mixed_b = o.mix([new Tuple2("b", ch_b_fast), new Tuple2("b", ch_b_slow)])

    def grouped = o.group("a", [posted_a, mixed_b], ["target"], 1, ["b": 2])
    grouped.view {{ idx, a_vals, b_vals ->
        def elapsed = System.currentTimeMillis() - t0
        def a_names = a_vals.collect {{ it.name }}.join(",")
        def b_names = b_vals.collect {{ it.name }}.join(",")
        "G:${{elapsed}}:${{a_names}}:${{b_names}}"
    }}
}}
'''
    result = nxf_runner.run(script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)

    emit_lines = [
        line for line in result.stdout.splitlines() if line.startswith("G:")
    ]
    assert emit_lines, (
        "group() produced no emissions at all — workflow plumbing broken.\n"
        f"stdout tail:\n{result.stdout[-1500:]}"
    )

    assert len(emit_lines) == 2, f"expected one task per key, got {emit_lines}"
    for line in emit_lines:
        assert len(line.split(":")[-1].split(",")) == 2, (
            f"group() emitted a partial group: {line}"
        )

    earliest_ms = min(int(re.match(r"G:(\d+):", line).group(1)) for line in emit_lines)

    assert earliest_ms < INCREMENTAL_EMIT_THRESHOLD_MS, (
        f"group()'s first emission landed at {earliest_ms}ms after workflow start, "
        f"which is >= {INCREMENTAL_EMIT_THRESHOLD_MS}ms — key 11's group was "
        f"complete immediately but waited on key 12's tail closing at "
        f"~{SLOW_TAIL_SECONDS * 1000}ms. All emissions:\n  "
        + "\n  ".join(emit_lines)
    )
