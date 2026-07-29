"""MRE for inbox #16: Orchestrator.group() buffers non-parent streams until
upstream channel closes, blocking incremental emission.

Bug site: src/metasmith/nextflow_config/Orchestrator.groovy:218-241 — the
non-parent-stream branch of group() collects items into pending_groups and
only flushes when the upstream channel emits its terminal `null` sentinel
(via .concat(this.one_null)). Downstream combine() therefore stalls until
that flush, which surfaces as:

  - Latency: downstream tasks submit ~T after the slowest upstream item, not
    incrementally as matches become available.
  - Deadlock: if upstream never reaches a terminal state (errorStrategy='ignore'
    masking + retries on walltime, or a hung task), the channel never closes,
    the buffer never flushes, and the downstream aggregation is never submitted.
    This is what surfaced as W1's p06__assembly_stats showing zero entries in
    trace.tsv on the 2026-05-27 original run and 2026-05-28 resume.

This test demonstrates the latency variant — it does not literally wedge the
test runner. The assertion is timestamp-based: the first group() emission
should land before the slow upstream tail closes its channel.

Scope path note: inbox #16's "tests to add" lists
`tests/test_orchestrator_group_incremental.py`, but the docker_image fixture
lives in `tests/integration/conftest.py`. Placing the file here keeps the
existing fixture wiring; move it back to `tests/` only if the fixture is
hoisted.
"""

import re
import pytest

from tests.e2e.docker.test_orchestrator_exec import NxfTestRunner


pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]


@pytest.fixture
def nxf_runner(tmp_path, docker_image):
    return NxfTestRunner(tmp_path / "nxf_test", docker_image)


# Delay (seconds) on the last non-parent item, controlling when posted_b
# closes. Threshold below which we expect the first emission to land if
# group() emits incrementally. Generous slack absorbs container start +
# Nextflow channel scheduling jitter.
SLOW_TAIL_SECONDS = 5
INCREMENTAL_EMIT_THRESHOLD_MS = 2000


def test_group_emits_a_whole_key_before_another_keys_stream_closes(nxf_runner):
    """A key whose group is complete emits at ~t=0; it does not wait for the
    slowest OTHER key's tail, nor for the shared channel to close.

    Reshaped from single-key to cross-key. As originally written this gave ONE
    by-key two DISTINCT b items with a 5s tail and required an emission inside
    2s — i.e. it required `group()` to hand a downstream task half of a group.
    That is the split `0088d24` removed early emission to prevent, and the
    failure that handed ppanggolin one genome out of two; under the `group_by`
    contract that key's answer really is unknown until its second item lands.
    The property worth having, and the one a fan-out actually needs, is that
    one slow sample must not hold up every other sample's downstream work.

    So: two keys of two items each, key 11's pair immediate and key 12's pair
    5s out. Key 11 must emit COMPLETE and early. `group()` is told how many
    items a key expects, which is what lets it tell "whole" from "so far";
    without that it can only flush at close.
    """
    for n in ("a0", "a1", "b_fast_0", "b_fast_1", "b_slow_0", "b_slow_1"):
        (nxf_runner.work_dir / f"{n}.txt").write_text(n)

    # Workflow records workflow.start as a baseline `START:<ms>` line, then
    # each group emission prints `G:<elapsed_ms>:<a_name>:<b_name>`.
    # `posted_a` is the by-stream; `posted_b` is the descendant stream
    # (b descends from a per seedParents) — DESCENDANT branch.
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
    # Generous timeout — sleep + container startup + Nextflow init.
    result = nxf_runner.run(script, timeout=60)
    NxfTestRunner.assert_nxf_ok(result)

    emit_lines = [
        line for line in result.stdout.splitlines() if line.startswith("G:")
    ]
    assert emit_lines, (
        "group() produced no emissions at all — workflow plumbing broken.\n"
        f"stdout tail:\n{result.stdout[-1500:]}"
    )

    # One task per key, and — the point of the whole exercise — each one
    # WHOLE. An early emission carrying a single b would be the regression
    # this reshape exists to keep out.
    assert len(emit_lines) == 2, f"expected one task per key, got {emit_lines}"
    for line in emit_lines:
        assert len(line.split(":")[-1].split(",")) == 2, (
            f"group() emitted a partial group: {line}"
        )

    # Earliest emission elapsed_ms is the bug-revealing signal.
    earliest_ms = min(int(re.match(r"G:(\d+):", line).group(1)) for line in emit_lines)

    assert earliest_ms < INCREMENTAL_EMIT_THRESHOLD_MS, (
        f"group()'s first emission landed at {earliest_ms}ms after workflow start, "
        f"which is >= {INCREMENTAL_EMIT_THRESHOLD_MS}ms — key 11's group was "
        f"complete immediately but waited on key 12's tail closing at "
        f"~{SLOW_TAIL_SECONDS * 1000}ms. All emissions:\n  "
        + "\n  ".join(emit_lines)
    )
