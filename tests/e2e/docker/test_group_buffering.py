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


def test_group_emits_first_match_before_nonparent_stream_closes(nxf_runner):
    """The first matching (a, b) tuple should be emitted by group() as soon
    as both arrive — not held until the non-parent stream closes.

    Current behavior (Orchestrator.groovy:218-241): the non-parent stream is
    buffered into pending_groups and flushed only on the upstream `null`
    sentinel. With a slow tail on `posted_b`, the buffer flush — and thus
    every group() emission — is delayed until SLOW_TAIL_SECONDS.

    Expected behavior after fix #2 (incremental emit when batch_size=1 and
    every input stream shares per-item index): the first emission lands at
    ~t=0, while the second waits naturally for posted_b's tail.

    Failure mode without the fix: the earliest G:<ms>:... line in stdout has
    `ms` >= SLOW_TAIL_SECONDS * 1000.
    """
    (nxf_runner.work_dir / "a.txt").write_text("a")
    (nxf_runner.work_dir / "b_early.txt").write_text("b early")
    (nxf_runner.work_dir / "b_late.txt").write_text("b late")

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
        [["a": [11L]], file("${{projectDir}}/a.txt")],
    ])
    ch_b_early = Channel.fromList([
        [["a": [11L], "b": [10L]], file("${{projectDir}}/b_early.txt")],
    ])
    ch_b_late = Channel.fromList([
        [["a": [11L], "b": [20L]], file("${{projectDir}}/b_late.txt")],
    ]).map {{ x ->
        sleep {SLOW_TAIL_SECONDS * 1000}
        return x
    }}

    def posted_a = new Tuple2("a", ch_a_raw)
    def posted_b_early = new Tuple2("b", ch_b_early)
    def posted_b_late = new Tuple2("b", ch_b_late)
    def mixed_b = o.mix([posted_b_early, posted_b_late])

    def grouped = o.group("a", [posted_a, mixed_b], ["target"], 1)
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

    # Earliest emission elapsed_ms is the bug-revealing signal.
    earliest_ms = min(int(re.match(r"G:(\d+):", line).group(1)) for line in emit_lines)

    assert earliest_ms < INCREMENTAL_EMIT_THRESHOLD_MS, (
        f"group()'s first emission landed at {earliest_ms}ms after workflow start, "
        f"which is >= {INCREMENTAL_EMIT_THRESHOLD_MS}ms — non-parent stream "
        f"`posted_b` was buffered until its channel closed at "
        f"~{SLOW_TAIL_SECONDS * 1000}ms (Orchestrator.groovy:218-241). "
        f"Expected incremental emit. All emissions:\n  "
        + "\n  ".join(emit_lines)
    )
