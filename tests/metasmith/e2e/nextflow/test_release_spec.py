# The grouping and early-start spec, as the user set it in the interview of
# 2026-10-04. One test per acceptance criterion (B1-B11, B15 and B16 in the
# plan that introduced this file), each a workflow of mock steps run on the
# host's Nextflow against the real Orchestrator.groovy. The evidence is only
# what a user of the workflow could see: which tasks ran, which files each
# received, and when each started and finished. States the spec calls
# impossible are assertions in Orchestrator.groovy, not tests here.
#
# MSM_TEST_ORCHESTRATOR points the suite at another Orchestrator.groovy.
# MSM_SPEC_SEED replays the random fan-out counts a failing run printed.

import os
import random
from math import prod

import pytest

from tests.metasmith.e2e.nextflow.harness import (
    assert_ok,
    assert_runs_ahead,
    assert_slot,
    assert_slow_task_was_slow,
    assert_started_after,
    product,
    reads,
)

SAMPLES = ["s0", "s1", "s2"]
SLOW = "s0"
FAST = ["s1", "s2"]
SLOW_S = 6


@pytest.fixture
def rng(request) -> random.Random:
    seed = int(os.environ.get("MSM_SPEC_SEED", random.randrange(1 << 30)))
    print(f"MSM_SPEC_SEED={seed} ({request.node.name})")
    return random.Random(seed)


def _distinct_fans(rng: random.Random, hi: int, hops: int, samples: list[str]) -> dict[str, list[int]]:
    while True:
        fans = {s: [rng.randint(1, hi) for _ in range(hops)] for s in samples}
        if len({prod(f) for f in fans.values()}) == len(samples):
            return fans


# ------------------------------------------------- one task per group-key item


# B1. Every step aggregates by its group-by. A consumer grouped by a
# coassembly runs once per coassembly, and that task holds every read set the
# coassembly was built from, because the coassembly's lineage names them all.
def test_a_coassembly_consumer_runs_once_per_coassembly_with_every_read_set(ws):
    ws.given("cfg", ["c"])
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.spec("p03", label="coasm", by="cfg")
    ws.spec("p04", label="cobins", by="cfg")

    result = ws.run("parent_join.nf")
    assert_ok(result)

    p04 = result.members("p04", {"c"})
    assert_slot(p04["c"], 0, names={product("c", "coasm")})
    assert_slot(p04["c"], 1, names={reads(s) for s in SAMPLES})


# B2. A consumer grouped by a fan-out step's product treats each fanned-out
# file as its own group-key item: it runs once per file, and each task
# receives exactly that one file.
def test_a_consumer_grouped_by_a_fan_out_runs_once_per_fanned_out_file(ws):
    fans = {"s0": 1, "s1": 3, "s2": 2}
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fan=fans)
    ws.spec("p02", label="bins", by="asm")
    ws.spec("p03", label="qc")

    result = ws.run("other_key_producer.nf")
    assert_ok(result)

    expected = sorted(product(s, "asm", item=i + 1) for s in SAMPLES for i in range(fans[s]))
    got = result.received("p02")
    assert all(len(r.slots[0]) == 1 for r in got), (
        f"each p02 task must receive exactly one asm file, got "
        f"{[r.slots[0] for r in got]}\n{result.tail}"
    )
    assert sorted(r.slots[0][0] for r in got) == expected, (
        f"p02 must run exactly once per fanned-out asm file: expected {expected}, "
        f"got {sorted(r.slots[0][0] for r in got)}\n{result.tail}"
    )
    assert len(result.tasks("p02", status="COMPLETED")) == len(expected)


# ------------------------------------------------ start as soon as inputs are done


# B3. Q5: "start immediately" -- a sample's next step does not wait for
# another sample's slow step.
def test_a_sample_starts_when_its_own_input_is_done(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain.nf")
    assert_ok(result)

    slow = result.completed("p01", SLOW)
    assert_slow_task_was_slow(result, "p01", SLOW, SLOW_S)
    for s in FAST:
        assert_runs_ahead(result, result.completed("p02", s), slow)
        assert_runs_ahead(result, result.completed("p03", s), slow)
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 1, names={product(s, "bins")})


# B4. Q5, for a step grouped by each sample's assembly that also takes the raw
# reads the assembly was built from.
def test_a_step_joining_the_raw_reads_starts_per_sample(ws):
    ws.given("cfg", ["c"])
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="coasm", by="cfg")
    ws.spec("p04", label="cobins", by="cfg")

    result = ws.run("parent_join.nf")
    assert_ok(result)

    slow = result.completed("p01", SLOW)
    assert_slow_task_was_slow(result, "p01", SLOW, SLOW_S)
    for s in FAST:
        assert_runs_ahead(result, result.completed("p02", s), slow)
    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={product(s, "asm")})
        assert_slot(p02[s], 1, names={reads(s)})


# B5. Q9: early start holds when two steps produce the same type and their
# outputs are merged into the consumer's input.
def test_a_sample_starts_once_every_merged_producer_is_done(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01a", label="asm")
    ws.spec("p01b", label="asmb", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")

    result = ws.run("mixed_producers.nf")
    assert_ok(result)

    slow = result.completed("p01b", SLOW)
    assert_slow_task_was_slow(result, "p01b", SLOW, SLOW_S)
    for s in FAST:
        assert_runs_ahead(result, result.completed("p02", s), slow)
    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 1, names={product(s, "asm"), product(s, "asmb")})


# B6. Q9: early start holds for an input two steps below the group key. p01
# writes two assemblies per sample, p02 bins each one, and p03 -- grouped by
# sample -- needs both of its sample's bins.
def test_a_sample_starts_once_its_two_step_input_is_done(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fan={s: 2 for s in SAMPLES})
    ws.spec("p02", label="bins", by="asm", slow_in={rf".*-2-1\.{SLOW}-asm\.out": SLOW_S})
    ws.spec("p03", label="qc")

    result = ws.run("other_key_producer.nf")
    assert_ok(result)

    slow = max(result.tasks("p02", SLOW, "COMPLETED"), key=lambda t: t.complete - t.start)
    assert slow.complete - slow.start >= (SLOW_S - 2) * 1000, "the slow bin was not slow"
    for s in FAST:
        assert_runs_ahead(result, result.completed("p03", s), slow)
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 1, count=2)


# B7. Q9: the dastool shape. Three binners find a different number of bins per
# sample, QC writes a different number of files per bin, and the scorer --
# grouped by sample -- starts as soon as its own sample's QC is all done.
def test_dastool_starts_per_sample_once_every_binner_and_qc_is_done(ws, rng):
    bin_fans = _distinct_fans(rng, 4, 3, SAMPLES)
    qc_fans = {s: rng.randint(1, 3) for s in SAMPLES}
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    for b, binner in enumerate(["b1", "b2", "b3"]):
        ws.spec(binner, label=f"bin{b + 1}", fan={s: bin_fans[s][b] for s in SAMPLES})
    ws.spec("qc", label="qc", by="bins", fan=qc_fans, slow_in={rf".*-1-1\.{SLOW}-bin2\.out": SLOW_S})
    ws.spec("score", label="score")

    result = ws.run("dastool.nf")
    assert_ok(result)

    slow = max(result.tasks("qc", SLOW, "COMPLETED"), key=lambda t: t.complete - t.start)
    assert slow.complete - slow.start >= (SLOW_S - 2) * 1000, "the slow QC task was not slow"
    for s in FAST:
        assert_runs_ahead(result, result.completed("score", s), slow)
    score = result.members("score", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(score[s], 1, count=sum(bin_fans[s]) * qc_fans[s])


# B15. Q9 and the user's "arbitrary lineage length": a chain of D steps, each
# grouped by the one before, fanning out by a random amount per sample at
# every step. The consumer, grouped by sample, starts once its own sample's
# whole subtree is done.
def _chain_script(depth: int) -> str:
    hops = [f"h{k}" for k in range(1, depth + 1)]
    lines = [
        f"include {{ given; {'; '.join(f'mock1 as {h}' for h in hops)}; mock2 as pc }} from './mocks.nf'",
        "",
        "workflow {",
        "main:",
        "o = new Orchestrator(Channel.fromList([null]))",
        'l = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text).lineage',
        '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
    ]
    prev = "reads"
    for k, h in enumerate(hops, start=1):
        lines.append(f"k = ['x{k}']")
        lines.append(
            f"_x{k} = (o.post(o.asStreams({h}(o.group('{prev}', [_{prev}], k, 1))), k, ['slot-x{k}']))[0]"
        )
        prev = f"x{k}"
    lines.append("k = ['out']")
    lines.append(
        f"_out = (o.post(o.asStreams(pc(o.group('reads', [_reads, _{prev}], k, 1))), k, ['slot-out']))[0]"
    )
    lines += ["o.seal()", "}"]
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize(
    "depth,slow_hop",
    [(1, "first"), (2, "first"), (2, "last"), (3, "first"), (3, "last"), (5, "first"), (5, "last")],
)
def test_a_sample_starts_once_its_whole_subtree_is_done(ws, rng, depth, slow_hop):
    fans = _distinct_fans(rng, 3 if depth <= 3 else 2, depth, FAST)
    fans[SLOW] = [1] * depth
    slow_alias = "h1" if slow_hop == "first" else f"h{depth}"
    ws.given("reads", SAMPLES)
    for k in range(1, depth + 1):
        behaviour = {"label": f"x{k}", "fan": {s: fans[s][k - 1] for s in SAMPLES}}
        if k > 1:
            behaviour["by"] = f"x{k - 1}"
        if f"h{k}" == slow_alias:
            behaviour["slow"] = {SLOW: SLOW_S}
        ws.spec(f"h{k}", **behaviour)
    ws.spec("pc", label="out")

    result = ws.run(script=_chain_script(depth))
    assert_ok(result)

    slow = result.completed(slow_alias, SLOW)
    assert_slow_task_was_slow(result, slow_alias, SLOW, SLOW_S)
    for s in FAST:
        assert_runs_ahead(result, result.completed("pc", s), slow)
    pc = result.members("pc", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(pc[s], 1, count=prod(fans[s]))


# Q5, for an input grouped by an ancestor of the consumer's key: E5's
# assembly_stats, grouped by the read set but consuming the assembly, feeding
# binners grouped by the assembly.
def test_a_sample_starts_when_its_input_was_grouped_by_an_ancestor_key(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="stats", slow={SLOW: SLOW_S})
    ws.spec("p03", label="bins")

    result = ws.run("upstream_key_input.nf")
    assert_ok(result)

    slow = result.completed("p02", SLOW)
    assert_slow_task_was_slow(result, "p02", SLOW, SLOW_S)
    for s in FAST:
        assert_runs_ahead(result, result.completed("p03", s), slow)
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={product(s, "asm")})
        assert_slot(p03[s], 1, names={product(s, "stats")})


# The same shape over a coassembly: every read set's member yields a stats item
# of the one coassembly, so its consumer holds all of them, however they arrive.
def test_a_coassembly_consumer_holds_every_read_sets_input_grouped_by_reads(ws):
    ws.given("cfg", ["c"])
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="coasm", by="cfg")
    ws.spec("p02", label="stats", slow={SLOW: SLOW_S})
    ws.spec("p03", label="bins", by="cfg")

    result = ws.run("upstream_key_coassembly.nf")
    assert_ok(result)

    assert_slow_task_was_slow(result, "p02", SLOW, SLOW_S)
    p03 = result.members("p03", {"c"})
    assert_slot(p03["c"], 0, names={product("c", "coasm")})
    assert_slot(p03["c"], 1, names={product(s, "stats") for s in SAMPLES})


# ------------------------------------------------------------------- batching


# B8. Q8 and Q10: batches are formed from groups in the order they become
# ready, and whatever is left at the end runs as a smaller batch.
def test_batches_fill_in_readiness_order_and_the_remainder_runs_at_close(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.params["batch"] = 2

    result = ws.run("batched_consumer.nf")
    assert_ok(result)

    batches = {frozenset(r.tokens): r for r in result.received("p02")}
    assert set(batches) == {frozenset(FAST), frozenset([SLOW])}, (
        f"expected one batch of the two fast samples and one of the slow one, got "
        f"{[sorted(b) for b in batches]}\n{result.tail}"
    )
    tasks = {frozenset(t.tag.split(",")): t for t in result.tasks("p02", status="COMPLETED")}
    slow = result.completed("p01", SLOW)
    assert_slow_task_was_slow(result, "p01", SLOW, SLOW_S)
    assert_runs_ahead(result, tasks[frozenset(FAST)], slow)


# B16. Q8 and Q10, the checkM case: batching mixed with a group-by and a
# fan-out. checkM is grouped by assembly over the assembly and all of its
# bins, two assembly groups per task. Groups batch in the order they become
# ready, each group holds every bin of its own assembly and no other, and the
# slow sample's group runs in whatever is left at close.
def test_checkm_batches_whole_assembly_groups_in_readiness_order(ws, rng):
    samples = [f"s{i}" for i in range(5)]
    fast = {s for s in samples if s != SLOW}
    while True:
        fans = {s: rng.randint(1, 4) for s in samples}
        if len(set(fans.values())) >= 3:
            break
    ws.given("reads", samples)
    ws.spec("p01", label="asm")
    ws.spec("binner", label="bin", fan=fans, slow={SLOW: SLOW_S})
    ws.spec("checkm", label="checkm")
    ws.params["batch"] = 2

    result = ws.run("checkm.nf")
    assert_ok(result)

    slow = result.completed("binner", SLOW)
    assert_slow_task_was_slow(result, "binner", SLOW, SLOW_S)
    got = result.received("checkm")
    assert len(got) == 3 and len(result.tasks("checkm", status="COMPLETED")) == 3, (
        f"5 assembly groups at batch size 2 make exactly 3 tasks, got "
        f"{[r.tokens for r in got]}\n{result.tail}"
    )
    assert sorted(s for r in got for s in r.tokens) == samples, (
        f"every assembly group runs exactly once, got {[r.tokens for r in got]}"
    )
    for r in got:
        assert_slot(r, 0, names={product(s, "asm") for s in r.tokens})
        for s in r.tokens:
            bins = [n for n in r.slots[1] if f".{s}-bin.out" in n]
            assert len(bins) == fans[s], (
                f"checkM group {s} received {sorted(bins)}, expected all {fans[s]} of its bins"
            )
        assert len(r.slots[1]) == sum(fans[s] for s in r.tokens), (
            f"checkM task {r.tokens} received bins of another assembly: {sorted(r.slots[1])}"
        )
    slow_batch = next(r for r in got if SLOW in r.tokens)
    assert slow_batch.tokens == [SLOW], (
        f"the four fast groups fill two batches, so the slow group runs alone, "
        f"got {[r.tokens for r in got]}"
    )
    tasks = {frozenset(t.tag.split(",")): t for t in result.tasks("checkm", status="COMPLETED")}
    for r in got:
        if r is not slow_batch:
            assert_runs_ahead(result, tasks[frozenset(r.tokens)], slow)
    assert {s for r in got if r is not slow_batch for s in r.tokens} == fast


# ------------------------------------------------------------ unrelated input


# B9. Q11: an input that shares no lineage with the group key reaches every
# task whole, so no task starts before that input is finished.
def test_an_unrelated_input_reaches_every_task_after_it_finishes(ws):
    ws.given("cfg", ["c"])
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    # A member with no `reads` lineage is named "x" by the mocks.
    ws.spec("pu", label="ref", by="cfg", fan={"x": 2}, slow_in={r"cfg_c\.txt": SLOW_S})
    ws.spec("p02", label="bins")

    result = ws.run("unrelated_input.nf")
    assert_ok(result)

    made = result.completed("pu", "x")
    assert_slow_task_was_slow(result, "pu", "x", SLOW_S)
    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_started_after(result, ("p02", s), ("pu", "x"))
        assert_slot(p02[s], 2, names={product("c", "ref", item=i + 1) for i in range(2)})
    assert made


# -------------------------------------------------------- failure and resume


# B10. Q1: a failed task drops the groups that needed it, and only those. No
# error: the run completes every other sample and exits 0.
def test_a_failed_task_drops_only_its_own_groups(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fail=["s1"])
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain.nf")
    assert_ok(result)

    survivors = {"s0", "s2"}
    result.members("p02", survivors)
    p03 = result.members("p03", survivors)
    for s in survivors:
        assert_slot(p03[s], 1, names={product(s, "bins")})


# B11. Q1: "on fix and resume, if full group is present, the group is now not
# dropped."
def test_rerunning_after_the_fix_forms_the_dropped_group(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fail=["s1"])
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")
    first = ws.run("linear_chain.nf")
    assert_ok(first)
    first.members("p03", {"s0", "s2"})

    ws.spec("p01", label="asm")
    second = ws.run("linear_chain.nf")
    assert_ok(second)

    p03 = second.members("p03", set(SAMPLES))
    assert_slot(p03["s1"], 1, names={product("s1", "bins")})


# ----------------------------------------------------- several cases in a run

# s1 and s2 are case A, short reads alone. s0 is case B, short and long reads.
# Each case has its own assembler, both posting into one assembly stream, and
# p03 bins every assembly. p04 serves both cases and p05 serves A alone.
CASES_HEAD = [
    "include { given; mock1 as p01; mock2 as p02; mock1 as p03; mock2 as p04; mock2 as p05 } from './mocks.nf'",
    "",
    "workflow {",
    "main:",
    "o = new Orchestrator(Channel.fromList([null]))",
    '_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)',
    "l = _lf.lineage",
    "o.seedParents(_lf.child2parent)",
    "o.seedCases(_lf.cases)",
]
LANES_SCRIPT = "\n".join(CASES_HEAD + [
    "_reads = (o.postIn([given(\"inputs/reads\", l)], [\"reads\"], ['A', 'B']))[0]",
    "_long = (o.postIn([given(\"inputs/long\", l)], [\"long\"], ['B']))[0]",
    "k = ['asm']",
    "__cases_1 = ['A']",
    "_asm_1 = (o.post(o.asStreams(p01(o.group('reads', [o.cases(_reads, __cases_1)], k, 1))), k, ['slot-asm-1']))[0]",
    "k = ['asm']",
    "__cases_2 = ['B']",
    "_asm_2 = (o.post(o.asStreams(p02(o.group('reads', [o.cases(_reads, __cases_2), o.cases(_long, __cases_2)], k, 1))), k, ['slot-asm-2']))[0]",
    "_asm = o.mix([_asm_1, _asm_2])",
    "k = ['bins']",
    "_bins = (o.post(o.asStreams(p03(o.group('asm', [_asm], k, 1))), k, ['slot-bins']))[0]",
    "k = ['qc']",
    "_qc = (o.post(o.asStreams(p04(o.group('reads', [_reads, _bins], k, 1))), k, ['slot-qc']))[0]",
    "k = ['aq']",
    "__cases_5 = ['A']",
    "_aq = (o.post(o.asStreams(p05(o.group('reads', [o.cases(_reads, __cases_5), o.cases(_bins, __cases_5)], k, 1))), k, ['slot-aq']))[0]",
    "o.seal()",
    "}",
]) + "\n"


def _two_lanes(ws):
    ws.given("reads", SAMPLES, cases={"s0": ["B"], "s1": ["A"], "s2": ["A"]})
    ws.given("long", ["l0"], parents={"reads": "s0"}, cases={"l0": ["B"]})
    ws.spec("p01", label="megahit")
    ws.spec("p02", label="hybrid", slow={SLOW: SLOW_S})
    ws.spec("p03", label="bins")
    ws.spec("p04", label="qc")
    ws.spec("p05", label="aq")
    result = ws.run(script=LANES_SCRIPT)
    assert_ok(result)
    assert_slow_task_was_slow(result, "p02", SLOW, SLOW_S)
    return result


# B17. A step two cases share, below a stream each case fills from its own
# assembler, starts a sample once that sample's own lane is done. The other
# case's assembler never delivers for it, so it must not be waited on.
def test_a_merged_step_starts_a_sample_once_its_own_lane_is_done(ws):
    result = _two_lanes(ws)
    slow = result.completed("p02", SLOW)
    for s in FAST:
        assert_runs_ahead(result, result.completed("p04", s), slow)
    p04 = result.members("p04", {"s0~B", "s1~A", "s2~A"})
    assert_slot(p04["s0~B"], 1, names={product("s0~B", "bins")})
    for s in FAST:
        assert_slot(p04[f"{s}~A"], 1, names={product(f"{s}~A", "bins")})


# B18. A step for one case reads through `o.cases` and starts as early as a
# step for every case.
def test_a_case_filtered_step_starts_a_sample_once_its_own_lane_is_done(ws):
    result = _two_lanes(ws)
    slow = result.completed("p02", SLOW)
    for s in FAST:
        assert_runs_ahead(result, result.completed("p05", s), slow)
    p05 = result.members("p05", {"s1~A", "s2~A"})
    for s in FAST:
        assert_slot(p05[f"{s}~A"], 1, names={product(f"{s}~A", "bins")})


# B19. s0 arrives with an assembly and is its own case, so the given post into
# the assembly stream serves only that case. The aligner starts every other
# sample as soon as that sample's assembly is done.
GIVEN_MIX_SCRIPT = "\n".join(CASES_HEAD + [
    "_reads = (o.postIn([given(\"inputs/reads\", l)], [\"reads\"], ['G', 'R']))[0]",
    "_asm_1 = (o.postIn([given(\"inputs/asm\", l)], [\"asm\"], ['G']))[0]",
    "k = ['asm']",
    "__cases_1 = ['R']",
    "_asm_2 = (o.post(o.asStreams(p01(o.group('reads', [o.cases(_reads, __cases_1)], k, 1))), k, ['slot-asm']))[0]",
    "_asm = o.mix([_asm_1, _asm_2])",
    "k = ['bam']",
    "_bam = (o.post(o.asStreams(p02(o.group('reads', [_reads, _asm], k, 1))), k, ['slot-bam']))[0]",
    "o.seal()",
    "}",
]) + "\n"


def test_a_given_post_into_a_mixed_stream_does_not_hold_other_cases_back(ws):
    ws.given("reads", SAMPLES, cases={"s0": ["G"], "s1": ["R"], "s2": ["R"]})
    ws.given("asm", ["a0"], parents={"reads": "s0"}, cases={"a0": ["G"]})
    ws.spec("p01", label="asm", slow={"s1": SLOW_S})
    ws.spec("p02", label="bam")
    result = ws.run(script=GIVEN_MIX_SCRIPT)
    assert_ok(result)

    slow = result.completed("p01", "s1")
    assert_slow_task_was_slow(result, "p01", "s1", SLOW_S)
    assert_runs_ahead(result, result.completed("p02", "s2"), slow)
    p02 = result.members("p02", {"s0~G", "s1~R", "s2~R"})
    assert_slot(p02["s0~G"], 1, names={"asm_a0.txt"})
    for s in ("s1", "s2"):
        assert_slot(p02[f"{s}~R"], 1, names={product(f"{s}~R", "asm")})


# B20. A fork task writes one of its two groups for s1, and a stream below
# both groups feeds a step grouped by sample. s1 starts once the group it wrote
# is done, without the close flush: the group it left empty posts a zero.
FORK_SCRIPT = "\n".join([
    "include { given; mock1_2 as p01; mock1 as p02; mock1 as p03; mock2 as p04 } from './mocks.nf'",
    "",
    "workflow {",
    "main:",
    "o = new Orchestrator(Channel.fromList([null]))",
    '_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)',
    "l = _lf.lineage",
    "o.seedParents(_lf.child2parent)",
    "_reads = (o.postIn([given(\"inputs/reads\", l)], [\"reads\"]))[0]",
    "k = ['asm', 'extra']",
    "(_asm, _extra) = o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm', 'slot-extra'])",
    "k = ['bins']",
    "_bins_1 = (o.post(o.asStreams(p02(o.group('asm', [_asm], k, 1))), k, ['slot-bins-1']))[0]",
    "k = ['bins']",
    "_bins_2 = (o.post(o.asStreams(p03(o.group('extra', [_extra], k, 1))), k, ['slot-bins-2']))[0]",
    "_bins = o.mix([_bins_1, _bins_2])",
    "k = ['qc']",
    "_qc = (o.post(o.asStreams(p04(o.group('reads', [_reads, _bins], k, 1))), k, ['slot-qc']))[0]",
    "o.seal()",
    "}",
]) + "\n"


def test_a_sample_that_left_a_group_empty_starts_once_its_written_group_is_done(ws):
    empty = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", empty=[empty], slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="xbins")
    ws.spec("p04", label="qc")
    result = ws.run(script=FORK_SCRIPT)
    assert_ok(result)

    assert_slow_task_was_slow(result, "p01", SLOW, SLOW_S)
    assert_runs_ahead(result, result.completed("p04", empty), result.completed("p01", SLOW))
    result.members("p03", set(SAMPLES) - {empty})
    p04 = result.members("p04", set(SAMPLES))
    assert_slot(p04[empty], 1, names={product(empty, "bins")})
    for s in set(SAMPLES) - {empty}:
        assert_slot(p04[s], 1, names={product(s, "bins"), product(s, "xbins")})
