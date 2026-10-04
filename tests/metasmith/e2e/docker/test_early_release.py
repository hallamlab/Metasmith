# Black-box tests of Orchestrator early release. Each test runs a hand-written
# workflow from e2e/nextflow/fixtures/ against the real Orchestrator.groovy in
# the repo's docker image, with mock processes whose behaviour depends on the
# sample they receive. No planner, no codegen, no reading of the Groovy: the
# evidence is the Nextflow trace, what each task appended to the receive log,
# and the run's exit status and error text.
#
# Every timing claim is an ORDERING relation with a 20 s sleep behind it, never
# an absolute threshold, so scheduler noise cannot flip it.

import pytest

from tests.metasmith.e2e.nextflow.harness import (
    Workspace,
    assert_finished,
    assert_ok,
    assert_slot,
    assert_started_before,
    assert_slow_task_was_slow as _assert_slow_task_was_slow,
    product as _product,
    reads as _reads,
)

pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]

SAMPLES = ["s0", "s1", "s2"]
SLOW = "s0"
FAST = [s for s in SAMPLES if s != SLOW]
SLOW_S = 20


def assert_slow_task_was_slow(result, process: str, tag: str) -> None:
    _assert_slow_task_was_slow(result, process, tag, SLOW_S)


@pytest.fixture
def ws(tmp_path, docker_image) -> Workspace:
    return Workspace(tmp_path / "ws", docker_image)



# ---------------------------------------------------------------- early release happens


def test_fast_samples_run_ahead_of_a_slow_sample(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain.nf")
    assert_ok(result)

    assert_slow_task_was_slow(result, "p01", SLOW)
    for s in FAST:
        assert_started_before(result, ("p02", s), ("p01", SLOW))
        assert_started_before(result, ("p03", s), ("p01", SLOW))

    p02 = result.members("p02", set(SAMPLES))
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={_product(s, "asm")})
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


def test_fast_samples_run_ahead_through_a_cache_twin(ws):
    hit = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")
    ws.shard(hit, _product(hit, "bins"))
    ws.params["cacheable"] = True
    ws.params["helper"] = ["python3", ws.path("helpers", "cache_helper.py"), ws.path("shards"), hit]

    result = ws.run("cache_twin.nf")
    assert_ok(result)

    assert_slow_task_was_slow(result, "p01", SLOW)
    for s in FAST:
        assert_started_before(result, ("p03", s), ("p01", SLOW))

    result.members("p02_cached", {hit})
    result.members("p02", set(SAMPLES) - {hit})
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


def test_a_finished_batch_runs_ahead_of_an_unfinished_one(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.params["batch"] = 2

    result = ws.run("batched_producer.nf")
    assert_ok(result)

    p01_tasks = result.tasks("p01", status="COMPLETED")
    assert len(p01_tasks) == 2, f"batch_size 2 over 3 samples is 2 tasks, got {[(t.tag) for t in p01_tasks]}"
    slow_batch = next(t for t in p01_tasks if SLOW in t.tag.split(","))
    fast_batch = next(t for t in p01_tasks if SLOW not in t.tag.split(","))
    assert_slow_task_was_slow(result, "p01", slow_batch.tag)
    for s in fast_batch.tag.split(","):
        assert_started_before(result, ("p02", s), ("p01", slow_batch.tag))

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, count=1, token=s)


# ------------------------------------------- whole members where release must wait


def test_fan_out_member_holds_every_file(ws):
    fan = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fan={fan: 3})
    ws.spec("p02", label="bins")
    ws.params["batch"] = 1

    result = ws.run("batched_producer.nf")
    assert_ok(result)

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        n = 3 if s == fan else 1
        assert_slot(p02[s], 1, names={_product(s, "asm", item=i + 1) for i in range(n)})


def test_a_fanned_out_member_runs_ahead_once_its_last_file_arrives(ws):
    fan = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S}, fan={fan: 3})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain.nf")
    assert_ok(result)

    assert_slow_task_was_slow(result, "p01", SLOW)
    assert_started_before(result, ("p02", fan), ("p01", SLOW))
    p02 = result.members("p02", set(SAMPLES))
    assert_slot(p02[fan], 1, names={_product(fan, "asm", item=i + 1) for i in range(3)})


def test_collector_gets_one_whole_member(ws):
    ws.given("proj", ["p"])
    ws.given("reads", SAMPLES, parents={"proj": "p"})
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="pan", by="proj")

    result = ws.run("collector.nf")
    assert_ok(result)

    p02 = result.members("p02", {"p"})
    assert_slot(p02["p"], 0, names={"proj_p.txt"})
    assert_slot(p02["p"], 1, names={_product(s, "asm") for s in SAMPLES})
    collector = result.completed("p02", "+".join(SAMPLES))
    slow = result.completed("p01", SLOW)
    assert collector.start >= slow.complete, "the collector ran before its slowest input existed"


def test_aggregate_then_distribute_hands_every_product_to_every_sample(ws):
    ws.given("proj", ["p"])
    ws.given("reads", SAMPLES, parents={"proj": "p"})
    ws.spec("p01", label="asm")
    ws.spec("p02", label="merged", by="proj", fan={"+".join(SAMPLES): 2})
    ws.spec("p03", label="dist")

    result = ws.run("aggregate_distribute.nf")
    assert_ok(result)

    result.members("p02", {"p"})
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, names={_product("p", "merged", item=1), _product("p", "merged", item=2)})


def test_two_producers_mixed_into_one_stream_both_reach_every_member(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01a", label="asm")
    ws.spec("p01b", label="asmb", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")

    result = ws.run("mixed_producers.nf")
    assert_ok(result)

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={_product(s, "asm"), _product(s, "asmb")})


def test_producer_grouped_by_another_key_waits_for_every_item(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fan={s: 2 for s in SAMPLES})
    ws.spec("p02", label="bins", by="asm", slow_in={rf".*-2-1\.{SLOW}-asm\.out": SLOW_S})
    ws.spec("p03", label="qc")

    result = ws.run("other_key_producer.nf")
    assert_ok(result)

    assert len(result.received("p02")) == 6, "one p02 task per asm file"
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, count=2)
        feeders = [r for r in result.received("p02") if r.slots[0] and f".{s}-asm" in r.slots[0][0]]
        assert {r.tokens[0] for r in feeders} == {
            n.split(".", 1)[1].rsplit("-", 1)[0] for n in p03[s].slots[1]
        }, f"p03 ({s}) did not receive exactly the bins of its own two asm files"


# ----------------------------------------------------------------------- no hangs


def test_a_dropped_task_does_not_hang_the_other_samples(ws):
    dropped = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fail=[dropped])
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain.nf")
    assert_ok(result)

    failed = result.tasks("p01", dropped, "FAILED")
    assert len(failed) == 2 and all(t.exit == "1" for t in failed), (
        f"p01 ({dropped}) should fail twice (retry, then ignore), got {[(t.status, t.attempt) for t in result.tasks('p01', dropped)]}"
    )
    survivors = set(SAMPLES) - {dropped}
    p02 = result.members("p02", survivors)
    p03 = result.members("p03", survivors)
    for s in survivors:
        assert_slot(p02[s], 1, names={_product(s, "asm")})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


def test_an_empty_optional_branch_does_not_hang_the_other_samples(ws):
    empty = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", empty=[empty])
    ws.spec("p02", label="qc")

    result = ws.run("optional_branch.nf")
    assert_ok(result)

    others = set(SAMPLES) - {empty}
    p02 = result.members("p02", others)
    for s in others:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={_product(s, "asm2", branch=2)})


def test_a_broken_cache_probe_runs_every_member(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")
    ws.params["cacheable"] = True
    ws.params["helper"] = ["bash", ws.path("helpers", "broken_helper.sh")]

    result = ws.run("cache_twin.nf")
    assert_ok(result)

    assert "helper" in result.output, "a failed cache probe must be reported, not swallowed"
    assert not result.received("p02_cached"), "nothing can be a hit when the probe cannot answer"
    p02 = result.members("p02", set(SAMPLES))
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 1, names={_product(s, "asm")})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


# ------------------------------------------------------- SIBLING with two ancestors


@pytest.mark.parametrize("root", ["cfg", "proj"])
@pytest.mark.parametrize("order", [["r1", "r2"], ["r2", "r1"]])
def test_coassembly_sibling_join_emits_one_whole_member(ws, root, order):
    # `cfg` has no lineage relation to the reads, so `reads` is the ONLY
    # ancestor the clean reads and the coassembly share. `proj` is a real
    # parent of the reads, so both it and `reads` are shared and the join
    # join must hold whichever of them it buckets on.
    ws.given(root, ["c"])
    ws.given("reads", order, parents={"proj": "c"} if root == "proj" else None)
    ws.spec("p01", label="clean")
    ws.spec("p02", label="coasm", by=root)
    ws.spec("p03", label="bins", by="coasm")
    ws.params["root"] = root

    result = ws.run("sibling_coassembly.nf")
    assert_ok(result)

    coasm = result.members("p02", {"c"})
    assert_slot(coasm["c"], 1, names={_reads(r) for r in order})
    p03 = result.received("p03")
    assert len(p03) == 1, (
        f"one coassembly must yield ONE member, got {len(p03)}: "
        f"{[r.slots for r in p03]}. Two members with one clean read each is the "
        f"SIBLING by-side fan-out bug.\n{result.tail}"
    )
    assert_slot(p03[0], 0, names={_product("c", "coasm")})
    assert_slot(p03[0], 1, names={_product(r, "clean") for r in order})


def test_a_parent_join_holds_every_parent_the_by_item_names(ws):
    ws.given("cfg", ["c"])
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="coasm", by="cfg")
    ws.spec("p04", label="cobins", by="cfg")

    result = ws.run("parent_join.nf")
    assert_ok(result)

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_product(s, "asm")})
        assert_slot(p02[s], 1, names={_reads(s)})
    assert_slow_task_was_slow(result, "p01", SLOW)
    for s in FAST:
        assert_started_before(result, ("p02", s), ("p01", SLOW))

    p04 = result.members("p04", {"c"})
    assert_slot(p04["c"], 0, names={_product("c", "coasm")})
    assert_slot(p04["c"], 1, names={_reads(s) for s in SAMPLES})


def test_a_shared_reference_does_not_mix_samples_in_a_sibling_join(ws):
    # Every assembly and every stats file descend from both their sample and
    # the one reference, so `reads` and `aref` are equally near shared
    # ancestors. The stats are released per sample on their `reads` stamps.
    # The metadata given carries no stamp, so its join has no producer key to
    # prefer and must still keep each sample to its own file.
    ws.given("reads", SAMPLES)
    ws.given("aref", ["r"])
    ws.child2parent["meta"] = {"reads", "aref"}
    meta = []
    for s in SAMPLES:
        p = ws.root / "inputs" / f"meta_{s}.txt"
        p.write_text(s)
        meta.append({"id": f"m-{s}", "reads": s, "aref": "r", "path": ws.path("inputs", p.name)})
    ws.params["meta"] = meta
    ws.spec("p01", label="asm")
    ws.spec("p02", label="stats", slow={SLOW: SLOW_S})
    ws.spec("p03", label="qc")

    result = ws.run("shared_reference.nf")
    assert_ok(result)

    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={_product(s, "asm")})
        assert_slot(p03[s], 1, names={_product(s, "stats")})
        assert_slot(p03[s], 2, names={f"meta_{s}.txt"})
    assert_slow_task_was_slow(result, "p02", SLOW)
    for s in FAST:
        assert_started_before(result, ("p03", s), ("p02", SLOW))


# --------------------------------------------------------------------------- seal


def test_an_unsealed_workflow_refuses_to_run(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain_unsealed.nf")
    assert_finished(result)

    assert result.returncode != 0, (
        "a workflow body that never calls o.seal() ran to completion; every "
        "registry read happened before a seal that never came, and that must be "
        f"a loud failure, not a quiet count.\n{result.tail}"
    )
    assert "seal" in result.output.lower(), f"the refusal must name the seal\n{result.tail}"


def test_every_source_kind_sees_the_seal(ws):
    ws.given("reads", SAMPLES)
    ws.child2parent["meta"] = {"reads"}
    meta = []
    for s in SAMPLES:
        p = ws.root / "inputs" / f"meta_{s}.txt"
        p.write_text(s)
        meta.append({"id": f"m-{s}", "reads": s, "path": ws.path("inputs", p.name)})
    (ws.root / "inputs" / "ref.txt").write_text("ref")
    ws.params["meta"] = meta
    ws.params["ref"] = ws.path("inputs", "ref.txt")
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc", by="bins")

    result = ws.run("seal_sources.nf")
    assert_ok(result)

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={f"meta_{s}.txt"})
        assert_slot(p02[s], 2, names={_product(s, "asm")})
    p03 = result.received("p03")
    assert len(p03) == 3 and len({r.token for r in p03}) == 3, f"one p03 member per bins item, got {p03}"
    assert {r.slots[0][0] for r in p03} == {_product(s, "bins") for s in SAMPLES}
    for r in p03:
        assert_slot(r, 1, names={"ref.txt"})


# ------------------------------------------------------------- late item is a crash


def test_a_late_item_for_a_released_key_crashes_the_run(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.params["late_ms"] = 10000

    result = ws.run("late_item.nf")
    assert_finished(result)

    assert result.returncode != 0, (
        "a second, distinct `asm` item arrived for every sample after its member "
        "was released, and the run still exited 0. Under the sibling-stamp "
        "invariant that item cannot exist, so it must stop the run.\n"
        f"p02 received: {[r.slots for r in result.received('p02')]}\n{result.tail}"
    )
    for needle, what in (("[asm]", "the stream"), ("[reads]", "the by-key")):
        assert needle in result.output, f"the error must name {what} ({needle})\n{result.tail}"
    seen: dict[str, int] = {}
    for r in result.received("p02"):
        seen[r.token] = seen.get(r.token, 0) + 1
        assert not any(n.endswith("-late.out") for n in r.slots[1]), (
            f"a late item was folded into a member instead of stopping the run: {r.slots[1]}"
        )
    assert all(n == 1 for n in seen.values()), (
        f"a late item was emitted as a second member: {seen}"
    )
