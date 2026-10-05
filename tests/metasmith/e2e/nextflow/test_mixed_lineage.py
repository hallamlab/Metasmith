# Joins across two lineage hops of givens, and o.mix of two producers whose
# inputs differ. Each workflow is the shape codegen emits on the agent for a
# plan the solver returns: givens posted parents first, each given row
# carrying its whole ancestor closure, and `child2parent` holding the edges.
#
# Within one case, the solver's Derived clause makes a product's lineage exactly
# what its step consumed plus those endpoints' own parents, and an Endpoint's
# signature folds that lineage in. So two producers of one endpoint share one
# ancestor closure, whatever each consumed directly. The mix tests below hold
# that and vary only what it leaves free: which streams each producer read,
# what it was grouped by, and whether a member ran from the cache. A stream
# that merges cases mixes producers of different lineage, which
# tests/metasmith/e2e/nextflow/test_cases.py covers.

import pytest

from tests.metasmith.e2e.nextflow.harness import (
    assert_ok,
    assert_runs_ahead,
    assert_slot,
    assert_slow_task_was_slow,
    product,
    reads,
)

SAMPLES = ["s0", "s1", "s2"]
SLOW = "s0"
FAST = ["s1", "s2"]
SLOW_S = 6

HEAD = [
    "workflow {",
    "main:",
    "o = new Orchestrator(Channel.fromList([null]))",
    '_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)',
    "l = _lf.lineage",
    "o.seedParents(_lf.child2parent)",
]
TAIL = ["o.seal()", "}"]


def _script(include: str, body: list[str]) -> str:
    return "\n".join([f"include {{ {include} }} from './mocks.nf'", "", *HEAD, *body, *TAIL]) + "\n"


def _given_chain(ws) -> None:
    # meta <- reads <- asm, all given. The agent loads the data library from
    # disk, and Load re-expands the stored parents into their closure, so each
    # asm row names its read set AND the metadata above it.
    ws.given("meta", ["m"])
    ws.given("reads", SAMPLES, parents={"meta": "m"})
    ws.given("asm", [f"a{s[1:]}" for s in SAMPLES])
    ws.lineage["inputs/asm"] = [{"meta": ["m"], "reads": [s], "__self__": [f"a{s[1:]}"]} for s in SAMPLES]
    ws.child2parent["asm"] = {"reads", "meta"}


GIVEN_CHAIN = [
    '_meta = (o.postIn([given("inputs/meta", l)], ["meta"]))[0]',
    '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
    '_asm = (o.postIn([given("inputs/asm", l)], ["asm"]))[0]',
]


# ------------------------------------------------ a given two hops below the key


# A step grouped by the sample metadata that collects every assembly given
# beneath it. The solver plans this from a transform requiring `meta` and
# `asm` anchored to it, over a library of meta <- reads <- asm.
def test_a_step_grouped_by_a_grandparent_given_collects_every_descendant(ws):
    _given_chain(ws)
    ws.spec("p01", label="out", by="meta")

    result = ws.run(script=_script("given; mock2 as p01", GIVEN_CHAIN + [
        "k = ['out']",
        "_out = (o.post(o.asStreams(p01(o.group('meta', [_meta, _asm], k, 1))), k, ['slot-out']))[0]",
    ]))
    assert_ok(result)

    p01 = result.members("p01", {"m"})
    assert_slot(p01["m"], 1, names={f"asm_a{s[1:]}.txt" for s in SAMPLES})


# The mirror: a step grouped by each given assembly that takes the sample
# metadata two hops above it.
def test_a_step_grouped_by_a_given_takes_its_grandparent_given(ws):
    _given_chain(ws)
    ws.spec("p01", label="out")

    result = ws.run(script=_script("given; mock2 as p01", GIVEN_CHAIN + [
        "k = ['out']",
        "_out = (o.post(o.asStreams(p01(o.group('asm', [_asm, _meta], k, 1))), k, ['slot-out']))[0]",
    ]))
    assert_ok(result)

    p01 = result.members("p01", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p01[s], 0, names={f"asm_a{s[1:]}.txt"})
        assert_slot(p01[s], 1, names={"meta_m.txt"})


# ------------------------------------------- o.mix of producers that differ


# A per-sample assembly and a coassembly of the same type, both read from
# [reads, meta]. One is grouped by the read set, one by the metadata, so the
# mixed stream holds items carrying one read-set hash and an item carrying
# all three. Each sample's consumer takes its own assembly and the
# coassembly, and the coassembly's route makes every key wait for close.
def test_a_per_sample_and_a_coassembly_producer_merged_reach_each_sample_once(ws):
    ws.given("meta", ["m"])
    ws.given("reads", SAMPLES, parents={"meta": "m"})
    ws.spec("pa", label="asm", slow={SLOW: SLOW_S})
    ws.spec("pb", label="coasm")
    ws.spec("pc", label="bins")
    ws.spec("pd", label="cobins")

    result = ws.run(script=_script("given; mock2 as pa; mock2 as pb; mock2 as pc; mock2 as pd", [
        '_meta = (o.postIn([given("inputs/meta", l)], ["meta"]))[0]',
        '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
        "k = ['asm']",
        "_asm_1 = (o.post(o.asStreams(pa(o.group('reads', [_reads, _meta], k, 1))), k, ['slot-asm-1']))[0]",
        "k = ['asm']",
        "_asm_2 = (o.post(o.asStreams(pb(o.group('meta', [_meta, _reads], k, 1))), k, ['slot-asm-2']))[0]",
        "_asm = o.mix([_asm_1, _asm_2])",
        "k = ['bins']",
        "_bins = (o.post(o.asStreams(pc(o.group('reads', [_reads, _asm], k, 1))), k, ['slot-bins']))[0]",
        "k = ['cobins']",
        "_cobins = (o.post(o.asStreams(pd(o.group('asm', [_asm, _reads], k, 1))), k, ['slot-cobins']))[0]",
    ]))
    assert_ok(result)

    co = "s0+s1+s2"
    pc = result.members("pc", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(pc[s], 0, names={reads(s)})
        assert_slot(pc[s], 1, names={product(s, "asm"), product(co, "coasm")})
    pd = result.members("pd", set(SAMPLES) | {co})
    for s in SAMPLES:
        assert_slot(pd[s], 0, names={product(s, "asm")})
        assert_slot(pd[s], 1, names={reads(s)})
    assert_slot(pd[co], 0, names={product(co, "coasm")})
    assert_slot(pd[co], 1, names={reads(s) for s in SAMPLES})


# Two producers of one type at different depths below the sample. `pa` is
# grouped by each assembly and reads only it; `pb` is grouped by the read set
# and reads it with the assembly. Both confer {asm, reads}, so codegen merges
# them. `pa` fans out per sample; the consumer, grouped by the read set,
# starts once its own sample's items from both producers are in.
@pytest.mark.parametrize("slow_in", ["pa", "pb"])
def test_producers_at_different_depths_merged_release_each_sample_early(ws, slow_in):
    fans = {"s0": 1, "s1": 3, "s2": 2}
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("pa", label="ta", fan=fans, **({"slow": {SLOW: SLOW_S}} if slow_in == "pa" else {}))
    ws.spec("pb", label="tb", **({"slow": {SLOW: SLOW_S}} if slow_in == "pb" else {}))
    ws.spec("pc", label="out")

    result = ws.run(script=_script("given; mock1 as p01; mock1 as pa; mock2 as pb; mock2 as pc", [
        '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
        "k = ['asm']",
        "_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]",
        "k = ['t']",
        "_t_1 = (o.post(o.asStreams(pa(o.group('asm', [_asm], k, 1))), k, ['slot-t-1']))[0]",
        "k = ['t']",
        "_t_2 = (o.post(o.asStreams(pb(o.group('reads', [_reads, _asm], k, 1))), k, ['slot-t-2']))[0]",
        "_t = o.mix([_t_1, _t_2])",
        "k = ['out']",
        "_out = (o.post(o.asStreams(pc(o.group('reads', [_reads, _t], k, 1))), k, ['slot-out']))[0]",
    ]))
    assert_ok(result)

    slow = result.completed(slow_in, SLOW)
    assert_slow_task_was_slow(result, slow_in, SLOW, SLOW_S)
    for s in FAST:
        assert_runs_ahead(result, result.completed("pc", s), slow)
    pc = result.members("pc", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(pc[s], 1, names={product(s, "ta", item=i + 1) for i in range(fans[s])} | {product(s, "tb")})


# The same merge with `pb` routed through the member cache: one sample's
# member is a hit served by the `_cached` twin, joined by o.mixOuts before
# its one post, and the merged stream still releases per sample.
def test_a_merged_producer_served_from_the_cache_still_releases_each_sample(ws):
    hit = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("pa", label="ta", slow={SLOW: SLOW_S})
    ws.spec("pb", label="tb")
    ws.spec("pc", label="out")
    ws.shard(hit, product(hit, "tb"))
    ws.params["cacheable"] = True
    ws.params["helper"] = ["python3", ws.path("helpers", "cache_helper.py"), ws.path("shards"), hit]

    result = ws.run(script=_script(
        "given; mock1 as p01; mock1 as pa; mock2 as pb; mock_cached as pb_cached; mock2 as pc", [
            '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
            "k = ['asm']",
            "_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]",
            "k = ['t']",
            "_t_1 = (o.post(o.asStreams(pa(o.group('asm', [_asm], k, 1))), k, ['slot-t-1']))[0]",
            "k = ['t']",
            "(__miss_2, __hit_2) = o.group('reads', [_reads, _asm], k, 1, [",
            "    tk: 'tk-pb', sig: 'sig-pb', slk: ['reads', 'asm'],",
            f"    cache_root: \"{ws.path('shards')}\", cacheable: params.cacheable,",
            f"    helper: params.helper, hits_log: \"{ws.path('hits.jsonl')}\",",
            "    step: 2, step_name: 'pb',",
            "])",
            "__out_2 = o.mixOuts(o.asStreams(pb(__miss_2)), o.asStreams(pb_cached(__hit_2)))",
            "_t_2 = (o.post(__out_2, k, ['slot-t-2']))[0]",
            "_t = o.mix([_t_1, _t_2])",
            "k = ['out']",
            "_out = (o.post(o.asStreams(pc(o.group('reads', [_reads, _t], k, 1))), k, ['slot-out']))[0]",
        ]))
    assert_ok(result)

    slow = result.completed("pa", SLOW)
    assert_slow_task_was_slow(result, "pa", SLOW, SLOW_S)
    result.members("pb_cached", {hit})
    result.members("pb", set(SAMPLES) - {hit})
    for s in FAST:
        assert_runs_ahead(result, result.completed("pc", s), slow)
    pc = result.members("pc", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(pc[s], 1, names={product(s, "ta"), product(s, "tb")})
