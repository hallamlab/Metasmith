# Two cases over one study, in the shape codegen emits for them. Case A has
# short reads, case B short and long reads. A short-read assembler serves A and
# a hybrid assembler serves B, both posting into one assembly stream. Viral ID
# serves both, the vOTU step pools both under the shared study, and checkV
# serves B alone, reading the pooled table through `o.cases`.
#
# A member's token is its by-hash and its cases (mocks.nf), so every assertion
# on a token is also an assertion on the tag the member carries.

import json
from pathlib import Path

from tests.metasmith.e2e.nextflow.harness import assert_ok, assert_slot, product, reads

HEAD = [
    "include { given; mock1 as p01; mock2 as p02; mock1 as p03; mock2 as p04; mock1 as p05;"
    " mock2 as p06; mock1 as p07; mock2 as p08 } from './mocks.nf'",
    "",
    "workflow {",
    "main:",
    "o = new Orchestrator(Channel.fromList([null]))",
    '_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)',
    "l = _lf.lineage",
    "o.seedParents(_lf.child2parent)",
    "o.seedCases(_lf.cases)",
    '_study = (o.postIn([given("inputs/study", l)], ["study"]))[0]',
    '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
    '_long = (o.postIn([given("inputs/long", l)], ["long"]))[0]',
    "k = ['asm']",
    "__cases_1 = ['A']",
    "_asm_1 = (o.post(o.asStreams(p01(o.group('reads', [o.cases(_reads, __cases_1)], k, 1))), k, ['slot-asm-1']))[0]",
    "k = ['asm']",
    "__cases_2 = ['B']",
    "_asm_2 = (o.post(o.asStreams(p02(o.group('reads', [o.cases(_reads, __cases_2), o.cases(_long, __cases_2)], k, 1))), k, ['slot-asm-2']))[0]",
    "_asm = o.mix([_asm_1, _asm_2])",
    "k = ['vc']",
    "_vc = (o.post(o.asStreams(p03(o.group('asm', [_asm], k, 1))), k, ['slot-vc']))[0]",
    "k = ['vt']",
    "_vt = (o.post(o.asStreams(p04(o.group('study', [_study, _vc], k, 1))), k, ['slot-vt']))[0]",
    "k = ['cv']",
    "__cases_5 = ['B']",
    "_cv = (o.post(o.asStreams(p05(o.group('vt', [o.cases(_vt, __cases_5)], k, 1))), k, ['slot-cv']))[0]",
]
TAIL = ["o.seal()", "}"]

# Per-read-set steps that read the assembly stream, as codegen emits them when
# both cases share one read set. The aligner's assembly slot is a lane: each
# case reads it from its own assembler.
SHARED_READS_STEPS = [
    "k = ['bam']",
    "(__miss_6, __hit_6) = o.group('reads', [_reads, _asm], k, 1, [cacheable: false, step: 6, step_name: 'align'], ['asm'])",
    "_bam = (o.post(o.asStreams(p06(__miss_6)), k, ['slot-bam']))[0]",
    "k = ['st']",
    "_st = (o.post(o.asStreams(p07(o.group('reads', [_reads], k, 1))), k, ['slot-st']))[0]",
    "k = ['q']",
    "_q = (o.post(o.asStreams(p08(o.group('asm', [_asm, _st], k, 1))), k, ['slot-q']))[0]",
]

SPECS = {
    "p01": dict(label="megahit"),
    "p02": dict(label="hybrid"),
    "p03": dict(label="vc"),
    "p04": dict(label="vt", by="study"),
    "p05": dict(label="cv", by="study"),
    "p06": dict(label="bam"),
    "p07": dict(label="st"),
    "p08": dict(label="q"),
}


def _script(extra: list[str] = ()) -> str:
    return "\n".join(HEAD + list(extra) + TAIL) + "\n"


def _study(ws, a_reads: str, b_reads: str) -> None:
    ws.given("study", ["X"], cases={"X": ["A", "B"]})
    shared = a_reads == b_reads
    ws.given(
        "reads", [a_reads] if shared else [a_reads, b_reads], parents={"study": "X"},
        cases={a_reads: ["A", "B"]} if shared else {a_reads: ["A"], b_reads: ["B"]},
    )
    ws.given("long", ["l"], parents={"reads": b_reads}, cases={"l": ["B"]})
    for p, spec in SPECS.items():
        ws.spec(p, **spec)


def test_the_cases_share_viral_id_and_pool_the_votu_table(ws):
    _study(ws, "rA", "rB")
    result = ws.run(script=_script())
    assert_ok(result)

    p01 = result.members("p01", {"rA~A"})
    assert_slot(p01["rA~A"], 0, names={reads("rA")})
    p02 = result.members("p02", {"rB~B"})
    assert_slot(p02["rB~B"], 0, names={reads("rB")})
    assert_slot(p02["rB~B"], 1, names={"long_l.txt"})

    p03 = result.members("p03", {"rA~A", "rB~B"})
    assert_slot(p03["rA~A"], 0, names={product("rA~A", "megahit")})
    assert_slot(p03["rB~B"], 0, names={product("rB~B", "hybrid")})

    p04 = result.members("p04", {"X~A+B"})
    assert_slot(p04["X~A+B"], 0, names={"study_X.txt"})
    assert_slot(p04["X~A+B"], 1, names={product("rA~A", "vc"), product("rB~B", "vc")})

    p05 = result.members("p05", {"X~B"})
    assert_slot(p05["X~B"], 0, names={product("X~A+B", "vt")})


def test_a_shared_read_set_runs_each_lane_once_and_keeps_the_lanes_apart(ws):
    _study(ws, "r", "r")
    result = ws.run(script=_script(SHARED_READS_STEPS))
    assert_ok(result)

    result.members("p01", {"r~A"})
    result.members("p02", {"r~B"})
    p03 = result.members("p03", {"r~A", "r~B"})
    assert_slot(p03["r~A"], 0, names={product("r~A", "megahit")})
    assert_slot(p03["r~B"], 0, names={product("r~B", "hybrid")})

    p04 = result.members("p04", {"X~A+B"})
    assert_slot(p04["X~A+B"], 1, names={product("r~A", "vc"), product("r~B", "vc")})

    p06 = result.members("p06", {"r~A", "r~B"})
    for token, lane in (("r~A", "megahit"), ("r~B", "hybrid")):
        assert_slot(p06[token], 0, names={reads("r")})
        assert_slot(p06[token], 1, names={product(token, lane)})

    p07 = result.members("p07", {"r~A+B"})
    p08 = result.members("p08", {"r~A", "r~B"})
    for token, lane in (("r~A", "megahit"), ("r~B", "hybrid")):
        assert_slot(p08[token], 0, names={product(token, lane)})
        assert_slot(p08[token], 1, names={product("r~A+B", "st")})
    assert p07


# A hit never runs its task, so the hit log is the only record of its cases.
def test_a_cache_hit_logs_the_cases_of_its_member(ws):
    _study(ws, "rA", "rB")
    ws.shard("rB", product("rB~B", "vc"))
    ws.params["helper"] = ["python3", ws.path("helpers", "cache_helper.py"), ws.path("shards"), "rB"]
    vc_line = next(i for i, ln in enumerate(HEAD) if ln.startswith("_vc = "))
    head = [ln.replace(" } from", "; mock_cached as p03_cached } from") for ln in HEAD[:vc_line]] + [
        "(__miss_3, __hit_3) = o.group('asm', [_asm], k, 1, [",
        "    tk: 'tk-vc', sig: 'sig-vc', slk: ['asm'],",
        f"    cache_root: \"{ws.path('shards')}\", cacheable: true,",
        f"    helper: params.helper, hits_log: \"{ws.path('hits.jsonl')}\",",
        "    step: 3, step_name: 'vc',",
        "])",
        "_vc = (o.post(o.mixOuts(o.asStreams(p03(__miss_3)), o.asStreams(p03_cached(__hit_3))), k, ['slot-vc']))[0]",
    ] + HEAD[vc_line + 1:]
    result = ws.run(script="\n".join(head + TAIL) + "\n")
    assert_ok(result)

    result.members("p03", {"rA~A"})
    result.members("p03_cached", {"rB"})
    (hit,) = [json.loads(ln) for ln in Path(ws.path("hits.jsonl")).read_text().splitlines()]
    assert hit["cases"] == ["B"]
    assert "CASES" not in hit["entry"]
