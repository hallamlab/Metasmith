# The short-read and hybrid study as one fork, in the shape codegen emits for
# it. The fetch step p01 writes {short} on branch 1 and {short, long} on branch
# 2, and each branch posts its own stream, so a product both groups share is
# one file with a pointer per written group. s0 writes branch 1 only, s1 branch
# 2 only, and s2 both. Each assembler reads its own branch, viral ID reads both
# assemblies, short-read stats are placed once per branch, and the vOTU step
# pools the whole study.

from tests.metasmith.e2e.nextflow.harness import assert_ok, assert_slot, product

SCRIPT = "\n".join([
    "include { given; mock1_2 as p01; mock1 as p02; mock1 as p03; mock1 as p04;"
    " mock1 as p05; mock1 as p06; mock2 as p07 } from './mocks.nf'",
    "",
    "workflow {",
    "main:",
    "o = new Orchestrator(Channel.fromList([null]))",
    '_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)',
    "l = _lf.lineage",
    "o.seedParents(_lf.child2parent)",
    '_study = (o.postIn([given("inputs/study", l)], ["study"]))[0]',
    '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
    "k = ['short', 'short2']",
    "(_short, _short2) = o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-short', 'slot-short2'])",
    "k = ['asm']",
    "_asm_1 = (o.post(o.asStreams(p02(o.group('short', [_short], k, 1))), k, ['slot-asm-1']))[0]",
    "k = ['asm']",
    "_asm_2 = (o.post(o.asStreams(p03(o.group('short2', [_short2], k, 1))), k, ['slot-asm-2']))[0]",
    "_asm = o.mix([_asm_1, _asm_2])",
    "k = ['vc']",
    "_vc = (o.post(o.asStreams(p04(o.group('asm', [_asm], k, 1))), k, ['slot-vc']))[0]",
    "k = ['st']",
    "_st_1 = (o.post(o.asStreams(p05(o.group('short', [_short], k, 1))), k, ['slot-st-1']))[0]",
    "k = ['st']",
    "_st_2 = (o.post(o.asStreams(p06(o.group('short2', [_short2], k, 1))), k, ['slot-st-2']))[0]",
    "_st = o.mix([_st_1, _st_2])",
    "k = ['vt']",
    "_vt = (o.post(o.asStreams(p07(o.group('study', [_study, _vc], k, 1))), k, ['slot-vt']))[0]",
    "o.seal()",
    "}",
]) + "\n"


def test_each_sample_runs_the_routes_of_the_groups_it_wrote(ws):
    ws.given("study", ["X"])
    ws.given("reads", ["s0", "s1", "s2"], parents={"study": "X"})
    ws.spec("p01", label="short", empty=["s0"], skip1=["s1"], share=True)
    for p, label in {"p02": "megahit", "p03": "hybrid", "p05": "st", "p06": "st"}.items():
        ws.spec(p, label=label)
    # Named by its own member, as a real member's key names its products, so
    # s2's two viral ID products do not share a name.
    ws.spec("p04", label="vc", by="asm")
    ws.spec("p07", label="vt", by="study")
    result = ws.run(script=SCRIPT)
    assert_ok(result)

    p02 = result.members("p02", {"s0", "s2"})
    p03 = result.members("p03", {"s1", "s2"})
    for s in ("s0", "s2"):
        assert_slot(p02[s], 0, names={product(s, "short")})
    for s in ("s1", "s2"):
        assert_slot(p03[s], 0, names={product(s, "short2", branch=2)})
    assert len(result.received("p04")) == 4
    result.members("p05", {"s0", "s2"})
    result.members("p06", {"s1", "s2"})
    p07 = result.members("p07", {"X"})
    assert_slot(p07["X"], 1, count=4)
