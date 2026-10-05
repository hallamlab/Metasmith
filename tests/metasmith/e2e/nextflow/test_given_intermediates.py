# A plan where s0 arrives with an assembly and s1, s2 with reads alone, in the
# shape codegen emits for it: the two shapes are two cases, the assembler
# serves only the reads-alone case and reads the read sets through `o.cases`,
# and the aligner serves both and reads the given and the assembled assemblies
# as one mixed stream.

from tests.metasmith.e2e.nextflow.harness import assert_ok, assert_slot, product, reads

SAMPLES = ["s0", "s1", "s2"]

SCRIPT = "\n".join([
    "include { given; mock1 as p01; mock2 as p02 } from './mocks.nf'",
    "",
    "workflow {",
    "main:",
    "o = new Orchestrator(Channel.fromList([null]))",
    '_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)',
    "l = _lf.lineage",
    "o.seedParents(_lf.child2parent)",
    "o.seedCases(_lf.cases)",
    '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
    '_asm_1 = (o.postIn([given("inputs/asm", l)], ["asm"]))[0]',
    "k = ['asm']",
    "__cases_1 = ['reads']",
    "_asm_2 = (o.post(o.asStreams(p01(o.group('reads', [o.cases(_reads, __cases_1)], k, 1))), k, ['slot-asm']))[0]",
    "_asm = o.mix([_asm_1, _asm_2])",
    "k = ['bam']",
    "_bam = (o.post(o.asStreams(p02(o.group('asm', [_reads, _asm], k, 1))), k, ['slot-bam']))[0]",
    "o.seal()",
    "}",
]) + "\n"


def test_a_given_assembly_is_used_and_the_rest_are_assembled(ws):
    ws.given("reads", SAMPLES, cases={"s0": ["asm+reads"], "s1": ["reads"], "s2": ["reads"]})
    ws.given("asm", ["a0"], parents={"reads": "s0"}, cases={"a0": ["asm+reads"]})
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bam")

    result = ws.run(script=SCRIPT)
    assert_ok(result)

    assembled = {s: f"{s}~reads" for s in ("s1", "s2")}
    p01 = result.members("p01", set(assembled.values()))
    for s, token in assembled.items():
        assert_slot(p01[token], 0, names={reads(s)})
    p02 = result.members("p02", {"s0~asm+reads", *assembled.values()})
    assert_slot(p02["s0~asm+reads"], 0, names={reads("s0")})
    assert_slot(p02["s0~asm+reads"], 1, names={"asm_a0.txt"})
    for s, token in assembled.items():
        assert_slot(p02[token], 0, names={reads(s)})
        assert_slot(p02[token], 1, names={product(token, "asm")})
