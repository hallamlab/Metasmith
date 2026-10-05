# A plan where s0 arrives with an assembly and s1, s2 with reads alone, in the
# shape codegen emits for it: the assembler reads the read sets through
# `o.exclude`, which drops every item whose lineage names a sample it does not
# serve, and the aligner reads the given and the assembled assemblies as one
# mixed stream.

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
    '_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]',
    '_asm_1 = (o.postIn([given("inputs/asm", l)], ["asm"]))[0]',
    "k = ['asm']",
    "__excl_1 = ['a0', 's0']",
    "_asm_2 = (o.post(o.asStreams(p01(o.group('reads', [o.exclude(_reads, __excl_1)], k, 1))), k, ['slot-asm']))[0]",
    "_asm = o.mix([_asm_1, _asm_2])",
    "k = ['bam']",
    "_bam = (o.post(o.asStreams(p02(o.group('asm', [_reads, _asm], k, 1))), k, ['slot-bam']))[0]",
    "o.seal()",
    "}",
]) + "\n"


def test_a_given_assembly_is_used_and_the_rest_are_assembled(ws):
    ws.given("reads", SAMPLES)
    ws.given("asm", ["a0"], parents={"reads": "s0"})
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bam")

    result = ws.run(script=SCRIPT)
    assert_ok(result)

    p01 = result.members("p01", {"s1", "s2"})
    for s in ("s1", "s2"):
        assert_slot(p01[s], 0, names={reads(s)})
    p02 = result.members("p02", set(SAMPLES))
    assert_slot(p02["s0"], 0, names={reads("s0")})
    assert_slot(p02["s0"], 1, names={"asm_a0.txt"})
    for s in ("s1", "s2"):
        assert_slot(p02[s], 0, names={reads(s)})
        assert_slot(p02[s], 1, names={product(s, "asm")})
