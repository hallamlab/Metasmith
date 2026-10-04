// Two steps produce the same type and codegen merges them with o.mix. The
// consumer's `asm` stream therefore has two producing posts and every sample's
// member needs a product from each.
include { given; mock1 as p01a; mock1 as p01b; mock2 as p02 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm']
_asm_a = (o.post(o.asStreams(p01a(o.group('reads', [_reads], k, 1))), k, ['slot-asm-a']))[0]
_asm_b = (o.post(o.asStreams(p01b(o.group('reads', [_reads], k, 1))), k, ['slot-asm-b']))[0]
_asm = o.mix([_asm_a, _asm_b])
k = ['bins']
_bins = (o.post(o.asStreams(p02(o.group('reads', [_reads, _asm], k, 1))), k, ['slot-bins']))[0]
o.seal()
}
