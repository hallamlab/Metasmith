// A collecting step: p02 is grouped by the project root and consumes the
// per-sample products of p01, so its one member must hold every sample.
include { given; mock1 as p01; mock2 as p02 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_proj = (o.postIn([given("inputs/proj", l)], ["proj"]))[0]
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm']
_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]
k = ['pan']
_pan = (o.post(o.asStreams(p02(o.group('proj', [_proj, _asm], k, 1))), k, ['slot-pan']))[0]
o.seal()
}
