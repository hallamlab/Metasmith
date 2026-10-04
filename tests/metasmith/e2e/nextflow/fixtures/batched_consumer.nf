// p02 folds `params.batch` ready samples into one task. Samples whose input
// is done batch together; the slow sample runs in whatever is left at close.
include { given; mock1 as p01; mock2 as p02 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm']
_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p02(o.group('reads', [_reads, _asm], k, params.batch))), k, ['slot-bins']))[0]
o.seal()
}
