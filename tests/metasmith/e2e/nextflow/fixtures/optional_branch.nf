// p01 has two optional output branches and emits nothing on the second for
// some samples. p02 consumes that second branch.
include { given; mock1_2 as p01; mock2 as p02 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm', 'extra']
(_asm, _extra) = o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm', 'slot-extra'])
k = ['qc']
_qc = (o.post(o.asStreams(p02(o.group('reads', [_reads, _extra], k, 1))), k, ['slot-qc']))[0]
o.seal()
}
