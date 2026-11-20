// The producer is grouped by a key the consumer does not join on: p01 fans
// two asm files out of each sample, p02 runs once per asm file, and p03 --
// grouped by sample -- needs both of a sample's bins.
include { given; mock1 as p01; mock1 as p02; mock2 as p03 } from './mocks.nf'

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
_bins = (o.post(o.asStreams(p02(o.group('asm', [_asm], k, 1))), k, ['slot-bins']))[0]
k = ['qc']
_qc = (o.post(o.asStreams(p03(o.group('reads', [_reads, _bins], k, 1))), k, ['slot-qc']))[0]
o.seal()
}
