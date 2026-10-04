// Aggregate then distribute: p02 collects every sample's asm into one member
// whose products carry ALL the sample hashes; p03, grouped by sample again,
// must hand every one of those products to every sample.
include { given; mock1 as p01; mock2 as p02; mock2 as p03 } from './mocks.nf'

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
k = ['merged']
_merged = (o.post(o.asStreams(p02(o.group('proj', [_proj, _asm], k, 1))), k, ['slot-merged']))[0]
k = ['dist']
_dist = (o.post(o.asStreams(p03(o.group('reads', [_reads, _merged], k, 1))), k, ['slot-dist']))[0]
o.seal()
}
