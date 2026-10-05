// An input grouped by an ancestor of the consumer's key. p02 is grouped by
// the sample but consumes the sample's asm, as assembly_stats is; p03 is
// grouped by the asm and takes p02's output. Each stats item descends from
// exactly one asm, through a member keyed by that asm's one read set.
include { given; mock1 as p01; mock2 as p02; mock2 as p03 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm']
_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]
k = ['stats']
_stats = (o.post(o.asStreams(p02(o.group('reads', [_reads, _asm], k, 1))), k, ['slot-stats']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p03(o.group('asm', [_asm, _stats], k, 1))), k, ['slot-bins']))[0]
o.seal()
}
