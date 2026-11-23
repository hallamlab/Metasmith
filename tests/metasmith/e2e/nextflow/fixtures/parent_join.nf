// PARENT_OF_BY joins: the by-stream descends from the stream joined in.
// p02, grouped by each sample's asm, takes that sample's raw reads -- one
// parent hash per by-item. p04, grouped by the coassembly p03 built from
// every read set, takes the raw reads again -- every sample's hash on one
// by-item, so its one member must hold every read set.
include { given; mock1 as p01; mock2 as p02; mock2 as p03; mock2 as p04 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_root = (o.postIn([given("inputs/cfg", l)], ["cfg"]))[0]
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm']
_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p02(o.group('asm', [_asm, _reads], k, 1))), k, ['slot-bins']))[0]
k = ['coasm']
_coasm = (o.post(o.asStreams(p03(o.group('cfg', [_root, _reads], k, 1))), k, ['slot-coasm']))[0]
k = ['cobins']
_cobins = (o.post(o.asStreams(p04(o.group('coasm', [_coasm, _reads], k, 1))), k, ['slot-cobins']))[0]
o.seal()
}
