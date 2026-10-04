// The coassembly shape. p01 cleans each read set; p02, grouped by a root
// (`params.root`), assembles ALL read sets into one item that carries every
// reads hash; p03, grouped by that coassembly, consumes the clean reads --
// a SIBLING join through the shared `reads` ancestor. One coassembly, one
// member, both clean read sets.
include { given; mock1 as p01; mock2 as p02; mock2 as p03 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_root = (o.postIn([given("inputs/${params.root}", l)], [params.root]))[0]
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['clean']
_clean = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-clean']))[0]
k = ['coasm']
_coasm = (o.post(o.asStreams(p02(o.group(params.root, [_root, _reads], k, 1))), k, ['slot-coasm']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p03(o.group('coasm', [_coasm, _clean], k, 1))), k, ['slot-bins']))[0]
o.seal()
}
