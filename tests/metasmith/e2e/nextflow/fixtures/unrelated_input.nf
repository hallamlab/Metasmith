// `ref` is built from `cfg`, which shares no lineage with the samples. Every
// sample's p02 therefore takes every `ref` file, and cannot start until the
// slow `pu` that makes them has finished.
include { given; mock1 as p01; mock1 as pu; mock3 as p02 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_cfg = (o.postIn([given("inputs/cfg", l)], ["cfg"]))[0]
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm']
_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]
k = ['ref']
_ref = (o.post(o.asStreams(pu(o.group('cfg', [_cfg], k, 1))), k, ['slot-ref']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p02(o.group('reads', [_reads, _asm, _ref], k, 1))), k, ['slot-bins']))[0]
o.seal()
}
