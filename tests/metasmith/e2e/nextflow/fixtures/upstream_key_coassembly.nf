// upstream_key_input.nf with a coassembly in place of the per-sample asm.
// p02, grouped by each sample, consumes the one coassembly, so every read set
// yields a stats item of that coassembly. p03, grouped by the coassembly,
// needs all of them, and no single read set's member can tell it so.
include { given; mock2 as p01; mock2 as p02; mock2 as p03 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_root = (o.postIn([given("inputs/cfg", l)], ["cfg"]))[0]
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['coasm']
_coasm = (o.post(o.asStreams(p01(o.group('cfg', [_root, _reads], k, 1))), k, ['slot-coasm']))[0]
k = ['stats']
_stats = (o.post(o.asStreams(p02(o.group('reads', [_reads, _coasm], k, 1))), k, ['slot-stats']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p03(o.group('coasm', [_coasm, _stats], k, 1))), k, ['slot-bins']))[0]
o.seal()
}
