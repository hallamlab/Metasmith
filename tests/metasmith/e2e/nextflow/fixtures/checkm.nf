// The checkM shape: batching mixed with a group-by and a fan-out. The binner,
// grouped by each assembly, emits however many bins it finds. checkM is
// grouped by assembly, takes the assembly and all of its bins, and folds
// `params.batch` whole assembly groups into one task.
include { given; mock1 as p01; mock1 as binner; mock2 as checkm } from './mocks.nf'

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
_bins = (o.post(o.asStreams(binner(o.group('asm', [_asm], k, 1))), k, ['slot-bins']))[0]
k = ['checkm']
_checkm = (o.post(o.asStreams(checkm(o.group('asm', [_asm, _bins], k, params.batch))), k, ['slot-checkm']))[0]
o.seal()
}
