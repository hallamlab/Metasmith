// The dastool shape. Three binners, each grouped by the sample's assembly,
// emit however many bins they find, and codegen merges them into one `bins`
// stream. QC runs once per bin and emits however many files it writes. The
// scorer, grouped by sample, needs every QC file of every bin of every
// binner, and nothing in the plan says how many that is.
include { given; mock1 as p01; mock1 as b1; mock1 as b2; mock1 as b3; mock1 as qc; mock2 as score } from './mocks.nf'

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
_bins_1 = (o.post(o.asStreams(b1(o.group('asm', [_asm], k, 1))), k, ['slot-bins-1']))[0]
k = ['bins']
_bins_2 = (o.post(o.asStreams(b2(o.group('asm', [_asm], k, 1))), k, ['slot-bins-2']))[0]
k = ['bins']
_bins_3 = (o.post(o.asStreams(b3(o.group('asm', [_asm], k, 1))), k, ['slot-bins-3']))[0]
_bins = o.mix([_bins_1, _bins_2, _bins_3])
k = ['qc']
_qc = (o.post(o.asStreams(qc(o.group('bins', [_bins], k, 1))), k, ['slot-qc']))[0]
k = ['score']
_score = (o.post(o.asStreams(score(o.group('reads', [_reads, _qc], k, 1))), k, ['slot-score']))[0]
o.seal()
}
