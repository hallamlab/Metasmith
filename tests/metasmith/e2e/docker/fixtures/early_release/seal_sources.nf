// Every channel source kind the generated workflow uses, under one seal:
// the `one_null` sentinel handed to the constructor, a given read through
// fromPath + splitCsv, a given built with Channel.fromList, a reference
// handed over as Channel.value, and process outputs. Data flows only after
// the body finishes evaluating, so each operator must see the sealed state.
include { given; mock1 as p01; mock3 as p02; mock2 as p03 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]
_meta = (o.postIn([Channel.fromList(params.meta.collect { m -> [["__self__": [m.id], "reads": [m.reads]], file(m.path)] })], ["meta"]))[0]
_ref = (o.postIn([Channel.value([[:], file(params.ref)])], ["ref"]))[0]

k = ['asm']
_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads], k, 1))), k, ['slot-asm']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p02(o.group('reads', [_reads, _meta, _asm], k, 1))), k, ['slot-bins']))[0]
k = ['qc']
_qc = (o.post(o.asStreams(p03(o.group('bins', [_bins, _ref], k, 1))), k, ['slot-qc']))[0]
o.seal()
}
