// A reference every sample is built against is a shared ancestor as near as
// the samples themselves. p01 and p02 each take one sample with the single
// reference `aref`. p03, grouped by the assembly, takes the stats and a
// per-sample metadata given, both SIBLING joins through `reads` and `aref`.
// `aref` sorts before `reads`, so a join keyed on the first nearest ancestor
// by name hands every assembly every sample's stats and metadata.
include { given; mock2 as p01; mock2 as p02; mock3 as p03 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]
_aref = (o.postIn([given("inputs/aref", l)], ["aref"]))[0]
_meta = (o.postIn([Channel.fromList(params.meta.collect { m -> [["__self__": [m.id], "reads": [m.reads], "aref": [m.aref]], file(m.path)] })], ["meta"]))[0]

k = ['asm']
_asm = (o.post(o.asStreams(p01(o.group('reads', [_reads, _aref], k, 1))), k, ['slot-asm']))[0]
k = ['stats']
_stats = (o.post(o.asStreams(p02(o.group('reads', [_reads, _aref], k, 1))), k, ['slot-stats']))[0]
k = ['qc']
_qc = (o.post(o.asStreams(p03(o.group('asm', [_asm, _stats, _meta], k, 1))), k, ['slot-qc']))[0]
o.seal()
}
