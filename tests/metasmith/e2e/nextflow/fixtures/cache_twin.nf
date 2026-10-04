// linear_chain.nf with p02 routed through the member cache: hits run in the
// `_cached` twin, misses in the real process, and o.mixOuts joins the two
// before ONE o.post -- so downstream still sees one producer of `bins`.
// `params.helper` is the probe command; `params.cacheable` is the step flag.
include { given; mock1 as p01; mock2 as p02; mock_cached as p02_cached; mock2 as p03 } from './mocks.nf'

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
(__miss_2, __hit_2) = o.group('reads', [_reads, _asm], k, 1, [
    tk: 'tk-p02', sig: 'sig-p02', slk: ['reads', 'asm'],
    cache_root: "/ws/shards", cacheable: params.cacheable,
    helper: params.helper, hits_log: "/ws/hits.jsonl",
    step: 2, step_name: 'p02',
])
__out_2 = o.mixOuts(o.asStreams(p02(__miss_2)), o.asStreams(p02_cached(__hit_2)))
_bins = (o.post(__out_2, k, ['slot-bins']))[0]
k = ['qc']
_qc = (o.post(o.asStreams(p03(o.group('reads', [_reads, _bins], k, 1))), k, ['slot-qc']))[0]
o.seal()
}
