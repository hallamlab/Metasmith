// A hand-wired violation of the sibling-stamp invariant. p01's output tuples
// are duplicated: every member's product comes round a second time, as a
// DIFFERENT file, `params.late_ms` after the original, through the SAME
// o.post. Each copy is stamped like a whole single-file product, so a
// consumer that released the sample on the first copy meets a late item for
// a key it has already emitted. That is a crash, never a second member and
// never a silent drop.
include { given; mock1 as p01; mock2 as p02 } from './mocks.nf'

workflow {
main:
o = new Orchestrator(Channel.fromList([null]))
_lf = new groovy.json.JsonSlurper().parseText(file("workflow.lineage_of_given.json").text)
l = _lf.lineage
o.seedParents(_lf.child2parent)
_reads = (o.postIn([given("inputs/reads", l)], ["reads"]))[0]

k = ['asm']
__raw_1 = o.asStreams(p01(o.group('reads', [_reads], k, 1)))[0]
__late_1 = __raw_1.map { idx, f ->
    sleep(params.late_ms as int)
    def copy = file("${params.late_dir}/${f.name.replaceFirst(/\.out$/, '-late.out')}")
    copy.text = "late"
    return [idx, copy]
}
_asm = (o.post([__raw_1.mix(__late_1)], k, ['slot-asm']))[0]
k = ['bins']
_bins = (o.post(o.asStreams(p02(o.group('reads', [_reads, _asm], k, 1))), k, ['slot-bins']))[0]
o.seal()
}
