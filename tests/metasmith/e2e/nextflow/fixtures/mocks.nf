// Mock processes for the Orchestrator early-release fixtures.
//
// Every process here takes the tuple shape the generated workflow.nf feeds a
// step -- `tuple val(index), path(_01), ...` where `index` is the LIST of
// per-member indexes `_collateBatch` emits -- and produces files named the way
// bootstrap names a product: `<pos>-<item>-<branch>.<token>-<label>.out`, so
// `_debatch` can split a batched tuple back into members and `_post` can mint
// a per-file identity from the canonical name.
//
// Behaviour is driven by `params.spec[<process alias>]`, a map of:
//   by       the key whose hash names the member's files   (default 'reads')
//   label    the file label                                 (default: the alias)
//   slow     {sample: seconds}   sleep before producing
//   slow_in  {regex: seconds}    sleep when a staged input name matches
//   fan      {sample: n}         emit n files on branch 1    (default 1)
//   fail     [sample, ...]       exit 1 on every attempt
//   empty    [sample, ...]       emit zero files on branch 2 (mock1_2 only)
// A "sample" is the member's `reads` hashes joined with '+', which the
// fixtures seed as readable ids, so a collector member reads "s0+s1+s2".
//
// Every task appends one line to `params.recv_log`:
//   RECV|<alias>|<attempt>|<member tokens>|<slot 1 names>;<slot 2 names>;...
// which is the test's only view of what a task actually received.

def given(f, l) {
    def rows = Channel.fromPath(f).splitCsv(header: false)
    if (f in l) {
        rows = Channel.fromList(l[f]).merge(rows)
    }
    return rows.map { row ->
        if (row.size() > 1) {
            def (ri, rx) = row
            return tuple(ri, file(rx))
        } else {
            return tuple([:], file(row[0]))
        }
    }
}

def members_of(index) {
    return (index instanceof List) ? index : [index]
}

// A member's index is the UNION of its items' indexes, so a sample-grouped
// member that also holds an aggregate item carries every sample's hash. The
// member's own identity is the by-item's, which PROV keeps per slot; every
// fixture lists the by-stream first.
def by_hashes(m, key) {
    def items = (m.PROV instanceof List && m.PROV.size() > 0) ? m.PROV[0] : [m]
    def hs = items.collect { it[key] ?: [] }.flatten().unique()
    if (hs.isEmpty()) hs = (m[key] ?: ['x'])
    return hs.sort().join('+')
}

def samples_of(index) {
    return members_of(index).collect { m -> by_hashes(m, 'reads') }
}

def mock_script(task, index, slots, branches) {
    def spec = ((params.spec ?: [:])[task.process]) ?: [:]
    def members = members_of(index)
    def samples = samples_of(index)
    def by = spec.by ?: 'reads'
    def label = spec.label ?: task.process
    def tokens = members.collect { m -> by_hashes(m, by) }
    def staged = slots.collect { s -> [s].flatten().collect { it.name }.sort() }
    def lines = [
        "echo 'RECV|${task.process}|${task.attempt}|${tokens.join(',')}|${staged.collect { it.join(',') }.join(';')}' >> ${params.recv_log}",
    ]
    def failing = samples.findAll { s -> (spec.fail ?: []).contains(s) }
    if (failing) {
        lines << "echo 'FAIL ${failing.join(',')}' >&2"
        lines << "exit 1"
    }
    def naps = samples.collect { s -> ((spec.slow ?: [:])[s] ?: 0) as int }
    def all_inputs = staged.flatten()
    (spec.slow_in ?: [:]).each { pattern, seconds ->
        if (all_inputs.any { n -> n ==~ pattern }) naps << (seconds as int)
    }
    def nap = naps.max()
    if (nap > 0) lines << "sleep ${nap}"
    members.eachWithIndex { m, i ->
        def s = samples[i]
        def n = ((spec.fan ?: [:])[s] ?: 1) as int
        (0..<n).each { j ->
            lines << "echo '${s}' > ${i + 1}-${j + 1}-1.${tokens[i]}-${label}.out"
        }
        if (branches > 1) {
            def n2 = (spec.empty ?: []).contains(s) ? 0 : 1
            (0..<n2).each { j ->
                lines << "echo '${s}' > ${i + 1}-${j + 1}-2.${tokens[i]}-${label}2.out"
            }
        }
    }
    return lines.join('\n')
}

process mock1 {
    tag { samples_of(index).join(',') }
    input:
        tuple val(index), path(_01)
    output:
        tuple val(index), path("*-1.*.out")
    script:
        def sh = mock_script(task, index, [_01], 1)
        """
        ${sh}
        """
}

process mock2 {
    tag { samples_of(index).join(',') }
    input:
        tuple val(index), path(_01), path(_02)
    output:
        tuple val(index), path("*-1.*.out")
    script:
        def sh = mock_script(task, index, [_01, _02], 1)
        """
        ${sh}
        """
}

process mock3 {
    tag { samples_of(index).join(',') }
    input:
        tuple val(index), path(_01), path(_02), path(_03)
    output:
        tuple val(index), path("*-1.*.out")
    script:
        def sh = mock_script(task, index, [_01, _02, _03], 1)
        """
        ${sh}
        """
}

// Two output branches, both optional, as codegen emits for a transform with
// more than one product group.
process mock1_2 {
    tag { samples_of(index).join(',') }
    input:
        tuple val(index), path(_01)
    output:
        tuple val(index), path("*-1.*.out"), optional: true
        tuple val(index), path("*-2.*.out"), optional: true
    script:
        def sh = mock_script(task, index, [_01], 2)
        """
        ${sh}
        """
}

// The `_cached` twin: places a shard's products under the member's position,
// as codegen's prepare_twin does.
process mock_cached {
    executor 'local'
    cache false
    tag { samples_of(index).join(',') }
    input:
        tuple val(index), val(sources)
    output:
        tuple val(index), path("*-1.*.out")
    script:
        def spec = ((params.spec ?: [:])[task.process]) ?: [:]
        def by = spec.by ?: 'reads'
        def tokens = members_of(index).collect { m -> by_hashes(m, by) }
        def names = sources.collect { s -> s[2] }.sort().join(',')
        def place = sources.collect { s -> "cp ${s[1]} ${s[0]}-${s[2]}" }.join('\n')
        """
        echo 'RECV|${task.process}|${task.attempt}|${tokens.join(',')}|${names}' >> ${params.recv_log}
        ${place}
        """
}
