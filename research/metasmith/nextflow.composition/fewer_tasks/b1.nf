process s1 {
    input:
        tuple val(p), path(x)

    output:
        tuple val("$p"), path('*.s1')

    // [[ $x.name != *"2"* ]] && touch ${x}.s1
    """
    echo $task.attempt
    [[ $task.attempt -gt 2 ]] && touch ${x}.s1
    """
}

process s2 {
    input:
        tuple val(p), path(x)

    output:
        tuple val("$p"), path('*.s2')
        tuple val("$p"), path('*.x2')

    """
    touch ${x}.s2
    touch ${x}.x2
    """
}

def Pad(i) {
    return String.format('%05d', i)
}


process s3 {
    // publishDir "$params.output", saveAs: {f -> String.format("%05d_%s", sample_id, filename) }

    input:
        tuple val(p), path(x1), path(x2), path(x3)

    output:
        tuple val("$p"), path('*.s3')

    script:
    """
    touch ${x1.name}.${x2.name}-${x3.name}.s3
    """
}


index = Channel.fromList(1..3)
def In(f) {
    data = Channel.fromPath(f).splitCsv(header: false).map({row -> file(row[0])})
    return index.merge(data)
}

workflow {
    a = In("../inputs.a")
    b = In("../inputs.b")
    c = s1(a)
    (d, d2) = s2(b)
    e = s3(c.join(d).join(d2))
    e.map({x->x.get(1).name}).view()
}
