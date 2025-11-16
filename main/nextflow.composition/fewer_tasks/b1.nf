process s1 {
    input:
        tuple val(p), path(x)

    output:
        tuple val("$p"), path('*.s1')

    """
    [[ $x.name != *"2"* ]] && touch ${x}.s1
    """
}

process s2 {
    input:
        tuple val(p), path(x)

    output:
        tuple val("$p"), path('*.s2')

    """
    touch ${x}.s2
    """
}

process s3 {
    input:
        tuple val(p), path(x1), path(x2)

    output:
        tuple val("$p"), path('*.s3')

    """
    touch ${x1.name}.${x2.name}.s3
    """
}

workflow {
    plans = Channel.fromList([1, 2, 3])
    a = plans.merge(Channel.fromList([
        'a1',
        'a2',
        'a3'
    ]).map({x -> file(x)}))
    b = plans.merge(Channel.fromList([
        'b1',
        'b2',
        'b3'
    ]).map({x -> file(x)}))

    c = s1(a)
    d = s2(b)
    e = s3(c.join(d))
    e.map({x->x.get(1).name}).view()
}
