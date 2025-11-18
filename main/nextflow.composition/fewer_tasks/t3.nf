process s1 {
    output:
        path 'a*'

    """
    touch a1
    touch a2
    """
}

process s2 {
    input:
        path x

    output:
        path '*b*'

    """
    touch ${x}.b1
    touch ${x}.b2
    """
}


workflow {
    a = s1()
    b = s2(a.flatten())
    b.map(x -> x.name).view()
    // b is
    // [
    //     [a1.b1, a1.b2]
    //     [a2.b1, a2.b2]
    // ]
}
