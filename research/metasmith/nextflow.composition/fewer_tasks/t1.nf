process get_given {
    output:
        path 'a*'
        path 'b*'
        path 'c*'

    """
    touch a1
    touch a2
    
    touch b1
    touch b2
    touch b3

    touch c1
    touch c2
    """
}

process mock_job {
    input:
        tuple path(a), path(b), path(c)

    output:
        path out

    """
    echo "$a, $b, $c" >out
    """
}


workflow {
    (ga, gb, gc) = get_given()
    // https://www.nextflow.io/docs/latest/reference/operator.html#merge
    // merge truncates to shortest
    a = mock_job(ga.flatten().merge(gb.flatten()).merge(gc.flatten()))
}
