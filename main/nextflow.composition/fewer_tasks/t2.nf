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
        path a
        path b
        path c

    output:
        path out

    """
    echo "$a, $b, $c" >out
    """
}


workflow {
    (ga, gb, gc) = get_given()
    // this looks like parameters are implicitly merged
    a = mock_job(ga.flatten(), gb.flatten(), gc.flatten())
}
