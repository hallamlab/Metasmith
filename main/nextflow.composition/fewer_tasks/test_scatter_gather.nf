process metawrap {
    output:
        path 'bin_*'

    """
    touch bin_1
    touch bin_2
    touch bin_3
    """
}

process gtdbtk {
    input:
        tuple path(a), path(b), path(c)

    output:
        path out

    """
    echo "$a, $b, $c" >out
    """
}

process mock_job2 {
    input:
        path x

    output:
        path out

    """
    echo "$x" >out
    """
}

workflow {
    // given = Channel
    //     .fromPath("/home/tony/workspace/tools/Metasmith/main/nextflow.composition/fewer_tasks/inputs")
    //     .splitCsv()
    (ga, gb, gc) = get_given()

    a = mock_job(ga.flatten().merge(gb.flatten()).merge(gc.flatten()))
    // b = mock_job2(given)
}
