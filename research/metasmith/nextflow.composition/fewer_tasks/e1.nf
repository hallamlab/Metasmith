process s1 {
    errorStrategy = 'retry'
    // errorStrategy = { task.attempt<=3 ? 'retry' : 'ignore' }
    
    input:
    val x

    output:
    path "*.out"

    """
    [[ $task.attempt -gt 1 ]] && touch ${task.attempt}.out
    """
}

workflow {
    x = Channel.fromList([1, 2, 3, 4, 5])
    y = s1(x)
    y.map({y->y.name}).view()
}
