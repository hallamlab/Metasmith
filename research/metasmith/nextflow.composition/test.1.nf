process gather {
    publishDir "$params.output", mode: 'copy', pattern: 'given_1'
    publishDir "$params.output", mode: 'copy', pattern: 'given_2'

    output:
    path "given_1"
    path "given_2"
    
    """
    mkdir given_1
    echo "given_1" > given_1/data.txt
    mkdir given_2
    echo "given_2" > given_2/data.txt
    """
}

workflow {
    (g1, g2) = gather()
}
