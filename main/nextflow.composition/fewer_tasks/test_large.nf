// process get_given {
//     input:
//         path manifest

//     output:
//         stdout

//     """
//     cat $manifest
//     """
// }

process count {
    input:
        tuple val(i), val(x)

    output:
        path "*.out",  emit: generated_files
        // path "${i}.*.out"
    
    """
    n=\$(cat $x | wc -l)
    for j in {0..\$n}; do
        echo "$x" > ${i}.\${j}.out
    done
    """
}

process p {
    publishDir "$params.output", mode: "copy"

    input:
        path x

    output:
        path x

    """
    echo $x
    """
}

workflow {
    given = Channel
        .fromPath("/home/tony/workspace/tools/Metasmith/main/nextflow.composition/fewer_tasks/inputs")
        .splitCsv()
    a = count(given)
    b = p(a)
}
