process pprodigal {
    input:
        path contigs

    output:
        path 'orfs.gff3'

    script:
        """
        pwd -P
        echo ""
        ls -lh
        echo ""
        find -L .
        echo ""
        touch orfs.gff3
        """
}

process fastal__asdf {
    publishDir "$params.output", mode: 'copy', pattern: "annotations.csv"

    input:
        path orfs
        path fastal_protein_reference

    output:
        path 'annotations.csv'

    script:
        """
        pwd -P
        echo ""
        ls -lh
        echo ""
        find -L .
        echo ""
        touch annotations.csv
        """
}

workflow {
    _OVtA = Channel.fromPath(params.given_OVtA)
    _bAYL = Channel.fromPath(params.given_bAYL)

    _QAaL = pprodigal(_OVtA)
    _PGmm = fastal__asdf(_QAaL, _bAYL)
}
