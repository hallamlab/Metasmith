process start {
    output:
        path '*.fq'

    """
    touch s1.fq
    """
}

process assemble {
    input:
        path reads

    output:
        path '*.fna'

    """
    touch ${reads.name}.asm.fna
    """
}

process binning {
    input:
        path reads
        path assembly

    output:
        path '*bin*.fna'

    """
    touch ${reads.name}.bin1.fna
    touch ${reads.name}.bin2.fna
    touch ${reads.name}.bin3.fna
    """
}

// ex: gtdbtk
process taxcls {
    input:
        path bin

    output:
        path '*.tax', optional: true
    // simulates failure on bin2
    """
    [[ $bin.name != *"bin2"* ]] && touch ${bin.name}.tax
    """
}

// ex: checkm
process bin_stats {
    input:
        path bin

    output:
        path '*.stats'

    """
    touch ${bin.name}.stats
    """
}

process compile_mag {
    input:
        path bin
        path stats
        path tax

    output:
        path '*.mag'
    """
    touch ${bin.name}.${stats.name}.${tax.name}.mag
    """
}

workflow {
    reads = start()
    asm = assemble(reads)
    bins = binning(reads, asm)
    bins = bins.flatten()
    tax = taxcls(bins)
    stats = bin_stats(bins)
    mags = compile_mag(bins, stats, tax)
    mags.map(x -> x.name).view()
    
    // to implement binning
    // have a "distributor" step run before each transform
    // which gets all items in channel
    // it should then "groupby" the items into input sets
    // for each instance of the transform to run, using instances of a lineage constraint 
}
