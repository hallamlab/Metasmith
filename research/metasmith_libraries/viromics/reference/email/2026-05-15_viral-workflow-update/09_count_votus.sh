#!/bin/bash

FASTA="/SCRATCH/RNM270/ach/Viromics/08_vOTUs/vOTU_representatives.fna"

echo "Counting vOTUs by length..."

seqkit fx2tab -n -l "${FASTA}" | awk '
{
    total++

    if($2 >= 1000) kb1++
    if($2 >= 5000) kb5++
    if($2 >= 10000) kb10++
}
END{
    print "-----------------------------------"
    print "Total vOTUs        :", total
    print "vOTUs >=1 kb       :", kb1
    print "vOTUs >=5 kb       :", kb5
    print "vOTUs >=10 kb      :", kb10
    print "-----------------------------------"
}'