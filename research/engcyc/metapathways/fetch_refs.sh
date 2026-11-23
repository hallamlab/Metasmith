#!/bin/bash
# Download the pilot's GTDB and NCBI references on a host with internet, laid out
# as downloadGtdbMetadata, downloadGtdbRrna and downloadNcbiTaxdump write them.
# The SSU BLAST index is built afterwards on a compute node (index_ssu.sbatch).
# usage: fetch_refs.sh <refs_root>
set -euo pipefail
root=$(realpath -m "$1")
gtdb=https://data.ace.uq.edu.au/public/gtdb/data/releases/release232/232.0
mkdir -p "$root"/{gtdb_metadata,gtdb_rrna,ncbi_taxdump}

fetch() {
    local url=$1 dest=$2
    if [ -s "$dest" ]; then echo "have $dest"; return; fi
    wget -q -O "$dest.download" "$url"
    mv "$dest.download" "$dest"
    echo "got $dest"
}

for f in bac120_metadata_r232.tsv.gz ar53_metadata_r232.tsv.gz; do
    fetch "$gtdb/$f" "$root/gtdb_metadata/$f"
    gzip -t "$root/gtdb_metadata/$f"
done

if [ ! -s "$root/gtdb_rrna/ssu.fna" ]; then
    fetch "$gtdb/genomic_files_all/ssu_all_r232.fna.gz" "$root/gtdb_rrna/ssu.fna.gz"
    gunzip -f "$root/gtdb_rrna/ssu.fna.gz"
fi

if [ ! -s "$root/ncbi_taxdump/nodes.dmp" ]; then
    fetch https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump_archive/taxdmp_2026-10-01.zip "$root/ncbi_taxdump/taxdmp.zip"
    unzip -o -d "$root/ncbi_taxdump" "$root/ncbi_taxdump/taxdmp.zip" nodes.dmp names.dmp merged.dmp
    rm "$root/ncbi_taxdump/taxdmp.zip"
fi
ls -la "$root"/*
