#!/bin/bash
# Download an MPDB's reference files on a host with internet, at the paths
# MetaPathways 4.0.1 build_db (nf_databases.py) writes them. A later build_db
# on an offline compute node adopts these files and only indexes.
# usage: fetch_mpdb.sh <mpdb_root> <swissprot|uniref50>...
set -euo pipefail
root=$(realpath -m "$1"); shift

mkdir -p "$root"/{functional/formatted,functional_categories,ncbi_tree,taxonomic/formatted,.metapathways}
test -e "$root/.metapathways/database-directories.ready" || touch "$root/.metapathways/database-directories.ready"

fetch() {
    local url=$1 dest=$2 compressed=${3:-}
    if [ -s "$dest" ]; then echo "have $dest"; return; fi
    wget -q -O "$dest.download" "$url"
    if [ -n "$compressed" ]; then
        gzip -dc "$dest.download" > "$dest.tmp"
        mv "$dest.tmp" "$dest"
        rm "$dest.download"
    else
        mv "$dest.download" "$dest"
    fi
    echo "got $dest"
}

silva=https://www.arb-silva.de/fileadmin/silva_databases/release_138_1/Exports
for db in SILVA_138.1_LSURef_NR99_tax_silva_trunc SILVA_138.1_SSURef_NR99_tax_silva_trunc; do
    fetch "$silva/$db.fasta.gz" "$root/taxonomic/$db" gz
done
fetch https://ftp.expasy.org/databases/enzyme/enzyme.dat "$root/functional_categories/enzyme.dat"
ec_note=$root/functional_categories/Expasy_reldate.txt
if [ ! -s "$ec_note" ]; then
    wget -q -O "$ec_note.tmp" https://ftp.expasy.org/databases/enzyme/enzclass.txt
    date -u > "$ec_note"
    grep "^Release:" "$ec_note.tmp" >> "$ec_note"
    rm "$ec_note.tmp"
fi
fetch https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz "$root/ncbi_tree/new_taxdump.tar.gz"

uniprot=https://ftp.uniprot.org/pub/databases/uniprot
for db in "$@"; do
    case $db in
    swissprot)
        fetch $uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz "$root/functional/swissprot" gz
        fetch $uniprot/current_release/knowledgebase/complete/reldate.txt "$root/functional_categories/SwissProt_reldate.txt"
        ;;
    uniref50|uniref90)
        fetch $uniprot/uniref/$db/$db.fasta.gz "$root/functional/$db" gz
        fetch $uniprot/uniref/$db/$db.release_note "$root/functional_categories/${db}_reldate.txt"
        fetch $uniprot/current_release/knowledgebase/idmapping/idmapping.dat.gz "$root/functional_categories/idmapping.dat.gz"
        ;;
    *) echo "unknown db $db" >&2; exit 1 ;;
    esac
done
