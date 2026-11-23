#!/bin/bash
# PathoLogic on every <name>.pf in a directory, several at once, inside the
# Pathway Tools 27.0 pathologic image. Writes <out>/<name>/ (the PGDB's 1.0
# directory), <out>/<name>.log, and one stats row per genome on stdout.
#
# The image's own `pathologic` wrapper builds in the fixed ptools-local at
# /data, so two concurrent builds share PGDB-counter.dat and the pgdbs registry.
# This runs the image's launcher with PTOOLS_LOCAL_PATH unpinned instead, one
# private ptools-local per genome, and otherwise repeats what the wrapper does.
# usage: pathologic.sh <pf_dir> <out_dir> <jobs>
set -uo pipefail
pf_dir=$(realpath "$1"); out=$(realpath -m "$2"); jobs=${3:-1}
mkdir -p "$out"
work=$(mktemp -d "${TMPDIR:-/tmp}/pathologic.XXXXXX")
launcher=$work/pathway-tools
sed 's|^PTOOLS_LOCAL_PATH=.*|export PTOOLS_LOCAL_PATH|' /opt/pathway-tools/pathway-tools > "$launcher"
chmod +x "$launcher"
export work launcher out

one() {
    local pf=$1 name g pgdb status=ok
    name=$(basename "$pf" .pf)
    g=$work/$name
    mkdir -p "$g"/ptools-local/pgdbs/user "$g"/ptools-local/html "$g"/home "$g"/ws
    cp /data/ptools-local/ptools-init.dat "$g"/ptools-local/
    printf 'ID\t0\nNAME\t0\nTYPE\t:CONTIG\nANNOT-FILE\t0.pf\n//\n' > "$g"/ws/genetic-elements.dat
    printf 'ID\t%s\nNAME\t%s\nABBREV-NAME\t%s\nSTORAGE\tFILE\nSTRAIN\t1\nRANK\t|species|\nNCBI-TAXON-ID\t12908\n' \
        "$name" "$name" "$name" > "$g"/ws/organism-params.dat
    cp "$pf" "$g"/ws/0.pf
    local options="-no-taxonomic-pruning -no-web-cel-overview -tip -no-patch-download -no-cel-overview -disable-metadata-saving -nologfile"
    local dump="(handler-case(progn(with-organism(:org-id'$name)(dump-frames-to-attribute-value-files(org-data-dir)))(exit))(error(c)(exit)))"
    SECONDS=0
    PTOOLS_LOCAL_PATH=$g HOME=$g/home "$launcher" $options -patho "$g"/ws > "$out/$name.log" 2>&1 < /dev/null
    PTOOLS_LOCAL_PATH=$g HOME=$g/home "$launcher" $options -eval "$dump" >> "$out/$name.log" 2>&1 < /dev/null
    pgdb=$(ls -d "$g"/ptools-local/pgdbs/user/*cyc/1.0 2>/dev/null | head -1)
    if [ -n "$pgdb" ] && [ -s "$pgdb/data/reactions.dat" ]; then
        mv "$pgdb" "$out/$name"
    else
        status=failed
    fi
    local rxns=0 pwys=0
    if [ $status = ok ]; then
        rxns=$(grep -c '^UNIQUE-ID' "$out/$name/data/reactions.dat")
        pwys=$(grep -c '^UNIQUE-ID' "$out/$name/data/pathways.dat")
    fi
    printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$status" "$SECONDS" "$rxns" "$pwys"
}
export -f one

printf 'genome\tpathologic\tpathologic_seconds\treactions\tpathways\n'
find "$pf_dir" -maxdepth 1 -name '*.pf' -print0 | sort -z | xargs -0 -P "$jobs" -I{} bash -c 'one "$1"' _ {}
