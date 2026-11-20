#!/bin/bash
# PathoLogic on one prepared input directory (0.pf, genetic-elements.dat,
# organism-params.dat) inside the Pathway Tools image. Copies the PGDB's 1.0
# directory to <out_pgdb> and its log to <out_pgdb>.log.
#
# The image's own `pathologic` wrapper builds in the fixed ptools-local at /data,
# so two builds on one host share PGDB-counter.dat and the pgdbs registry. This
# runs the image's launcher with PTOOLS_LOCAL_PATH unpinned instead, with a
# private ptools-local, as lib/metapathways_pgdb/pathologic.sh does.
# usage: run.sh <input_dir> <out_pgdb>
set -uo pipefail
input=$(realpath "$1"); out=$(realpath -m "$2")
work=$(mktemp -d "${TMPDIR:-/tmp}/pathologic.XXXXXX")
launcher=$work/pathway-tools
sed 's|^PTOOLS_LOCAL_PATH=.*|export PTOOLS_LOCAL_PATH|' /opt/pathway-tools/pathway-tools > "$launcher"
chmod +x "$launcher"

mkdir -p "$work"/ptools-local/pgdbs/user "$work"/ptools-local/html "$work"/home "$work"/ws
cp /data/ptools-local/ptools-init.dat "$work"/ptools-local/
cp "$input"/0.pf "$input"/genetic-elements.dat "$input"/organism-params.dat "$work"/ws/
org=$(awk -F'\t' '$1=="ID"{print $2; exit}' "$work"/ws/organism-params.dat)

options="-no-taxonomic-pruning -no-web-cel-overview -tip -no-patch-download -no-cel-overview -disable-metadata-saving -nologfile"
dump="(handler-case(progn(with-organism(:org-id'$org)(dump-frames-to-attribute-value-files(org-data-dir)))(exit))(error(c)(exit)))"
SECONDS=0
PTOOLS_LOCAL_PATH=$work HOME=$work/home "$launcher" $options -patho "$work"/ws > "$out.log" 2>&1 < /dev/null
PTOOLS_LOCAL_PATH=$work HOME=$work/home "$launcher" $options -eval "$dump" >> "$out.log" 2>&1 < /dev/null
pgdb=$(ls -d "$work"/ptools-local/pgdbs/user/*cyc/1.0 2>/dev/null | head -1)
if [ -z "$pgdb" ] || [ ! -s "$pgdb/data/reactions.dat" ]; then
    echo "[pathologic] $org: no PGDB after ${SECONDS}s, see $out.log" >&2
    exit 1
fi
mkdir -p "$(dirname "$out")"
cp -r "$pgdb" "$out"
echo "[pathologic] $org: PGDB in ${SECONDS}s"
