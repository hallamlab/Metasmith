#!/bin/bash
# MetaCyc's protein FASTA and attribute-value flat files out of the Pathway Tools image's
# bundled MetaCyc, as MetaPathways 4's metacyc_db.export_image does, with a private
# ptools-local as run.sh uses. LICENSED output: it never leaves the user's own storage.
# usage: export_metacyc.sh <out_dir>
set -euo pipefail
out=$(realpath -m "$1")
work=$(mktemp -d "${TMPDIR:-/tmp}/metacyc.XXXXXX")
launcher=$work/pathway-tools
sed 's|^PTOOLS_LOCAL_PATH=.*|export PTOOLS_LOCAL_PATH|' /opt/pathway-tools/pathway-tools > "$launcher"
chmod +x "$launcher"
mkdir -p "$work"/ptools-local/pgdbs/user "$work"/home "$out"
cp /data/ptools-local/ptools-init.dat "$work"/ptools-local/

meta_root=/opt/pathway-tools/aic-export/pgdbs/biocyc/metacyc
version=$(cat "$meta_root/default-version")
cp "$meta_root/$version/data/protseq.fsa" "$out"/protseq.fsa
printf '%s\n' "$version" > "$out"/version.txt
PTOOLS_LOCAL_PATH=$work HOME=$work/home "$launcher" -no-patch-download -no-cel-overview \
    -disable-metadata-saving -nologfile -lisp \
    -eval "(progn (with-organism (:org-id 'META) (dump-frames-to-attribute-value-files \"$out/\")) (exit))" \
    < /dev/null
for f in protseq.fsa proteins.dat enzrxns.dat reactions.dat pathways.dat compounds.dat classes.dat; do
    test -s "$out/$f" || { echo "[metacyc] the export wrote no $f" >&2; exit 1; }
done
echo "[metacyc] MetaCyc $version exported"
