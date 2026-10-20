#!/bin/bash
# Pack each genome's MetaPathways output and PGDB into <out>/<name>.tar.gz,
# rooted at <name>/ with the PGDB under <name>/pgdb/, and join the annotate.sh
# and pathologic.sh stats into <out>/stats.tsv.
# usage: pack.sh <scratch> <out_dir>
set -uo pipefail
s=$(realpath "$1"); out=$(realpath -m "$2")
mkdir -p "$out"

sort_body() { head -1 "$1"; tail -n +2 "$1" | sort -t$'\t' -k1,1; }
sort_body "$s"/annotate.tsv > "$s"/annotate.sorted.tsv
sort_body "$s"/pathologic.tsv > "$s"/pathologic.sorted.tsv
join -t$'\t' --header -a1 -e - -o auto "$s"/annotate.sorted.tsv "$s"/pathologic.sorted.tsv > "$s"/joined.tsv

{
    printf '%s\ttar_bytes\tfiles\n' "$(head -1 "$s"/joined.tsv)"
    tail -n +2 "$s"/joined.tsv | while IFS= read -r row; do
        name=${row%%$'\t'*}
        bytes=0 files=0
        if [ -d "$s/mp/$name" ]; then
            [ -d "$s/pgdb/$name" ] && mv "$s/pgdb/$name" "$s/mp/$name/pgdb"
            [ -f "$s/pgdb/$name.log" ] && mv "$s/pgdb/$name.log" "$s/mp/$name/pathologic.log"
            tar -czf "$out/$name.tar.gz" --exclude="$name/pgdb/input" -C "$s"/mp "$name"
            bytes=$(stat -c %s "$out/$name.tar.gz")
            files=$(find "$s/mp/$name" -type f | wc -l)
        fi
        printf '%s\t%s\t%s\n' "$row" "$bytes" "$files"
    done
} > "$out"/stats.tsv
