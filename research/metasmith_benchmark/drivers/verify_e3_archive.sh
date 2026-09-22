#!/bin/bash
# Verify E3's chinook archive against the source-side manifest, independently of Globus.
#
# Globus reporting SUCCEEDED proves that every file it decided to send arrived intact. It does
# not prove that it was offered every file. Only a path-by-path diff against a manifest taken
# before the transfer catches a subtree that was never enumerated. Run this before deleting
# anything.
#
# CAUTION The manifest covers the whole home minus nxf_work/, results/ and relay/, but it DOES
# include `.staging` -- 20 truncated partial downloads that the transfer deliberately skipped.
# The expected set is therefore the manifest MINUS `.staging`: 110,760 files, 30,928 directories,
# 3,511 symlinks, 3,776,862,826,105 bytes. The larger figure of 3,832,855,343,605 is the manifest
# total and includes `.staging`'s 55,992,517,500 bytes.
#
# The listing is cached per top-level subtree under WORK, so a rerun after an interrupted listing
# resumes instead of restarting.
set -uo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
CHINOOK=2602486c-1e0f-47a0-be15-eec1b0ff0f96
FIR=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
ARCHIVE=/Workspace_backups/Tony_Liu/fir_bench_e3
MANIFEST="$(dirname "$0")/../results/e3/archive_manifests/manifest_full.tsv.gz"
WORK="${WORK:-$HOME/.cache/e3_verify}"

CACHE_SHA=eaee257a460bf743cd4631fb1dd66853fa680129caaaa0ade316f8e79598bc0a
FIR_VERIFY=/scratch/phyberos/bench/e3_verify

usage() { echo "usage: $0 {listing|diff|cache-fetch|cache-check}" >&2; exit 2; }
[ $# -ge 1 ] || usage

# Recursive listing of one destination subtree, normalised to "type<TAB>size<TAB>relpath".
# Globus caps --recursive-depth-limit at a default of 3; the shard tree needs far more.
list_subtree() {  # list_subtree <relpath under pratama2026, or "" for the root level>
    local rel="$1" out="$WORK/ls.${1//\//__}.tsv"
    [ -n "$rel" ] || out="$WORK/ls.__root__.tsv"
    if [ -s "$out" ]; then echo "cached  $rel ($(wc -l < "$out") entries)"; return 0; fi
    local path="$ARCHIVE/pratama2026${rel:+/$rel}"
    "$GLOBUS" ls -a -r --recursive-depth-limit 40 -F json "$CHINOOK:$path" \
        | python3 -c '
import json, sys
pre = sys.argv[1]
for e in json.load(sys.stdin)["DATA"]:
    name = e["name"].rstrip("/")
    print("%s\t%s\t%s" % (e["type"], e.get("size") or 0, (pre + "/" + name) if pre else name))
' "$rel" > "$out.part" && mv "$out.part" "$out" || { echo "FAILED  $rel"; rm -f "$out.part"; return 1; }
    echo "listed  $rel ($(wc -l < "$out") entries)"
}

listing() {
    mkdir -p "$WORK"
    # Walk the top level of pratama2026 and list each child separately, so one slow or failed
    # subtree does not cost the whole listing.
    local tops
    tops=$("$GLOBUS" ls -a -F json "$CHINOOK:$ARCHIVE/pratama2026/" \
           | python3 -c 'import json,sys; [print(e["name"].rstrip("/")) for e in json.load(sys.stdin)["DATA"]]')
    [ -n "$tops" ] || { echo "cannot list $ARCHIVE/pratama2026" >&2; exit 1; }
    for t in $tops; do list_subtree "$t"; done
    # The root level itself supplies the top-level regular files.
    list_subtree ""
    echo "listings in $WORK"
}

diff_manifest() {
    mkdir -p "$WORK"
    # Expected: the manifest minus .staging, with the type vocabulary mapped onto Globus's.
    zcat "$MANIFEST" | awk -F'\t' '
        $5 == "" || $5 ~ /^\.staging(\/|$)/ { next }
        { t = ($1=="d") ? "dir" : ($1=="l") ? "link" : "file"
          print t "\t" $2 "\t" $5 }' | sort -k3,3 > "$WORK/expected.tsv"

    # Actual: every cached subtree listing, deduplicated (the root listing repeats the children).
    cat "$WORK"/ls.*.tsv | sort -u -k3,3 > "$WORK/actual.tsv"

    echo "== counts =="
    printf "%-10s %10s %10s\n" "" expected actual
    for t in dir file link; do
        printf "%-10s %10d %10d\n" "$t" \
            "$(awk -F'\t' -v t=$t '$1==t' "$WORK/expected.tsv" | wc -l)" \
            "$(awk -F'\t' -v t=$t '$1 ~ t' "$WORK/actual.tsv" | wc -l)"
    done

    echo "== bytes in regular files =="
    printf "expected %d\nactual   %d\n" \
        "$(awk -F'\t' '$1=="file"{s+=$2} END{printf "%d", s}' "$WORK/expected.tsv")" \
        "$(awk -F'\t' '$1=="file"{s+=$2} END{printf "%d", s}' "$WORK/actual.tsv")"

    echo "== paths expected but absent from the archive =="
    comm -23 <(cut -f3 "$WORK/expected.tsv" | sort -u) <(cut -f3 "$WORK/actual.tsv" | sort -u) \
        | tee "$WORK/missing.txt" | head -40
    echo "missing: $(wc -l < "$WORK/missing.txt")"

    echo "== regular files whose size differs =="
    join -t$'\t' -1 3 -2 3 -o 0,1.2,2.2 \
        <(awk -F'\t' '$1=="file"' "$WORK/expected.tsv") \
        <(awk -F'\t' '$1=="file"' "$WORK/actual.tsv") \
        | awk -F'\t' '$2 != $3' | tee "$WORK/size_mismatch.txt" | head -20
    echo "size mismatches: $(wc -l < "$WORK/size_mismatch.txt")"
}

# The index is the one file whose loss makes every shard unaddressable, so it is checked by
# content rather than by size. Pull the archived copy back onto fir and hash it there.
cache_fetch() {
    "$GLOBUS" transfer \
        "$CHINOOK:$ARCHIVE/pratama2026/metasmith/task_cache/cache.sqlite" \
        "$FIR:$FIR_VERIFY/cache.sqlite.fromarchive" \
        --verify-checksum --notify off --label "E3 archive cache.sqlite round trip"
    echo "then: $0 cache-check"
}

cache_check() {
    ssh fir "sha256sum '$FIR_VERIFY/cache.sqlite.fromarchive'"
    echo "expected: $CACHE_SHA"
    ssh fir "sqlite3 'file:$FIR_VERIFY/cache.sqlite.fromarchive?immutable=1' \
        \"SELECT 'entries', COUNT(*) FROM entries
          UNION ALL SELECT 'lineage', COUNT(*) FROM entries WHERE origin='lineage'
          UNION ALL SELECT 'imported', COUNT(*) FROM entries WHERE origin='imported'
          UNION ALL SELECT 'tombstoned', COUNT(*) FROM entries WHERE tombstoned_at IS NOT NULL;\""
    echo "expected: 11326 / 11083 / 243 / 0"
}

case "$1" in
    listing)     listing ;;
    diff)        diff_manifest ;;
    cache-fetch) cache_fetch ;;
    cache-check) cache_check ;;
    *)           usage ;;
esac
