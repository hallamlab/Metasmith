#!/bin/bash
set -uo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
CHINOOK=2602486c-1e0f-47a0-be15-eec1b0ff0f96
FIR=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
ARCHIVE=/Workspace_backups/Tony_Liu/fir_metagem
INV_FIR=/scratch/phyberos/bench/metagem_archive
WORK="${WORK:-$HOME/.cache/metagem_verify}"
INV="$WORK/inventory"
FIR_VERIFY=/scratch/phyberos/bench/metagem_archive/fetchback
HOME_PATH=/scratch/phyberos/metagem

SKIP_LS="metasmith/task_cache"
LS_TIMEOUT=240

usage() { echo "usage: $0 {fetch-inventory|delivered <task id>|listing|diff|roundtrip-fetch|roundtrip-check|stamp <tree task id> <inventory task id>}" >&2; exit 2; }

fetch_inventory() {
    mkdir -p "$INV"
    local f
    for f in manifest_full.tsv symlinks.tsv empty_dirs.txt per_top_level.tsv cache_sqlite.sha256; do
        scp -q "fir:$INV_FIR/$f" "$INV/$f" || { echo "cannot fetch $f" >&2; exit 1; }
    done
    wc -l "$INV/manifest_full.tsv"
}
[ $# -ge 1 ] || usage

# Every manifest file must appear in the task's successful transfers at its own path and size.
# Globus checksummed each one, so this covers metasmith/task_cache, which the listing skips.
delivered() {
    local task_id="$2"
    [ -n "${task_id:-}" ] || usage
    mkdir -p "$WORK"
    local out="$WORK/successful_transfers.$task_id.json"
    if [ ! -s "$out" ]; then
        "$GLOBUS" task show --successful-transfers "$task_id" -F json > "$out.part" && mv "$out.part" "$out" \
            || { echo "cannot read the successful transfers of $task_id" >&2; return 1; }
    fi
    python3 - "$out" "$INV/manifest_full.tsv" "$ARCHIVE/metagem/" "$WORK/undelivered.txt" <<'PY'
import json, sys
data, manifest, prefix, report = sys.argv[1:]
got = {}
for e in json.load(open(data))["DATA"]:
    path = e["destination_path"]
    if path.startswith(prefix):
        got[path[len(prefix):]] = e["size"]
bad = []
with open(manifest) as fh:
    for n, line in enumerate(fh, 1):
        path, size = line.rstrip("\n").split("\t")[:2]
        if got.get(path) != int(size):
            bad.append(f"{path}\t{size}\t{got.get(path)}")
with open(report, "w") as fh:
    fh.writelines(b + "\n" for b in bad)
print(f"manifest files {n}, delivered {len(got)}, undelivered or resized {len(bad)} -- must be 0")
sys.exit(1 if bad else 0)
PY
}

to_rows() {
    python3 -c '
import json, sys
pre = sys.argv[1]
for e in json.load(sys.stdin)["DATA"]:
    name = e["name"].rstrip("/")
    print("%s\t%s\t%s" % (e["type"], e.get("size") or 0, (pre + "/" + name) if pre else name))
' "$1"
}

list_subtree() {
    local rel="$1" out
    out="$WORK/ls.$(echo -n "$rel" | tr / _).tsv"
    if [ -s "$out" ]; then echo "cached  $rel ($(wc -l < "$out") entries)"; return 0; fi
    timeout "$LS_TIMEOUT" "$GLOBUS" ls -a -r --recursive-depth-limit 40 -F json "$CHINOOK:$ARCHIVE/metagem/$rel" \
        | to_rows "$rel" > "$out.part" 2>/dev/null && mv "$out.part" "$out" || { rm -f "$out.part"; return 1; }
    echo "listed  $rel ($(wc -l < "$out") entries)"
}

# A recursive ls of a wide tree (nxf_work's 256 shards, Tara's 246 samples) times out, so a
# subtree that fails is listed one level at a time instead. Each level records its own files.
list_level() {
    local rel="$1" out child
    out="$WORK/ls.level.$(echo -n "${rel:-__root__}" | tr / _).tsv"
    "$GLOBUS" ls -a -F json "$CHINOOK:$ARCHIVE/metagem${rel:+/$rel}/" | to_rows "$rel" > "$out.part" \
        && mv "$out.part" "$out" || { echo "cannot list ${rel:-<root>}" >&2; exit 1; }
    for child in $(awk -F'\t' '$1=="dir"{print $3}' "$out"); do
        if [ "$child" = "$SKIP_LS" ]; then
            echo "skipped $child (verified by delivered)"
        elif [[ "$SKIP_LS" == "$child"/* ]] || ! list_subtree "$child"; then
            list_level "$child"
        fi
    done
}

listing() {
    mkdir -p "$WORK"
    list_level ""
    echo "listings in $WORK"
}

diff_manifest() {
    mkdir -p "$WORK"
    awk -F'\t' -v skip="$SKIP_LS" '
        index($1, skip "/") == 1 { next }
        { print "file\t" $2 "\t" $1 }' "$INV/manifest_full.tsv" \
        | LC_ALL=C sort -t$'\t' -k3,3 > "$WORK/expected.tsv"

    cat "$WORK"/ls.*.tsv 2>/dev/null | LC_ALL=C sort -t$'\t' -u -k3,3 > "$WORK/actual.tsv"

    echo "== counts (task_cache excluded from both sides; covered by delivered) =="
    printf "%-8s %10s %10s\n" "" expected actual
    printf "%-8s %10d %10d\n" "file" \
        "$(awk -F'\t' '$1=="file"' "$WORK/expected.tsv" | wc -l)" \
        "$(awk -F'\t' '$1=="file"' "$WORK/actual.tsv" | wc -l)"

    echo "== bytes in regular files =="
    printf "expected %d\n" "$(awk -F'\t' '$1=="file"{s+=$2} END{printf "%d", s+0}' "$WORK/expected.tsv")"
    printf "actual   %d\n" "$(awk -F'\t' '$1=="file"{s+=$2} END{printf "%d", s+0}' "$WORK/actual.tsv")"

    echo "== paths expected but absent from the archive =="
    comm -23 <(cut -f3 "$WORK/expected.tsv" | LC_ALL=C sort -u) \
             <(cut -f3 "$WORK/actual.tsv"   | LC_ALL=C sort -u) \
        | tee "$WORK/missing.txt" | head -40
    echo "MISSING: $(wc -l < "$WORK/missing.txt")  -- must be 0"

    echo "== paths in the archive but not expected =="
    comm -13 <(cut -f3 "$WORK/expected.tsv" | LC_ALL=C sort -u) \
             <(cut -f3 "$WORK/actual.tsv"   | LC_ALL=C sort -u) \
        | tee "$WORK/extra.txt" | head -40
    echo "extra: $(wc -l < "$WORK/extra.txt")"

    echo "== regular files whose size differs =="
    LC_ALL=C join -t$'\t' -1 3 -2 3 -o 0,1.2,2.2 \
        <(LC_ALL=C sort -t$'\t' -k3,3 "$WORK/expected.tsv") \
        <(LC_ALL=C sort -t$'\t' -k3,3 "$WORK/actual.tsv") \
        2>"$WORK/join.err" | awk -F'\t' '$2 != $3' > "$WORK/size_mismatch.txt"
    head -20 "$WORK/size_mismatch.txt"
    echo "size mismatches: $(wc -l < "$WORK/size_mismatch.txt")  -- must be 0"
    [ -s "$WORK/join.err" ] && { echo "JOIN WARNINGS -- the comparison is not trustworthy:"; cat "$WORK/join.err"; }

    echo "== per top level entry (from inventory) =="
    cat "$INV/per_top_level.tsv"

    echo "== symlinks in the source (not transferred) =="
    wc -l < "$INV/symlinks.tsv"

    echo "== empty directories in the source (recreate individually if missing) =="
    wc -l < "$INV/empty_dirs.txt"
}

roundtrip_fetch() {
    local batch; batch=$(mktemp)
    echo "$ARCHIVE/metagem/metasmith/task_cache/cache.sqlite $FIR_VERIFY/cache.sqlite" >> "$batch"
    cat "$batch"
    "$GLOBUS" transfer --batch "$batch" "$CHINOOK" "$FIR" \
        --verify-checksum --notify off --label "metagem archive round trip"
    rm -f "$batch"
    echo "then: $0 roundtrip-check"
}

roundtrip_check() {
    ssh fir "sha256sum '$FIR_VERIFY/cache.sqlite'" | awk '{print "archived " $1}'
    awk '{print "recorded " $1}' "$INV/cache_sqlite.sha256"
    echo "== cache index, read from the fetched-back copy =="
    ssh fir "sqlite3 'file:$FIR_VERIFY/cache.sqlite?immutable=1' \
        \"SELECT 'entries', COUNT(*) FROM entries
          UNION ALL SELECT 'lineage', COUNT(*) FROM entries WHERE origin='lineage'
          UNION ALL SELECT 'imported', COUNT(*) FROM entries WHERE origin='imported'
          UNION ALL SELECT 'tombstoned', COUNT(*) FROM entries WHERE tombstoned_at IS NOT NULL;\""
    ssh fir "sha256sum '$FIR_VERIFY/cache.sqlite'" > "$WORK/fetchback_cache.sha256" 2>/dev/null || true
    ssh fir "sha256sum '$FIR_VERIFY/cache.sqlite'" | awk '{print $1}' > "$WORK/fetchback_cache.hash"
}

stamp() {
    local tree_task="$2" inv_task="$3"
    [ -n "${tree_task:-}" ] && [ -n "${inv_task:-}" ] || usage
    mkdir -p "$WORK"
    local fail=0

    echo "== tree transfer task =="
    local out status failed requested
    out=$("$GLOBUS" task show "$tree_task" 2>&1) || { echo "cannot read $tree_task" >&2; exit 1; }
    status=$(echo "$out" | awk -F': *' '/^Status:/{print $2; exit}')
    failed=$(echo "$out" | awk -F': *' '/^Subtasks Failed:/{print $2; exit}')
    requested=$(echo "$out" | sed -n 's/^Request Time:[[:space:]]*//p')
    printf "  status %s  failed %s  requested %s\n" "$status" "$failed" "$requested"
    [ "$status" = SUCCEEDED ] && [ "$failed" = 0 ] || { echo "  FAIL"; fail=1; }
    [ -n "$requested" ] || { echo "  FAIL: no request time"; fail=1; }

    echo "== inventory transfer task =="
    local out2 status2 failed2
    out2=$("$GLOBUS" task show "$inv_task" 2>&1) || { echo "cannot read $inv_task" >&2; exit 1; }
    status2=$(echo "$out2" | awk -F': *' '/^Status:/{print $2; exit}')
    failed2=$(echo "$out2" | awk -F': *' '/^Subtasks Failed:/{print $2; exit}')
    printf "  status %s  failed %s\n" "$status2" "$failed2"
    [ "$status2" = SUCCEEDED ] && [ "$failed2" = 0 ] || { echo "  FAIL"; fail=1; }

    echo "== every manifest file delivered at its size (delivered) =="
    delivered x "$tree_task" && echo "  ok" || { echo "  FAIL"; fail=1; }

    echo "== path diff has no missing/mismatched entries =="
    if [ -e "$WORK/missing.txt" ] && [ -e "$WORK/size_mismatch.txt" ]; then
        local nmiss nmis
        nmiss=$(wc -l < "$WORK/missing.txt")
        nmis=$(wc -l < "$WORK/size_mismatch.txt")
        [ "$nmiss" -eq 0 ] || { echo "  FAIL: $nmiss missing paths"; fail=1; }
        [ "$nmis" -eq 0 ] || { echo "  FAIL: $nmis size mismatches"; fail=1; }
        [ "$nmiss" -eq 0 ] && [ "$nmis" -eq 0 ] && echo "  ok"
    else
        echo "  FAIL: run '$0 diff' first"; fail=1
    fi

    echo "== cache.sqlite round trip matches =="
    if [ -s "$WORK/fetchback_cache.hash" ]; then
        local recorded; recorded=$(awk '{print $1}' "$INV/cache_sqlite.sha256")
        local fetched; fetched=$(cat "$WORK/fetchback_cache.hash")
        [ "$recorded" = "$fetched" ] && echo "  ok $recorded" || { echo "  FAIL: $recorded != $fetched"; fail=1; }
    else
        echo "  FAIL: run '$0 roundtrip-fetch' then '$0 roundtrip-check' first"; fail=1
    fi

    local result=PASS; [ "$fail" -eq 0 ] || result=FAIL
    local text
    text=$(printf 'RESULT=%s\nTREE_TASK=%s\nTREE_STATUS=%s\nTREE_REQUESTED_AT=%s\nINVENTORY_TASK=%s\nINVENTORY_STATUS=%s\nMANIFEST_FILES=%s\nSTAMPED_AT=%s\n' \
        "$result" "$tree_task" "$status" "$requested" "$inv_task" "$status2" "$(wc -l < "$INV/manifest_full.tsv")" "$(date -Is)")
    echo
    echo "$text"
    [ "$result" = PASS ] || { echo "not stamping: a check failed" >&2; exit 1; }
    echo "$text" | ssh fir "cat > '$INV_FIR/VERIFIED'" || { echo "cannot write the stamp on fir" >&2; exit 1; }
    ssh fir "cat '$INV_FIR/VERIFIED'"
    echo "stamped fir:$INV_FIR/VERIFIED"
}

case "$1" in
    fetch-inventory) fetch_inventory ;;
    delivered)       delivered "$@" ;;
    listing)         listing ;;
    diff)            diff_manifest ;;
    roundtrip-fetch) roundtrip_fetch ;;
    roundtrip-check) roundtrip_check ;;
    stamp)           stamp "$@" ;;
    *)               usage ;;
esac
