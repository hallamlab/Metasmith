#!/bin/bash
# Verify E3's chinook archive against the source-side manifest, independently of Globus.
#
# Globus reporting SUCCEEDED proves that every file it decided to send arrived intact. It does
# not prove that it was offered every file. Only a path-by-path diff against a manifest taken
# before the transfer catches a subtree that was never enumerated. Run this before deleting
# anything.
#
# The expected set is the manifest, minus three things and plus three things.
#
# CAUTION The manifest covers the home minus nxf_work/, results/ and relay/, but it still lists
# `.staging` -- 20 truncated partial downloads the transfer skips. Subtract them.
#
# CAUTION The transfer runs with recursive_symlinks=ignore, so Globus transfers none of the
# manifest's 3,511 symlinks. It neither follows them nor recreates them. Subtract them too, or
# the diff reports 3,511 false absences and a real one hides inside the noise. Losing them costs
# nothing: 3,510 point into the excluded nxf_work/ and would have dangled, and their content is
# archived as meta/step_logs_Qt0rbV1R.tar.gz. The 1 survivor is a logs.latest convenience link.
#
# Added after the manifest was taken, so the script adds them back: results/_metadata (10 files
# and 2 directories, moved by its own task because they are the only files under results/ that
# are not hardlinks), and metasmith/relay/msm_relay (the relay executable, which the relay/
# exclusion dropped along with the 25 worthless per-node sockets).
#
# The listing is cached per top-level subtree under WORK, so a rerun after an interrupted listing
# resumes instead of restarting.
set -uo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
CHINOOK=2602486c-1e0f-47a0-be15-eec1b0ff0f96
FIR=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
ARCHIVE=/Workspace_backups/Tony_Liu/fir_bench_e3
META="$(cd "$(dirname "$0")/../results/e3/archive_manifests" && pwd)"
MANIFEST="$META/manifest_full.tsv.gz"
WORK="${WORK:-$HOME/.cache/e3_verify}"
FIR_VERIFY=/scratch/phyberos/bench/e3_verify

# Expected under pratama2026/ once the three additions and three subtractions are applied.
EXP_DIRS=30929
EXP_FILES=110771
EXP_BYTES=3777095098371

usage() { echo "usage: $0 {listing|diff|roundtrip-fetch|roundtrip-check}" >&2; exit 2; }
[ $# -ge 1 ] || usage

# Recursive listing of one destination subtree, normalised to "type<TAB>size<TAB>relpath".
# Globus defaults --recursive-depth-limit to 3; the deepest archive path is 10 components.
list_subtree() {  # list_subtree <relpath under pratama2026, or "" for the root level>
    local rel="$1" out
    out="$WORK/ls.$(echo -n "${rel:-__root__}" | tr / _).tsv"
    if [ -s "$out" ]; then echo "cached  ${rel:-<root>} ($(wc -l < "$out") entries)"; return 0; fi
    local path="$ARCHIVE/pratama2026${rel:+/$rel}"
    "$GLOBUS" ls -a -r --recursive-depth-limit 40 -F json "$CHINOOK:$path" \
        | python3 -c '
import json, sys
pre = sys.argv[1]
for e in json.load(sys.stdin)["DATA"]:
    name = e["name"].rstrip("/")
    print("%s\t%s\t%s" % (e["type"], e.get("size") or 0, (pre + "/" + name) if pre else name))
' "$rel" > "$out.part" && mv "$out.part" "$out" || { echo "FAILED  ${rel:-<root>}"; rm -f "$out.part"; return 1; }
    echo "listed  ${rel:-<root>} ($(wc -l < "$out") entries)"
}

listing() {
    mkdir -p "$WORK"
    local tops
    tops=$("$GLOBUS" ls -a -F json "$CHINOOK:$ARCHIVE/pratama2026/" \
           | python3 -c 'import json,sys; [print(e["name"].rstrip("/")) for e in json.load(sys.stdin)["DATA"]]')
    [ -n "$tops" ] || { echo "cannot list $ARCHIVE/pratama2026" >&2; exit 1; }
    for t in $tops; do list_subtree "$t"; done
    list_subtree ""
    echo "listings in $WORK"
}

diff_manifest() {
    mkdir -p "$WORK"
    # Expected: manifest minus .staging, minus every symlink, minus the root row; plus the two
    # subtrees that arrived outside the manifest.
    { zcat "$MANIFEST" | awk -F'\t' '
        $5 == "" || $1 == "l" || $5 ~ /^\.staging(\/|$)/ { next }
        { print (($1=="d") ? "dir" : "file") "\t" $2 "\t" $5 }'
      printf 'file\t1799672\tmetasmith/relay/msm_relay\n'
      printf 'dir\t0\tmetasmith/runs/Qt0rbV1R/results\n'
      printf 'dir\t0\tmetasmith/runs/Qt0rbV1R/results/_metadata\n'
    } | LC_ALL=C sort -t$'\t' -k3,3 > "$WORK/expected.tsv"

    # Actual: every cached subtree listing. The root listing repeats its children, so dedup.
    cat "$WORK"/ls.*.tsv | LC_ALL=C sort -t$'\t' -u -k3,3 > "$WORK/actual.tsv"

    echo "== counts (results/_metadata's 10 files are expected as extras) =="
    printf "%-8s %10s %10s\n" "" expected actual
    for t in dir file; do
        printf "%-8s %10d %10d\n" "$t" \
            "$(awk -F'\t' -v t=$t '$1==t' "$WORK/expected.tsv" | wc -l)" \
            "$(awk -F'\t' -v t=$t '$1==t' "$WORK/actual.tsv" | wc -l)"
    done
    printf "%-8s %10s %10d\n" "link" "0 (ignored)" \
        "$(awk -F'\t' '$1 ~ /link|symlink/' "$WORK/actual.tsv" | wc -l)"
    echo "targets: $EXP_DIRS dirs, $EXP_FILES files, $EXP_BYTES bytes"

    echo "== bytes in regular files =="
    printf "actual %d  (target %d)\n" \
        "$(awk -F'\t' '$1=="file"{s+=$2} END{printf "%d", s}' "$WORK/actual.tsv")" "$EXP_BYTES"

    echo "== paths expected but absent from the archive =="
    comm -23 <(cut -f3 "$WORK/expected.tsv" | LC_ALL=C sort -u) \
             <(cut -f3 "$WORK/actual.tsv"   | LC_ALL=C sort -u) \
        | tee "$WORK/missing.txt" | head -40
    echo "MISSING: $(wc -l < "$WORK/missing.txt")  -- must be 0"

    # Extras are not a failure, but an unexplained one means the manifest and the archive
    # describe different trees. The 10 results/_metadata files are the known set.
    echo "== paths in the archive but not expected =="
    comm -13 <(cut -f3 "$WORK/expected.tsv" | LC_ALL=C sort -u) \
             <(cut -f3 "$WORK/actual.tsv"   | LC_ALL=C sort -u) \
        | tee "$WORK/extra.txt" | head -40
    echo "extra: $(wc -l < "$WORK/extra.txt")"

    echo "== regular files whose size differs =="
    LC_ALL=C join -t$'\t' -1 3 -2 3 -o 0,1.2,2.2 \
        <(awk -F'\t' '$1=="file"' "$WORK/expected.tsv" | LC_ALL=C sort -t$'\t' -k3,3) \
        <(awk -F'\t' '$1=="file"' "$WORK/actual.tsv"   | LC_ALL=C sort -t$'\t' -k3,3) \
        2>"$WORK/join.err" | awk -F'\t' '$2 != $3' > "$WORK/size_mismatch.txt"
    head -20 "$WORK/size_mismatch.txt"
    echo "size mismatches: $(wc -l < "$WORK/size_mismatch.txt")  -- must be 0"
    [ -s "$WORK/join.err" ] && { echo "JOIN WARNINGS -- the comparison is not trustworthy:"; cat "$WORK/join.err"; }
}

# Three files cannot be re-derived from anywhere once the scratch copy is gone, so they are
# checked by content rather than by size: the cache index, whose loss makes every shard
# unaddressable; p38's rescued annotations, which is the only product of a step that cannot
# finish inside the walltime cap; and the step-log tarball, which is now the only copy of the
# 3,510 per-step logs. Pull each back onto fir and hash it there.
ROUNDTRIP=(
    "pratama2026/metasmith/task_cache/cache.sqlite|cache.sqlite|cache.sqlite.sha256"
    "rescue/dramv_kofam_annotations.60887542.tsv|dramv_kofam_annotations.60887542.tsv|e3_rescue.sha256"
    "meta/step_logs_Qt0rbV1R.tar.gz|step_logs_Qt0rbV1R.tar.gz|step_logs.sha256"
)

roundtrip_fetch() {
    local batch; batch=$(mktemp)
    for spec in "${ROUNDTRIP[@]}"; do
        IFS='|' read -r src dst _ <<< "$spec"
        echo "$ARCHIVE/$src $FIR_VERIFY/$dst" >> "$batch"
    done
    cat "$batch"
    "$GLOBUS" transfer --batch "$batch" "$CHINOOK" "$FIR" \
        --verify-checksum --notify off --label "E3 archive round trip"
    rm -f "$batch"
    echo "then: $0 roundtrip-check"
}

roundtrip_check() {
    for spec in "${ROUNDTRIP[@]}"; do
        IFS='|' read -r _ dst shafile <<< "$spec"
        echo "== $dst =="
        ssh fir "sha256sum '$FIR_VERIFY/$dst'" | awk '{print "  archived " $1}'
        awk '{print "  recorded " $1}' "$META/$shafile"
    done
    echo "== cache index counts, read from the archived copy =="
    ssh fir "sqlite3 'file:$FIR_VERIFY/cache.sqlite?immutable=1' \
        \"SELECT 'entries', COUNT(*) FROM entries
          UNION ALL SELECT 'lineage', COUNT(*) FROM entries WHERE origin='lineage'
          UNION ALL SELECT 'imported', COUNT(*) FROM entries WHERE origin='imported'
          UNION ALL SELECT 'tombstoned', COUNT(*) FROM entries WHERE tombstoned_at IS NOT NULL;\""
    echo "expected: 11326 / 11083 / 243 / 0"
}

case "$1" in
    listing)         listing ;;
    diff)            diff_manifest ;;
    roundtrip-fetch) roundtrip_fetch ;;
    roundtrip-check) roundtrip_check ;;
    *)               usage ;;
esac
