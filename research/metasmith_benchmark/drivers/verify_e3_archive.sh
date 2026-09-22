#!/bin/bash
# Verify E3's chinook archive before releasing the scratch copy. Three checks, in this order.
#
# Globus reporting SUCCEEDED proves every file it decided to send arrived intact. It does not prove
# it was offered every file, and that is the failure worth catching.
#
#   resync           the primary check. Resubmit the identical batch with --sync-level checksum.
#                    Zero bytes transferred means every source file is at the destination with
#                    matching content. Covers task_cache, where no listing is tractable.
#   listing + diff   a path diff against the pre-transfer manifest, for everything except
#                    task_cache. Catches a subtree dropped by name, and names which one.
#   roundtrip-*      hashes the three files that exist nowhere else, fetched back from the archive.
#
# The diff's expected set is the manifest, minus four things and plus two things.
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

# Expected within the diff's scope -- everything under pratama2026/ except task_cache, which
# `resync` covers instead. The two partitions reconcile: 2,756 + 108,004 files is the manifest's
# 110,760, and 1,575,120,046,427 + 2,201,742,779,678 bytes is its 3,776,862,826,105.
EXP_DIRS=279
EXP_FILES=2767
EXP_BYTES=1575352318693

# `globus ls` cannot see task_cache. A non-recursive listing of its single `1e/` directory, 11,583
# children, does not return inside two minutes, and the tree below it holds ~138,655 entries. So the
# path diff is scoped to everything else, and the whole set is verified by `resync` instead.
SKIP_LS="metasmith/task_cache"

usage() { echo "usage: $0 {resync|listing|diff|roundtrip-fetch|roundtrip-check}" >&2; exit 2; }
[ $# -ge 1 ] || usage

# The primary check. Resubmitting the identical batch with --sync-level checksum makes Globus walk
# the source, checksum both ends of every file, and transfer only what differs. SUCCEEDED with
# `Bytes Transferred: 0` and no faults therefore proves that every source file is present at the
# destination with matching content -- which is stronger than comparing a path listing, and covers
# task_cache, where no listing is tractable. It repairs as it verifies: anything absent is sent.
#
# CAUTION This compares the archive against the source as it stands now, not against the manifest.
# Run it only while no driver is live, or an unrelated write shows up as a difference.
resync() {
    ssh fir 'squeue -u $USER -h -o "%.12i %.20j" | head' > /tmp/e3_squeue.$$ 2>&1 || true
    if [ -s /tmp/e3_squeue.$$ ]; then
        echo "refusing: jobs are running on fir, so the source is not quiescent" >&2
        cat /tmp/e3_squeue.$$ >&2; rm -f /tmp/e3_squeue.$$; exit 1
    fi
    rm -f /tmp/e3_squeue.$$
    local batch; batch=$(mktemp)
    ssh fir 'bash -s' < "$(dirname "$0")/e3_archive_batch.sh" > "$batch"
    echo "batch lines: $(wc -l < "$batch")"
    "$GLOBUS" transfer --batch "$batch" "$FIR" "$CHINOOK" \
        --verify-checksum --preserve-timestamp --sync-level checksum --notify off \
        --label "E3 archive resync verify $(date +%F)"
    rm -f "$batch"
    echo
    echo "PASS means: SUCCEEDED, Faults 0, Bytes Transferred 0."
    echo "Any non-zero byte count names a file the first task did not deliver -- read its"
    echo "successful-transfer list before treating the archive as verified."
}

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
    for t in $tops; do
        if [ "$t" = "metasmith" ]; then
            # Descend one level so task_cache can be skipped on its own.
            local subs
            subs=$("$GLOBUS" ls -a -F json "$CHINOOK:$ARCHIVE/pratama2026/metasmith/" \
                   | python3 -c 'import json,sys; [print(e["name"].rstrip("/")) for e in json.load(sys.stdin)["DATA"]]')
            for s in $subs; do
                [ "metasmith/$s" = "$SKIP_LS" ] && { echo "skipped metasmith/$s (see SKIP_LS)"; continue; }
                list_subtree "metasmith/$s"
            done
            list_subtree_shallow "metasmith"
            continue
        fi
        list_subtree "$t"
    done
    list_subtree ""
    echo "listings in $WORK"
}

# One level only, so metasmith's own children are recorded without descending into task_cache.
list_subtree_shallow() {
    local rel="$1" out="$WORK/ls.shallow_${1//\//_}.tsv"
    [ -s "$out" ] && { echo "cached  $rel (shallow)"; return 0; }
    "$GLOBUS" ls -a -F json "$CHINOOK:$ARCHIVE/pratama2026/$rel" \
        | python3 -c '
import json, sys
pre = sys.argv[1]
for e in json.load(sys.stdin)["DATA"]:
    print("%s\t%s\t%s/%s" % (e["type"], e.get("size") or 0, pre, e["name"].rstrip("/")))
' "$rel" > "$out.part" && mv "$out.part" "$out"
    echo "listed  $rel (shallow, $(wc -l < "$out") entries)"
}

diff_manifest() {
    mkdir -p "$WORK"
    # Expected: manifest minus .staging, minus every symlink, minus the root row; plus the two
    # subtrees that arrived outside the manifest.
    { zcat "$MANIFEST" | awk -F'\t' -v skip="$SKIP_LS" '
        $5 == "" || $1 == "l" || $5 ~ /^\.staging(\/|$)/ { next }
        index($5, skip "/") == 1 { next }
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
    resync)          resync ;;
    listing)         listing ;;
    diff)            diff_manifest ;;
    roundtrip-fetch) roundtrip_fetch ;;
    roundtrip-check) roundtrip_check ;;
    *)               usage ;;
esac
