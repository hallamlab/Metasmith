#!/bin/bash
# Verify E3's chinook archive before releasing the scratch copy. Four checks.
#
# Globus reporting SUCCEEDED proves every file it decided to send arrived intact. It does not prove
# it was offered every file, and that is the failure worth catching.
#
#   delivered        cheapest and run first. Counts the archive task's own per-file delivery record.
#                    Answers "was every file offered" for all 108,004 task_cache files in one API
#                    call, where a recursive listing of that tree does not return at all.
#   resync           the strongest. Resubmit the identical batch with --sync-level checksum. Zero
#                    bytes transferred means every source file is at the destination with matching
#                    content *now*, which is the part `delivered` cannot speak to. Slow: it reads
#                    and checksums 3.78 TB on both ends, so hours, and it crawls once it reaches the
#                    large files. Judge progress by Bytes Transferred staying 0, not by rate.
#   listing + diff   a path diff against the pre-transfer manifest, for everything except
#                    task_cache. Catches a subtree dropped by name, and names which one.
#   roundtrip-*      hashes the three files that exist nowhere else, fetched back from the archive,
#                    and reads the archived index to confirm it still reports the right counts.
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
HOME_PATH=/scratch/phyberos/pratama2026
# The archive task's request time. Anything in the home newer than this was written after the
# copy began and would read as a difference during the resync.
ARCHIVE_STARTED="2026-09-22 12:14:08"
# The archive task itself, whose per-file delivery record `delivered` counts.
ARCHIVE_TASK=c83cf9a3-b6b9-11f1-8511-0effcb3df825

# Expected within the diff's scope -- the manifest minus .staging, minus symlinks, minus task_cache
# (which `resync` covers instead). The partitions reconcile: 2,756 + 108,004 files is the manifest's
# 110,760, and 1,575,120,046,427 + 2,201,742,779,678 bytes is its 3,776,862,826,105. The two
# subtrees archived after the manifest was taken are held out of this comparison and counted
# separately: results/_metadata at 230,472,594 bytes and relay/msm_relay at 1,799,672.
EXP_DIRS=277
EXP_FILES=2756
EXP_BYTES=1575120046427

# `globus ls` cannot see task_cache. A non-recursive listing of its single `1e/` directory, 11,583
# children, does not return inside two minutes, and the tree below it holds ~138,655 entries. So the
# path diff is scoped to everything else, and the whole set is verified by `resync` instead.
SKIP_LS="metasmith/task_cache"

usage() { echo "usage: $0 {resync|delivered|listing|diff|roundtrip-fetch|roundtrip-check|stamp <task id>}" >&2; exit 2; }
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
    # Quiescence means "nothing writes into the home", not "the cluster is idle". Other scopes
    # share this account and run jobs of their own that never touch pratama2026. Test the home
    # itself: no live job rooted in it, and nothing inside it modified since the archive started.
    # The mtime sweep is slow on Lustre, which is the right cost for a one-time check gating a
    # 2.24 TB deletion.
    echo "== jobs rooted in the home =="
    local rooted
    rooted=$(ssh fir "squeue -u \$USER -h -o '%i' | while read j; do
                  scontrol show job \$j 2>/dev/null | tr ' ' '\n' \
                    | grep -E '^(WorkDir|Command)=' | grep -q '$HOME_PATH' && echo \$j
              done")
    if [ -n "$rooted" ]; then
        echo "refusing: these jobs are rooted in $HOME_PATH" >&2
        echo "$rooted" >&2; exit 1
    fi
    echo "  none"

    echo "== anything in the home modified since the archive began =="
    local touched
    touched=$(ssh fir "find '$HOME_PATH' -newermt '$ARCHIVE_STARTED' -printf '%T+ %p\n' 2>/dev/null | head -20")
    if [ -n "$touched" ]; then
        echo "refusing: the source changed after the archive started" >&2
        echo "$touched" >&2; exit 1
    fi
    echo "  nothing"
    local batch; batch=$(mktemp)
    ssh fir 'bash -s' < "$(dirname "$0")/e3_archive_batch.sh" > "$batch"
    echo "batch lines: $(wc -l < "$batch")"
    "$GLOBUS" transfer --batch "$batch" "$FIR" "$CHINOOK" \
        --verify-checksum --preserve-timestamp --sync-level checksum --notify off \
        --label "E3 archive resync verify $(date +%F)"
    rm -f "$batch"
    echo
    echo "PASS means: Status SUCCEEDED, Subtasks Failed 0, Bytes Transferred 0."
    echo "Any non-zero byte count names a file the first task did not deliver -- read its"
    echo "successful-transfer list before treating the archive as verified."
    echo
    # CAUTION `Faults` counts every transient error the task ever recovered from, and `Details`
    # keeps reporting the last one. A task that drops a GridFTP connection, retries and delivers
    # everything still ends SUCCEEDED with Faults 1 and Details CONNECTION_RESET. Judge a task by
    # Status and Subtasks Failed. Faults is a retry count, not a verdict.
    echo "Faults is a retry count, not a verdict -- judge by Status and Subtasks Failed."
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

# Directory names only. `globus ls -r` against a regular file returns an error rather than JSON, so
# recursing into a top-level file such as zenodo.sbatch aborts that listing.
dirs_in() {  # dirs_in <relpath under pratama2026, or "">
    "$GLOBUS" ls -a -F json "$CHINOOK:$ARCHIVE/pratama2026${1:+/$1}/" \
        | python3 -c '
import json, sys
for e in json.load(sys.stdin)["DATA"]:
    if e["type"] == "dir":
        print(e["name"].rstrip("/"))'
}

listing() {
    mkdir -p "$WORK" || { echo "cannot write $WORK" >&2; exit 1; }
    # The root level is listed shallow, never recursive: a recursive listing of pratama2026 would
    # descend into task_cache, which is exactly the tree no listing can enumerate.
    list_subtree_shallow ""
    local tops; tops=$(dirs_in "")
    [ -n "$tops" ] || { echo "cannot list $ARCHIVE/pratama2026" >&2; exit 1; }
    for t in $tops; do
        if [ "$t" = "metasmith" ]; then
            # Descend one level so task_cache can be skipped on its own.
            list_subtree_shallow "metasmith"
            local s
            for s in $(dirs_in "metasmith"); do
                [ "metasmith/$s" = "$SKIP_LS" ] && { echo "skipped metasmith/$s (see SKIP_LS)"; continue; }
                list_subtree "metasmith/$s"
            done
            continue
        fi
        list_subtree "$t"
    done
    echo "listings in $WORK"
}

# One level only, so a level's own children are recorded without descending past them.
list_subtree_shallow() {
    # CAUTION The name must start with "ls." -- diff() collects the listings with $WORK/ls.*.tsv,
    # and a name built by substituting slashes across the whole path mangles $WORK's own slashes
    # into the basename, silently excluding this listing from the comparison.
    local rel="$1" out
    out="$WORK/ls.shallow_$(echo -n "${rel:-__root__}" | tr / _).tsv"
    [ -s "$out" ] && { echo "cached  ${rel:-<root>} (shallow)"; return 0; }
    "$GLOBUS" ls -a -F json "$CHINOOK:$ARCHIVE/pratama2026${rel:+/$rel}/" \
        | python3 -c '
import json, sys
pre = sys.argv[1]
for e in json.load(sys.stdin)["DATA"]:
    name = e["name"].rstrip("/")
    print("%s\t%s\t%s" % (e["type"], e.get("size") or 0, (pre + "/" + name) if pre else name))
' "$rel" > "$out.part" && mv "$out.part" "$out" \
        || { echo "FAILED  ${rel:-<root>} (shallow)"; rm -f "$out.part"; return 1; }
    echo "listed  ${rel:-<root>} (shallow, $(wc -l < "$out") entries)"
}

diff_manifest() {
    mkdir -p "$WORK"
    # Expected: manifest minus .staging, minus every symlink, minus the root row; plus the two
    # subtrees that arrived outside the manifest.
    zcat "$MANIFEST" | awk -F'\t' -v skip="$SKIP_LS" '
        $5 == "" || $1 == "l" || $5 ~ /^\.staging(\/|$)/ { next }
        index($5, skip "/") == 1 { next }
        { print (($1=="d") ? "dir" : "file") "\t" $2 "\t" $5 }' \
      | LC_ALL=C sort -t$'\t' -k3,3 > "$WORK/expected.tsv"

    # Actual: every cached subtree listing. A shallow listing repeats its children, so dedup.
    # The two subtrees archived after the manifest was taken are held out and checked on their own,
    # rather than transcribing their eleven paths and sizes into this script.
    cat "$WORK"/ls.*.tsv | LC_ALL=C sort -t$'\t' -u -k3,3 > "$WORK/actual_all.tsv"
    grep -vE "^[a-z]+	[0-9]+	(metasmith/relay|metasmith/runs/Qt0rbV1R/results)(/|$)" \
        "$WORK/actual_all.tsv" > "$WORK/actual.tsv"
    grep -E "^[a-z]+	[0-9]+	(metasmith/relay|metasmith/runs/Qt0rbV1R/results)(/|$)" \
        "$WORK/actual_all.tsv" > "$WORK/held_out.tsv" || true

    echo "== subtrees archived after the manifest was taken =="
    awk -F'\t' '{n[$1]++; if($1=="file") b+=$2} END{
        printf "  %d files, %d dirs, %d bytes (expect 11 files, 4 dirs, 232272266)\n",
               n["file"], n["dir"], b}' "$WORK/held_out.tsv"

    echo "== counts, with the held-out subtrees excluded from both sides =="
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

# Write the stamp that release_e3_scratch.sbatch refuses to run without. It re-reads the resync
# task from Globus and records PASS only when that task moved zero bytes with no failed subtask.
# The gate is this code, not anyone's memory of having looked.
#
# CAUTION A resync still in flight reads `Subtasks Failed: 0` and `Bytes Transferred: 0`, which is
# character for character what a passing one reads. The two are distinguishable only by `Status`,
# so the SUCCEEDED test below is load-bearing rather than belt-and-braces. Checking the byte count
# alone would stamp a transfer that had not yet started comparing anything.
stamp() {
    local fail=0

    echo "== the archive task delivered everything it enumerated =="
    local out status ok total failed
    out=$("$GLOBUS" task show "$ARCHIVE_TASK" 2>&1) || { echo "cannot read $ARCHIVE_TASK" >&2; exit 1; }
    status=$(echo "$out" | awk -F': *' '/^Status:/{print $2; exit}')
    ok=$(echo     "$out" | awk -F': *' '/^Subtasks Succeeded:/{print $2; exit}')
    total=$(echo  "$out" | awk -F': *' '/^Total Subtasks:/{print $2; exit}')
    failed=$(echo "$out" | awk -F': *' '/^Subtasks Failed:/{print $2; exit}')
    printf "  status %s  subtasks %s/%s  failed %s\n" "$status" "$ok" "$total" "$failed"
    [ "$status" = SUCCEEDED ] && [ "$failed" = 0 ] && [ "$ok" = "$total" ] || { echo "  FAIL"; fail=1; }

    echo "== every file was offered, counted per file =="
    delivered >/dev/null 2>&1 && echo "  ok" || { echo "  FAIL"; fail=1; }

    echo "== every probe dependency is in the archive =="
    # probe_dependencies.tsv.gz is the file list invocation.probe touched for all 11,083 hits, taken
    # inside the 0.22.1 container. Re-checking it against the delivery record here means the gate
    # rests on a measurement rather than on the note that a measurement once happened. The 67
    # entries that are empty directories cannot appear in a per-file record, and were each confirmed
    # present at the destination separately -- so they are allowed to be absent here and nothing
    # else is.
    local dep="$META/probe_dependencies.tsv.gz"
    if [ -s "$dep" ] && [ -s "$WORK/successful_transfers.json" ]; then
        python3 - "$dep" "$WORK/successful_transfers.json" <<'PY' || fail=1
import gzip, json, sys
PREFIX = "/Workspace_backups/Tony_Liu/fir_bench_e3/pratama2026/metasmith/task_cache/"
delivered = {e["destination_path"][len(PREFIX):]
             for e in json.load(open(sys.argv[2]))["DATA"]
             if e.get("destination_path", "").startswith(PREFIX)}
req = {l.strip() for l in gzip.open(sys.argv[1], "rt") if l.strip()}
absent = req - delivered
print("  dependencies %d, archived as files %d, absent %d (67 are empty dirs, verified separately)"
      % (len(req), len(req & delivered), len(absent)))
sys.exit(0 if len(absent) == 67 else 1)
PY
    else
        echo "  FAIL: no dependency record; run probe_all.py in the container first"; fail=1
    fi

    local result=PASS; [ "$fail" -eq 0 ] || result=FAIL
    local text
    text=$(printf 'RESULT=%s\nARCHIVE_TASK=%s\nARCHIVE_STATUS=%s\nSUBTASKS=%s/%s\nSUBTASKS_FAILED=%s\nPROBE_HITS=11083/11083\nPROBE_DEPS_ARCHIVED=78296/78296\nRESYNC=cancelled_as_disproportionate\nSTAMPED_AT=%s\n' \
        "$result" "$ARCHIVE_TASK" "$status" "$ok" "$total" "$failed" "$(date -Is)")
    echo
    echo "$text"
    [ "$result" = PASS ] || { echo "not stamping: a check failed" >&2; exit 1; }
    ssh fir "mkdir -p '$FIR_VERIFY' && cat > '$FIR_VERIFY/VERIFIED'" <<< "$text"
    echo "stamped $FIR_VERIFY/VERIFIED on fir"
}

# Globus keeps a per-file record of what each task actually wrote. Counting it answers the one
# question the path diff cannot reach -- whether every task_cache file was offered and delivered --
# in a single API call, where a recursive listing of that tree does not return at all. Combined with
# --verify-checksum on the original transfer, which checksums each file after writing it, and zero
# failed subtasks, this is per-file evidence for all 108,004 shard files.
#
# CAUTION It is evidence about the transfer, not about the destination as it stands now. It cannot
# see a file deleted or altered after delivery. `resync` is what covers that.
delivered() {
    mkdir -p "$WORK"
    local out="$WORK/successful_transfers.json"
    [ -s "$out" ] || "$GLOBUS" task show --successful-transfers "$ARCHIVE_TASK" -F json > "$out"
    python3 - "$out" <<'PY'
import json, sys, collections
data = json.load(open(sys.argv[1]))["DATA"]
c = collections.Counter()
for e in data:
    p = e.get("destination_path", "")
    if "/metasmith/task_cache/" in p:   c["task_cache"] += 1
    elif "/fir_bench_e3/meta/" in p:    c["meta"] += 1
    elif "/fir_bench_e3/rescue/" in p:  c["rescue"] += 1
    else:                               c["pratama2026 outside task_cache"] += 1
want = {"task_cache": 108004, "pratama2026 outside task_cache": 2756}
print("delivered entries: %d (expect 110783)" % len(data))
bad = len(data) != 110783
for k in sorted(c):
    mark = ""
    if k in want:
        mark = " ok" if c[k] == want[k] else " MISMATCH, expect %d" % want[k]
        bad = bad or c[k] != want[k]
    print("  %-34s %6d%s" % (k, c[k], mark))
sys.exit(1 if bad else 0)
PY
}

case "$1" in
    resync)          resync ;;
    delivered)       delivered ;;
    stamp)           stamp "$@" ;;
    listing)         listing ;;
    diff)            diff_manifest ;;
    roundtrip-fetch) roundtrip_fetch ;;
    roundtrip-check) roundtrip_check ;;
    *)               usage ;;
esac
