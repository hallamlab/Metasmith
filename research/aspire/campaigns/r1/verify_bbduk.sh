#!/bin/bash
# Verify bbduk read the reads it was supposed to read.
#
# WHY THIS EXISTS IN THIS FORM
#
# The previous version reported `completed=34 ok=0 mismatch=0 no_input_line=34`
# on run j60YFVIo -- a tidy summary, no failures, and completely wrong. Every
# one of those 34 tasks had a perfectly good `Input:` line. Two things had
# rotted underneath it:
#
#   1. It anchored on `^Input:`. Task logs are timestamp-prefixed
#      (`2026-07-28_21-07-57 E| Input:  896 reads`), so the anchor matched
#      nothing and every task counted as "no input line" rather than as a
#      failure.
#   2. It pulled the input path with a `/msm_home/...` pattern. Inputs now
#      arrive as a task_cache list (`[/scratch/.../out/x.fq.gz, ...]`), so the
#      extraction produced a bracketed multi-path string, `basename` errored,
#      and sample attribution silently degraded to empty.
#
# What it was hiding: all 34 bbduk tasks read the SAME file. Every one reported
# `Input: 896 reads` -- 448 pairs, which is NTC, the smallest library in the
# set. 0.19.1 replayed only the first member of a cached batch to every
# consumer, so 33 of 34 samples were bbduk'd against the negative control.
#
# So this version asserts the thing that would have caught it in one line: the
# input counts must be DISTINCT. Equality against truth is still checked, but
# distinctness is the load-bearing assertion, because the failure mode is
# "every task agreed" and an all-identical set can still satisfy a per-task
# comparison if attribution breaks in the same direction.
#
# Attribution is now structural rather than inferred. bbduk consumes the
# interleaved products, which this run supplies as givens from a sample-named
# hardlink backup (S25.interleaved.fq.gz), so the sample is IN the path. That
# is stronger than the old basename-through-a-side-table lookup.
#
# bbduk TRIMS, so its OUTPUT count is legitimately below its input and cannot
# be compared to truth. Its own reported `Input:` count is the honest number:
# it is what the reader actually delivered, before any trimming.

set -uo pipefail

RUN=${1:?usage: verify_bbduk.sh <run_dir> [truth_dir]}
TRUTH=${2:-/scratch/phyberos/gmcf3495/truth}

[ -d "$RUN" ]   || { echo "no such run dir: $RUN" >&2; exit 2; }
[ -d "$TRUTH" ] || { echo "no such truth dir: $TRUTH" >&2; exit 2; }

declare -A EXPECT
for f in "$TRUTH"/*.truth; do
    [ -e "$f" ] || continue
    EXPECT[$(cut -f1 "$f")]=$(cut -f4 "$f")
done
[ ${#EXPECT[@]} -gt 0 ] || { echo "no .truth files in $TRUTH" >&2; exit 2; }

ok=0; bad=0; nolog=0; unattributed=0; seen=0
counts_file=$(mktemp); samples_file=$(mktemp)
trap 'rm -f "$counts_file" "$samples_file"' EXIT

for d in "$RUN"/nxf_work/*/*; do
    [ -f "$d/.command.sh" ] || continue
    grep -q 'bbduk' "$d/.command.sh" 2>/dev/null || continue
    [ -f "$d/.exitcode" ] && [ "$(cat "$d/.exitcode")" = "0" ] || continue
    seen=$((seen+1))

    # Sample from the interleaved input path: <sample>.interleaved.fq.gz
    s=$(grep -oE '[A-Za-z0-9_]+\.interleaved\.fq\.gz' "$d/.command.sh" 2>/dev/null \
        | head -1 | sed 's/\.interleaved\.fq\.gz//')

    # `Input:` may carry any log prefix. Anchor on the token, not the line start.
    got=$(grep -m1 -E '(^|[[:space:]|])Input:[[:space:]]+[0-9]+ reads' "$d/.command.log" 2>/dev/null \
          | grep -oE 'Input:[[:space:]]+[0-9]+' | grep -oE '[0-9]+$')

    if [ -z "$got" ]; then
        echo "  NO_INPUT_LINE sample=${s:-?} $d"; nolog=$((nolog+1)); continue
    fi
    echo "$got" >> "$counts_file"

    if [ -z "$s" ]; then
        echo "  UNATTRIBUTED (no <sample>.interleaved.fq.gz in command) input=$got $d"
        unattributed=$((unattributed+1)); continue
    fi
    echo "$s" >> "$samples_file"

    exp=${EXPECT[$s]:-}
    if [ -z "$exp" ]; then
        echo "  NO_TRUTH_FOR sample=$s input=$got $d"; bad=$((bad+1)); continue
    fi
    if [ "$got" = "$exp" ]; then
        ok=$((ok+1))
    else
        echo "  MISMATCH sample=$s expected=$exp bbduk_input=$got $d"; bad=$((bad+1))
    fi
done

n_counts=$(sort -u "$counts_file" 2>/dev/null | grep -c . || true)
n_total=$(grep -c . "$counts_file" 2>/dev/null || true)
n_samples=$(sort -u "$samples_file" 2>/dev/null | grep -c . || true)
n_counts=${n_counts:-0}; n_total=${n_total:-0}; n_samples=${n_samples:-0}

echo
echo "bbduk      : completed=$seen ok=$ok mismatch=$bad no_input_line=$nolog unattributed=$unattributed"
echo "distinct   : $n_counts distinct input counts across $n_total task(s); $n_samples distinct sample(s)"

rc=0
[ "$seen" -gt 0 ] || { echo "FAIL: no completed bbduk tasks found -- check the run dir"; rc=1; }
[ "$bad" -eq 0 ] || rc=1
[ "$nolog" -eq 0 ] || rc=1
[ "$unattributed" -eq 0 ] || rc=1

# The assertion that would have caught the cache-replay bug immediately.
if [ "$n_total" -gt 1 ] && [ "$n_counts" -lt "$n_total" ]; then
    echo "FAIL: $n_total tasks produced only $n_counts distinct input counts."
    echo "      Libraries in this set have distinct read counts, so a repeat means"
    echo "      two tasks read the same file -- the cache batch-replay failure."
    sort "$counts_file" | uniq -c | sort -rn | head -5 | sed 's/^/      /'
    rc=1
fi
if [ "$n_samples" -gt 0 ] && [ "$n_samples" -ne "$n_total" ]; then
    echo "FAIL: $n_total tasks but only $n_samples distinct samples -- duplicate attribution."
    rc=1
fi

[ "$rc" -eq 0 ] && echo "PASS" || echo "FAILED"
exit $rc
