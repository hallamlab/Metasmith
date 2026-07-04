#!/bin/bash
# R-local: deterministic validation of the TARBALL dev-overlay staging logic used
# by the bootstrap heredoc in `agents.py` (`run_container`). No HPC / no real
# Lustre needed. Mirrors the deployed shell: the overlay is delivered as a single
# tarball; each node natively `cp`s it to local scratch, `tar -x` extracts it,
# verifies completeness (key submodule + non-trivial file count), stamps it under
# an flock (per-node-once), keys the cache by the tarball's own `stat` (mtime+size),
# and fails-open to the shared source on any error.
#
# Why a tarball beats the old rsync tree-walk: the per-node Lustre read becomes ONE
# streaming file instead of a ~70-file readdir walk (the op-class that triggers
# errno-108 / ESHUTDOWN), the small-file writes land on node-local disk during
# `tar -x`, and a truncated archive fails `tar -x` LOUDLY instead of returning a
# silently-incomplete copy (the old exit-0-on-partial-readdir -> exit-127 trap).
# Faithful HPC evidence: RCA plan 02-errno108-overlay-fanout-rca.md
# (naive rsync fan-out 558 errno-108 / 93-of-97 incomplete; tarball 0 / 0).
set -u
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
PASS=0; FAIL=0
ok(){ echo "  PASS: $1"; PASS=$((PASS+1)); }
no(){ echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

# --- build a fake overlay source + its tarball (the "Lustre" copy) ------------
SRC="$TMP/src/metasmith"
mkdir -p "$SRC/models" "$SRC/coms"
: > "$SRC/__init__.py"
: > "$SRC/models/__init__.py"
: > "$SRC/models/workflow.py"          # the key module that must be present
: > "$SRC/coms/__init__.py"
for i in $(seq 1 60); do : > "$SRC/coms/mod$i.py"; done   # >=50 files total
KEYREL="models/workflow.py"
# archive carries a `metasmith/` prefix -> a node extracts to <stage>/metasmith
GOOD="$TMP/metasmith.tar"
tar -c -C "$TMP/src" -f "$GOOD" metasmith
# a corrupt/partial-transfer tarball: truncated mid-stream
BADTAR="$TMP/bad.tar"; head -c 300 "$GOOD" > "$BADTAR"

# msm_stage_tarball TARBALL STAGE_BASE KEYREL FALLBACK
#   native-cp -> extract -> verify -> stamp, per-node-once under flock, fail-open.
#   echoes the path to bind (NODE_DEV on verified success, FALLBACK otherwise).
#   If $STAGE_COUNT is set, appends a line each time it actually cp+extracts
#   (so a test can prove per-node-once / re-extract-on-change).
msm_stage_tarball(){
    local tarball="$1" stage_base="$2" keyrel="$3" fallback="$4"
    local key stage_dir node_dev stamp tries=3 i n lt
    key=$(stat -c '%Y-%s' "$tarball" 2>/dev/null || echo nokey)
    stage_dir="$stage_base/$key"
    node_dev="$stage_dir/metasmith"
    stamp="$stage_dir/.msm_stage_ok"
    mkdir -p "$stage_base"
    (
        exec 9>"$stage_dir.lock"
        if flock -w 30 9; then
            if [ ! -e "$stamp" ]; then
                i=1
                while [ "$i" -le "$tries" ]; do
                    rm -rf "$node_dev"; mkdir -p "$stage_dir"
                    lt="$TMP/localcopy.$key.$$.$i.tar"
                    [ -n "${STAGE_COUNT:-}" ] && echo x >> "$STAGE_COUNT"
                    if cp -f "$tarball" "$lt" 2>/dev/null \
                        && tar -xf "$lt" -C "$stage_dir" 2>/dev/null; then
                        n=$(find "$node_dev" -type f 2>/dev/null | wc -l)
                        if [ -e "$node_dev/$keyrel" ] && [ -e "$node_dev/coms" ] && [ "$n" -ge 50 ]; then
                            rm -f "$lt"; : > "$stamp"; break
                        fi
                    fi
                    rm -f "$lt"
                    i=$((i+1)); sleep 0.05
                done
            fi
        fi
    )
    if [ -e "$stamp" ]; then echo "$node_dev"; else rm -rf "$node_dev" 2>/dev/null; echo "$fallback"; fi
}

# ============================================================================
# PART 1: a truncated tarball fails LOUDLY (no silent-incomplete bind)
# ============================================================================
echo "== PART 1: truncated tarball -> extraction fails/incomplete (not silent) =="
mkdir -p "$TMP/badextract"
if tar -xf "$BADTAR" -C "$TMP/badextract" 2>/dev/null && [ -e "$TMP/badextract/metasmith/$KEYREL" ]; then
    no "truncated tarball unexpectedly yielded a COMPLETE extraction"
else
    ok "truncated tarball fails/incomplete extraction -> caught, unlike rsync's silent exit-0"
fi

# ============================================================================
# PART 2: hardened staging of a good tarball -> verified, importable
# ============================================================================
echo "== PART 2: good tarball staged via cp+extract, verified, importable =="
KEY_GOOD=$(stat -c '%Y-%s' "$GOOD")
BIND=$(msm_stage_tarball "$GOOD" "$TMP/stage2" "$KEYREL" "$SRC")
if [ "$BIND" = "$TMP/stage2/$KEY_GOOD/metasmith" ] && [ -e "$BIND/$KEYREL" ] && [ -e "$BIND/coms" ]; then
    ok "good tarball -> complete verified node-local copy at [$BIND]"
else
    no "good tarball staging did not produce a complete verified copy (bind=$BIND)"
fi
if PYTHONPATH="$(dirname "$BIND")" python3 -c "import metasmith.models.workflow" 2>/dev/null; then
    ok "import metasmith.models.workflow succeeds against the staged copy"
else
    no "import failed against the staged copy"
fi

# ============================================================================
# PART 3: fail-open on a genuinely broken tarball (never bind a partial dest)
# ============================================================================
echo "== PART 3: broken tarball -> fail-open to shared source, no partial dest =="
KEY_BAD=$(stat -c '%Y-%s' "$BADTAR")
BIND=$(msm_stage_tarball "$BADTAR" "$TMP/stage3" "$KEYREL" "$SRC")
if [ "$BIND" = "$SRC" ] && [ ! -e "$TMP/stage3/$KEY_BAD/metasmith" ]; then
    ok "fail-open: returned SRC and left NO partial dest to bind"
else
    no "fail-open broken: bind=$BIND dest-exists=$([ -e "$TMP/stage3/$KEY_BAD/metasmith" ] && echo yes || echo no)"
fi

# ============================================================================
# PART 4: per-node-once flock — concurrent tasks cp+extract exactly once
# ============================================================================
echo "== PART 4: 12 concurrent tasks -> tarball extracted exactly ONCE =="
STAGE_COUNT="$TMP/extract_count"; : > "$STAGE_COUNT"
export STAGE_COUNT
for t in $(seq 1 12); do
    msm_stage_tarball "$GOOD" "$TMP/stage4" "$KEYREL" "$SRC" >/dev/null &
done
wait
N4=$(wc -l < "$STAGE_COUNT" | tr -d ' ')
if [ "$N4" = "1" ]; then
    ok "12 concurrent tasks -> extracted exactly ONCE (per-node-once works)"
else
    no "per-node-once failed: extracted $N4 times (expected 1)"
fi
unset STAGE_COUNT

# ============================================================================
# PART 5: stat-key freshness — same tarball reused, changed tarball re-extracted
# ============================================================================
echo "== PART 5: stat key -> reuse identical tarball, re-extract a changed one =="
FC="$TMP/fresh_count"; : > "$FC"
STAGE_COUNT="$FC"; export STAGE_COUNT
KEY_A=$(stat -c '%Y-%s' "$GOOD")
msm_stage_tarball "$GOOD" "$TMP/stage5" "$KEYREL" "$SRC" >/dev/null   # extracts (1)
msm_stage_tarball "$GOOD" "$TMP/stage5" "$KEYREL" "$SRC" >/dev/null   # same key -> reuse (no extract)
N_SAME=$(wc -l < "$FC" | tr -d ' ')
if [ "$N_SAME" = "1" ]; then
    ok "identical tarball (same stat key) reused without re-extract"
else
    no "identical tarball re-extracted $N_SAME times (expected 1 — stamp reuse broken)"
fi
# change the overlay -> rebuild tarball -> new size/mtime -> new stat key
: > "$SRC/coms/mod61.py"
sleep 1.1   # ensure mtime ticks even at 1s stat granularity
tar -c -C "$TMP/src" -f "$GOOD" metasmith
KEY_B=$(stat -c '%Y-%s' "$GOOD")
BIND_B=$(msm_stage_tarball "$GOOD" "$TMP/stage5" "$KEYREL" "$SRC")
N_CHANGED=$(wc -l < "$FC" | tr -d ' ')
if [ "$KEY_A" != "$KEY_B" ] && [ "$N_CHANGED" = "2" ] \
    && [ "$BIND_B" = "$TMP/stage5/$KEY_B/metasmith" ] && [ -e "$BIND_B/$KEYREL" ]; then
    ok "changed tarball -> new stat key -> re-extracted to a fresh dir (no stale serve)"
else
    no "freshness failed: keyA=$KEY_A keyB=$KEY_B extracts=$N_CHANGED bind=$BIND_B"
fi
unset STAGE_COUNT

echo ""
echo "RESULT: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ] && echo "ALL GREEN" || echo "SOME RED"
exit $FAIL
