#!/bin/bash
# R-local: deterministic reproduction of the errno-108 staging failure MECHANISM,
# and validation of the hardened staging logic (integrity-verify + retry + flock
# per-node-once + fail-open). No HPC / no real Lustre needed.
#
# The real bug: rsync's per-file stat-walk over a contended Lustre source hits
# errno-108 (ESHUTDOWN) and can return a SILENTLY-INCOMPLETE copy (exit 0), which
# is then bound -> `import metasmith.models.workflow` fails -> exit 127.
#
# We simulate a "flaky source read" with a wrapper that, on the first attempt,
# copies all files EXCEPT the key module (mimicking a partial readdir that rsync
# reports as success), and on later attempts copies everything.
set -u
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
PASS=0; FAIL=0
ok(){ echo "  PASS: $1"; PASS=$((PASS+1)); }
no(){ echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

# --- build a fake overlay source (the "Lustre" copy) --------------------------
SRC="$TMP/lustre/metasmith"
mkdir -p "$SRC/models" "$SRC/coms"
: > "$SRC/__init__.py"
: > "$SRC/models/workflow.py"     # the key module that vanishes
: > "$SRC/models/__init__.py"
for i in $(seq 1 40); do : > "$SRC/coms/mod$i.py"; done
SRC_KEY="models/workflow.py"

# flaky_cp SRC DST ATTEMPT : copies SRC->DST; on ATTEMPT==1 drops the key file
# (simulating a partial readdir that still exits 0), else full copy.
flaky_cp(){
    local s="$1" d="$2" attempt="$3"
    mkdir -p "$d"
    ( cd "$s" && find . -type f | while read -r f; do
        if [ "$attempt" = "1" ] && [ "$f" = "./$SRC_KEY" ]; then continue; fi
        mkdir -p "$d/$(dirname "$f")"; cp "$s/$f" "$d/$f"
      done )
    return 0   # <-- like rsync's exit-0-on-partial-readdir
}

# ============================================================================
# PART 1: reproduce the NAIVE failure (current ddd5130 behaviour, no verify)
# ============================================================================
echo "== PART 1: naive staging reproduces the silent-incomplete bind =="
NAIVE_DEST="$TMP/naive_stage/metasmith"
flaky_cp "$SRC" "$NAIVE_DEST" 1     # attempt 1: partial, exit 0
if [ -e "$NAIVE_DEST/$SRC_KEY" ]; then
    no "naive stage should have produced an INCOMPLETE copy (missing $SRC_KEY)"
else
    ok "naive stage produced an incomplete copy (missing $SRC_KEY), exit 0 -> would be bound"
fi
# simulate the import against the bound (incomplete) copy
if PYTHONPATH="$TMP/naive_stage" python3 -c "import metasmith.models.workflow" 2>/dev/null; then
    no "import unexpectedly succeeded against incomplete copy"
else
    ok "import metasmith.models.workflow FAILS against incomplete copy (== the exit-127 mechanism)"
fi

# ============================================================================
# PART 2: hardened staging — integrity-verify + retry + flock + fail-open
# ============================================================================
# msm_stage_dir SRC DEST KEYREL LOCK : node-local-once, verified, retried, fail-open.
#   echoes the path to bind (DEST on verified success, SRC on fail-open).
msm_stage_dir(){
    local src="$1" dest="$2" keyrel="$3" lock="$4"
    local tries=3 i rc
    # per-node-once: only one process stages; others wait and reuse.
    exec 9>"$lock"
    if ! flock 9; then echo "$src"; return 0; fi   # fail-open if lock unavailable
    if [ -e "$dest/.msm_stage_ok" ]; then echo "$dest"; return 0; fi   # already staged by a sibling
    i=1
    while [ "$i" -le "$tries" ]; do
        rm -rf "$dest"; mkdir -p "$dest"
        flaky_cp "$src" "$dest" "$i"   # (stand-in for `rsync -a`)
        rc=$?
        # integrity: staging command ok AND key module present AND non-trivial file count
        local n; n=$(find "$dest" -type f 2>/dev/null | wc -l)
        if [ "$rc" = 0 ] && [ -e "$dest/$keyrel" ] && [ "$n" -ge 10 ]; then
            : > "$dest/.msm_stage_ok"
            echo "$dest"; return 0
        fi
        i=$((i+1)); sleep 0.05
    done
    rm -rf "$dest"          # never leave a partial copy that could be bound
    echo "$src"; return 0   # fail-open to the shared source
}

echo "== PART 2: hardened staging retries past the partial read, then verifies =="
H_DEST="$TMP/hardened_stage/metasmith"
BIND=$(msm_stage_dir "$SRC" "$H_DEST" "$SRC_KEY" "$TMP/h.lock")
if [ "$BIND" = "$H_DEST" ] && [ -e "$H_DEST/$SRC_KEY" ]; then
    ok "hardened stage retried past attempt-1 partial and produced a COMPLETE copy"
else
    no "hardened stage did not produce a complete verified copy (bind=$BIND)"
fi
if PYTHONPATH="$TMP/hardened_stage" python3 -c "import metasmith.models.workflow" 2>/dev/null; then
    ok "import succeeds against the hardened (verified) copy"
else
    no "import failed against hardened copy"
fi

echo "== PART 3: fail-open when the source is genuinely broken (never bind partial) =="
BROKEN="$TMP/lustre_broken/metasmith"; mkdir -p "$BROKEN/models"; : > "$BROKEN/__init__.py"
# no key file, few files -> every attempt fails verify -> must fail-open to SRC
B_DEST="$TMP/broken_stage/metasmith"
BIND=$(msm_stage_dir "$BROKEN" "$B_DEST" "$SRC_KEY" "$TMP/b.lock")
if [ "$BIND" = "$BROKEN" ] && [ ! -e "$B_DEST" ]; then
    ok "fail-open: returned SRC and left NO partial dest to bind"
else
    no "fail-open broken: bind=$BIND dest-exists=$([ -e "$B_DEST" ] && echo yes || echo no)"
fi

echo "== PART 4: per-node-once flock — concurrent tasks stage exactly once =="
# instrument: count how many times the real copy body runs under N concurrent callers
CNT="$TMP/stage_count"; : > "$CNT"
msm_stage_once(){   # simplified: increments CNT only when it actually copies
    local dest="$3" lock="$4"
    exec 8>"$lock"; flock 8
    if [ -e "$dest/.ok" ]; then echo reuse; return 0; fi
    echo "x" >> "$CNT"; mkdir -p "$dest"; : > "$dest/.ok"; echo staged
}
SHARED_DEST="$TMP/shared_once/metasmith"
for t in $(seq 1 12); do msm_stage_once "$SRC" "" "$SHARED_DEST" "$TMP/once.lock" >/dev/null & done
wait
STAGED_N=$(wc -l < "$CNT" | tr -d ' ')
if [ "$STAGED_N" = "1" ]; then
    ok "12 concurrent tasks -> source read exactly ONCE (per-node-once works)"
else
    no "per-node-once failed: staged $STAGED_N times (expected 1)"
fi

echo ""
echo "RESULT: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = 0 ] && echo "ALL GREEN" || echo "SOME RED"
exit $FAIL
