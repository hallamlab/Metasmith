#!/bin/bash
# test_batched_bounce_fir.sh — Run on fir compute node to debug batched bounce bind paths
# Usage: bash /scratch/phyberos/metasmith/dev/test_batched_bounce_fir.sh
set -euo pipefail

MSM="/scratch/phyberos/metasmith/msm"
WORKDIR="${SLURM_TMPDIR:-/tmp}/test_batched_bounce_$$"

echo "=== Batched Bounce Debug Test ==="
echo "WORKDIR=$WORKDIR"
echo "SLURM_TMPDIR=${SLURM_TMPDIR:-unset}"
echo "TMPDIR=${TMPDIR:-unset}"

# Create a minimal workspace that mimics a batched transform
mkdir -p "$WORKDIR/_metasmith"

# Create dummy input files
for i in 1 2 3; do
    echo ">sample_${i}\nACGTACGTACGT" > "$WORKDIR/sample_${i}.fna"
done

echo "--- Created test inputs ---"
ls -la "$WORKDIR/"

# Try to re-run the failed bounce from the existing failed run if available
FAILED_RUN="/scratch/phyberos/metasmith/pipeline/work"
if [ -d "$FAILED_RUN" ]; then
    echo ""
    echo "=== Looking for failed batched runs ==="
    # Find recent bounce scripts that failed (exit code != 0)
    find "$FAILED_RUN" -name ".bounce.*" -newer "$FAILED_RUN" -mmin -1440 2>/dev/null | head -5 | while read bounce; do
        dir=$(dirname "$bounce")
        echo ""
        echo "--- Found bounce: $bounce ---"
        echo "Contents:"
        cat "$bounce"
        echo ""
        echo "Directory listing:"
        ls -la "$dir/" | head -10
        # Check for exitcode files
        ls "$dir"/exitcode.* 2>/dev/null | while read ec; do
            echo "Exit code file $ec: $(cat $ec)"
        done
    done
fi

# If the msm launcher exists, try a container exec with debug logging
if [ -x "$MSM" ] || [ -f "$MSM" ]; then
    echo ""
    echo "=== Testing container bind resolution ==="
    echo "Running metasmith with debug logging enabled via dev overlay..."

    # Use the dev overlay's libraries.py (already has debug logging from our changes)
    # Run a simple container test - just list the workspace inside the container
    cd "$WORKDIR"

    # Create a minimal bounce script manually to test apptainer bind behavior
    cat > "$WORKDIR/_metasmith/.bounce.test" << 'SCRIPT'
cd /ws
echo '[BOUNCE] pwd='$(pwd)
echo '[BOUNCE] ls /ws/:'
ls -la /ws/
echo '[BOUNCE] ls /ws/_metasmith/.bounce.*:' $(ls /ws/_metasmith/.bounce.* 2>&1)
echo '[BOUNCE] TMPDIR='$TMPDIR
echo '[BOUNCE] whoami='$(whoami)
echo '[BOUNCE] id='$(id)
SCRIPT
    chmod +x "$WORKDIR/_metasmith/.bounce.test"

    echo "--- Bounce script created ---"
    cat "$WORKDIR/_metasmith/.bounce.test"

    # Try running with apptainer directly to test bind paths
    if command -v apptainer &>/dev/null; then
        echo ""
        echo "=== Direct apptainer test ==="
        # Find the metasmith SIF
        SIF=$(find /scratch/phyberos/metasmith -name "*.sif" -maxdepth 2 2>/dev/null | head -1)
        if [ -n "$SIF" ]; then
            echo "Using SIF: $SIF"
            echo "Bind: $WORKDIR:/ws"
            echo ""
            echo "--- Test 1: --pwd (correct for apptainer) ---"
            apptainer exec \
                --no-home --cleanenv \
                --env "TMPDIR=${TMPDIR:-/tmp}" \
                --bind "${TMPDIR:-/tmp}:${TMPDIR:-/tmp}" \
                --bind "$WORKDIR:/ws" \
                --pwd "/ws" \
                "$SIF" bash /ws/_metasmith/.bounce.test 2>&1 || echo "Exit code: $?"

            echo ""
            echo "--- Test 2: --workdir (potentially wrong for apptainer) ---"
            apptainer exec \
                --no-home --cleanenv \
                --env "TMPDIR=${TMPDIR:-/tmp}" \
                --bind "${TMPDIR:-/tmp}:${TMPDIR:-/tmp}" \
                --bind "$WORKDIR:/ws" \
                --workdir "/ws" \
                "$SIF" bash /ws/_metasmith/.bounce.test 2>&1 || echo "Exit code: $?"
        else
            echo "No SIF found, skipping direct apptainer test"
        fi
    else
        echo "apptainer not available"
    fi
else
    echo "msm launcher not found at $MSM"
fi

echo ""
echo "=== Test complete ==="
echo "Cleanup: rm -rf $WORKDIR"
