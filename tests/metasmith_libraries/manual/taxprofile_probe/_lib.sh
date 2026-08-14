# Shared helpers for probe_*.sh scripts. Sourced, not executed.
# Configured for UBC ARC Sockeye (st-shallam-1).
#
# Usage in each probe:
#   source "$(dirname "$0")/_lib.sh"
#   probe_init kraken2          # sets RUN_DIR, IMAGE, R1, R2
#   probe_alloc                 # ensures $JOBID is set (reuses if present)
#   probe_run "<command>"       # runs cmd inside $IMAGE via srun apptainer exec

set -euo pipefail

WORK=${TAXPROFILE_WORK:-/scratch/st-shallam-1/txyliu/taxprofile_probe}
ACCOUNT=${TAXPROFILE_ACCOUNT:-st-shallam-1}
PARTITION=${TAXPROFILE_PARTITION:-interactive_cpu}
MEM=${TAXPROFILE_MEM:-32G}
CPUS=${TAXPROFILE_CPUS:-8}
TIME=${TAXPROFILE_TIME:-2:00:00}
JOB_NAME=${TAXPROFILE_JOB:-taxprofile-probe}

# Sockeye-specific: apptainer is module-loaded, not on PATH by default.
ensure_apptainer() {
    if ! command -v apptainer >/dev/null; then
        module load gcc/9.4.0 apptainer/1.3.1 2>/dev/null || true
    fi
    command -v apptainer >/dev/null || { echo "apptainer not available after module load" >&2; exit 1; }
}

probe_init() {
    TOOL="$1"
    IMAGE="$WORK/containers/${TOOL}.sif"
    RUN_DIR="$WORK/runs/${TOOL}"
    R1="$WORK/fixtures/tiny_pe_R1.fq.gz"
    R2="$WORK/fixtures/tiny_pe_R2.fq.gz"
    LOG="$RUN_DIR/probe.log"
    mkdir -p "$RUN_DIR"
    : >"$LOG"
    ensure_apptainer
    [[ -f "$IMAGE" ]] || { echo "missing $IMAGE - run ./bootstrap.sh first" | tee -a "$LOG" >&2; exit 1; }
    [[ -f "$R1" && -f "$R2" ]] || { echo "missing tiny reads - run ./bootstrap.sh first" | tee -a "$LOG" >&2; exit 1; }
    echo "[probe:$TOOL] image=$IMAGE" | tee -a "$LOG"
}

probe_alloc() {
    JOBID=$(squeue -u "$USER" -h -n "$JOB_NAME" -o %i 2>/dev/null | head -1)
    if [[ -z "$JOBID" ]]; then
        salloc --no-shell \
            --account="$ACCOUNT" --partition="$PARTITION" \
            --nodes=1 --ntasks=1 \
            --cpus-per-task="$CPUS" --mem="$MEM" --time="$TIME" \
            --job-name="$JOB_NAME"
        # Wait for allocation to materialize
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            JOBID=$(squeue -u "$USER" -h -n "$JOB_NAME" -o %i 2>/dev/null | head -1)
            [[ -n "$JOBID" ]] && break
            sleep 2
        done
    fi
    [[ -n "$JOBID" ]] || { echo "could not get JOBID" >&2; exit 1; }
    echo "[probe:$TOOL] using JOBID=$JOBID" | tee -a "$LOG"
}

probe_run() {
    local cmd="$1"
    echo "[probe:$TOOL] \$ $cmd" | tee -a "$LOG"
    srun --jobid="$JOBID" apptainer exec \
        --bind "$WORK:$WORK" \
        --bind /scratch/st-shallam-1:/scratch/st-shallam-1:ro \
        --pwd "$RUN_DIR" \
        "$IMAGE" bash -c "$cmd" 2>&1 | tee -a "$LOG"
}

probe_record() {
    echo "[probe:$TOOL] output tree:" | tee -a "$LOG"
    (cd "$RUN_DIR" && find . -maxdepth 3 -type f | sort) | tee -a "$LOG"
    echo "[probe:$TOOL] sizes:" | tee -a "$LOG"
    (cd "$RUN_DIR" && ls -lh) | tee -a "$LOG"
}
