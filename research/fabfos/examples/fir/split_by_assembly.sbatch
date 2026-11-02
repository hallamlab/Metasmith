#!/bin/bash
# Phase 1 (partition) and phase 2 (compact) of the per-assembly split.
#
#   MODE=partition sbatch --array=0-31 split_by_assembly.sbatch
#   MODE=compact   sbatch --array=0-31 split_by_assembly.sbatch
#   MODE=finish    sbatch --array=0    split_by_assembly.sbatch
#
# Memory is sized for the WORST sample, not the median. The corpus median is
# 6,081 ORFs (~71k evidence rows) but its maximum is 1,667,518 ORFs -- at the
# measured ~11.7 rows/ORF that one assembly compacts ~19.5M rows in pandas, and
# a task sized for the median dies only on the sample that matters most.
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=cv_split
#SBATCH --cpus-per-task=2
#SBATCH --mem=64G
#SBATCH --time=3:00:00
#SBATCH --output=/project/rpp-shallam/phyberos/cyanoverse/gpr/audit/logs/split_%A_%a.out
set -euo pipefail
shopt -s nullglob     # an unmatched glob must expand to NOTHING, not to itself:
                      # the literal pattern reaches python as a path that does
                      # not exist, and every array task then exits 0 having done
                      # nothing, surfacing one phase later as a missing part.
module load apptainer

ROOT=/project/rpp-shallam/phyberos/cyanoverse
GPR=$ROOT/gpr
SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_python_for_data_science..1.2.5.sif
MODE=${MODE:?set MODE=partition|compact|finish}

export SHARD_N=${SLURM_ARRAY_TASK_COUNT:-1}
export FABFOS_LIB=$GPR/scripts/lib
# Pins the contract the delivered tables are re-validated against to the copy
# `gpr_4lane` ran under. Written by `sync_split_lib.sh` at sync time; unset
# means unpinned, which the splitter allows but does not recommend.
if [ -f "$GPR/scripts/lib/fabfos_evidence.sha256" ]; then
    export FABFOS_LIB_SHA256=$(cat "$GPR/scripts/lib/fabfos_evidence.sha256")
fi

ARGS=()
if [ "$MODE" = partition ]; then
    # Every batch's results directory, read in place on the cluster rather than
    # retrieved -- the shard tables are tens of GB and the split's output is the
    # same volume again, so a round trip through the workstation buys nothing.
    # `shard_tables` keys by the table's own `source`, and a shard reachable
    # from two runs REFUSES rather than picking one: the two may come from
    # different library commits. Keep superseded runs out of this glob.
    ARGS=("$GPR"/agent_home/runs/*/results)
    if [ ${#ARGS[@]} -eq 0 ]; then
        echo "no run results directories under $GPR/agent_home/runs/*/results" >&2
        exit 1
    fi
fi

apptainer exec -B /project -B /scratch "$SIF" \
    python3 "$GPR/scripts/split_by_assembly.py" "$MODE" "$GPR" "${ARGS[@]}"
