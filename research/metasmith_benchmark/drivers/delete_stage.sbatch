#!/bin/bash
# Delete one leftover staging directory once its permanent copy is the only thing in use.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/delete_stage.sbatch <stage dir> <permanent dir>
#
# WARNING the deletion cannot be undone.
# CAUTION the job aborts before deleting anything when a symlink under <permanent dir> resolves into
# <stage dir>, or when a run's workflow files or a benchmark checkout name <stage dir>.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/delete_stage.%j.out
set -euo pipefail

STAGE="$(realpath "$1")"
PERM="$(realpath "$2")"
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }

[[ "$STAGE" == /scratch/phyberos/?* && -d "$STAGE" ]] || { echo "ABORT: $STAGE is not a directory under /scratch/phyberos"; exit 1; }
[[ "$PERM" != "$STAGE" && "$PERM" != "$STAGE"/* && -d "$PERM" ]] || { echo "ABORT: $PERM is not a separate directory"; exit 1; }

links=$(find "$PERM" -type l -exec readlink -f {} + 2>/dev/null | grep -c "^$STAGE" || true)
[ "$links" -eq 0 ] || { echo "ABORT: $links symlinks under $PERM resolve into $STAGE"; exit 1; }
refs=$(grep -rlF "$STAGE" /scratch/phyberos/*/metasmith/runs/*/workflow.* /scratch/phyberos/bench/checkout/*/research/metasmith_benchmark/drivers 2>/dev/null | grep -vc "^$STAGE/" || true)
[ "$refs" -eq 0 ] || { echo "ABORT: $refs files name $STAGE"; exit 1; }

echo "stage $STAGE: $(lfs find "$STAGE" | wc -l) inodes, $(du -sb "$STAGE" | cut -f1) B"
echo "quota before: $(quota)"
rm -rf -- "$STAGE"
echo "left: $([ -e "$STAGE" ] && echo present || echo gone)"
sleep 300
echo "quota 5 min after: $(quota)"
