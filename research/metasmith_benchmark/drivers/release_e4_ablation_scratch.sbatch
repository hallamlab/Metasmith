#!/bin/bash
# Delete fir's /scratch/phyberos/e4_ablation once verify_e4_ablation_archive.sh has stamped its
# chinook copy. GUARD_ONLY=1 runs every check and stops before the delete.
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=release_e4abl
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=6:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/release_e4abl.%j.out
set -euo pipefail

TARGET=/scratch/phyberos/e4_ablation
STAMP=/scratch/phyberos/bench/e4_ablation_archive_20261105/VERIFIED

quota() { { lfs quota -p 83115734 /scratch | tail -1; } || true; }

[[ "$TARGET" == "/scratch/phyberos/e4_ablation" && -d "$TARGET" && ! -L "$TARGET" ]] \
    || { echo "ABORT: $TARGET is not the e4_ablation directory"; exit 1; }

echo "== the archive is verified =="
[ -s "$STAMP" ] || { echo "ABORT: no verification stamp at $STAMP"; exit 1; }
cat "$STAMP"
grep -q '^RESULT=PASS$' "$STAMP" || { echo "ABORT: the stamp does not record a pass"; exit 1; }
SINCE=$(sed -n 's/^INVENTORY_STARTED_AT=//p' "$STAMP")
[ -n "$SINCE" ] || { echo "ABORT: stamp has no INVENTORY_STARTED_AT"; exit 1; }

echo "== nothing is writing to the tree =="
others=$(squeue -u "$USER" -h -o '%i' | awk -v self="$SLURM_JOB_ID" '$1 != self && $1 !~ "^"self"_"')
rooted=""
for j in $others; do
    if scontrol show job "$j" 2>/dev/null | tr ' ' '\n' | grep -E '^(WorkDir|Command|StdOut)=' | grep -q "$TARGET"; then
        rooted="$rooted $j"
    fi
done
[ -z "$rooted" ] || { echo "ABORT: jobs rooted in the tree:$rooted"; exit 1; }
named=$(squeue -u "$USER" -h -o '%i %j' | awk -v self="$SLURM_JOB_ID" '$1 != self' | grep -iE 'e4abl|e4_abl' || true)
[ -z "$named" ] || { echo "ABORT: jobs named for e4_ablation: $named"; exit 1; }
echo "  none"

echo "== nothing changed after the inventory =="
touched=$(find "$TARGET" \( -newermt "$SINCE" -o -newerct "$SINCE" \) -print -quit 2>/dev/null || true)
[ -z "$touched" ] || { echo "ABORT: $touched changed after $SINCE"; exit 1; }
echo "  nothing"

echo "== before =="
echo "target inodes: $(lfs find "$TARGET" | wc -l)"
echo "quota:         $(quota)"

if [ "${GUARD_ONLY:-0}" = 1 ]; then
    echo "GUARD_ONLY: every check passed; nothing deleted"
    exit 0
fi

rm -rf -- /scratch/phyberos/e4_ablation

echo "== after =="
echo "target: $([ -e "$TARGET" ] && echo STILL PRESENT || echo gone)"
sleep 300
echo "quota 5 min after: $(quota)"
