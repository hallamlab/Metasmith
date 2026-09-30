#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=release_metagem_scratch
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=6:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/release_metagem_scratch.%j.out
set -euo pipefail

TARGET=/scratch/phyberos/metagem
STAMP=/scratch/phyberos/bench/metagem_archive/VERIFIED

quota() { { lfs quota -p 83115734 /scratch | tail -1; } || true; }

for forbidden in \
    /scratch/phyberos/refs /scratch/phyberos/viromics_refs /scratch/phyberos/databases \
    /scratch/phyberos/staging /scratch/phyberos/wave2_b3_nfcore /scratch/phyberos/cami \
    /scratch/phyberos/cami_nfcore_split_reads /scratch/phyberos/pratama2026 \
    /scratch/phyberos/bench /scratch/phyberos/cache /scratch/phyberos/apptainer_cache \
    /scratch/phyberos/msm_lib /scratch/phyberos/aspire_mock
do
    [[ "$TARGET" != "$forbidden" && "$TARGET" != "$forbidden"/* ]] \
        || { echo "ABORT: target $TARGET lies in protected $forbidden"; exit 1; }
done
[[ "$TARGET" == "/scratch/phyberos/metagem" && -d "$TARGET" ]] \
    || { echo "ABORT: $TARGET is not the metagem directory"; exit 1; }

echo "== the archive is verified =="
[ -s "$STAMP" ] || { echo "ABORT: no verification stamp at $STAMP"; exit 1; }
cat "$STAMP"
grep -q '^RESULT=PASS$' "$STAMP" || { echo "ABORT: the stamp does not record a pass"; exit 1; }
eval "$(grep -E '^TREE_REQUESTED_AT=' "$STAMP")"
ARCHIVE_STARTED="$TREE_REQUESTED_AT"
[ -n "$ARCHIVE_STARTED" ] || { echo "ABORT: stamp has no TREE_REQUESTED_AT"; exit 1; }

echo "== nothing is writing to the tree =="
rooted=$(squeue -u "$USER" -h -o '%i' | awk -v self="$SLURM_JOB_ID" '$1 != self' | while read -r j; do
             if scontrol show job "$j" 2>/dev/null | tr ' ' '\n' \
                  | grep -E '^(WorkDir|Command)=' | grep -q "$TARGET"; then
                 echo "$j"
             fi
         done)
[ -z "$rooted" ] || { echo "ABORT: jobs rooted in the tree: $rooted"; exit 1; }
namedjobs=$(squeue -u "$USER" -h -o '%i %j' | awk -v self="$SLURM_JOB_ID" '$1 != self' | grep -i metagem || true)
[ -z "$namedjobs" ] || { echo "ABORT: jobs named metagem: $namedjobs"; exit 1; }
echo "  none"

echo "== nothing changed after the archive was stamped =="
touched=$(find "$TARGET" \( -newermt "$ARCHIVE_STARTED" -o -newerct "$ARCHIVE_STARTED" \) -print -quit 2>/dev/null || true)
[ -z "$touched" ] || { echo "ABORT: $touched changed after the archive was stamped"; exit 1; }
echo "  nothing"

echo "== before =="
echo "target inodes: $(lfs find "$TARGET" | wc -l)"
echo "target bytes:  $(du -sb "$TARGET" | cut -f1)"
echo "quota:         $(quota)"

rm -rf -- /scratch/phyberos/metagem

echo "== after =="
echo "target: $([ -e "$TARGET" ] && echo STILL PRESENT || echo gone)"
sleep 600
echo "quota 10 min after: $(quota)"
