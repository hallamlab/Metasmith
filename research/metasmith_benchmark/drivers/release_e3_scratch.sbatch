#!/bin/bash
# Delete E3's metasmith tree from scratch, once the chinook archive is verified.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/release_e3_scratch.sbatch
#
# WARNING This is the only irreversible step in the archive plan. It removes 164,979 inodes and
# 2.24 TB, including the sole local copy of a content-addressed cache that eight runs share and
# that no code can rebuild. Everything before it is additive.
#
# CAUTION The gate is a stamp file, not a human's recollection. `verify_e3_archive.sh stamp` writes
# it only after re-reading the resync task from Globus and confirming it moved zero bytes with no
# failed subtask. This job refuses to start without that stamp, and refuses if the stamp names a
# task that did not pass. Do not write the stamp by hand.
#
# It deletes ONLY $HOME_PATH/metasmith. The read corpus beside it stays: `interleaved/`,
# `reads_2019/`, `reads_2022/` and `zenodo_*` cost about 600 inodes between them, so removing them
# would buy 0.4% of the inode goal while giving up the corpus on the one filesystem that holds it.
#SBATCH --account=rpp-shallam
#SBATCH --job-name=release_e3_scratch
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=6:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/release_e3_scratch.%j.out
set -euo pipefail

HOME_PATH=/scratch/phyberos/pratama2026
TARGET="$HOME_PATH/metasmith"
STAMP=/scratch/phyberos/bench/e3_verify/VERIFIED
ARCHIVE_STARTED="2026-09-22 12:14:08"

quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }

# Never touch these, whatever else changes. A typo in TARGET must fail rather than delete a corpus,
# another experiment's tree, or a reference database whose mtime is itself a cache key.
for forbidden in \
    /scratch/phyberos/refs /scratch/phyberos/viromics_refs /scratch/phyberos/databases \
    /scratch/phyberos/staging /scratch/phyberos/wave2_b3_nfcore /scratch/phyberos/cami \
    /scratch/phyberos/metagem /scratch/phyberos/bench /scratch/phyberos/cache \
    "$HOME_PATH/interleaved" "$HOME_PATH/reads_2019" "$HOME_PATH/reads_2022"
do
    [[ "$TARGET" != "$forbidden" && "$TARGET" != "$forbidden"/* ]] \
        || { echo "ABORT: target $TARGET lies in protected $forbidden"; exit 1; }
done
[[ "$TARGET" == "$HOME_PATH/metasmith" && -d "$TARGET" ]] \
    || { echo "ABORT: $TARGET is not E3's metasmith directory"; exit 1; }

echo "== the archive is verified =="
[ -s "$STAMP" ] || { echo "ABORT: no verification stamp at $STAMP"; exit 1; }
cat "$STAMP"
grep -q '^RESULT=PASS$' "$STAMP" || { echo "ABORT: the stamp does not record a pass"; exit 1; }

echo "== nothing is writing to the home =="
rooted=$(squeue -u "$USER" -h -o '%i' | while read -r j; do
             scontrol show job "$j" 2>/dev/null | tr ' ' '\n' \
               | grep -E '^(WorkDir|Command)=' | grep -q "$HOME_PATH" && echo "$j"
         done)
[ -z "$rooted" ] || { echo "ABORT: jobs rooted in the home: $rooted"; exit 1; }
[ ! -e "$HOME_PATH/metasmith/runs/Qt0rbV1R/RUN.pgid" ] || echo "note: RUN.pgid present but no driver holds it"
touched=$(find "$TARGET" -newermt "$ARCHIVE_STARTED" -print -quit 2>/dev/null || true)
[ -z "$touched" ] || { echo "ABORT: $touched changed after the archive began"; exit 1; }

echo "== before =="
echo "target inodes: $(lfs find "$TARGET" | wc -l)"
echo "target bytes:  $(du -sb "$TARGET" | cut -f1)"
echo "quota:         $(quota)"

rm -rf -- "$TARGET"

echo "== after =="
echo "target: $([ -e "$TARGET" ] && echo STILL PRESENT || echo gone)"
echo "the corpus beside it must survive:"
for keep in interleaved reads_2019 reads_2022 zenodo_17897233 zenodo_17897233_unpacked; do
    printf "  %-26s %s\n" "$keep" "$([ -d "$HOME_PATH/$keep" ] && echo ok || echo 'GONE -- investigate')"
done
sleep 600
echo "quota 10 min after: $(quota)"
