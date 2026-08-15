#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=truthcount
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --time=2:00:00
#SBATCH --array=0-33
#SBATCH --output=/scratch/phyberos/gmcf3495/truth/slurm-%A_%a.out

# Independent ground-truth read counts for every staged library.
#
# This exists because of F1: bbmap's BGZF reader can mangle a plain-gzip stream,
# and two of the three candidate fixes pass an exit-code check while destroying
# data (multithreadedbgzf=f silently dropped 90% of reads and exited 0). So the
# interleave step's own reported counts are NOT sufficient evidence -- they come
# from the same reader that could be wrong. pigz is a genuinely independent
# decompressor, so these counts are the external check the DAG's outputs get
# compared against.

set -uo pipefail

OUT=/scratch/phyberos/gmcf3495/truth
mkdir -p "$OUT"

READS=/scratch/phyberos/gmcf3495/reads
mapfile -t SAMPLES < <(ls "$READS"/*_R1.fastq.gz | xargs -n1 basename | sed 's/_R1\.fastq\.gz$//' | sort)

s="${SAMPLES[$SLURM_ARRAY_TASK_ID]}"
[ -n "$s" ] || { echo "no sample at index $SLURM_ARRAY_TASK_ID"; exit 1; }

echo "sample=$s start=$(date -u +%FT%TZ)"

count_reads () {
    # lines/4; pigz exit status is checked separately because wc always succeeds
    local f="$1"
    local lines
    lines=$(pigz -p 4 -dc "$f" | wc -l)
    local rc=${PIPESTATUS[0]}
    if [ "$rc" -ne 0 ]; then
        echo "PIGZ_FAIL rc=$rc on $f" >&2
        return 1
    fi
    if [ $(( lines % 4 )) -ne 0 ]; then
        echo "TRUNCATED $f: $lines lines not divisible by 4" >&2
        return 1
    fi
    echo $(( lines / 4 ))
}

r1=$(count_reads "$READS/${s}_R1.fastq.gz") || { echo "FAILED r1 $s"; exit 1; }
r2=$(count_reads "$READS/${s}_R2.fastq.gz") || { echo "FAILED r2 $s"; exit 1; }

if [ "$r1" != "$r2" ]; then
    echo "MATE_MISMATCH $s r1=$r1 r2=$r2"
    status=MISMATCH
else
    status=OK
fi

# expected interleaved read count is r1 + r2
printf '%s\t%s\t%s\t%s\t%s\n' "$s" "$r1" "$r2" "$(( r1 + r2 ))" "$status" > "$OUT/${s}.truth"
echo "sample=$s r1=$r1 r2=$r2 interleaved_expected=$(( r1 + r2 )) status=$status end=$(date -u +%FT%TZ)"
