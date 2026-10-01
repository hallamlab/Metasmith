#!/bin/bash
# Score one packed tar of e4_ablation_memote.py's models with MEMOTE 0.17 as the modern lane ran it
# (memote_score.py), PARALLEL models at a time. Everything is unpacked and scored in $SLURM_TMPDIR. The
# task writes one tar of results to <work>/memote/<kind>_NN.tar, holding <source>/<bin>.score.json,
# <source>/<bin>.json.gz and status.tsv (source, bin, exit code, seconds). Exit 124 is a timeout.
#
#   sbatch --array=1-<n> e4_ablation_memote.sbatch <work> <images dir> <memote_score.py> [block|retry]
#
# CAUTION a task killed at its time limit packs what it scored so far, on the USR1 that --signal sends.
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=8
#SBATCH --mem=24G
#SBATCH --time=4:00:00
#SBATCH --signal=B:USR1@600
#SBATCH --output=/scratch/phyberos/bench/logs/e4_ablation_memote.%A_%a.out
set -euo pipefail

export WORK="$1" IMAGES="$2" SCORE="$3"
KIND="${4:-block}"
NAME="${KIND}_$(printf %02d "$SLURM_ARRAY_TASK_ID")"
export T="${SLURM_TMPDIR:-/tmp}/e4m.$NAME"
export MODEL_TIMEOUT="${MODEL_TIMEOUT:-30m}"
PARALLEL="${PARALLEL:-4}"
mkdir -p "$T/in" "$T/out" "$T/home" "$WORK/memote"
module load apptainer 2>/dev/null || true

score_one() {
    local src="$1" bin="$2" start=$SECONDS rc=0
    mkdir -p "$T/out/$src" "$T/run/$src.$bin"
    gunzip -c "$T/in/$src/$bin.xml.gz" > "$T/run/$src.$bin/model.xml"
    timeout "$MODEL_TIMEOUT" apptainer exec --cleanenv --env "PYTHONNOUSERSITE=1,HOME=$T/home" \
        -B "$T,$(dirname "$SCORE")" --pwd "$T/run/$src.$bin" "$IMAGES/memote_017.sif" \
        python "$SCORE" "$T/run/$src.$bin/model.xml" "$T/run/$src.$bin/report.html" \
        "$T/out/$src/$bin.json.gz" "$T/out/$src/$bin.score.json" > "$T/run/$src.$bin/log" 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || tail -5 "$T/run/$src.$bin/log" | sed "s|^|$src/$bin: |"
    rm -r "$T/run/$src.$bin"
    printf '%s\t%s\t%s\t%s\n' "$src" "$bin" "$rc" "$((SECONDS - start))" >> "$T/out/status.tsv"
}
export -f score_one

pack() {
    tar -cf "$WORK/memote/$NAME.tar.part" -C "$T/out" .
    mv "$WORK/memote/$NAME.tar.part" "$WORK/memote/$NAME.tar"
    echo "packed $(grep -c . "$T/out/status.tsv" || true) models, $(awk '$3 != 0' "$T/out/status.tsv" | wc -l) failed"
}
trap 'echo "time limit near, packing"; kill "$scorer" 2>/dev/null || true; pack; exit 1' USR1

tar -xf "$WORK/models/$NAME.tar" -C "$T/in"
touch "$T/out/status.tsv"
(cd "$T/in" && ls */*.xml.gz) | sed 's|/| |; s|\.xml\.gz$||' \
    | xargs -P "$PARALLEL" -n 2 bash -c 'score_one "$@"' _ &
scorer=$!
wait "$scorer" || echo "scorer exit $?"
pack
