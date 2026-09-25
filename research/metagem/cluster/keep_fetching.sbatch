#!/bin/bash
#SBATCH --job-name=metagem_keeper
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=0:20:00
#SBATCH --output=/scratch/phyberos/metagem/_logs/%x.%j.out
# Re-arms the fetch until one study's manifest rows are satisfied, because one pass will not
# do it. Runs after the array, rebuilds the worklist from what is actually on disk, and either
# submits a fresh array plus another copy of itself, or stops and lets the verifier run.
#
# ONE STUDY PER CHAIN. DATASETS takes a single dataset here, not a list, and the chain names
# every file it owns after it -- worklist, array job name, keeper job name, verify tag. Four
# chains sharing one $ROOT/_worklist.tsv would each rebuild it from disk under the other three,
# so an array would launch against a worklist describing a different study. It is also the only
# shape that reports a rate per study, and throughput here has measured per BioProject rather
# than per network: two Pratama legs against this same ENA front end differed by 250x, one at
# 57 MiB/s and one at 220 KB/s.
#
# WIDTH is a claim on a budget shared with every other chain and with the Pratama leg. ENA
# answers eighteen concurrent streams with `curl (7) Failed to connect ... after 145 ms`, a
# refusal that outlives the run which caused it, so the four WIDTHs plus Pratama's four tasks
# have to sum below that. Do not widen one chain without narrowing another.
set -euo pipefail

ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
DATASETS=${DATASETS:?set DATASETS to one dataset}
MAX_ROUNDS=${MAX_ROUNDS:-30}
ROUND=${ROUND:-1}
WIDTH=${WIDTH:-4}
TAG=${DATASETS// /_}
WORKLIST=$ROOT/_worklist.$TAG.tsv

cd "$ROOT"
python3 build_worklist.py --manifest manifest.tsv --root . --out "$WORKLIST" --dataset $DATASETS
N=$(wc -l < "$WORKLIST")
echo "[$TAG] round $ROUND: $N outstanding"

if [ "$N" -eq 0 ]; then
    echo "[$TAG] ALL FETCHED $(date -Is)"
    TAG="$TAG" DATASETS="$DATASETS" sbatch --job-name="metagem_verify_$TAG" --array=0-3 verify.sbatch
    exit 0
fi
if [ "$ROUND" -ge "$MAX_ROUNDS" ]; then
    echo "[$TAG] STOPPING after $ROUND rounds with $N outstanding"
    exit 1
fi

A=$(WORKLIST="$WORKLIST" sbatch --parsable --job-name="metagem_fetch_$TAG" \
        --array=0-$((N - 1))%${WIDTH} fetch_array.sbatch)
echo "[$TAG] round $ROUND submitted array $A width $WIDTH"
ROUND=$((ROUND + 1)) DATASETS="$DATASETS" MAX_ROUNDS="$MAX_ROUNDS" WIDTH="$WIDTH" \
    sbatch --job-name="metagem_keeper_$TAG" --dependency=afterany:$A keep_fetching.sbatch
