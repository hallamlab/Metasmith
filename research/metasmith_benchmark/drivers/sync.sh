#!/bin/bash
# Put this commit's drivers, engine and libraries on fir, and push its dev overlay to every
# agent home, for a wave's driver jobs. Run from the workstation. Prints the checkout path.
#
# CAUTION the checkout carries ignored build products (the solver binaries and every
# library's _metadata), so it copies the working tree and refuses when that tree differs
# from HEAD.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
HOST="${MSM_HPC_HOST:-fir}"
SHA="$(git -C "$REPO" rev-parse --short=8 HEAD)"
DEST="/scratch/phyberos/bench/checkout/$SHA"
HOMES=(/scratch/phyberos/cami/metasmith /scratch/phyberos/pratama2026/metasmith /scratch/phyberos/metagem/metasmith)
PATHS=(
    src/metasmith
    src/metasmith_libraries
    research/metasmith_benchmark/drivers
    research/metasmith_benchmark/library
    research/cami/samples.tsv
    research/pratama2026/runs.tsv
    research/metagem/manifest.tsv
    research/metasmith_libraries/carveme_m8_medium.tsv
)

dirty="$(git -C "$REPO" status --porcelain -- "${PATHS[@]}")"
[ -z "$dirty" ] || { echo "commit first; these differ from HEAD:" >&2; echo "$dirty" >&2; exit 1; }
[ -x "$REPO/src/metasmith/engine/msm_solver.x86_64-linux" ] || { echo "no solver binary under src/metasmith/engine" >&2; exit 1; }

cd "$REPO"
ssh "$HOST" "mkdir -p $DEST /scratch/phyberos/bench/logs"
rsync -a --relative --exclude='__pycache__' --exclude='*.pyc' --exclude='.cache' "${PATHS[@]}" "$HOST:$DEST/"
ssh "$HOST" "echo $SHA > $DEST/COMMIT"

for home in "${HOMES[@]}"; do
    MSM_HPC_HOST="$HOST" MSM_AGENT_HOME="$home" "$REPO/research/cami/ops/push_dev_overlay.sh"
done

echo "checkout: $HOST:$DEST"
