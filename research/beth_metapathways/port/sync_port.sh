#!/bin/bash
# Put this commit on fir for the port driver, under the port's own directory: the engine, the
# libraries with their compiled _metadata, the bench drivers and this directory. Then push the
# engine as the port home's dev overlay. Run from the workstation after `run_port.py setup --deploy`.
# Prints the checkout path. Then, from a fir login node:
#   sbatch -J beth_mp_port --output=/scratch/phyberos/beth_mp/port/logs/%x.%j.out \
#       <checkout>/research/metasmith_benchmark/drivers/submit_driver.sbatch \
#       <checkout> <checkout>/research/beth_metapathways/port/run_port.py import --sample S
# CAUTION the checkout copies the working tree, ignored build products included, so it refuses
# when that tree differs from HEAD.
# CAUTION a pushed overlay restages on every node of a live run, since the stage key is the tarball's
# mtime. Set SKIP_OVERLAY=1 when only the driver changed and a run is live.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
HOST="${MSM_HPC_HOST:-fir}"
SHA="$(git -C "$REPO" rev-parse --short=8 HEAD)"
PORT=/scratch/phyberos/beth_mp/port
DEST="$PORT/checkout/$SHA"
HOME_DIR="$PORT/metasmith"
PATHS=(
    src/metasmith
    src/metasmith_libraries
    research/metasmith_benchmark/drivers
    research/beth_metapathways/port
)

dirty="$(git -C "$REPO" status --porcelain -- "${PATHS[@]}")"
[ -z "$dirty" ] || { echo "commit first; these differ from HEAD:" >&2; echo "$dirty" >&2; exit 1; }
[ -x "$REPO/src/metasmith/engine/msm_solver.x86_64-linux" ] || { echo "no solver binary under src/metasmith/engine" >&2; exit 1; }
ls "$REPO"/src/metasmith_libraries/transforms/*/_metadata/index.yml >/dev/null 2>&1 \
    || { echo "no library _metadata; run dev/libraries.sh -bm" >&2; exit 1; }

cd "$REPO"
ssh "$HOST" "mkdir -p $DEST $PORT/logs && test -d $HOME_DIR"
rsync -a --relative --exclude='__pycache__' --exclude='*.pyc' --exclude='.cache' "${PATHS[@]}" "$HOST:$DEST/"
ssh "$HOST" "echo $SHA > $DEST/COMMIT"
[ "${SKIP_OVERLAY:-0}" = 1 ] || MSM_HPC_HOST="$HOST" MSM_AGENT_HOME="$HOME_DIR" "$REPO/research/cami/ops/push_dev_overlay.sh"
echo "checkout: $HOST:$DEST"
