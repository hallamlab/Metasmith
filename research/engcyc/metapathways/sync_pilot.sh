#!/bin/bash
# Put this commit on fir for the pilot driver: the bench checkout and dev overlay (sync.sh),
# plus this directory, which sync.sh does not carry. Run from the workstation. Prints the checkout path.
# Then, from a fir login node, one job at a time per home:
#   sbatch -J mp_pilot <checkout>/research/metasmith_benchmark/drivers/submit_driver.sbatch \
#       <checkout> <checkout>/research/engcyc/metapathways/run_pilot.py import --sample smoke
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
HOST="${MSM_HPC_HOST:-fir}"
SHA="$(git -C "$REPO" rev-parse --short=8 HEAD)"
DEST="/scratch/phyberos/bench/checkout/$SHA"

dirty="$(git -C "$REPO" status --porcelain -- research/engcyc/metapathways)"
[ -z "$dirty" ] || { echo "commit first; these differ from HEAD:" >&2; echo "$dirty" >&2; exit 1; }

SYNC_HOMES=/scratch/phyberos/engcyc/mp_pilot/metasmith "$REPO/research/metasmith_benchmark/drivers/sync.sh"
cd "$REPO"
rsync -a --relative --exclude='__pycache__' --exclude='.cache' research/engcyc/metapathways "$HOST:$DEST/"
echo "checkout: $HOST:$DEST"
