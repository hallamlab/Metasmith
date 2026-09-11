#!/bin/bash
# Run one transform from the shipping library directly, no solver and no Nextflow.
#
#   run_one.sh <workdir-name> <transform-relpath> <TYPE=PATH> [<TYPE=PATH> ...]
#
# METASMITH_WORK_ROOT is pinned to the work directory so the container's /ws bind
# and the host paths the protocol is handed are the same directory. Without the
# pin the outputs still land, but only because some input's bind happens to cover
# them -- which is luck, not a guarantee.
set -euo pipefail
HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/../../../.." &> /dev/null && pwd )
NAME=$1; shift
TRANSFORM=$1; shift
W="$HERE/data/scratch/r5_runs/$NAME"
rm -rf "$W"; mkdir -p "$W"

args=()
for spec in "$@"; do args+=(-i "$spec"); done

systemd-run --user --scope -p MemoryMax=8G --quiet \
  env PYTHONPATH="$HERE/src" METASMITH_WORK_ROOT="$W" \
  mamba run -n msm python -m metasmith run \
    -l "$HERE/src/metasmith_libraries/transforms/kbase" \
    -t "$TRANSFORM" \
    "${args[@]}" \
    -w "$W"
echo "--- products in $W ---"
ls -la "$W"
