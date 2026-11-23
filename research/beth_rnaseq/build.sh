#!/usr/bin/env bash
# Compile the isolate_rnaseq library against this tree's standard library, then solve and render
# both plans. Needs the standard library's _metadata first: dev/libraries.sh -bm
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
export PYTHONPATH=$ROOT/src

mamba run -n msm python -m metasmith build all --types "$ROOT/src/metasmith_libraries/data_types" \
  --types "$HERE/library/data_types" --transforms "$HERE/library/transforms/isolate_rnaseq" \
  > "$HERE/results/build_library.log"
mamba run -n msm python "$HERE/run_sockeye.py" dag
