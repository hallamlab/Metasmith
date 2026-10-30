#!/usr/bin/env bash
# Compile the mock library against this tree's standard library, then solve and render.
# Needs the standard library's _metadata first: dev/libraries.sh -bm
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
export MSM_SRC=$ROOT/src
msm() { PYTHONPATH=$MSM_SRC mamba run -n msm python -m metasmith "$@"; }

mkdir -p "$HERE/results"
PYTHONPATH=$MSM_SRC mamba run -n msm python "$HERE/mocklib/generate.py"
msm build all --types "$MSM_SRC/metasmith_libraries/data_types" --types "$HERE/mocklib/data_types" \
  --transforms "$HERE/mocklib/transforms/isolate_rnaseq" > "$HERE/results/build_mock.log"

PYTHONPATH=$MSM_SRC mamba run -n msm python "$HERE/render.py" "$HERE/results"
