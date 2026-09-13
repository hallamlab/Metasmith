#!/bin/bash
# Compile the E2 library's _metadata. Run it after editing a transform, a type or an image pin.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
MAMBA=${MAMBA:-$HOME/.local/bin/mamba}

PYTHONPATH="$REPO/src" "$MAMBA" run -n msm python -m metasmith build all \
    -t "$REPO/src/metasmith_libraries/data_types" -t "$HERE/data_types" \
    -u "$HERE/resources/e2" \
    -r "$HERE/transforms/e2"
