#!/bin/bash
# Compile a benchmark library's _metadata: `build.sh e3`, or every library with no argument.
# Run it after editing a transform, a type or an image pin, for that library only.
# CAUTION a transform's id hashes its path and mtime, so rebuilding an untouched library forks its cache.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
MAMBA=${MAMBA:-$HOME/.local/bin/mamba}

for lib in ${*:-e2 e3}; do
    uniques=()
    [[ -d "$HERE/resources/$lib" ]] && uniques=(-u "$HERE/resources/$lib")
    PYTHONPATH="$REPO/src" "$MAMBA" run -n msm python -m metasmith build all \
        -t "$REPO/src/metasmith_libraries/data_types" -t "$HERE/data_types" \
        "${uniques[@]}" \
        -r "$HERE/transforms/$lib"
done
