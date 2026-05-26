#!/bin/bash
# Build local artifacts so the e2e suite tests the *current* build instead of
# pulling release versions:
#   1. pip wheel    via dev.sh -bp
#   2. conda pkg    via dev.sh -bc      (lands under ./conda_build/)
#   3. docker image via dev.sh -bd      (tags quay.io/hallamlab/metasmith:<VER>)
#   4. (optional) apptainer .sif via dev.sh -bs (only if --apptainer)
#
# Per-test conda envs are created inside each test's ephemeral sandbox by the
# harness — there is NO shared msm_env_test any more.
#
# Idempotent: each step is skipped if its output already looks fresh.
# Force a full rebuild with `FORCE=1` or `--force`.
set -euo pipefail

# Isolate the build from any host PYTHONPATH pointing at an older metasmith
# (e.g. /home/tony/lib/locals). setup.py's sys.path uses a set() that
# reshuffles import priority — if a stale metasmith is reachable, the wheel
# gets built with the wrong version baked in.
unset PYTHONPATH

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT=$( cd "$HERE/../../.." &> /dev/null && pwd )
VER=$(cat "$PROJECT/src/metasmith/version.txt")
DOCKER_TAG="${VER//+/-}"
DOCKER_IMAGE="quay.io/hallamlab/metasmith:$DOCKER_TAG"

WANT_APPTAINER=0
FORCE=${FORCE:-0}
for arg in "$@"; do
    case "$arg" in
        --apptainer) WANT_APPTAINER=1 ;;
        --force)     FORCE=1 ;;
        *)
            echo "unknown flag: $arg" >&2
            echo "usage: $0 [--apptainer] [--force]" >&2
            exit 2
            ;;
    esac
done

cd "$PROJECT"
echo "=== build_local_artifacts (version $VER) ==="

# --- 1. pip wheel ----------------------------------------------------------
WHEEL="$PROJECT/dist/metasmith-${VER}-py3-none-any.whl"
if [ "$FORCE" = "1" ] || [ ! -f "$WHEEL" ]; then
    echo "[1/4] building pip wheel"
    ./dev.sh -bp
else
    echo "[1/4] wheel already present: $WHEEL"
fi

# --- 2. conda package ------------------------------------------------------
if [ "$FORCE" = "1" ] || ! find "$PROJECT/conda_build" -name "metasmith-${VER}*.tar.bz2" 2>/dev/null | grep -q .; then
    echo "[2/4] building conda package"
    ./dev.sh -bc
else
    echo "[2/4] conda package already present"
fi

# --- 3. docker image -------------------------------------------------------
if [ "$FORCE" = "1" ] || ! docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
    echo "[3/4] building docker image $DOCKER_IMAGE"
    ./dev.sh -bd
else
    echo "[3/4] docker image already present: $DOCKER_IMAGE"
fi

# --- 4. apptainer .sif (optional) ------------------------------------------
SIF="$PROJECT/metasmith.sif"
if [ "$WANT_APPTAINER" = "1" ]; then
    if [ "$FORCE" = "1" ] || [ ! -f "$SIF" ] || [ "$SIF" -ot "$PROJECT/src/metasmith/version.txt" ]; then
        echo "[4/4] building apptainer .sif from local docker image"
        ./dev.sh -bs
    else
        echo "[4/4] apptainer .sif already present: $SIF"
    fi
else
    echo "[4/4] skipping apptainer (.sif) — pass --apptainer to enable"
fi

echo "=== done ==="
echo "version:      $VER"
echo "docker image: $DOCKER_IMAGE"
echo "conda chan:   $PROJECT/conda_build  (use as: file://\$PROJECT/conda_build)"
echo "apptainer:    $([ -f "$SIF" ] && echo "$SIF" || echo "(none — pass --apptainer)")"
