#!/bin/bash
# ECSPr runtime -- build and check the ecspr image locally.
#
# THE BUILD CONTEXT IS THE REPO ROOT, not this directory: the image installs the
# package and its env spec from src/ecspr, and a copy of either under docker/
# would be a second place for them to be wrong.
#
# The image is NOT what the dev loop uses. `./dev.sh -e` runs the editable install
# in the `ecspr` conda env, and `env::ecspr.env` carries a `conda:` key, so the
# transform runs under `--runtime mamba` with no image at all. This image is for
# the container runtimes.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO=$( cd -- "$HERE/../.." &> /dev/null && pwd )
IMAGE=${ECSPR_IMAGE:-ecspr:local}

VERIFY="from sksparse.cholmod import cho_factor; import numpy as np, scipy.sparse as sp; \
A = sp.csc_matrix(np.array([[4.0,1.0],[1.0,3.0]])); \
x = cho_factor(A).solve(np.array([1.0,2.0]).reshape(-1,1)).ravel(); \
assert np.allclose(x, np.linalg.solve(A.toarray(), [1.0,2.0])), 'CHOLMOD solve is wrong'; \
import ecspr, cobra, pandas, pyarrow; \
from ecspr.directed import _HAVE_CHOLMOD; assert _HAVE_CHOLMOD, 'built without CHOLMOD'; \
print('ecspr OK:', ecspr.__version__, '| cholmod + cobra', cobra.__version__)"

case "${1:-}" in
    --build|-b)
        mkdir -p "$HERE/load"
        # tini is the one thing the image needs that is not in the repo. Take the
        # copy docker/fabfos already downloaded when it is there.
        [ -f "$HERE/load/tini" ] || cp "$REPO/docker/fabfos/load/tini" \
            "$HERE/load/tini" 2>/dev/null || wget -q \
            https://github.com/krallin/tini/releases/download/v0.19.0/tini \
            -O "$HERE/load/tini"
        export DOCKER_BUILDKIT=1
        docker build --build-arg="VERIFY=$VERIFY" \
            -t "$IMAGE" -f "$HERE/dockerfile" "$REPO"
    ;;
    --check|-c)
        docker run --rm "$IMAGE" python -c "$VERIFY"
        docker run --rm "$IMAGE" ecspr --where
        docker run --rm "$IMAGE" python -m pytest -q /opt/tests/ecspr
    ;;
    --run|-r)
        docker run -it --rm \
            --mount type=bind,source="$REPO",target="/ws" --workdir="/ws" \
            "$IMAGE" /bin/bash
    ;;
    *)
        echo "usage: dev.sh [--build|--check|--run]"
        echo "typical: --build && --check"
        exit 1
    ;;
esac
