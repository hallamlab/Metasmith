#!/bin/bash
# cobra runtime -- build, verify and publish the model-solving image.
#
# `quay.io/biocontainers/cobra` is cobrapy and is public, and its LP solver works, but
# it carries neither `requests` nor `scipy` (checked against 0.29.1, digest
# sha256:2fd2e976..., 2026-09-05) and `lib::modelling` needs requests to fetch a BiGG
# model. Its newest tag is also December 2024, because the package moved off bioconda
# to conda-forge, which is where env.yml takes 0.31.*. So this image is the route.
#
# --push is one-way and public. It re-runs --check first, and the `container:` line in
# `resources/env/cobra.env` goes in AFTER a successful push and never before: naming an
# unpublished image points every solve at a registry entry that does not exist, and it
# fails at stage time on whichever host drew the task rather than in front of whoever
# caused it.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
# The tag is the cobra version env.yml pins. Bump both together or the tag lies.
VERSION=0.31.1
IMAGE=${COBRA_IMAGE:-quay.io/hallamlab/cobra:$VERSION}

VERIFY="import cobra, numpy as np, pandas, pyarrow, scipy, requests; \
assert np.__version__ < '2', 'numpy<2 is the whole reason this image is separate'; \
from cobra import Model, Reaction, Metabolite; \
mm = Model('probe'); a = Metabolite('a_c', compartment='c'); b = Metabolite('b_c', compartment='c'); \
r = Reaction('r'); mm.add_reactions([r]); r.add_metabolites({a: -1, b: 1}); r.bounds = (0, 10); \
ex = Reaction('EX_a_e'); mm.add_reactions([ex]); ex.add_metabolites({a: -1}); ex.bounds = (-10, 0); \
sink = Reaction('DM_b_c'); mm.add_reactions([sink]); sink.add_metabolites({b: -1}); sink.bounds = (0, 1000); \
mm.objective = sink; v = mm.slim_optimize(); \
assert v is not None and abs(v - 10) < 1e-6, f'the LP solver is wrong: {v}'; \
print('cobra OK:', cobra.__version__, '| numpy', np.__version__, '| LP solves')"

case "${1:-}" in
    --build|-b)
        mkdir -p "$HERE/load"
        [ -f "$HERE/load/tini" ] || wget -q \
            https://github.com/krallin/tini/releases/download/v0.19.0/tini \
            -O "$HERE/load/tini"
        export DOCKER_BUILDKIT=1
        docker build \
            --build-arg="VERIFY=$VERIFY" \
            -t "$IMAGE" -f "$HERE/dockerfile" "$HERE"
    ;;
    --check|-c)
        docker run --rm "$IMAGE" python -c "$VERIFY"
        # The tag is a claim about what is inside. Prove it rather than trust the
        # build order: a cache-reusing rebuild can carry an older tree under a
        # freshly stamped name.
        got=$(docker run --rm "$IMAGE" python -c "import cobra; print(cobra.__version__)")
        [ "$VERSION" = "$got" ] || {
            echo "ERROR: image reports cobra $got but is tagged $VERSION" >&2
            echo "  bump VERSION here and in env.yml together, then --build again." >&2
            exit 1
        }
        echo "image version matches the tag: $got"
    ;;
    --push|-p)
        # Gated on a --check re-run rather than on a stamp file: a stamp goes stale
        # silently, and this is a public, one-way publication.
        "$HERE/dev.sh" --check
        echo "pushing $IMAGE"
        docker push "$IMAGE"
        echo
        echo "record this digest in resources/env/cobra.env, with the date:"
        docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE"
    ;;
    --run|-r)
        docker run -it --rm \
            --mount type=bind,source="$HERE/../..",target="/ws" --workdir="/ws" \
            "$IMAGE" /bin/bash
    ;;
    *)
        echo "usage: dev.sh [--build|--check|--push|--run]"
        echo "typical: --build && --check && --push"
        echo "  --push re-runs --check; it will not publish an image that fails it"
        echo "  COBRA_IMAGE overrides the derived name:tag for a local experiment"
        exit 1
    ;;
esac
