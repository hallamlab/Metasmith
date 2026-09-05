#!/bin/bash
# cobra runtime -- build and run the model-solving image locally.
#
# NOT PUSHED. `resources/env/cobra.env` still names `conda: build-refs-cobra` as its
# working route and carries no `container:` line, in the style resources/env/ecspr.env
# already uses for an unpublished image: naming an image in `container:` before it is
# pushed points every solve at a registry entry that does not exist, which fails at
# stage time on whichever host drew the task rather than here.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
IMAGE=cobra:local

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
    ;;
    --run|-r)
        docker run -it --rm \
            --mount type=bind,source="$HERE/../..",target="/ws" --workdir="/ws" \
            "$IMAGE" /bin/bash
    ;;
    *)
        echo "usage: dev.sh [--build|--check|--run]"
        echo "typical: --build && --check"
        exit 1
    ;;
esac
