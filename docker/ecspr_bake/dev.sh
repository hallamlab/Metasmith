#!/bin/bash
# ecspr_bake -- build the R6 metabolism images, and put them where Sockeye will find them.
#
# THREE IMAGES, ONE DIRECTORY, and they split for two different reasons. `:aam` holds
# the mapping stack and the curation arbiter; `:direction` holds the eQuilibrator member
# and the combiner; `:dgbyg` holds the learned member. :aam cannot join the other two --
# equilibrator-cache 0.7.1 requires numpy>=2 and torch 2.2.1 is built against the numpy
# 1.x C API (see load/env_aam.yml). :dgbyg is separate from :direction on PYTHON, not on
# numpy: dGbyG needs 3.12 to parse and the eQuilibrator stack is pinned at 3.11.
#
# THE SIF IS SYNCED, NOT PULLED. No registry round-trip: each image is built from the
# local docker daemon and rsynced into the apptainer image store on the cluster.
# Metasmith finds it there WITHOUT a pull because it derives the cache filename from the
# image URI deterministically --
#
#     Environment._cached_name() = image.replace("://","..").replace(":","..").replace("/","_")
#
# -- so a file named `docker..quay.io_hallamlab_ecspr_bake..aam.sif` sitting in
# $APPTAINER_CACHEDIR is exactly what `docker://quay.io/hallamlab/ecspr_bake:aam`
# resolves to. `--sync` names the files that way on purpose. The URI in the .env files is
# therefore a NAME, not a promise that the registry has it; `--push` is available if that
# promise is ever wanted, but nothing here requires it.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
NAME=ecspr_bake
DOCKER_IMAGE=quay.io/hallamlab/$NAME
REMOTE=sockeye
REMOTE_CACHE=/arc/project/st-shallam-1/metasmith/container_images

# Order matters: `://` first, then the ONE colon left (the tag), then the slashes.
cached_name() { echo "docker://$DOCKER_IMAGE:$1" | sed 's|://|..|; s|:|..|; s|/|_|g'; }

# tag -> (env file, build-time verification). Each assertion is a break already had.
env_file() {
    case "$1" in
        aam)       echo "load/env_aam.yml" ;;
        direction) echo "load/env_direction.yml" ;;
        dgbyg)     echo "load/env_dgbyg.yml" ;;
    esac
}
verify_for() {
    case "$1" in
        aam) echo "import pkg_resources, setuptools, rdkit, numpy, pandas, torch, torchdata.datapipes; \
from rxnmapper import RXNMapper; from localmapper import localmapper; from indigo import Indigo; \
assert rdkit.__version__.startswith('2026.03'), rdkit.__version__; \
assert int(setuptools.__version__.split('.')[0]) < 81, setuptools.__version__; \
assert numpy.__version__.startswith('1.'), 'numpy '+numpy.__version__+' breaks torch 2.2.1'; \
import numpy as _n, torch as _t; assert _t.from_numpy(_n.zeros(3)).sum().item() == 0, 'torch<->numpy bridge is broken'; \
print('aam OK: rdkit', rdkit.__version__, '| torch', torch.__version__, '| indigo', Indigo().version())" ;;
        direction) echo "import rdkit, numpy, pandas, sqlalchemy; \
from equilibrator_api import ComponentContribution; from equilibrator_cache import Compound; \
assert rdkit.__version__.startswith('2026.03'), rdkit.__version__; \
assert int(numpy.__version__.split('.')[0]) >= 2, numpy.__version__; \
print('direction OK: rdkit', rdkit.__version__, '| numpy', numpy.__version__)" ;;
        # The dGbyG assertion IMPORTS THE API and evaluates the guard, rather than
        # checking a version string. Both halves are breaks already had: the package
        # installs and then fails to parse under python 3.11, and the guard is the only
        # thing standing between an R-group wildcard and a confident fabricated number.
        dgbyg) echo "import sys, rdkit, numpy, torch; \
assert sys.version_info[:2] >= (3, 12), 'dGbyG source needs 3.12 to parse: %s' % sys.version; \
assert rdkit.__version__.startswith('2026.03'), rdkit.__version__; \
assert torch.__version__.startswith('2.8'), torch.__version__; \
from dGbyG.api import Compound, Reaction; \
from rdkit import Chem; \
assert any(a.GetAtomicNum() == 0 for a in Chem.MolFromSmiles('*C(=O)O').GetAtoms()), \
'the wildcard guard cannot see an R-group and the member would score it'; \
print('dgbyg OK: torch', torch.__version__, '| rdkit', rdkit.__version__)" ;;
    esac
}

# All three by default. `TAGS=dgbyg dev.sh --build --sif --sync` for one.
TAGS="${TAGS:-aam direction dgbyg}"

case "${1:-}" in
    --build|-b)
        mkdir -p "$HERE/load"
        [ -f "$HERE/load/tini" ] || wget -q \
            https://github.com/krallin/tini/releases/download/v0.19.0/tini \
            -O "$HERE/load/tini"
        export DOCKER_BUILDKIT=1
        for t in $TAGS; do
            echo "== building $DOCKER_IMAGE:$t from $(env_file "$t")"
            docker build \
                --build-arg="CONDA_ENV=${NAME}_env" \
                --build-arg="ENV_FILE=$(env_file "$t")" \
                --build-arg="VERIFY=$(verify_for "$t")" \
                --build-arg="POST_INSTALL=$t" \
                -t "$DOCKER_IMAGE:$t" -f "$HERE/dockerfile" "$HERE"
        done
    ;;
    --sif|-s)
        for t in $TAGS; do
            echo "== $(cached_name "$t").sif"
            apptainer build --force "$HERE/$(cached_name "$t").sif" \
                "docker-daemon://$DOCKER_IMAGE:$t"
        done
    ;;
    --sync)
        ssh $REMOTE "mkdir -p $REMOTE_CACHE"
        for t in $TAGS; do
            rsync -a --partial --info=progress2 "$HERE/$(cached_name "$t").sif" \
                "$REMOTE:$REMOTE_CACHE/"
        done
        ssh $REMOTE "ls -la $REMOTE_CACHE/${NAME}* 2>/dev/null || ls -la $REMOTE_CACHE"
    ;;
    --check|-c)
        for t in $TAGS; do
            docker run --rm "$DOCKER_IMAGE:$t" python -c "$(verify_for "$t")"
        done
    ;;
    --check-remote)
        for t in $TAGS; do
            ssh $REMOTE "module load gcc/7.5.0 apptainer/1.3.1 >/dev/null 2>&1; \
                apptainer exec $REMOTE_CACHE/$(cached_name "$t").sif \
                python -c \"$(verify_for "$t")\""
        done
    ;;
    --push|-p)
        # Not part of the sync workflow; here only if a registry copy is ever wanted.
        for t in $TAGS; do docker push "$DOCKER_IMAGE:$t"; done
    ;;
    --run|-r)
        docker run -it --rm \
            --mount type=bind,source="$HERE",target="/ws" --workdir="/ws" \
            -u "$(id -u):$(id -g)" "$DOCKER_IMAGE:${2:-aam}" /bin/bash
    ;;
    *)
        echo "usage: dev.sh [--build|--sif|--sync|--check|--check-remote|--push|--run]"
        echo "       TAGS='aam' dev.sh --build   # one tag only"
        echo "typical: --build && --sif && --sync && --check-remote"
        exit 1
    ;;
esac
