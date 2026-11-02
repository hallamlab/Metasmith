#!/bin/bash
# FabFos dev/build automation.
#
# FabFos is a thin metasmith front end: the planner/executor comes from the
# `metasmith` conda package, and the fosmid pipeline definition (transforms,
# data types, container/conda env resources) is the metasmith library bundled
# into the wheel as `fabfos/_library`.
set -e
# this script sits at repo_root/dev/; the package is at src/fabfos (src-layout)
HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )
LIB_SRC="$HERE/src/metasmith_libraries"
# metasmith_libraries' own envs/ is a shared top-level directory now, not
# nested under its src/ tree -- bundled separately from data_types/resources/
# transforms below.
LIB_ENVS="$HERE/envs/metasmith_libraries"
LIB_DST="$HERE/src/fabfos/_library"
# FabFos's own algorithm library. Inside the package already, so the bundle does
# not copy it -- but its _metadata still has to be regenerated in place.
ALGO_SRC="$HERE/src/fabfos/algorithm"

case $1 in
    --ibase) # create the dev conda env
        mamba env create --no-default-packages -f "$HERE/envs/fabfos/base.yml"
    ;;
    --idev) # layer the packaging toolchain (conda-build, boa, anaconda-client) on top
        # Everything that BUILDS or PUBLISHES a package -- -bc here, -be/-ue in
        # dev/ecspr.sh -- needs this. Defaults to the base env; pass a name to
        # overlay a different one.
        mamba env update -n "${2:-fabfos}" -f "$HERE/envs/fabfos/dev.yml"
    ;;
    -b|--bundle-library) # copy the metasmith library into the package for shipping
        # Compile before copy: _metadata/ is a build product (see -bm below),
        # not tracked source, so a fresh checkout has none until this runs.
        # vendor-library only COPIES -- run this first or it ships an empty
        # bundle, which `--check`/-bp/-bc now refuse rather than shipping.
        "$HERE/dev/fabfos.sh" --build-metadata
        echo "bundling metasmith library: $LIB_SRC (+ $LIB_ENVS) -> $LIB_DST"
        # the same verb metasmith exposes; the engine itself no longer vendors:
        # data_types/resources/transforms are the pieces the planner loads at
        # runtime; envs/ -- the conda recipes behind each `conda:` declaration --
        # the planner never reads but a `--runtime mamba` install needs to create
        # its tool envs.
        PYTHONPATH="$HERE/src" python -m metasmith build vendor-library \
            --src "data_types=$LIB_SRC/data_types" \
            --src "resources=$LIB_SRC/resources" \
            --src "transforms=$LIB_SRC/transforms" \
            --src "envs=$LIB_ENVS" \
            --dst "$LIB_DST"
    ;;
    -bm|--build-metadata) # regenerate the _metadata snapshots the planner resolves against
        # `--bundle-library` only COPIES; the per-library _metadata/ snapshots are
        # generated, and a stale one keeps resolving against the type names it was
        # built with. src/fabfos/algorithm is not under the library root, so it is
        # not copied by the bundle and has to be rebuilt in place.
        echo "regenerating _metadata snapshots"
        CMD=(python -m metasmith build all --types "$LIB_SRC/data_types")
        for d in "$LIB_SRC"/resources/*/; do CMD+=(--uniques "$d"); done
        for d in "$LIB_SRC"/transforms/*/; do CMD+=(--transforms "$d"); done
        "${CMD[@]}"
        python -m metasmith build uniques \
            --types "$LIB_SRC/data_types" --uniques "$ALGO_SRC"
    ;;
    -bp|--build-pip) # build the wheel/sdist (bundle first)
        "$HERE/dev/fabfos.sh" --bundle-library
        cd "$HERE/src/fabfos"
        rm -rf build dist *.egg-info
        python -m build
    ;;
    -bc|--build-conda) # compile + build the conda package
        "$HERE/dev/fabfos.sh" --bundle-library
        python "$HERE/conda_recipe/fabfos/compile_recipe.py"
        "$HERE/conda_recipe/fabfos/call_build.sh"
    ;;
    -r|--run) # run the CLI from source (dev): ./dev/fabfos.sh -r --plan-only ...
        shift
        PYTHONPATH="$HERE/src:$PYTHONPATH" \
        FABFOS_LIBRARY="${FABFOS_LIBRARY:-$LIB_SRC}" \
        python -m fabfos "$@"
    ;;

    *)
        echo "usage: dev/fabfos.sh [--ibase|--idev|-b|-bm|-bp|-bc|-r ...]"
        echo "  --ibase              create the dev conda env"
        echo "  --idev [env]         add conda-build/boa/anaconda-client (needed by -bc, and by dev/ecspr.sh -be/-ue)"
        echo "  -b|--bundle-library  copy the metasmith library into the package for shipping"
        echo "  -bm|--build-metadata regenerate the _metadata snapshots"
        echo "  -bp|--build-pip      build the wheel/sdist"
        echo "  -bc|--build-conda    compile + build the conda package"
        echo "  -r|--run ...         run the CLI from source"
        echo ""
        echo "ecspr's build/test/run flags moved to dev/ecspr.sh -- ecspr is now a"
        echo "top-level peer module, not nested under fabfos."
    ;;
esac
