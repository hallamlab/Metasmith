#!/bin/bash
# dev script version 1.1
# This script sits at repo_root/dev/; metasmith_libraries itself is a sibling
# module at src/metasmith_libraries (src-layout, no submodule).
HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )
DEV_USER=hallamlab
LIB="$HERE/src/metasmith_libraries"
ENVS="$HERE/envs/metasmith_libraries"
TESTS="$HERE/tests/metasmith_libraries"

# this file contains a list of commands useful for dev,
# providing automation for some build tasks
#
# example workflow 1, pip:
# dev/libraries.sh --idev # create a local conda dev env
# # add pypi api token as file to ./secrets [https://pypi.org/help/#apitoken]
# # make some changes to source
# # bump up ./src/*/version.txt
# dev/libraries.sh -bp # build the pip package
# dev/libraries.sh -up # test upload to testpypi
# dev/libraries.sh -upload-pypi # release to pypi index for pip install
#
# example workflow 2, conda:
# dev/libraries.sh --idev # create a local conda dev env
# dev/libraries.sh -bp # build the pip package
# dev/libraries.sh -bc # build conda package from pip package
# dev/libraries.sh -uc # publish to conda index
#
# example workflow 3, containerization:
# dev/libraries.sh --idev # create a local conda dev env
# dev/libraries.sh -bd # build docker image
# dev/libraries.sh -ud # publish to quay.io
# dev/libraries.sh -bs # build apptainer image from local docker image

case $1 in
    ###################################################
    # environments

    --ibase) # base only
        cd "$ENVS"
        echo "creating new conda env: metasmith_libraries"
        sleep 2
        $CONDA env create --no-default-packages -n metasmith_libraries -f ./base.yml
    ;;
    --create-envs) # create the per-tool conda test envs from envs/metasmith_libraries/tools/*.yml (idempotent)
        # each recipe's `name:` is the env name referenced by resources/env/<tool>.env `conda:`.
        # override the conda frontend with CONDA=... (default: mamba).
        CONDA=${CONDA:-mamba}
        existing="$($CONDA env list | awk '{print $1}')"
        for recipe in "$ENVS"/tools/*.yml; do
            name=$(awk -F': *' '/^name:/{print $2; exit}' "$recipe")
            if echo "$existing" | grep -qxF "$name"; then
                echo "[skip] env exists: $name"
                continue
            fi
            echo "[create] $name  <- $(basename "$recipe")"
            $CONDA env create -n "$name" -f "$recipe" \
                || echo "[WARN] failed to solve/create env: $name (recipe $recipe)"
        done
    ;;
    --git-prune-local) # remove local branches not on remote
        git fetch -p
        git branch -r \
            | awk '{print $1}' \
            | egrep -v -f /dev/fd/0 <(git branch -vv \
            | grep origin) \
            | awk '{print $1}' \
            | xargs git branch -d
    ;;

    ###################################################
    # build
    -bm|--build-metadata) # regenerate _metadata/ -- what a fresh checkout needs, and all it needs
        # `_metadata/` is a build product and is not tracked, so a fresh clone
        # has none and every solve raises before planning. This is the arm that
        # fixes that, and it takes seconds. `-b` is this plus the template gate,
        # which is an author's check rather than a prerequisite for using the
        # library -- hence the split, mirroring dev/fabfos.sh.
        #
        # msm build's STEP positional must precede the flags; --types,
        # --uniques, --transforms are now single-value/repeatable. Build
        # the flag list by repeating each flag once per resolved path.
        if command -v msm >/dev/null 2>&1; then
            msm=msm
        else
            # metasmith is now a fixed sibling module, not a separately-cloned
            # repo -- resolve it through its own dev entry point instead of a
            # hardcoded sibling-repo path.
            msm="$HERE/dev/metasmith.sh -r"
        fi
        echo "$msm"
        args=(build all --types "$LIB/data_types")
        for d in "$LIB"/resources/*/; do args+=(--uniques "${d%/}"); done
        for d in "$LIB"/transforms/*/; do args+=(--transforms "${d%/}"); done
        $msm "${args[@]}" || exit 1
    ;;
    -b) # update std xgdbs: metadata, then solve every template against it
        "$HERE/dev/libraries.sh" -bm || exit 1
        # Templates are solved against the metadata just rebuilt: a transform
        # whose products changed shape takes its templates down here, by name,
        # instead of in someone's GUI a week later.
        #
        # This is the slow half: every template solves, at 1-3s each. Pass
        # template names to solve a subset while iterating. A template that
        # suddenly costs minutes rather than seconds has target ambiguity, not
        # size -- see the pinning note in `metagenomics_from_paired_reads.py`.
        ${PYTHON:-python} "$LIB/build_templates.py" "${@:2}" || exit 1
    ;;
    # The wheel/conda/upload arms and --stage-envs are gone. The library is no
    # longer a separately built and published artifact: it ships inside the
    # metasmith package, staged by `dev/metasmith.sh --vendor-library`, which
    # carries envs/ directly and so needs no staging step here.

    --test-binning)
        pytest "$TESTS"/test_*.py -v --ignore="$TESTS/cache"
    ;;
    --test-comebin)
        pytest "$TESTS/test_binning_workflow.py::TestBinningWorkflowExecution::test_comebin_e2e" \
            -v --ignore="$TESTS/cache" \
            -s --log-cli-level=INFO
    ;;
    --test-semibin2)
        pytest "$TESTS/test_binning_workflow.py::TestBinningWorkflowExecution::test_semibin2_e2e" \
            -v --ignore="$TESTS/cache" \
            -s --log-cli-level=INFO
    ;;
    --test-metabat2)
        pytest "$TESTS/test_binning_workflow.py::TestBinningWorkflowExecution::test_metabat2_e2e" \
            -v --ignore="$TESTS/cache" \
            -s --log-cli-level=INFO
    ;;
    --test-annotation)
        pytest "$TESTS/test_annotation_workflow.py"
    ;;
    ###################################################
    *)
        echo "bad option"
        echo $1
    ;;
esac
