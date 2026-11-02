#!/bin/bash
# ECSPr dev/build automation.
#
# ECSPr is the one transform whose protocol is an algorithm rather than a
# dispatch into somebody else's tool, so it is the one with its own package
# and its own env. `env::ecspr.env` carries both a container and `conda:
# ecspr`, so the SAME `ecspr ...` command runs under the planner and here --
# the only difference is where the code is read from.
#
# These flags used to live inside fabfos's dev.sh (ecspr was nested under
# fabfos's tree); ecspr is now a top-level peer module with its own dev
# entry point, so they moved here verbatim, same flag names.
set -e
HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." &> /dev/null && pwd )
ECSPR_SRC="$HERE/src/ecspr"
# ecspr's env spec lives under the shared top-level envs/ directory, not
# inside src/ecspr -- same reason the tool env recipe below does.
ECSPR_ENV="$HERE/envs/ecspr/base.yml"
# ecspr's tool env recipe lives with the rest of metasmith_libraries' per-tool
# envs, not inside src/metasmith_libraries itself -- envs/ is a shared
# top-level directory now, not nested under each module's own src/ tree.
LIB_TOOL_ENVS="$HERE/envs/metasmith_libraries/tools"

case $1 in
    --iecspr) # create the ecspr env and install src/ecspr EDITABLE into it
        # Editable is what makes the dev loop immediate: the console script
        # resolves to the worktree, so a source edit is live on the next
        # invocation with no rebuild, no image and no replan. `--no-deps` because
        # conda already solved them from the same env.yml.
        # Idempotent: re-running after a dependency change should reinstall the
        # package, not refuse because the env is already there.
        mamba env create -y -f "$ECSPR_ENV" \
            || echo "  (env 'ecspr' exists; reinstalling the package into it)"
        mamba run -n ecspr pip install -e "$ECSPR_SRC" --no-deps --no-build-isolation
        # Which copy did we get? The whole point of the editable install is that
        # this prints a path under the worktree.
        mamba run -n ecspr ecspr --where
    ;;
    -e|--ecspr) # run the ecspr CLI from source (dev): ./dev/ecspr.sh -e ground --gpr ...
        shift
        mamba run -n ecspr ecspr "$@"
    ;;
    -te|--test-ecspr) # run the ecspr test suite
        shift
        mamba run -n ecspr python -m pytest "$HERE/tests/ecspr" "$@"
    ;;
    -be|--build-ecspr) # compile + build the ecspr conda package, and refresh its tool env
        # Stamp the source hash FIRST: it becomes the conda build string, so a
        # package built from a stale stamp would name the wrong source state.
        PYTHONPATH="$HERE/src" python -m ecspr._build_hash --write
        python "$HERE/conda_recipe/ecspr/compile_recipe.py"
        "$HERE/conda_recipe/ecspr/call_build.sh"
        # envs/metasmith_libraries/tools/ecspr.yml is what a `--runtime mamba`
        # install creates the tool env from. It is env.yml plus the package
        # itself, and it is regenerated here so the two cannot silently disagree.
        python - "$ECSPR_ENV" "$LIB_TOOL_ENVS/ecspr.yml" <<'PY'
import sys, yaml
from pathlib import Path
src, dst = (Path(p) for p in sys.argv[1:3])
spec = yaml.safe_load(src.read_text())
skip = {"pytest", "networkx", "pyyaml", "pip"}
deps = [d for d in spec["dependencies"]
        if isinstance(d, str) and d.split("=")[0].split("<")[0].split(">")[0] not in skip]
head = dst.read_text().split("name:")[0] if dst.exists() else ""
dst.write_text(head + "name: ecspr\nchannels:\n  - hallamlab\n"
               + "".join(f"  - {c}\n" for c in spec["channels"])
               + "dependencies:\n" + "".join(f"  - {d}\n" for d in deps) + "  - ecspr\n")
print(f"wrote {dst}")
PY
    ;;
    -ue|--upload-ecspr) # publish the built ecspr conda package to the hallamlab channel
        # `anaconda login` is a human step and is deliberately NOT done here.
        # The command lives in the repo so it is not retyped from memory: the
        # channel is read from the package's own constants, not hard-coded.
        USER_ORG=$(PYTHONPATH="$HERE/src" python -c "import ecspr; print(ecspr.USER)")
        pkgs=$(ls "$HERE"/conda_build/noarch/ecspr-*.tar.bz2 2>/dev/null || true)
        [ -z "$pkgs" ] && { echo "upload-ecspr: no package in conda_build/noarch -- run -be first" >&2; exit 1; }
        echo "uploading to anaconda.org/$USER_ORG:"; echo "$pkgs"
        for p in $pkgs; do anaconda upload -u "$USER_ORG" "$p"; done
    ;;
    *)
        echo "usage: dev/ecspr.sh [--iecspr|-e ...|-te|-be|-ue]"
        echo "  --iecspr             create the ecspr env and install src/ecspr editable"
        echo "  -e|--ecspr           run the ecspr CLI from source: -e ground --gpr ..."
        echo "  -te|--test-ecspr     run tests/ecspr"
        echo "  -be|--build-ecspr    build the ecspr conda package + refresh envs/metasmith_libraries/tools/ecspr.yml"
        echo "  -ue|--upload-ecspr   publish it to the hallamlab channel (anaconda login first)"
        echo "  (the ecspr IMAGE is docker/ecspr/dev.sh --build|--check|--push)"
        echo "  (the packaging toolchain -be/-ue need comes from dev/fabfos.sh --idev)"
    ;;
esac
