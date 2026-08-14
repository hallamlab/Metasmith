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
        mamba env create -y -f "$ECSPR_SRC/env.yml" \
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
        python "$HERE/conda_recipe/ecspr/compile_recipe.py"
        "$HERE/conda_recipe/ecspr/call_build.sh"
        # envs/metasmith_libraries/tools/ecspr.yml is what a `--runtime mamba`
        # install creates the tool env from. It is env.yml plus the package
        # itself, and it is regenerated here so the two cannot silently disagree.
        python - "$ECSPR_SRC/env.yml" "$LIB_TOOL_ENVS/ecspr.yml" <<'PY'
import sys, yaml
from pathlib import Path
src, dst = (Path(p) for p in sys.argv[1:3])
spec = yaml.safe_load(src.read_text())
skip = {"pytest", "networkx", "pip"}
deps = [d for d in spec["dependencies"]
        if isinstance(d, str) and d.split("=")[0].split("<")[0].split(">")[0] not in skip]
head = dst.read_text().split("name:")[0] if dst.exists() else ""
dst.write_text(head + "name: ecspr\nchannels:\n  - hallamlab\n"
               + "".join(f"  - {c}\n" for c in spec["channels"])
               + "dependencies:\n" + "".join(f"  - {d}\n" for d in deps) + "  - ecspr\n")
print(f"wrote {dst}")
PY
    ;;
    *)
        echo "usage: dev/ecspr.sh [--iecspr|-e ...|-te|-be]"
        echo "  --iecspr             create the ecspr env and install src/ecspr editable"
        echo "  -e|--ecspr           run the ecspr CLI from source: -e ground --gpr ..."
        echo "  -te|--test-ecspr     run tests/ecspr"
        echo "  -be|--build-ecspr    build the ecspr conda package + refresh envs/metasmith_libraries/tools/ecspr.yml"
    ;;
esac
