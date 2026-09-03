#!/usr/bin/env bash
# Drive the charon/aeneas extraction image. Mirrors src/workflow_solver/dev.sh's
# conventions deliberately: same worktree mount, same "target dir lives outside
# the worktree" rule, so the two builds do not fight over cargo state.
set -euo pipefail

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO=$( cd "$HERE/../.." && pwd )

IMAGE=msm-solver-witness:latest

# Cargo's build directory for anything charon compiles on our behalf. Outside
# the worktree for the same reason dev.sh keeps its own outside: a root-owned
# target/ inside the tree breaks every host-side build afterwards.
TARGET_DIR="${MSM_WITNESS_TARGET_DIR:-$HOME/.cache/metasmith/witness-target}"
mkdir -p "$TARGET_DIR"

# Where extracted Lean lands. A build product, not source.
OUT_DIR="${MSM_WITNESS_OUT_DIR:-$HOME/.cache/metasmith/witness-lean}"
mkdir -p "$OUT_DIR"

# `--network=host` rather than the default bridge. crates.io fronts on Fastly
# and its AAAA record resolves inside a container that has no IPv6 route, so a
# cargo fetch can pick the v6 address and stall. Host networking sidesteps the
# whole question by using the resolver that is already known to work here.
NET="${MSM_WITNESS_NET:---network=host}"

in_container() {
    docker run --rm -i $NET \
        --mount type=bind,source="$REPO",target=/root/src \
        --mount type=bind,source="$TARGET_DIR",target=/root/target \
        --mount type=bind,source="$OUT_DIR",target=/root/out \
        --env CARGO_TARGET_DIR=/root/target \
        --workdir /root/src \
        "$IMAGE" "$@"
}

case "${1:---help}" in
    -b)
        # BuildKit's default output hides the failing command's context, and
        # this build fails in places that need it.
        docker build --progress=plain $NET -t "$IMAGE" "$HERE"
    ;;
    --versions)
        in_container bash -c 'echo "charon:   $(charon version 2>&1 | head -1)"; echo "toolchain: $(charon toolchain-version 2>&1 | head -1)"; echo "aeneas:   $(aeneas --help 2>&1 | head -1)"; echo "lean:     $(lean --version)"'
    ;;
    -x)
        # Extract one crate: dev.sh -x <path-to-crate-relative-to-repo>
        shift
        crate="${1:?usage: dev.sh -x <crate-dir>}"
        name=$(basename "$crate")
        # Passed through the environment rather than interpolated into the
        # container command: the nesting of quotes needed to expand one variable
        # in the outer shell and another in the inner one silently produced a
        # file called '$name.llbc' once already.
        docker run --rm -i $NET \
            --mount type=bind,source="$REPO",target=/root/src \
            --mount type=bind,source="$TARGET_DIR",target=/root/target \
            --mount type=bind,source="$OUT_DIR",target=/root/out \
            --env CARGO_TARGET_DIR=/root/target \
            --env CRATE="$crate" --env NAME="$name" \
            --workdir /root/src \
            "$IMAGE" bash -c '
                set -e
                cd "/root/src/$CRATE"
                charon cargo --preset=aeneas --dest-file "/root/out/$NAME.llbc"
                aeneas -backend lean -loops-to-rec -split-files -dest /root/out "/root/out/$NAME.llbc"
                echo "extracted to $OUT_DIR:"
            '
        ls -la "$OUT_DIR"
    ;;
    -s)
        shift
        in_container bash -c "${*:-bash}"
    ;;
    *)
        echo "usage: dev.sh [-b | --versions | -x <crate-dir> | -s <cmd>]"
        echo "  -b          build the image"
        echo "  --versions  report the pinned toolchain versions"
        echo "  -x DIR      extract crate DIR to Lean, into $OUT_DIR"
        echo "  -s CMD      run CMD in the container"
    ;;
esac
