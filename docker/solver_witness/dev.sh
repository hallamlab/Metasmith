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

# A PERSISTENT lean home. The container is `--rm`, so elan's toolchain download
# and every lake package fetch are lost the moment it exits -- and the aeneas
# backend depends on Mathlib, which is not something to download twice. This
# directory holds elan's toolchains and the lake project, and it is a build
# product like every other cache here: outside the worktree, safe to delete.
LEAN_HOME="${MSM_WITNESS_LEAN_HOME:-$HOME/.cache/metasmith/witness-lean-home}"
mkdir -p "$LEAN_HOME"

in_lean() {
    docker run --rm -i $NET \
        --mount type=bind,source="$REPO",target=/root/src \
        --mount type=bind,source="$OUT_DIR",target=/root/out \
        --mount type=bind,source="$LEAN_HOME",target=/root/leanhome \
        --env ELAN_HOME=/root/leanhome/elan \
        --env PATH=/root/leanhome/elan/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
        --workdir /root/leanhome \
        "$IMAGE" bash -c "$1"
}

# Adjudicate an extraction. Aeneas emits a hole rather than failing, and the
# Lean still compiles, so nothing downstream notices that the function a proof is
# about was never translated. This is the only thing standing between that and a
# proof of nothing.
#
# `core.*` and `alloc.*` axioms are the external surface: functions Aeneas
# deliberately does not model, whose meaning the caller supplies. An axiom naming
# anything else is one of OUR functions that failed to lower, which is a hole
# wearing the same clothes.
gate() {
    local out="$1" fail=0
    echo
    echo "== extraction gate =="
    # Every grep below is guarded. `set -o pipefail` is on, and a grep that
    # matches nothing exits 1 -- which under `set -e` aborts the gate silently,
    # in exactly the case where it should be reporting a clean run.
    local sorries
    sorries=$(grep -nE '\bsorry\b' "$out"/*.lean 2>/dev/null || true)
    if [ -n "$sorries" ]; then
        echo "FAIL: sorry in the extracted output -- these functions did not translate:"
        echo "$sorries" | sed 's/^/    /'
        fail=1
    fi
    local holes
    holes=$({ grep -hE '^\s*axiom\b' "$out"/*.lean 2>/dev/null || true; } \
            | awk '{print $2}' | { grep -vE '^(core|alloc)\.' || true; } | sort -u)
    if [ -n "$holes" ]; then
        echo "FAIL: axiomatised crate functions -- lowered to nothing, not to Lean:"
        echo "$holes" | sed 's/^/    /'
        fail=1
    fi
    local external divergent
    external=$({ grep -hE '^\s*axiom\b' "$out"/*.lean 2>/dev/null || true; } \
               | awk '{print $2}' | { grep -cE '^(core|alloc)\.' || true; })
    # Not failures. Each is a definition whose termination Lean did not get
    # structurally, so reasoning about it needs the fixpoint's unfolding lemmas
    # -- the count is what a proof over this extraction is budgeted against.
    # Aeneas picks `partial_fixpoint` where older versions emitted `divergent`,
    # so counting only the latter reports zero on an extraction full of them.
    local nonstruct
    nonstruct=$({ grep -hocE '\b(divergent|partial_fixpoint)\b' "$out"/*.lean 2>/dev/null || true; } \
                | awk '{n += $1} END {print n + 0}')
    echo "external axioms (expected): $external    non-structural recursions: $nonstruct"
    [ "$fail" = 0 ] && echo "PASS: no sorry, no axiomatised crate function"
    return $fail
}

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
        # Emptied first. `OUT_DIR` persists between runs, and a file left by an
        # earlier extraction is read by the gate as if this run had emitted it --
        # which counted every hole twice and would survive a crate that stopped
        # producing them at all.
        rm -f "$OUT_DIR"/*.lean "$OUT_DIR"/*.llbc
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
        gate "$OUT_DIR"
    ;;
    --gate)
        shift
        gate "${1:-$OUT_DIR}"
    ;;
    --lean-init)
        # Stand up the lean project the specification is checked in. Long: it
        # fetches a toolchain and Mathlib. Idempotent, and everything lands in
        # LEAN_HOME so a second run is cheap.
        in_lean '
            set -e
            mkdir -p /root/leanhome/elan
            if [ ! -x /root/leanhome/elan/bin/elan ]; then
                curl -sSf https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
                    -o /tmp/elan-init.sh
                sh /tmp/elan-init.sh -y --default-toolchain none
            fi
            export PATH=/root/leanhome/elan/bin:$PATH
            # A copy, not the image path: `lake build` writes `.lake/` beside the
            # sources, and anything written under /opt is lost with the container.
            [ -d /root/leanhome/aeneas ] || cp -r /opt/aeneas/backends/lean /root/leanhome/aeneas
            mkdir -p /root/leanhome/proj/SolverWitness
            cd /root/leanhome/proj
            cp /root/leanhome/aeneas/lean-toolchain .
            cat > lakefile.lean <<LAKE
import Lake
open Lake DSL

require aeneas from "../aeneas"

package «solverWitnessSpec» {}

@[default_target] lean_lib SolverWitness {}
LAKE
            lake update || true
            lake exe cache get || echo "NOTE: mathlib cache miss; falling back to a source build"
            lake build aeneas
            echo "LEAN-INIT-OK"
        '
    ;;
    --lean-check)
        # Copy the freshest extraction and the specification in, then build.
        in_lean '
            set -e
            export PATH=/root/leanhome/elan/bin:$PATH
            cd /root/leanhome/proj
            cp /root/out/Types.lean /root/out/Funs.lean SolverWitness/
            cp /root/src/src/solver_witness/lean/Spec.lean SolverWitness/
            lake build 2>&1 | tail -40
            echo "LEAN-CHECK-DONE"
        '
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
        echo "  --gate [DIR] adjudicate an extraction (default $OUT_DIR)"
        echo "  --lean-init  stand up the lean project (slow: toolchain + mathlib)"
        echo "  --lean-check build Spec.lean against the current extraction"
    ;;
esac
