#!/bin/bash
set -euxo pipefail

# The tarball carries four cross-built solvers for the lab-channel package.
# This package ships one, compiled here for the platform being built.
engine=src/metasmith/engine
rm -rf "$engine"
mkdir -p "$engine"

cd src/workflow_solver
cargo-bundle-licenses --format yaml --output "$SRC_DIR/THIRDPARTY.yml"
cargo install --locked --no-track --root "$SRC_DIR/solver_build" --path .
cd "$SRC_DIR"

# Must agree with platform_slot() in metasmith/models/solver_engine.py.
slot="$(uname -m | sed 's/aarch64/arm64/')-$(uname -s | tr 'A-Z' 'a-z')"
install -m 755 solver_build/bin/msm_solver "$engine/msm_solver.$slot"

# build_hash.txt stays as shipped: it names the agent image on quay.
"$PYTHON" -m pip install . --no-deps --no-build-isolation -vv
