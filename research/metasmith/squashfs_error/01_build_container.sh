#!/bin/bash
# Build the metasmith container (docker image + local SIF) with REAL relay
# binaries baked in, so a host that reads its rootfs through squashfuse can be
# exercised end to end.
#
# The relay binaries are gitignored build artifacts and the cross-compile
# container is not always around, so they are copied in from a sibling scope
# whose main/relay_agent/src is byte-identical (asserted below). That is enough:
# what matters for this experiment is that /app/msm_relay.* in the image are
# real ELF/Mach-O binaries, not the 28-byte stubs `dev.sh -bs` refuses on.
#
# The GUI bundle is deliberately NOT built (no node on this host), so
# MSM_SKIP_GUI_CHECK=1 rides along. Consequence: src/metasmith/gui/static/ is
# empty in this image, `msm gui` would serve nothing, and the build hash differs
# from a release build of the same commit. Neither touches the rootfs path.
set -euo pipefail

HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO=$(cd "$HERE/../.." && pwd)
DONOR=${DONOR:-/home/tony/agentic_workspace/projects/metasmith/dev}
ENV_NAME=${ENV_NAME:-msm}

TARGETS="x86_64-unknown-linux-musl aarch64-unknown-linux-musl x86_64-apple-darwin aarch64-apple-darwin"

echo "== relay binaries =="
diff -rq "$REPO/main/relay_agent/src" "$DONOR/main/relay_agent/src" >/dev/null \
    || { echo "ERROR: relay source differs from donor scope [$DONOR] — build them instead (dev.sh -brc && dev.sh -br)"; exit 1; }
for t in $TARGETS; do
    dst="$REPO/main/relay_agent/target/$t/release/msm_relay"
    if [ ! -x "$dst" ]; then
        mkdir -p "$(dirname "$dst")"
        cp -f "$DONOR/main/relay_agent/target/$t/release/msm_relay" "$dst"
        chmod +x "$dst"
    fi
    printf "  %-30s %9s bytes\n" "$t" "$(stat -c %s "$dst")"
done

cd "$REPO"
echo "== pip package =="
MSM_SKIP_GUI_CHECK=1 PYTHONPATH="$REPO/src" mamba run -n "$ENV_NAME" ./dev.sh -bp

echo "== docker image =="
MSM_SKIP_GUI_CHECK=1 PYTHONPATH="$REPO/src" mamba run -n "$ENV_NAME" ./dev.sh -bd

echo "== apptainer sif (asserts the relays are real) =="
PYTHONPATH="$REPO/src" mamba run -n "$ENV_NAME" ./dev.sh -bs

ls -la "$REPO/metasmith.sif"
