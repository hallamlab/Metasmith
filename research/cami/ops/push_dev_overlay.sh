#!/bin/bash
# Run the pinned engine source inside the published agent image.
#
# The image on quay (0.22.1, cut 2026-09-01) predates the collapsed
# ExecWithEnv(env=, cmd=) that every transform in this library now calls, so a
# task built against the pin dies at runtime with an unexpected-keyword TypeError
# while the solve, the stage and the launch all report success.
#
# metasmith already answers this. `agents/agent.py` renders both the `msm`
# wrapper and `lib/msm_bootstrap` with a conditional bind of
# $AGENT_HOME/dev/metasmith over the image's site-packages. The conditional is
# baked into those files at DEPLOY time, so an agent home deployed weeks ago
# already carries it and nothing here needs to redeploy.
#
# The tarball beside the tree is not tidying. At array width the bootstrap stages
# it once per node under flock and validates the extract; with no tarball every
# task instead reads ~240 small files off Lustre at once, which is the metadata
# storm that returned 93 silently-incomplete copies of 97 and 558 errno-108s.
# Its own integrity check looks for models/workflow and coms, so assert those
# here rather than discovering a fail-open at 229 samples.
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
HOST="${MSM_HPC_HOST:-fir}"
AGENT_HOME="${MSM_AGENT_HOME:-/scratch/phyberos/cami/metasmith}"
SRC="$HERE/src/metasmith"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

[ -d "$SRC" ] || { echo "no engine source at $SRC" >&2; exit 1; }

rsync -a --exclude='__pycache__' --exclude='*.pyc' "$SRC" "$STAGE"/
for required in models/workflow coms; do
    [ -e "$STAGE/metasmith/$required" ] || {
        echo "ERROR: $required missing -- msm_bootstrap validates it and would" >&2
        echo "       fail OPEN to the Lustre read this script exists to avoid." >&2
        exit 1
    }
done
n=$(find "$STAGE/metasmith" -type f | wc -l)
[ "$n" -ge 50 ] || { echo "ERROR: only $n files; the bootstrap requires 50+" >&2; exit 1; }

tar -cf "$STAGE/metasmith.tar" -C "$STAGE" metasmith

ssh "$HOST" "mkdir -p $AGENT_HOME/dev"
rsync -a --delete "$STAGE/metasmith" "$HOST:$AGENT_HOME/dev/"
rsync -a "$STAGE/metasmith.tar" "$HOST:$AGENT_HOME/dev/"

echo "pushed $n files + tarball to $HOST:$AGENT_HOME/dev/"
echo "commit: $(git -C "$HERE" rev-parse --short HEAD)"
echo
echo "A task log proves it took: it prints 'staged dev overlay (per-node-once"
echo "tarball, key ...)' and binds dev/metasmith over the image's site-packages."
