#!/bin/bash
# Ship the fir-side helpers and pin the contract copy they validate against.
#
#   bash examples/fir/sync_scripts.sh [host]
#
# The per-assembly split re-validates every delivered table with
# `fabfos_evidence.validate_gpr`. That module has to exist on the cluster as a
# COPY, because the split runs outside the workflow and outside the container
# the transform ran in -- and a copy is exactly the thing that can drift out
# from under a claim. A reordered SCHEMA_COLS refuses loudly; a WIDENED score
# range or an added lane set would pass in silence and quietly turn the
# re-validation into a decoration. So the digest goes with it, and the splitter
# refuses when they disagree.
set -euo pipefail

HOST=${1:-fir}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DEST=/project/rpp-shallam/phyberos/cyanoverse/gpr/scripts
LIB=$REPO/src/metasmith_libraries/resources/lib/fabfos_evidence.py

[ -f "$LIB" ] || { echo "no $LIB -- is the submodule checked out?" >&2; exit 1; }

rsync -a "$REPO"/examples/fir/*.py "$REPO"/examples/fir/*.sbatch "$HOST:$DEST/"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
cp "$LIB" "$TMP/"
sha256sum < "$LIB" | cut -d' ' -f1 > "$TMP/fabfos_evidence.sha256"
rsync -a "$TMP"/ "$HOST:$DEST/lib/"

# The git pins, recorded HERE and shipped, because `provenance.py` runs on the
# cluster where the repository does not exist -- it cannot read them itself, and
# a provenance record that quietly omits its pins is worse than one that fails.
# Written at the moment the code is shipped, so the pins name the commits the
# cluster copy was actually cut from.
{
    echo "driver repo       $(git -C "$REPO" rev-parse HEAD) ($(git -C "$REPO" rev-parse --abbrev-ref HEAD))"
    echo "transform library $(git -C "$REPO/src/metasmith_libraries" rev-parse HEAD)"
    echo "engine            $(git -C "$REPO/src/metasmith" rev-parse HEAD)"
    echo "synced at         $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    dirty=$(git -C "$REPO" status --porcelain)
    if [ -n "$dirty" ]; then
        echo "WORKING TREE NOT CLEAN at sync time:"
        echo "$dirty" | head -10 | sed 's/^/    /'
    fi
} > "$TMP/PINS.txt"
rsync -a "$TMP/PINS.txt" "$HOST:$DEST/PINS.txt"

echo "synced -> $HOST:$DEST"
echo "contract digest $(cat "$TMP/fabfos_evidence.sha256")"
