#!/usr/bin/env bash
# Move localhost:8080 onto whichever node is serving, using the existing ControlMaster.
#
# `-O forward` reuses the open master; opening a second `ssh -L` instead trips the guard
# that exists because a fresh direct connection to fir fires Duo. The old forward has to
# be cancelled first or the new one is refused as a duplicate bind.
set -uo pipefail
NEW="${1:?usage: retunnel.sh <node>}"
OLD="${2:-}"
[ -n "$OLD" ] && ssh -O cancel -L 8080:"$OLD":8080 fir 2>/dev/null
ssh -O forward -L 8080:"$NEW":8080 fir 2>&1 | tail -1
curl -sf --max-time 15 http://127.0.0.1:8080/v1/models >/dev/null \
    && echo "tunnel up on $NEW" || echo "tunnel FAILED"
