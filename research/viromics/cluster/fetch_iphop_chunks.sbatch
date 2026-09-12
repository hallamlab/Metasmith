#!/bin/bash
#SBATCH --job-name=iphop_chunks
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-16%6
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/viromics_refs/_logs/%x.%A_%a.out
# One array task per 10 GiB chunk, and the array is the whole point.
#
# A compute node here is bandwidth-capped, not server-limited: eight concurrent curl
# streams from ONE node aggregate to about 2 MB/s, the same 2 MB/s one stream gets,
# while the LOGIN node pulls 25 MB/s per stream from the same host. So the cap is on
# compute egress, and the one-task-per-chunk shape is about walls rather than speed:
# seventeen tasks across nodes still aggregated to 5 MB/s, and Slurm packs them onto one
# node anyway. Budget nine hours for the set and do not move this to a login node --
# 170 GB there is exactly the transfer the site asks you to put on a DTN.
#
# The %6 throttle is not politeness, it is arithmetic. Egress here looks globally capped
# at about 5 MB/s no matter how many nodes pull: seventeen tasks at once each got 0.3 MB/s,
# which is nine hours for a 10 GiB chunk and misses any sane wall. Six at a time gives each
# about 1 MB/s, so a chunk lands in under three hours and the set still takes the same
# nine hours overall -- the difference is that every task finishes.
#
# Every task is idempotent against the release's own md5 manifest, so a resubmission
# costs one hash per finished chunk. `-C -` resumes a partial rather than restarting it,
# and size is never the check: a resumed transfer can land the exact published byte count
# with a corrupt middle.
set -euo pipefail

ROOT=${IPHOP_ROOT:-/scratch/phyberos/viromics_refs}
DB=$ROOT/iphop_db
BASE=https://portal.nersc.gov/cfs/m342/iphop/db
DBV=${DBV:-iPHoP_db_Aug23_rw}

mkdir -p "$DB"
cd "$DB"
[ -s "$DBV.tar.gz.md5" ] || curl -sSf -o "$DBV.tar.gz.md5" "$BASE/$DBV.tar.gz.md5"

NAME=$(printf "%s.tar.gz.%02d" "$DBV" "$SLURM_ARRAY_TASK_ID")
WANT=$(awk -v n="$NAME" '$2 == n {print $1}' "$DBV.tar.gz.md5")
test -n "$WANT"

# --http1.1 is not a preference. Over HTTP/2 the portal drops the stream part way into a
# 10 GiB object -- `curl (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR` --
# every time, so all six tasks of the first array burnt three quarters of an hour each and
# failed. Forcing 1.1 is what makes the transfer finish.
#
# The loop is the transfer rather than a fallback around it: each attempt resumes where the
# last was cut, and a fixed handful of attempts is nowhere near enough. Run until the chunk
# is whole or the wall is nearly spent, and let the resubmission take it from there.
DEADLINE=$(( $(date +%s) + 11 * 3600 ))
attempt=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    if [ -s "$NAME" ] && [ "$(md5sum "$NAME" | cut -d' ' -f1)" = "$WANT" ]; then
        echo "ok $NAME after $attempt attempts"
        exit 0
    fi
    attempt=$((attempt + 1))
    before=$(stat -c%s "$NAME" 2>/dev/null || echo 0)
    curl -sS --http1.1 -C - --retry 5 --retry-delay 30 -o "$NAME" "$BASE/$NAME" || true
    after=$(stat -c%s "$NAME" 2>/dev/null || echo 0)
    echo "attempt $attempt on $NAME: $before -> $after"
    [ "$after" = "$before" ] && sleep 60
done
echo "INCOMPLETE $NAME at $(stat -c%s "$NAME" 2>/dev/null || echo 0)"
exit 1
