#!/bin/bash
# Pull one study's run-level .sra objects from the AWS Open Data SRA mirror.
#
# THIS RUNS ON A LOGIN NODE, and that is the measurement rather than a preference. Every fir
# compute node tested -- sixteen, across three racks -- moves 0.02-0.25 MB/s to every external
# destination alike: ENA, AWS S3 and cdn.kernel.org. A login node moves 32-36 MB/s per stream
# from S3 and 550 MB/s at sixteen. So a Slurm array cannot fetch this corpus at all, and the
# transfer is short enough not to need one: 714 GB of .sra is under half an hour at that rate.
#
# Run it in bounded chunks. MINUTES caps the wall, but that cap alone is not enough: killing
# the ssh session that started it does NOT kill it, so a chunk whose client timed out keeps
# running on the login node with no one holding its output. Wrap the remote side in its own
# hard bound rather than relying on the client:
#
#   nohup setsid timeout -k 30 1140 ./fetch_sra.sh karlsson2013 16 16 > _logs/... 2>&1 &
#
# A login node once reaped a whole detached process tree mid-transfer and left no message
# anywhere. Nothing is lost to that here -- the md5 gate below is what promotes an object and
# a partial is kept -- but the bound is what keeps a forgotten chunk from becoming permanent.
#
# Gating is on NCBI's published md5 for the .sra object, from build_sra_table.py. The
# manifest's md5 is over ENA's generated fastq and does NOT describe these bytes.
set -uo pipefail

ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
DS=${1:?usage: fetch_sra.sh <dataset> [width] [minutes]}
WIDTH=${2:-16}
MINUTES=${3:-10}
TABLE=$ROOT/runs.$DS.tsv
SRA=$ROOT/.sra/$DS
STAGE=$ROOT/.sra_staging/$DS
mkdir -p "$SRA" "$STAGE"

DEADLINE=$(( $(date +%s) + MINUTES * 60 ))
export SRA STAGE DEADLINE

one() {
    IFS=$'\t' read -r RUN SZ MD5 _ <<<"$1"
    dest=$SRA/$RUN.sra
    [ -s "$dest" ] && [ "$(stat -c%s "$dest")" = "$SZ" ] && return 0
    [ "$(date +%s)" -ge "$DEADLINE" ] && return 0
    stg=$STAGE/$RUN.sra
    # --retry 0 is deliberate. curl's own retry combined with -C - re-sends from the offset
    # resolved at invocation start, which has twice landed the exact published byte count with
    # a corrupt middle. Retrying is this loop's job, not curl's.
    code=$(curl -sS --retry 0 -C - -o "$stg" -w '%{http_code}' \
        "https://sra-pub-run-odp.s3.amazonaws.com/sra/$RUN/$RUN" 2>/dev/null)
    # An SDL answer is not proof the object is in this bucket. SDL published a size, an md5 and
    # this exact S3 key for karlsson2013's ERR260239 and ERR275252, and the bucket answers 404
    # for both -- 2 of the 245 in-scope runs. Without this branch curl saves S3's 294-byte XML
    # error body as the staging file and -C - then resumes from byte 294 of it on every later
    # attempt, so the object never completes and never fails either: it reports `short` forever.
    # A run that lands here goes through the ENA path, where the manifest's md5 does apply.
    case "$code" in
        200|206) ;;
        *) rm -f "$stg"; echo "http-$code $RUN -- not in this bucket, needs the ENA path"
           return 0 ;;
    esac
    got=$(stat -c%s "$stg" 2>/dev/null || echo 0)
    if [ "$got" != "$SZ" ]; then
        echo "short $RUN $got/$SZ"      # a partial is kept: only the md5 justifies deleting one
        return 0
    fi
    if [ "$(md5sum "$stg" | cut -d' ' -f1)" != "$MD5" ]; then
        echo "MD5 FAIL $RUN, discarding"
        rm -f "$stg"
        return 0
    fi
    mv "$stg" "$dest"
    echo "ok $RUN $SZ"
}
export -f one

# Passes rather than one sweep: a run that came up short leaves a resumable partial, and a
# second pass over the same table picks it up where the first stopped. One pass per run would
# leave a study a few objects short of done and need the whole script re-invoked.
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    tail -n +2 "$TABLE" | xargs -P "$WIDTH" -I{} bash -c 'one "$@"' _ {}
    left=$(tail -n +2 "$TABLE" | while IFS=$'\t' read -r r sz _; do
        [ -s "$SRA/$r.sra" ] && [ "$(stat -c%s "$SRA/$r.sra")" = "$sz" ] || echo x
    done | wc -l)
    [ "$left" -eq 0 ] && break
    echo "pass done, $left outstanding"
done

have=$(ls "$SRA" 2>/dev/null | wc -l)
want=$(( $(wc -l < "$TABLE") - 1 ))
bytes=$(du -sb "$SRA" 2>/dev/null | cut -f1)
printf '[%s] %s/%s runs staged, %.1f GB\n' "$DS" "$have" "$want" "$(echo "${bytes:-0}/1000000000" | bc -l)"
