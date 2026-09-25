#!/bin/bash
# Fill the gaps the AWS mirror cannot: fetch one study's still-missing manifest rows from ENA.
#
# It exists because an SDL answer is not proof the object is in the public bucket. Three of the
# 245 in-scope runs -- karlsson2013's ERR260239 and ERR275252, bissett_base's ERR671928 -- have
# a published .sra size and md5 and a named S3 key, and sra-pub-run-odp answers 404 for all
# three. Their six files have no .sra route, so they come from ENA.
#
# That is the better outcome for them rather than a worse one: these are ENA's own generated
# fastq, so the MANIFEST'S MD5 IS THE GATE HERE, which is not true of anything that arrives
# through convert_array.sbatch.
#
# Login node, for the same reason as fetch_sra.sh: compute nodes move 0.02-0.25 MB/s to
# everywhere. ENA is the slow source even here -- 1.9 MB/s per stream against S3's 32-36 -- so
# keep this to the leftovers and let the .sra route carry the corpus.
#
#   nohup setsid timeout -k 30 2700 ./fetch_ena.sh karlsson2013 6 40 > _logs/... 2>&1 &
set -uo pipefail

ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
DS=${1:?usage: fetch_ena.sh <dataset> [width] [minutes]}
WIDTH=${2:-6}
MINUTES=${3:-30}
STAGE=$ROOT/.ena_staging/$DS
mkdir -p "$STAGE" "$ROOT/$DS"

DEADLINE=$(( $(date +%s) + MINUTES * 60 ))
export ROOT DS STAGE DEADLINE

one() {
    IFS=$'\t' read -r _ RELPATH URL SZ MD5 _ <<<"$1"
    dest=$ROOT/$DS/$RELPATH
    [ -s "$dest" ] && [ "$(stat -c%s "$dest")" = "$SZ" ] && return 0
    [ "$(date +%s)" -ge "$DEADLINE" ] && return 0
    stg=$STAGE/$(echo "$RELPATH" | tr / _)
    # --retry 0: curl's own retry with -C - re-sends from the offset resolved at invocation
    # start and has twice landed the exact published byte count with a corrupt middle.
    code=$(curl -sS --retry 0 -C - -o "$stg" -w '%{http_code}' "$URL" 2>/dev/null)
    case "$code" in
        200|206) ;;
        *) rm -f "$stg"; echo "http-$code $RELPATH"; return 0 ;;
    esac
    got=$(stat -c%s "$stg" 2>/dev/null || echo 0)
    if [ "$got" != "$SZ" ]; then
        echo "short $RELPATH $got/$SZ"
        return 0
    fi
    if [ "$(md5sum "$stg" | cut -d' ' -f1)" != "$MD5" ]; then
        echo "MD5 FAIL $RELPATH, discarding"
        rm -f "$stg"
        return 0
    fi
    mkdir -p "$(dirname "$dest")"
    mv "$stg" "$dest"
    echo "ok $RELPATH $SZ"
}
export -f one

LIST=$ROOT/_ena_gap.$DS.tsv
GAP=$ROOT/no_s3.$DS.txt
# Restricted to the enumerated gap on purpose. Unrestricted, this would pull a whole study from
# ENA at 1.9 MB/s per stream while the .sra route was already carrying it at 32-36 -- the two
# would race for the same destinations and the slow one would win some of them. no_s3.<ds>.txt
# is written by the bucket pre-check; an absent file means the whole study, which is only right
# when the .sra route is being abandoned for it.
if [ -s "$GAP" ]; then
    awk -F'\t' -v d="$DS" 'NR==FNR{g[$1]=1; next} FNR>1 && $1==d{split($2,p,"/"); if (p[2] in g) print}' \
        "$GAP" "$ROOT/manifest.tsv" > "$LIST"
    echo "[$DS] gap list: $(wc -l < "$LIST") files across $(wc -l < "$GAP") runs absent from S3"
else
    awk -F'\t' -v d="$DS" 'NR>1 && $1==d' "$ROOT/manifest.tsv" > "$LIST"
    echo "[$DS] no gap file; taking all $(wc -l < "$LIST") manifest rows"
fi

while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    left=0
    while IFS=$'\t' read -r _ rel _ sz _; do
        [ -s "$ROOT/$DS/$rel" ] && [ "$(stat -c%s "$ROOT/$DS/$rel")" = "$sz" ] || left=$((left + 1))
    done < "$LIST"
    [ "$left" -eq 0 ] && break
    echo "[$DS] $left manifest rows still missing"
    xargs -P "$WIDTH" -I{} bash -c 'one "$@"' _ {} < "$LIST"
done
echo "[$DS] done at $(date -Is)"
