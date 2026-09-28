#!/bin/bash
# Fetch metaGEM's published per-study Zenodo results (assemblies, MAGs, GEMs, protein
# bins) -- the comparison targets for run R4, not the read inputs `fetch_sra.sh` and
# `fetch_ena.sh` pull. Reuses the login-node-only shape from the reads fetchers: fir's
# compute nodes move 0.02-0.25 MB/s to any external host, Zenodo included, so this has
# to run from a login node, bounded.
#
#   nohup setsid timeout -k 30 3600 ./fetch_zenodo_published.sh published_manifest.tsv \
#       /scratch/phyberos/metagem/published li2019 korem2015 karlsson2013 > _logs/zenodo_published.log 2>&1 &
#
# Idempotent: a destination file already at its manifest byte count and md5 is skipped.
# Study args restrict the run to those studies, in the order given -- matches R4's own
# study order (li2019 -> karlsson2013+korem2015 -> bissett_base -> sunagawa2015) so a
# session that runs out of time still lands the studies wave 3 reaches first.
set -uo pipefail

MANIFEST=${1:?usage: fetch_zenodo_published.sh <manifest.tsv> <dest_root> [study ...]}
DEST=${2:?usage: fetch_zenodo_published.sh <manifest.tsv> <dest_root> [study ...]}
shift 2
STUDIES=("$@")

mkdir -p "$DEST"

want_study() {
    [ ${#STUDIES[@]} -eq 0 ] && return 0
    for s in "${STUDIES[@]}"; do [ "$s" = "$1" ] && return 0; done
    return 1
}

# Column order: study record relpath url bytes md5
tail -n +2 "$MANIFEST" | while IFS=$'\t' read -r STUDY RECORD RELPATH URL SZ MD5; do
    want_study "$STUDY" || continue
    dest="$DEST/$STUDY/$RELPATH"
    if [ -s "$dest" ] && [ "$(stat -c%s "$dest")" = "$SZ" ]; then
        got_md5=$(md5sum "$dest" | cut -d' ' -f1)
        [ "$got_md5" = "$MD5" ] && { echo "ok (cached) $STUDY/$RELPATH"; continue; }
        echo "size ok but md5 MISMATCH, refetching $STUDY/$RELPATH"
    fi
    mkdir -p "$(dirname "$dest")"
    stg="$dest.partial"
    # --retry 0: curl's own retry with -C - has landed the exact published byte count
    # with a corrupt middle before (see research/metagem/README.md). Verify from scratch.
    code=$(curl -sS --retry 0 -L -o "$stg" -w '%{http_code}' "$URL" 2>/dev/null)
    case "$code" in
        200) ;;
        *) echo "http-$code $STUDY/$RELPATH"; rm -f "$stg"; continue ;;
    esac
    got=$(stat -c%s "$stg" 2>/dev/null || echo 0)
    if [ "$got" != "$SZ" ]; then
        echo "SIZE MISMATCH $STUDY/$RELPATH got=$got want=$SZ"
        rm -f "$stg"
        continue
    fi
    got_md5=$(md5sum "$stg" | cut -d' ' -f1)
    if [ "$got_md5" != "$MD5" ]; then
        echo "MD5 FAIL $STUDY/$RELPATH, discarding"
        rm -f "$stg"
        continue
    fi
    mv "$stg" "$dest"
    echo "ok $STUDY/$RELPATH $SZ"
done
echo "done at $(date -Is)"
