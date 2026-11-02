#!/bin/bash
#SBATCH --job-name=pratama_zenodo
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/pratama2026/_logs/%x.%j.out
# The Zenodo leg, start to finish, unattended.
#
# Two separate reasons this cannot just be a curl from the workstation. Zenodo answered the
# workstation's network with a traffic-reputation block ("unusual traffic from your network"),
# which is why it runs from the cluster at all. And on 2026-09-10 it answered 504 from every
# route tried, workstation and cluster alike, for at least the first hour -- a site outage on
# top of the block -- which is why this waits for the site rather than failing.
#
# Serial by instruction: --workers 1. The record is small enough that concurrency buys nothing
# and a reputation block is exactly what concurrency provokes.
set -uo pipefail
ROOT=${PRATAMA_ROOT:-/scratch/phyberos/pratama2026}
REC=17897233
cd "$ROOT"
JSON="_logs/zenodo.json"
LOG="_logs/zenodo_watch.log"

for i in $(seq 1 130); do          # 130 x 5 min ~ 11 h, inside the 12 h wall
    CODE=$(curl -sS -m 90 -o "$JSON.tmp" -w "%{http_code}" \
        "https://zenodo.org/api/records/$REC" 2>/dev/null)
    echo "$(date -Is) attempt $i http=$CODE" >> "$LOG"
    if [ "$CODE" = "200" ] && head -c 1 "$JSON.tmp" | grep -q "{"; then
        mv "$JSON.tmp" "$JSON"
        echo "$(date -Is) record metadata OK" >> "$LOG"
        break
    fi
    sleep 300
done
[ -s "$JSON" ] || { echo "zenodo never answered" >> "$LOG"; exit 1; }

# Rebuild the whole manifest into a new file rather than in place: the read legs may still be
# running against manifest.tsv, and a half-written manifest under them is a silent disaster.
python3 build_manifest.py --out manifest.new.tsv --runs-out runs.new.tsv \
    --zenodo-json "$JSON" >> "$LOG" 2>&1
grep -q "zenodo_$REC" manifest.new.tsv || { echo "no zenodo rows in manifest" >> "$LOG"; exit 1; }

python3 fetch.py --manifest manifest.new.tsv --root "$ROOT" \
    --dataset "zenodo_$REC" --workers 1 >> "_logs/zenodo_fetch.log" 2>&1
echo "zenodo rc=$? $(date -Is)" >> _logs/zenodo.done
