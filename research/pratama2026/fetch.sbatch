#!/bin/bash
#SBATCH --job-name=pratama_fetch
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/pratama2026/_logs/%x.%j.out
# The transfer belongs in the queue rather than on a login node, for two reasons learned the
# hard way on 2026-09-10. The login node reaped the whole detached process tree mid-transfer,
# taking the fetch and an unrelated poller with it and leaving no message anywhere. And a
# single ENA stream runs at about 2 MB/s, so the aggregate is bought entirely with
# concurrency -- which is what makes this look like abuse to a login-node watchdog.
#
# WORKERS is the one number that matters, and ENA's tolerance for it is not a fixed number.
# 8 gave 15 MiB/s and 24 gave 57 MiB/s, near-linear; 48 made ftp.sra.ebi.ac.uk refuse every
# connection within 150 ms. At 24 it held for the 2019 project and then, on the larger 2022
# one, degraded into closing transfers mid-stream. 12 is the setting chosen for an unattended
# run: slower on paper, but it finishes a pass instead of spending it on retries.
#
# Set DATASETS to resume one leg. Every leg is idempotent -- fetch.py promotes a file out of
# staging only at its exact byte count, so a re-run costs a stat per finished file.
set -u
ROOT=${PRATAMA_ROOT:-/scratch/phyberos/pratama2026}
WORKERS=${WORKERS:-12}
DATASETS=${DATASETS:-"reads_2019 reads_2022"}   # 2019 first: smaller, one platform
MAX_PASSES=${MAX_PASSES:-20}
cd "$ROOT"
for DS in $DATASETS; do
    RC=1
    PASS=0
    while [ "$PASS" -lt "$MAX_PASSES" ]; do
        PASS=$((PASS + 1))
        echo "=== $DS pass $PASS start $(date -Is) on $(hostname) ===" >> "_logs/${DS}.log"
        python3 fetch.py --manifest manifest.tsv --root "$ROOT" \
            --dataset "$DS" --workers "$WORKERS" >> "_logs/${DS}.log" 2>&1
        RC=$?
        echo "=== $DS pass $PASS end rc=$RC $(date -Is) ===" >> "_logs/${DS}.log"
        if [ "$RC" -eq 0 ]; then
            break
        fi
        # ENA's refusals are time-limited rather than permanent, so a pass that failed wide
        # is worth repeating -- but not immediately, and not without letting the block lapse.
        sleep 300
    done
    echo "$DS rc=$RC passes=$PASS $(date -Is)" >> _logs/run_fetch.done
done
echo "ALL LEGS DONE $(date -Is)" >> _logs/run_fetch.done
