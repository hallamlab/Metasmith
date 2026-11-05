#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=beth_compare
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=2:00:00
#SBATCH --output=/scratch/phyberos/beth_mp/logs/compare_%j.log
# Usage: sbatch [--mem=...] compare.sbatch assembly --sample SAMPLE
#        sbatch [--dependency=afterok:...] compare.sbatch combine
# Every assembly at once: compare.sbatch all (submits one job per ready assembly, then a combine).
set -euo pipefail
B=/scratch/phyberos/beth_mp
J=$B/tables/v1/comparison/scripts
SIF=$B/port/metasmith/container_images/docker..quay.io_hallamlab_metasmith..0.24.2.sif
MIN_FREE_INODES=60000
py() { apptainer exec --cleanenv -B $B -B /scratch/phyberos/fabfos_refs $SIF python3 $J/compare_pipelines.py "$@"; }

free=$(diskusage_report 2>/dev/null | awk 'function n(x) { return (x ~ /K$/ ? 1e3 : x ~ /M$/ ? 1e6 : 1) * (x + 0) }
    /\/scratch \(user/ { split($NF, a, "/"); print n(a[2]) - n(a[1]) }' || true)
if [ -n "$free" ] && [ "${free%.*}" -lt $MIN_FREE_INODES ]; then
    echo "$(date -Is) HOLD: $free inodes free on /scratch, below $MIN_FREE_INODES"
    exit 3
fi

if [ "${1:-}" = all ]; then
    ids=()
    for s in $(py ready); do
        mem=16G
        case $s in *_parent) mem=96G ;; esac
        ids+=($(sbatch --parsable --mem=$mem --job-name=beth_cmp_$s $J/compare.sbatch assembly --sample $s))
    done
    dep=$(IFS=:; echo "${ids[*]}")
    sbatch --parsable --dependency=afterok:$dep --job-name=beth_cmp_combine $J/compare.sbatch combine
    exit 0
fi
echo "$(date -Is) compare $*"
py "$@"
echo "$(date -Is) done"
