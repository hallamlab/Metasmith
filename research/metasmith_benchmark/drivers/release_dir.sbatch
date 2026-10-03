#!/bin/bash
# Delete scratch directories that E5's cleanup released, one after another.
#
#   sbatch --dependency=singleton <checkout>/research/metasmith_benchmark/drivers/release_dir.sbatch <dir>...
#
# WARNING the deletion cannot be undone. Each directory must have a verified chinook copy, or need
# none, before it is named here. findings/E5_RUN.md lists each one and its copy.
# CAUTION the job name is fixed so --dependency=singleton runs one deletion at a time. Parallel
# deletes load the Lustre metadata server for every other user of the account.
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e5_release
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/release_dir.%j.out
set -euo pipefail

S=/scratch/phyberos
KEEP=(
    "$S/refs" "$S/viromics_refs" "$S/databases" "$S/staging" "$S/cache" "$S/apptainer_cache"
    "$S/metagem" "$S/e5" "$S/wave2_b3_nfcore/checkm2_db" "$S/bench/e1" "$S/bench/checkout/a1c1b9d7"
    "$S/cami/work" "$S/cami/cami2_challenge" "$S/cami/cami3_toy_humangut"
    "$S/pratama2026/interleaved" "$S/pratama2026/reads_2019" "$S/pratama2026/reads_2022"
    "$S/pratama2026/hybrid_pairs" "$S/pratama2026/zenodo_17897233" "$S/pratama2026/zenodo_17897233_unpacked"
)
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }

for arg in "$@"; do
    t="$(realpath -e "$arg")" || { echo "ABORT: $arg does not exist"; exit 1; }
    [[ "$t" == "$S"/?* && -d "$t" && ! -L "$arg" ]] || { echo "ABORT: $t is not a directory under $S"; exit 1; }
    for k in "${KEEP[@]}"; do
        [[ "$t" == "$k" || "$t" == "$k"/* || "$k" == "$t"/* ]] && { echo "ABORT: $t overlaps kept $k"; exit 1; }
    done
done

echo "quota before: $(quota)"
for arg in "$@"; do
    t="$(realpath -e "$arg")"
    echo "== $t: $(lfs find "$t" | wc -l) inodes, $(du -sb "$t" | cut -f1) B"
    rm -rf -- "$t"
    echo "   $([ -e "$t" ] && echo present || echo gone)"
done
sleep 300
echo "quota 5 min after: $(quota)"
