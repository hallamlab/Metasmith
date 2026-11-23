#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH --time=12:00:00
#SBATCH -o /scratch/phyberos/bench/e4_mm/metapathways/run.%j.log
set -euo pipefail
cd /scratch/phyberos/bench/e4_mm/metapathways
export APPTAINER_CACHEDIR=$PWD/tmp
apptainer exec --cleanenv --bind $PWD:$PWD --pwd $PWD metapathways_3.5.1.sif \
    metapathways run -i $PWD/inputs -o $PWD/out -d $PWD/refdb -t 8
for s in out/*/; do
    echo "== $s"
    tail -3 $s/metapathways_steps_log.txt 2>/dev/null || true
    cat $s/errors_warnings_log.txt 2>/dev/null | tail -5 || true
done
