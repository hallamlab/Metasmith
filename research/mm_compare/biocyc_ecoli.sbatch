#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 4
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH -o /scratch/phyberos/bench/e4_mm/biocyc_ecoli.%j.log
set -euo pipefail
A=/home/phyberos/projects/rpp-shallam/phyberos/biocyc_pgdbs/pgdbs_20k.tar.gz
D=/scratch/phyberos/bench/e4_mm/biocyc26
mkdir -p $D
tar -I pigz -xf $A -C $D pgdbs/ecol316385cyc.pgdb pgdbs/ecol316407cyc.pgdb
ls -la $D/pgdbs
