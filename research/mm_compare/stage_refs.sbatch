#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 2
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH -o /scratch/phyberos/bench/e4_mm/stage_refs.%j.log
set -euo pipefail
S=/home/phyberos/projects/rpp-shallam/phyberos/cyanoverse/gpr/refs
D=/scratch/phyberos/fabfos_refs/processed
mkdir -p $D/uniref50_dmnd
cp -r $S/kofam_ref $D/
cp $S/uniref50_dmnd/uniref50.dmnd $D/uniref50_dmnd/
cd $D
echo "5c818ad769da08a3442524314b6034fb  uniref50_dmnd/uniref50.dmnd" > /scratch/phyberos/bench/e4_mm/uniref_md5.txt
md5sum -c --quiet /scratch/phyberos/bench/e4_mm/kofam_md5.txt && echo KOFAM_OK
md5sum -c --quiet /scratch/phyberos/bench/e4_mm/uniref_md5.txt && echo UNIREF_OK
