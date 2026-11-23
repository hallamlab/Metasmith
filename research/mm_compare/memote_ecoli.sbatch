#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 2
#SBATCH --mem=8G
#SBATCH --time=1:00:00
#SBATCH --array=0-5
#SBATCH -o /scratch/phyberos/bench/e4_mm/ecoli/logs/memote_%A_%a.log
# E5's modelling/memote_score transform replayed: the same MEMOTE 0.17.0 image and helper.
set -euo pipefail
module load apptainer
W=/scratch/phyberos/bench/e4_mm/ecoli
STRAINS=(k12 dh10b dh1 w3110 bw25113 epi300)
s=${STRAINS[$SLURM_ARRAY_TASK_ID]}
SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_biocontainers_memote..0.17.0--pyhdfd78af_0.sif
T=$SLURM_TMPDIR/$s; mkdir -p $T; cd $T
apptainer exec --cleanenv -B $W -B $T --pwd $T $SIF bash -c "export HOME=$T
python $W/lib_modelling/memote_score.py $W/out/$s/carveme_sbml.xml memote_report.html memote_results.json.gz memote.json"
cp memote.json memote_results.json.gz $W/out/$s/
echo DONE $s
