#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 4
#SBATCH --mem=16G
#SBATCH --time=3:00:00
#SBATCH --array=0-5
#SBATCH -o /scratch/phyberos/bench/e4_mm/ecoli/logs/%A_%a.log
# E5's e5_gem lane (bench-E5 library/transforms/e5_gem) replayed outside metasmith: the same image,
# DIAMOND command, helper, medium and SCIP solver, applied to the RefSeq proteomes.
set -euo pipefail
module load apptainer
W=/scratch/phyberos/bench/e4_mm/ecoli
STRAINS=(k12 dh10b dh1 w3110 bw25113 epi300)
s=${STRAINS[$SLURM_ARRAY_TASK_ID]}
SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_biocontainers_carveme..1.6.6--pyhdfd78af_1.sif
MEDIA=/scratch/phyberos/e5/metasmith/imports/ref/modelling::media@8ab71bcb59d9
MEDIUM=/scratch/phyberos/e5/metasmith/imports/ref/modelling::medium_name@61142030f3b2
T=$SLURM_TMPDIR/$s; mkdir -p $T; cd $T
cp "$MEDIA" media.tsv; cp "$MEDIUM" medium_name.txt
run() { apptainer exec --cleanenv -B $W -B $T --pwd $T $SIF bash -c "$1"; }
run "export HOME=$T XDG_CACHE_HOME=$T
db=\$(python -c 'import carveme; print(carveme.project_dir + carveme.config.get(\"generated\", \"diamond_db\"))')
diamond blastp -d \$db -q $W/proteomes/$s.faa -o $T/bigg_hits.tsv --more-sensitive --top 10 --quiet --threads 4"
cat > carve.py <<PY
import sys
sys.path.insert(0, "$W/lib_modelling")
import carveme.cli.carve as carve
import reframed.solvers
from carveme_from_orfs import carve_and_gapfill, read_medium_name

medium_name = read_medium_name("$T/medium_name.txt")
reframed.solvers.set_default_solver("scip")
maincall = carve.maincall
carve.maincall = lambda **kw: maincall(**{**kw, "input_type": "diamond"})
carve_and_gapfill("$T/bigg_hits.tsv", "$T/media.tsv", medium_name, "$T/carveme_sbml.xml")
PY
run "export HOME=$T XDG_CACHE_HOME=$T; python carve.py"
mkdir -p $W/out/$s
cp bigg_hits.tsv carveme_sbml.xml $W/out/$s/
echo DONE $s
