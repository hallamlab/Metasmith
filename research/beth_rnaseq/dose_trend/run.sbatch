#!/bin/bash
#SBATCH --account=st-shallam-1
#SBATCH --job-name=beth_dose_trend
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=/scratch/st-shallam-1/txyliu/beth_rnaseq_A/run_%j.log
module load gcc/9.4.0 apptainer/1.3.1
cd /scratch/st-shallam-1/txyliu/beth_rnaseq_A
export HOME=$PWD MPLCONFIGDIR=$PWD/.mpl
apptainer exec --no-home -B $PWD /scratch/st-shallam-1/txyliu/apptainer_cache/pydeseq2_0.5.4.sif python dose_trend.py input out
