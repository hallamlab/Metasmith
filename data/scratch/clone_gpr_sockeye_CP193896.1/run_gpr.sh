#!/bin/bash
#SBATCH --account=st-shallam-1
#SBATCH --job-name=gpr4lane_bw25113
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=2:00:00
#SBATCH --output=/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/routeB/gpr.%j.log
#SBATCH --error=/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/routeB/gpr.%j.log

set -euo pipefail
module load gcc/9.4.0 apptainer/1.3.1
D=/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/routeB
IMG=/arc/project/st-shallam-1/metasmith/container_images/docker..quay.io_hallamlab_python_for_data_science@sha256..7684b1bd22038de100f4438b2ae049ce7f507960c362d5d0c8495417318d4a9c.sif
cd "$D"
export GPR_THREADS=8
apptainer exec \
  --bind /scratch/st-shallam-1/txyliu/fabfos_b2/agent_home:/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home \
  --bind /arc/project/st-shallam-1/fabfos_refs/processed:/arc/project/st-shallam-1/fabfos_refs/processed \
  "$IMG" python3 "$D/_gpr_4lane.py"
echo "EXIT_OK"
ls -la "$D"
