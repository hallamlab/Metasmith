#!/bin/bash
# Pack MetaPop 0.0.60's environment tar (build_metapop_env.sh) into one squashfs image and check it runs
# through the bind the bench MetaPop transform uses.
#   sbatch --exclude=<bad nodes> <checkout>/research/metasmith_benchmark/drivers/refs/build_metapop_env_image.sbatch
# WARNING remove the tar only after this job prints IMAGE_OK.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=2:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/%x.%j.out
set -euo pipefail
module load apptainer

TAR=/scratch/phyberos/refs/_metapop_0.0.60_env.tar
IMG=/scratch/phyberos/refs/metapop_0.0.60_env.sqfs
BASE=/scratch/phyberos/cache/apptainer/docker..quay.io_biocontainers_metapop..1.0.2--hdfd78af_1.sif
[ ! -e "$IMG" ] || { echo "$IMG already exists"; exit 1; }

cd "$SLURM_TMPDIR"
tar -xf "$TAR"
n_tar=$(find metapop | wc -l)
mksquashfs metapop "$IMG.tmp" -noappend -no-xattrs -comp zstd -processors "$SLURM_CPUS_PER_TASK" -no-progress
n_image=$(unsquashfs -l "$IMG.tmp" | grep -c '^squashfs-root')
echo "entries: tar $n_tar, image $n_image"
[ "$n_tar" -eq "$n_image" ]

apptainer exec --cleanenv --bind "$IMG.tmp:/opt/metapop:image-src=/" --env PATH=/opt/metapop/bin:/usr/local/bin:/usr/bin:/bin \
  "$BASE" sh -c 'metapop --help | head -3; samtools --version | head -1; Rscript -e "library(ggrepel); cat(\"ggrepel ok\n\")"'

mv "$IMG.tmp" "$IMG"
echo "IMAGE_OK $IMG $(stat -c %s "$IMG") bytes"
