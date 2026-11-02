#!/bin/bash
# Pack GTDB r232's skani genome trees into one squashfs image, one inode for the ~427K the tree costs.
#   sbatch <checkout>/research/metasmith_benchmark/drivers/refs/build_gtdb_genomes_image.sbatch
# The image holds /GCF and /GCA. The bench GTDB-Tk transforms bind them over skani/database/GCF and
# skani/database/GCA, so the sketch files beside them stay visible.
# WARNING delete the tree only after a GTDB-Tk run through the image passes, and keep GCF/ and GCA/ as
# empty directories: apptainer needs them as mount points inside the /ref bind.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/%x.%j.out
set -euo pipefail

DB=/scratch/phyberos/staging/gtdb/release232/skani/database
IMG=/scratch/phyberos/staging/gtdb/release232_skani_genomes.sqfs
[ ! -e "$IMG" ] || { echo "$IMG already exists"; exit 1; }

# The genomes are gzipped already, so data and fragment compression would buy nothing.
mksquashfs "$DB/GCF" "$DB/GCA" "$IMG.tmp" -noappend -noD -noF -no-xattrs -processors "$SLURM_CPUS_PER_TASK"

n_tree=$(find "$DB/GCF" "$DB/GCA" -name '*_genomic.fna.gz' | wc -l)
n_image=$(unsquashfs -l "$IMG.tmp" | grep -c '_genomic.fna.gz$')
echo "genomes: tree $n_tree, image $n_image"
[ "$n_tree" -eq "$n_image" ]
unsquashfs -cat "$IMG.tmp" GCF/000/367/345/GCF_000367345.1_genomic.fna.gz | gzip -t

mv "$IMG.tmp" "$IMG"
echo "IMAGE_OK $IMG $(stat -c %s "$IMG") bytes"
