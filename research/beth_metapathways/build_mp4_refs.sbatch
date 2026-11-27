#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=beth_mp4_refs
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=8:00:00
#SBATCH --output=/scratch/phyberos/beth_mp/pffix/logs/mp4_refs_%j.log
# DIAMOND databases of the four protein references MetaPathways 4 searched, from the same files:
# UniProt 2025_04 (Juan Santana's MP4 build, the FASTAs its FAST indexes came from) and MetaCyc 27.0
# (exported from the lab's Pathway Tools 27.0 SIF by MP4's build_db). The MetaCyc files are
# licensed: they stay mode 600 under the licensed directory.
set -euo pipefail
SRC=/scratch/jcsm2010/metapathways_dbs
MPDB=/scratch/phyberos/beth_mp/MPDB
OUT=/scratch/phyberos/databases/uniprot_2025_04
LIC=/project/6004975/phyberos/licensed/pathway_tools_27.0
SIF=/scratch/phyberos/beth_mp/port/metasmith/container_images/docker..quay.io_biocontainers_diamond..2.1.8--h43eeafb_0.sif
dmnd() { apptainer exec --cleanenv -B /scratch/phyberos,/scratch/jcsm2010,$LIC $SIF \
    diamond makedb --threads 4 --in "$1" --db "$2"; }

mkdir -p $OUT/ec_maps
for db in swissprot uniref50 uniref90; do
    cp -L $MPDB/functional_categories/EC_map.$db.tsv $OUT/ec_maps/
done
cp -L $MPDB/functional_categories/{SwissProt,uniref50,uniref90}_reldate.txt $OUT/

umask 077
cp $MPDB/functional/metacyc $LIC/metacyc_proteins_27.0.faa
cp $MPDB/functional_categories/MetaCyc-monomer-rxn-pairs.tsv $MPDB/functional_categories/MetaCyc_provenance.json $LIC/
chmod 600 $LIC/metacyc_proteins_27.0.faa $LIC/MetaCyc-monomer-rxn-pairs.tsv $LIC/MetaCyc_provenance.json
dmnd $LIC/metacyc_proteins_27.0.faa $LIC/metacyc_proteins_27.0 &
umask 027
dmnd $SRC/functional/swissprot $OUT/swissprot &
dmnd $SRC/functional/uniref50 $OUT/uniref50 &
dmnd $SRC/functional/uniref90 $OUT/uniref90 &
wait
chmod 600 $LIC/metacyc_proteins_27.0.dmnd
ls -la $OUT $OUT/ec_maps $LIC
