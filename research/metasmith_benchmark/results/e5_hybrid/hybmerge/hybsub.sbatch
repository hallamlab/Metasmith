#!/bin/bash
#SBATCH -A def-shallam
#SBATCH -t 12:00:00
#SBATCH -c 32
#SBATCH --mem=128G
#SBATCH --array=1-7
#SBATCH -o /scratch/phyberos/bench/e5_hybrid/collect/hybmerge/logs/%x.%A_%a.log
# One CAMI sample per array task: Flye --subassemblies over each Flye assembly with MEGAHIT, then metaQUAST as the pilot scores.
# Flye reads only .fa/.fasta/.fq names, so the inputs go in as links.
set -euo pipefail
H=/scratch/phyberos/bench/e5_hybrid/collect/hybmerge
C=/scratch/phyberos/cache/apptainer
QUAST="apptainer exec -B /scratch/phyberos $C/docker..quay.io_biocontainers_quast..5.3.0--py313pl5321h5ca1c30_2.sif"
FLYE="apptainer exec -B /scratch/phyberos $C/docker..staphb_flye..2.9.6.sif"
read -r S MEGAHIT OPERA RAW POLCA < <(sed -n "${SLURM_ARRAY_TASK_ID}p" $H/manifest.txt)
W=$H/$S; mkdir -p $W && cd $W
declare -A T

t0=$SECONDS
ln -sf $MEGAHIT megahit.fasta
for base in raw polca; do
    B=$RAW; [ $base = polca ] && B=$POLCA
    rm -rf sub_$base
    ln -sf $B flye_$base.fasta
    $FLYE flye --meta --subassemblies flye_$base.fasta megahit.fasta --min-overlap 1000 -t 32 -o sub_$base > sub_$base.log 2>&1 || { echo "flye sub_$base failed"; tail -3 sub_$base.log; }
    T[sub_$base]=$((SECONDS - t0)); t0=$SECONDS
done
for k in "${!T[@]}"; do echo -e "$k\t${T[$k]}"; done > wall_sub_s.tsv

study=${S%%_long*}
case $study in
    marine) TAR=/scratch/phyberos/cami/cami2_challenge/marine/marmgCAMI2_genomes.tar.gz ;;
    plant_associated) TAR=/scratch/phyberos/cami/cami2_challenge/plant_associated/rhimgCAMI2_genomes.tar.gz ;;
    toy_humangut) TAR=/scratch/phyberos/cami/cami3_toy_humangut/long/source_genomes.tar.gz ;;
esac
awk -F'\t' -v s=$S '$1==s && $4!=""{print $4}' $H/sample_genomes.tsv | sort -u > keep_sub.txt
rm -rf unpacked_sub refs_sub; mkdir -p unpacked_sub refs_sub
tar xzf $TAR -C unpacked_sub
find unpacked_sub -type f \( -name '*.fna' -o -name '*.fa' -o -name '*.fasta' \) | while read f; do
    s=$(basename "$f"); if grep -qxF "${s%.*}" keep_sub.txt; then ln -sf "$PWD/$f" refs_sub/$s; fi
done
find unpacked_sub -type f \( -name '*.fna.gz' -o -name '*.fa.gz' -o -name '*.fasta.gz' \) | while read f; do
    s=$(basename "$f" .gz); if grep -qxF "${s%.*}" keep_sub.txt; then gzip -dc "$f" > refs_sub/$s; fi
done
echo "references: $(ls refs_sub | wc -l)"

fastas=(); labels=()
for v in sub_polca sub_raw; do
    f=$v.fna; [ -d $v ] && f=$v/assembly.fasta
    if [ -s $f ]; then fastas+=($W/$f); labels+=($v); fi
done
rm -rf mq_sub
$QUAST metaquast.py "${fastas[@]}" -l "$(IFS=,; echo "${labels[*]}")" -r refs_sub -o mq_sub -t 32 --fragmented --max-ref-number 0 \
    --no-icarus --no-plots --no-html
cd mq_sub && tar czf $H/$S.hybsub.tar.gz $(ls -d combined_reference/*.tsv summary/TSV runs_per_reference/*/report.tsv metaquast.log 2>/dev/null) -C $W wall_sub_s.tsv
cd $W && rm -rf unpacked_sub refs_sub mq_sub
