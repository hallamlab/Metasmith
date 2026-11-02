#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e1_close_gapfill
#SBATCH --time=03:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=16
#SBATCH --exclude=fc30557,fc30564
#SBATCH --output=/scratch/phyberos/bench/e1_close/logs/gapfill.%j.log
# One short sample whose E1 MetaBAT2 found no bins: nf-core/mag 5.5.0's METABAT2, DASTOOL over three
# binners and CHECKM2_PREDICT, as transcribed in library/transforms/e2/{metabat2,das_tool,checkm2}.py.
# Usage: sbatch e1_close_gapfill.sbatch <sample>
set -euo pipefail
S=$1
W=/scratch/phyberos/bench/e1_close
I=/scratch/phyberos/cache/apptainer
DB=/scratch/phyberos/wave2_b3_nfcore/checkm2_db/CheckM2_database/uniref100.KO.1.dmnd
IFS=$'\t' read -r idx arm sample dataset asm_id asm bam reads truth < <(awk -F'\t' -v s=$S '$3==s' $W/sheet.tsv)
O=$W/gapfill/$S
ASM=${asm_id%%-*}
mkdir -p $O
echo "START $(date -Is) $S on $(hostname)"
module load apptainer
cd $SLURM_TMPDIR
C=$SLURM_CPUS_PER_TASK
run() { apptainer exec --no-home --cleanenv -B /scratch/phyberos -B $SLURM_TMPDIR --pwd $SLURM_TMPDIR "$I/$1" bash -c "set -euo pipefail; $2"; }

run docker..quay.io_biocontainers_metabat2..2.15--h986a166_1.sif \
    "export OMP_NUM_THREADS=$C; jgi_summarize_bam_contig_depths --outputDepth depth.txt $bam"
run docker..quay.io_biocontainers_metabat2..2.17--hd498684_0.sif \
    "metabat2 -m 1500 --unbinned --seed 1 -i $asm -a depth.txt -t $C --saveCls -o $ASM-MetaBAT2-$S"
mkdir -p metabat2_bins
for f in $ASM-MetaBAT2-$S.*.fa; do
    n=${f%.fa}; n=${n##*.}
    [[ $n =~ ^[0-9]+$ ]] && mv $f metabat2_bins/
done
echo "MetaBAT2 bins: $(ls metabat2_bins | wc -l)"
awk 'NR>1{n++; s+=$3} END{printf "depth: %d contigs, mean %.4f\n", n, s/n}' depth.txt

for f in metabat2_bins/*.fa; do b=$(basename $f .fa); grep '^>' $f | sed "s/^>//; s/[[:space:]].*//; s/\$/\t$b/"; done > MetaBAT2.tsv
for b in SemiBin2 COMEBin; do
    awk -F'\t' -v b=$b 'NR>1 && $3==b && tolower($4) !~ /unbinned/ {sub(/[[:space:]].*/, "", $2); sub(/\.fa$/, "", $4); print $2"\t"$4}' $W/maps/$S.tsv > $b.tsv
done
wc -l MetaBAT2.tsv SemiBin2.tsv COMEBin.tsv
gunzip -c $asm > contigs.fa
run docker..quay.io_biocontainers_das_tool..1.1.7--r44hdfd78af_1.sif \
    "DAS_Tool --write_bins --write_unbinned --write_bin_evals --score_threshold 0.5 -t $C -i MetaBAT2.tsv,SemiBin2.tsv,COMEBin.tsv -c contigs.fa -o $ASM-DASTool-$S"
mkdir -p dastool_bins
for f in $ASM-DASTool-${S}_DASTool_bins/*.fa; do [ "$(basename $f .fa)" = unbinned ] || cp $f dastool_bins/; done
echo "DAS Tool bins: $(ls dastool_bins | wc -l)"

for b in MetaBAT2 DASTool; do
    run docker..community.wave.seqera.io_library_checkm2..1.1.0--60f287bc25d7a10d.sif \
        "checkm2 predict --input ${b,,}_bins -x fa --output-directory checkm2_$b --threads $C --database_path $DB"
    cp checkm2_$b/quality_report.tsv $O/$ASM-$b-$S.quality_report.tsv
done

tar -cf $O/bins.tar metabat2_bins dastool_bins
gzip -c depth.txt > $O/$asm_id-depth.txt.gz
cp MetaBAT2.tsv SemiBin2.tsv COMEBin.tsv $ASM-DASTool-${S}_DASTool_contig2bin.tsv $ASM-DASTool-${S}_DASTool_summary.tsv $O/
python3 $W/e1_close.py patch --sheet $W/sheet.tsv --maps $W/maps --sample $S --binner MetaBAT2 --contig2bin MetaBAT2.tsv
python3 $W/e1_close.py patch --sheet $W/sheet.tsv --maps $W/maps --sample $S --binner DASTool --contig2bin $ASM-DASTool-${S}_DASTool_contig2bin.tsv
ls -l $O
echo "EXIT 0 $(date -Is)"
