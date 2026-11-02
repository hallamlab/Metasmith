#!/bin/bash
#SBATCH --job-name=mw_testdata
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=32
#SBATCH --mem=120G
#SBATCH --time=4:00:00
#SBATCH --output=/scratch/phyberos/mw_test/_logs/%x.%j.out
# A real but small MAG-lane input, built here because the reads are already here.
#
# Its only purpose is to give the MetaWRAP transform something all three binners will
# finish on. Twelve contigs is not that: MaxBin2 refuses a dataset that small and takes
# the whole binning module down with it.
#
# Twenty million pairs off one 2019 run is about 6 Gbp, which assembles in minutes here
# and yields tens of thousands of contigs -- enough for MetaBAT2, MaxBin2 and CONCOCT to
# each produce bins and for refinement to have something to consolidate.
set -eu
# NOT pipefail: every truncation below is `zcat | head`, and head closing the pipe
# is how the truncation happens. With pipefail that SIGPIPE fails the job.

RUN=${RUN:-ERR3858114}
PAIRS=${PAIRS:-20000000}
SRC=/scratch/phyberos/pratama2026/reads_2019/$RUN
OUT=/scratch/phyberos/mw_test
SIF=$OUT/sif/megahit.sif

mkdir -p "$OUT/_logs" "$OUT/sif"
export APPTAINER_CACHEDIR=$OUT/.apptainer_cache
module load apptainer/1.3.5

cd "$OUT"
LINES=$((PAIRS * 4))
for m in 1 2; do
    if [ ! -s "${RUN}_sub_${m}.fastq.gz" ]; then
        zcat "$SRC/${RUN}_${m}.fastq.gz" | head -n $LINES | gzip -1 > "${RUN}_sub_${m}.fastq.gz"
    fi
done

# The transform's input type is one interleaved file, which it splits back apart itself.
if [ ! -s "${RUN}_sub_il.fastq.gz" ]; then
    paste <(zcat "${RUN}_sub_1.fastq.gz" | paste - - - -) \
          <(zcat "${RUN}_sub_2.fastq.gz" | paste - - - -) \
        | tr '\t' '\n' | gzip -1 > "${RUN}_sub_il.fastq.gz"
fi

if [ ! -s "$SIF" ]; then
    apptainer pull "$SIF" docker://quay.io/biocontainers/megahit:1.2.9--h43eeafb_4
fi

rm -rf megahit_out
apptainer exec --bind "$OUT":"$OUT" "$SIF" \
    megahit -1 "$OUT/${RUN}_sub_1.fastq.gz" -2 "$OUT/${RUN}_sub_2.fastq.gz" \
        -o "$OUT/megahit_out" -t 32 -m 0.8

grep -c '^>' megahit_out/final.contigs.fa
echo "MW TESTDATA DONE $(date -Is)"
