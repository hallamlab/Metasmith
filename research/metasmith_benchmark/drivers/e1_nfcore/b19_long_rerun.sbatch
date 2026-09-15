#!/bin/bash
# E1 long's MetaBAT2 and DAS Tool at a long-read identity floor of 80 (B19), over the
# head's existing BAMs and published assemblies. One array element per sample-sheet row.
#   sbatch --array=0-40%5 b19_long_rerun.sbatch <samplesheet.long.csv>
#   rebuild rows from extracted outputs: b19_long_rerun.sbatch <samplesheet.long.csv> rows
#   merge: b19_long_rerun.sbatch <samplesheet.long.csv> merge
#SBATCH -J e1_b19_long
#SBATCH --account=rrg-shallam-ab
#SBATCH -c 12
#SBATCH --mem=32G
#SBATCH -t 6:00:00
#SBATCH --exclude=fc30557,fc30604
#SBATCH -o /scratch/phyberos/bench/logs/e1_b19_long.%A_%a.out
set -euo pipefail

SHEET=$1
MODE=${2:-sample}
E1L=/scratch/phyberos/bench/e1/long
MAP=$E1L/out/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv
OUT=$E1L/b19
IMG=/scratch/phyberos/wave2_b3_nfcore/apptainer_cache/nxf
METABAT=$IMG/quay.io-biocontainers-metabat2-2.17--hd498684_0.img
DASTOOL=$IMG/quay.io-biocontainers-das_tool-1.1.7--r44hdfd78af_1.img
module load apptainer/1.3.5

# Lustre evicts a client that writes many small files at once (ESHUTDOWN), so a sample's
# outputs cross to Lustre as one tar and one rows file, each copied with retries.
cpr() { for i in 1 2 3 4 5; do cp "$@" && return 0; sleep 60; done; return 1; }

# Map rows in nf-core's columns, from a dir holding metabat2_bins/ and DAS Tool's
# contig2bin. That file lists DAS Tool's unbinned set as a bin named `unbinned`, some
# contigs twice, so only a selected bin counts as binned.
write_rows() {
    local S=$1 D=$2
    {
        for f in "$D"/metabat2_bins/*.fa; do
            [ -e "$f" ] || continue
            grep '^>' "$f" | sed 's/^>//; s/[[:space:]].*//' | awk -v a="FLYE-$S" -v bin="$(basename "$f")" '{print a "\t" $0 "\tMetaBAT2\t" bin}'
        done
        gzip -dc "$E1L/out/Assembly/FLYE/FLYE-$S.assembly.fasta.gz" | awk -v a="FLYE-$S" -v S="$S" -F'\t' '
            FNR==NR { if ($2 != "unbinned" && !($1 in binned)) binned[$1] = $2; next }
            /^>/ { c = substr($1, 2); sub(/[[:space:]].*/, "", c)
                   if (c in binned) print a "\t" c "\tDASTool\tFLYE-DASTool-" S "." binned[c] ".fa"
                   else print a "\t" c "\tDASTool\tFLYE-DASToolUnbinned-" S ".fa" }
        ' "$D/FLYE-DASTool-$S"_DASTool_contig2bin.tsv -
    } > "$D/rows.tsv"
    awk -F'\t' '{k = $3 (($4 ~ /Unbinned/) ? " unbinned" : " binned"); n[k]++} END {for (k in n) print S, k, n[k]}' S="$S" "$D/rows.tsv"
}

if [ "$MODE" = rows ]; then
    tail -n +2 "$SHEET" | cut -d, -f1 | while read -r S; do
        [ -d "$OUT/$S/metabat2_bins" ] && write_rows "$S" "$OUT/$S"
    done
    exit 0
fi

if [ "$MODE" = merge ]; then
    n=$(tail -n +2 "$SHEET" | wc -l)
    done_rows=$(ls "$OUT"/*/rows.tsv | wc -l)
    [ "$done_rows" -eq "$n" ] || { echo "ABORT: $done_rows of $n samples have rows"; exit 1; }
    {
        head -1 "$MAP"
        awk -F'\t' 'NR>1 && ($3=="COMEBin" || $3=="SemiBin2")' "$MAP"
        cat "$OUT"/*/rows.tsv
    } > "$OUT/contig_to_bin_map.tsv"
    cut -f3 "$OUT/contig_to_bin_map.tsv" | sort | uniq -c
    exit 0
fi

S=$(tail -n +2 "$SHEET" | sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" | cut -d, -f1)
[ -n "$S" ] || { echo "ABORT: no sample at index $SLURM_ARRAY_TASK_ID"; exit 1; }
BAM=""
for i in 1 2 3; do
    BAM=$(ls "$E1L"/work/*/*/"FLYE-$S-$S.bam" 2>/dev/null || true)
    [ "$(printf '%s' "$BAM" | grep -c .)" -eq 1 ] && break
    sleep 60
done
[ "$(printf '%s' "$BAM" | grep -c .)" -eq 1 ] || { echo "ABORT: no single BAM for $S: [$BAM]"; exit 1; }
W=${SLURM_TMPDIR:?}/b19/$S
mkdir -p "$W/metabat2_bins" "$W/lists"
gzip -dc "$E1L/out/Assembly/FLYE/FLYE-$S.assembly.fasta.gz" > "$W/asm.fasta"
echo "sample $S bam $BAM"

run() { local img=$1; shift; apptainer exec --no-home --cleanenv --bind "${SLURM_TMPDIR}" --pwd "$W" "$img" "$@"; }

run "$METABAT" jgi_summarize_bam_contig_depths --percentIdentity 80 --outputDepth "$W/depth.txt" "$BAM"
# jgi's depth for a long read can go negative on a long contig; MetaBAT2 warns and skips it.
echo "contigs with a negative depth: $(awk -F'\t' 'NR>1 && ($3<0 || $4<0)' "$W/depth.txt" | wc -l) of $(($(wc -l < "$W/depth.txt") - 1))"
gzip -c "$W/depth.txt" > "$W/FLYE-$S-depth.txt.gz"
run "$METABAT" metabat2 -m 1500 --unbinned --seed 1 -i "$W/asm.fasta" -a "$W/depth.txt" -t 12 --saveCls -o "$W/mb/FLYE-MetaBAT2-$S"
for f in "$W"/mb/FLYE-MetaBAT2-"$S".*.fa; do
    case "$f" in *unbinned*|*lowDepth*|*tooShort*) continue ;; esac
    mv "$f" "$W/metabat2_bins/"
done
echo "metabat2 bins $(ls "$W/metabat2_bins" | wc -l)"

run "$DASTOOL" Fasta_to_Contig2Bin.sh -i "$W/metabat2_bins" -e fa > "$W/lists/MetaBAT2.tsv"
for b in COMEBin SemiBin2; do
    awk -F'\t' -v a="FLYE-$S" -v b="$b" 'NR>1 && $1==a && $3==b {sub(/\.fa$/, "", $4); print $2 "\t" $4}' "$MAP" > "$W/lists/$b.tsv"
done
wc -l "$W"/lists/*.tsv
lists=$(for b in MetaBAT2 SemiBin2 COMEBin; do [ -s "$W/lists/$b.tsv" ] && printf '%s,' "$W/lists/$b.tsv"; done)
run "$DASTOOL" DAS_Tool --write_bins --write_unbinned --write_bin_evals --score_threshold 0.5 \
    -t 12 -i "${lists%,}" -c "$W/asm.fasta" -o "$W/FLYE-DASTool-$S"

write_rows "$S" "$W"
tar -C "$W" -cf "$W/$S.b19.tar" metabat2_bins lists "FLYE-$S-depth.txt.gz" \
    "FLYE-DASTool-$S"_DASTool_contig2bin.tsv "FLYE-DASTool-$S"_DASTool_summary.tsv \
    "FLYE-DASTool-$S"_allBins.eval "FLYE-DASTool-$S"_DASTool.log rows.tsv
D=$OUT/$S
rm -rf "$D"
mkdir -p "$D"
cpr "$W/$S.b19.tar" "$D/"
cpr "$W/rows.tsv" "$D/rows.tsv.part"
mv "$D/rows.tsv.part" "$D/rows.tsv"
echo DONE
