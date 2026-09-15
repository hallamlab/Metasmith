#!/bin/bash
# E1 long's MetaBAT2 and DAS Tool at a long-read identity floor of 80 (B19), over the
# head's existing BAMs and published assemblies. One array element per sample-sheet row.
#   sbatch --array=0-40 b19_long_rerun.sbatch <samplesheet.long.csv>
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

# Map rows in nf-core's columns. DAS Tool's contig2bin lists its unbinned set as a bin
# named `unbinned`, some contigs twice, so only a selected bin counts as binned.
write_rows() {
    local S=$1 D=$OUT/$1
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
    tail -n +2 "$SHEET" | cut -d, -f1 | while read -r S; do write_rows "$S"; done
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
mapfile -t BAMS < <(ls "$E1L"/work/*/*/"FLYE-$S-$S.bam")
[ "${#BAMS[@]}" -eq 1 ] || { echo "ABORT: ${#BAMS[@]} BAMs for $S"; exit 1; }
ASM_GZ=$E1L/out/Assembly/FLYE/FLYE-$S.assembly.fasta.gz
D=$OUT/$S
rm -rf "$D"
mkdir -p "$D/metabat2_bins" "$D/lists"
W=${SLURM_TMPDIR:?}/b19
mkdir -p "$W"
gzip -dc "$ASM_GZ" > "$W/asm.fasta"
echo "sample $S bam ${BAMS[0]}"

run() { local img=$1; shift; apptainer exec --no-home --cleanenv --bind "$W" --pwd "$W" "$img" "$@"; }
# Lustre returns transient EIO on writes from some nodes.
cpr() { for i in 1 2 3 4 5; do cp "$@" && return 0; sleep 60; done; return 1; }

run "$METABAT" jgi_summarize_bam_contig_depths --percentIdentity 80 --outputDepth "$W/depth.txt" "${BAMS[0]}"
gzip -c "$W/depth.txt" > "$W/depth.txt.gz"
cpr "$W/depth.txt.gz" "$D/FLYE-$S-depth.txt.gz"
# jgi's depth for a long read can go negative on a long contig; MetaBAT2 warns and skips it.
echo "contigs with a negative depth: $(awk -F'\t' 'NR>1 && ($3<0 || $4<0)' "$W/depth.txt" | wc -l) of $(($(wc -l < "$W/depth.txt") - 1))"
run "$METABAT" metabat2 -m 1500 --unbinned --seed 1 -i "$W/asm.fasta" -a "$W/depth.txt" -t 12 --saveCls -o "$W/FLYE-MetaBAT2-$S"
for f in "$W"/FLYE-MetaBAT2-"$S".*.fa; do
    case "$f" in *unbinned*|*lowDepth*|*tooShort*) continue ;; esac
    cpr "$f" "$D/metabat2_bins/"
done
nbins=$(ls "$D/metabat2_bins" | wc -l)
echo "metabat2 bins $nbins"

(cd "$D/metabat2_bins" && run "$DASTOOL" Fasta_to_Contig2Bin.sh -i "$D/metabat2_bins" -e fa) > "$D/lists/MetaBAT2.tsv"
for b in COMEBin SemiBin2; do
    awk -F'\t' -v a="FLYE-$S" -v b="$b" 'NR>1 && $1==a && $3==b {sub(/\.fa$/, "", $4); print $2 "\t" $4}' "$MAP" > "$D/lists/$b.tsv"
done
wc -l "$D"/lists/*.tsv
lists=$(for b in MetaBAT2 SemiBin2 COMEBin; do [ -s "$D/lists/$b.tsv" ] && printf '%s,' "$D/lists/$b.tsv"; done)
run "$DASTOOL" DAS_Tool --write_bins --write_unbinned --write_bin_evals --score_threshold 0.5 \
    -t 12 -i "${lists%,}" -c "$W/asm.fasta" -o "$W/FLYE-DASTool-$S"
cpr "$W/FLYE-DASTool-$S"_DASTool_contig2bin.tsv "$W/FLYE-DASTool-$S"_DASTool_summary.tsv "$W/FLYE-DASTool-$S"_allBins.eval "$W/FLYE-DASTool-$S"_DASTool.log "$D/"

write_rows "$S"
echo DONE
