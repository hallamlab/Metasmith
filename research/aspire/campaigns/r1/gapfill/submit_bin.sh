#!/bin/bash
# usage: submit_bin.sh <binner> <sample> [cpus] [mem] [time]
# env: KEEP=1  stage the whole tool workdir back, not just the results
set -eu
G=/scratch/phyberos/gmcf3495/gapfill
. $G/env.sh
BINNER=$1; SAMPLE=$2
CPUS=${3:-16}; MEM=${4:-64G}; TIME=${5:-8:00:00}
KEEP=${KEEP:-0}
MAP=/scratch/phyberos/gmcf3495/inv/sample_asm_bam.tsv

row=$(awk -F'\t' -v s="$SAMPLE" '$1==s' "$MAP")
[ -z "$row" ] && { echo "no map row for $SAMPLE"; exit 1; }
# Bare basenames resolve against the main run's results tree; an absolute path is
# taken as-is. S13 and S22 were carried by a separate scoped run and their
# products live under a different run key, so a map keyed only to $RES cannot
# reach them.
a=$(echo "$row" | cut -f2); b=$(echo "$row" | cut -f3)
case $a in /*) ASM=$a ;; *) ASM=$RES/sequences-megahit_assembly/$a ;; esac
case $b in /*) BAM=$b ;; *) BAM=$RES/alignment-bam/$b ;; esac
[ -f "$ASM" ] || { echo "missing asm $ASM"; exit 1; }
[ -f "$BAM" ] || { echo "missing bam $BAM"; exit 1; }

WD=$G/out/$SAMPLE/$BINNER
rm -rf "$WD"; mkdir -p "$WD"

ACCT=def-shallam_cpu; EXTRA=""; NV=""; PRECMD=""
case $BINNER in
metabat2)
  BODY="jgi_summarize_bam_contig_depths --outputDepth depth.txt '$BAM'
    metabat2 -i '$ASM' -a depth.txt -o bins/bin -t $CPUS -v"
  SIF=$SIF_METABAT2 ;;
metabat2fix)
  # jgi_summarize_bam_contig_depths 2.17 writes garbage rows for a handful of
  # contigs -- depth ~ -1.2e9 with variance ~1e18, i.e. uninitialised memory, not
  # an arithmetic overflow. On S1 it was 4 rows out of 4888; the other 4884 are
  # correct. Subsampling the BAM 50x did NOT change the magnitude, which is what
  # ruled out the overflow reading. It is also deterministic -- the pipeline's
  # own four retries named byte-identical contigs every time -- so re-running is
  # not a fix. metabat2 refuses to run when it sees them: zero the bad rows and
  # keep every good one.
  BODY="jgi_summarize_bam_contig_depths --outputDepth depth_raw.txt '$BAM'
    awk -f $G/sanitise_depth.awk depth_raw.txt > depth.txt
    metabat2 -i '$ASM' -a depth.txt -o bins/bin -t $CPUS -v -m 1500"
  SIF=$SIF_METABAT2 ;;
semibin2)
  BODY="export PATH=/opt/conda/bin:\$PATH
    SemiBin2 single_easy_bin -i '$ASM' -b '$BAM' -o semibin_out --environment global -t $CPUS"
  SIF=$SIF_SEMIBIN ;;
comebin)
  # Batch size comes from contigs COMEBin will actually keep (>=1000 bp), not
  # from the total. COMEBin drops shorter contigs before building its dataloader
  # and that dataloader has drop_last set, so a batch larger than the usable
  # count yields zero batches and COMEBin dies on an unbound `logits`. That is
  # exactly the ten r1 failures: every one under 1024 usable contigs, every
  # success over 2000, no overlap.
  US=$(awk '/^>/{if(l>=1000)n++; l=0; next}{l+=length($0)}END{if(l>=1000)n++; print n+0}' "$ASM")
  if [ "$US" -lt 2 ]; then
    echo "SKIP comebin $SAMPLE: $US usable contigs (>=1000bp); nothing to contrast against"
    printf 'comebin\t%s\tSKIPPED_NO_USABLE_CONTIGS\t%s\n' "$SAMPLE" "$US" > $WD/skipped.txt
    exit 0
  fi
  BS=$(( US > 1024 ? 1024 : US ))
  # COMEBin trains on augmented *cut* subsequences, not on the contigs
  # themselves, and re-applies the 1000 bp filter after cutting -- so the real
  # dataset is smaller than the usable-contig count and cannot be measured
  # before the run. When a sample still dies on `logits` at BS=usable, the
  # answer is a smaller batch, not a bigger one. COMEBIN_BS forces it.
  BS=${COMEBIN_BS:-$BS}
  # --nv on a CPU allocation emits a benign "no nv files" warning and torch falls
  # back to CPU. That is the validated path: the fir GPU queue stalled comebin
  # 24h+ while CPU nodes scheduled in seconds, and all nine r1 successes were CPU.
  NV="--nv"
  # Stage the assembly into the job's own scratch. COMEBin writes FragGeneScan
  # and hmmsearch intermediates (.frag.faa, .bacar_marker.hmmout, _lengths.txt)
  # NEXT TO THE INPUT FASTA, so handing it the published results path scatters
  # ~110 files across results/sequences-megahit_assembly/. metasmith never hit
  # this because nextflow stages inputs into the task work dir first.
  BODY="mkdir -p bam_input && cp -L '$BAM' bam_input/
    cp -L '$ASM' asm.fna
    mkdir -p comebin_out
    run_comebin.sh -a \$PWD/asm.fna -o \$PWD/comebin_out -p \$PWD/bam_input -t $CPUS -b $BS"
  SIF=$SIF_COMEBIN ;;
*) echo "unknown binner $BINNER"; exit 1 ;;
esac

if [ "$KEEP" = 1 ]; then
  STAGE='cp -r comebin_out semibin_out bins depth.txt '"$WD"'/ 2>/dev/null || true'
else
  STAGE='for d in bins semibin_out/output_bins comebin_out/comebin_res; do
  [ -d "$d" ] && cp -r "$d" '"$WD"'/ || true
done
cp depth.txt semibin_out/contig_bins.tsv '"$WD"'/ 2>/dev/null || true'
fi

cat > $WD/job.sh <<EOF
#!/bin/bash
#SBATCH --account=$ACCT
#SBATCH --job-name=gf_${BINNER}_${SAMPLE}
#SBATCH --nodes=1 --ntasks=1
#SBATCH --cpus-per-task=$CPUS
#SBATCH --mem=$MEM
#SBATCH --time=$TIME
#SBATCH --output=$G/logs/${BINNER}_${SAMPLE}.%j.out
#SBATCH --error=$G/logs/${BINNER}_${SAMPLE}.%j.err
$EXTRA
set -eu
cd \$SLURM_TMPDIR
mkdir -p bins
export OMP_NUM_THREADS=$CPUS
$PRECMD
apptainer exec $NV $BIND --env OMP_NUM_THREADS=$CPUS --bind "\$SLURM_TMPDIR" --pwd "\$SLURM_TMPDIR" '$SIF' bash -c "set -eux
$BODY
"
echo "=== staging results back ==="
mkdir -p $WD
$STAGE
ls -R $WD | head -50
echo DONE
EOF
jid=$(sbatch --parsable $WD/job.sh)
echo -e "${BINNER}\t${SAMPLE}\t${jid}\t${WD}"
