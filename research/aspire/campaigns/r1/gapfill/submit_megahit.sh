#!/bin/bash
# usage: submit_megahit.sh <sample> <cpus> <mem_gb> <time>
set -eu
G=/scratch/phyberos/gmcf3495/gapfill
. $G/env.sh
S=$1; CPUS=${2:-32}; MEMGB=${3:-128}; TIME=${4:-24:00:00}
WDR=$(awk -F'\t' -v x=$S '$1==x{print $2}' /scratch/phyberos/gmcf3495/sample_map.tsv)
READS=$(ls $WDR/*cDLsYLHR.fq.gz 2>/dev/null | head -1)
[ -f "$READS" ] || { echo "no clean reads for $S"; exit 1; }
WD=$G/out/$S/megahit; rm -rf "$WD"; mkdir -p "$WD"
# megahit --memory is BYTES; transform pins 85% of the allocation
MEMB=$(python3 -c "print(int($MEMGB*0.85*1024**3))")
cat > $WD/job.sh <<EOF
#!/bin/bash
#SBATCH --account=def-shallam_cpu
#SBATCH --job-name=gf_megahit_$S
#SBATCH --nodes=1 --ntasks=1
#SBATCH --cpus-per-task=$CPUS
#SBATCH --mem=${MEMGB}G
#SBATCH --time=$TIME
#SBATCH --output=$G/logs/megahit_$S.%j.out
#SBATCH --error=$G/logs/megahit_$S.%j.err
set -eu
cd \$SLURM_TMPDIR
df -h \$SLURM_TMPDIR
apptainer exec $BIND /scratch/phyberos/cache/apptainer/docker..biocontainers_megahit..1.2.9_cv1.sif bash -c "set -eux
  megahit --num-cpu-threads $CPUS --memory $MEMB --12 '$READS' -o megahit_ws
"
ls -la megahit_ws/final.contigs.fa
cp megahit_ws/final.contigs.fa $WD/${S}.contigs.fa
cp megahit_ws/log $WD/megahit.log 2>/dev/null || true
echo DONE
EOF
jid=$(sbatch --parsable $WD/job.sh)
echo -e "megahit\t$S\t$jid\t$WD  reads=$(du -h $READS | cut -f1)"
