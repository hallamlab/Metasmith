#!/bin/bash
# usage: submit_checkm.sh <sample> <binner> [cpus] [mem] [time]
# Runs CheckM2 over the gap-filled bins for one (sample, binner), reproducing the
# library transform's two-stage shape: per-bin prodigal in its own subprocess
# (CheckM2 v1.1.0 has a pyrodigal-gv heap bug that takes down the WHOLE batch when
# one bin trips it), then batched DIAMOND via --genes on the survivors.
set -eu
G=/scratch/phyberos/gmcf3495/gapfill
. $G/env.sh
S=$1; B=$2; CPUS=${3:-16}; MEM=${4:-64G}; TIME=${5:-4:00:00}
SRC=$G/bins/$S/$B
[ -d "$SRC" ] || { echo "no bins dir $SRC"; exit 1; }
N=$(ls -1 "$SRC"/*.fna 2>/dev/null | wc -l)
[ "$N" -gt 0 ] || { echo "no bins in $SRC"; exit 1; }
WD=$G/checkm/$S/$B; rm -rf "$WD"; mkdir -p "$WD"
cat > $WD/job.sh <<JOB
#!/bin/bash
#SBATCH --account=def-shallam_cpu
#SBATCH --job-name=gf_checkm_${S}_${B}
#SBATCH --nodes=1 --ntasks=1
#SBATCH --cpus-per-task=$CPUS
#SBATCH --mem=$MEM
#SBATCH --time=$TIME
#SBATCH --output=$G/logs/checkm_${S}_${B}.%j.out
#SBATCH --error=$G/logs/checkm_${S}_${B}.%j.err
set -eu
cd \$SLURM_TMPDIR
mkdir -p input faa
cp $SRC/*.fna input/
for fa in input/*.fna; do
  stem=\$(basename "\$fa" .fna)
  apptainer exec $BIND '$SIF_CHECKM' bash -c "
    export PATH=/opt/conda/envs/external_checkm2_env/bin:/opt/conda/bin:\\\$PATH
    prodigal -i \$fa -a faa/\$stem.faa -o /dev/null -p meta -q || true" || true
  [ -s "faa/\$stem.faa" ] || echo "PRODIGAL_FAILED \$stem"
done
apptainer exec $BIND '$SIF_CHECKM' bash -c "
  export PATH=/opt/conda/envs/external_checkm2_env/bin:/opt/conda/bin:\\\$PATH
  checkm2 predict --genes --input faa --output-directory checkm2_out -x faa --threads $CPUS --force" || true
cp -r checkm2_out $WD/ 2>/dev/null || true
ls -la $WD/checkm2_out/ 2>/dev/null || true
echo DONE
JOB
jid=$(sbatch --parsable $WD/job.sh)
echo -e "checkm\t$S\t$B\t$jid\tbins=$N"
