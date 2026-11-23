#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=beth_mp4p
#SBATCH --nodes=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=240G
#SBATCH --time=48:00:00
#SBATCH --signal=B:USR1@5400
#SBATCH --tmp=1000G
#SBATCH --output=/scratch/phyberos/beth_mp/logs/mp_parent_%j.log
# Usage: sbatch mp_analysis_parent.sbatch SAMPLE
# PathoLogic builds a parent's PGDBs at ~7 files per gene-bearing contig, several hundred
# thousand inodes, so the run lives on node-local disk and is copied back on exit:
#   runs/SAMPLE.full_output.tar      the whole output tree, also the resume source;
#                                    rewritten every 3 h as a checkpoint while the run is live
#   runs/SAMPLE/                     the same tree minus unpacked PGDB trees and PT temp dirs
#   .../results/pgdb/unpacked_pgdb_trees.tar   the unpacked PGDB trees, as for the small samples
# The output is bound at /mprun so receipt paths stay identical across resubmissions.
set -euo pipefail
SAMPLE=$1 THREADS=16
B=/scratch/phyberos/beth_mp
JUAN=/scratch/jcsm2010/metapathways_dbs
LIC=$B/images/licensed
SIF=$B/images/metapathways_4.0.3.sif
PT_SIF=$LIC/pathway-tools-27.0-3d267039644e-2fb1f8b8-3aa96f5584c1.sif
DEST=$B/mp4/runs/$SAMPLE
FULL=$B/mp4/runs/$SAMPLE.full_output.tar
LOCAL=$SLURM_TMPDIR/mprun
MEM_GB=$((SLURM_MEM_PER_NODE / 1024 - 16))
mkdir -p $LOCAL
if [ -s $FULL ]; then
    echo "$(date -Is) resuming from $FULL"
    tar -xf $FULL -C $LOCAL
fi
mkdir -p $LOCAL/$SAMPLE

checkpoint_loop() {
    while sleep 10800; do
        rc=0; tar -cf $FULL.tmp -C $LOCAL $SAMPLE 2>/dev/null || rc=$?
        # 1 = files changed while read, expected on a live tree
        if [ $rc -le 1 ]; then mv -f $FULL.tmp $FULL; echo "$(date -Is) checkpoint written ($(du -h $FULL | cut -f1))"; fi
    done
}

copy_back() {
    trap - EXIT USR1 TERM
    kill $CKPT 2>/dev/null || true
    echo "$(date -Is) copy back"
    tar -cf $FULL.part -C $LOCAL $SAMPLE
    [ "$(tar -tf $FULL.part | wc -l)" -eq "$(cd $LOCAL && find $SAMPLE | wc -l)" ] || { echo "FULL TAR COUNT MISMATCH"; exit 5; }
    mv -f $FULL.part $FULL
    mkdir -p $DEST
    rsync -a --exclude='/*/results/pgdb/community/1.0/' --exclude='/*/results/pgdb/MAGs/*/1.0/' \
        --exclude='.pt-run-*/' --exclude='failed-pgdbs/' $LOCAL/$SAMPLE/ $DEST/
    P=$LOCAL/$SAMPLE/$SAMPLE/results/pgdb
    if [ -d $P ] && cd $P && ls -d community/1.0 MAGs/*/1.0 >/dev/null 2>&1; then
        TREES=$(ls -d community/1.0 MAGs/*/1.0 2>/dev/null)
        tar -cf $DEST/$SAMPLE/results/pgdb/unpacked_pgdb_trees.tar $TREES
        diff <(find $TREES | sort) <(tar -tf $DEST/$SAMPLE/results/pgdb/unpacked_pgdb_trees.tar | sed 's:/$::' | sort) >/dev/null \
            || { echo "TREE TAR MISMATCH"; exit 6; }
        { echo "$(date -Is) built on node-local disk; unpacked PGDB trees packed into results/pgdb/unpacked_pgdb_trees.tar ($(tar -tf $DEST/$SAMPLE/results/pgdb/unpacked_pgdb_trees.tar | wc -l) entries, listing verified); PT temp dirs and failed-pgdbs only in $FULL"; echo "$TREES" | sed 's/^/  /'; } > $DEST/$SAMPLE/PACKED.txt
    fi
    echo "$(date -Is) copy back done: $(find $DEST | wc -l) inodes in $DEST"
}
on_signal() { echo "$(date -Is) signal: stopping MetaPathways"; kill -INT $PID 2>/dev/null || true; wait $PID || true; copy_back; exit 1; }
checkpoint_loop &
CKPT=$!
trap copy_back EXIT
trap on_signal USR1 TERM

echo "$(date -Is) $SAMPLE local=$LOCAL on $(hostname), ${SLURM_CPUS_PER_TASK} cpus, ${MEM_GB} GB ($(df -h --output=avail $SLURM_TMPDIR | tail -1) local)"
export APPTAINERENV_METAPATHWAYS_FAST_SCRATCH=$SLURM_TMPDIR APPTAINERENV_TMPDIR=$SLURM_TMPDIR
export APPTAINERENV_NXF_OPTS="-Xms512m -Xmx8g" APPTAINERENV_NXF_HOME=$B/nxf_home APPTAINERENV_NXF_OFFLINE=true APPTAINER_TMPDIR=$SLURM_TMPDIR
export APPTAINERENV_APPTAINER_TMPDIR=$SLURM_TMPDIR
apptainer exec --cleanenv -B $B,$JUAN,$SLURM_TMPDIR,$LIC,$LOCAL:/mprun --pwd $B $SIF \
    metapathways analysis_wf --manifest $B/mp4/manifest.tsv -o /mprun/$SAMPLE -d $B/MPDB --samples "$SAMPLE" \
    --annotation_dbs swissprot uniref50 uniref90 cazy eggnog metacyc \
    --threads $THREADS --max_cpus $SLURM_CPUS_PER_TASK --memory "16 GB" --ptools_memory "8 GB" \
    --max_memory "${MEM_GB} GB" --executor local --fast_local_db --keep_work --image $PT_SIF &
PID=$!
wait $PID
echo "$(date -Is) done $SAMPLE"
