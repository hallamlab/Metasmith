#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH --time=3:00:00
#SBATCH -o /scratch/phyberos/mp4/logs/t2.%j.log
# One genome through MetaPathways 4 (annotation only), then its 0.pf through
# the Pathway Tools 27.0 pathologic.sif.
# usage: sbatch t2_one_genome.sbatch <genome.fa> <mpdb_root> <out_dir>
set -uo pipefail
fa=$(realpath "$1"); mpdb=$(realpath "$2"); out=$3
name=$(basename "$fa" .fa)
module load apptainer/1.4.5
mkdir -p "$out"
trap 'cp -r $SLURM_TMPDIR/mp/$name "$out/mp" 2>/dev/null; cp -r $SLURM_TMPDIR/ws "$out/ws" 2>/dev/null' EXIT
cd $SLURM_TMPDIR
mkdir -p in mp ws/pgdbs ws/home ws/cyc
cp "$fa" in/
export APPTAINER_TMPDIR=$SLURM_TMPDIR

echo "== metapathways run $(date -Is)"
SECONDS=0
apptainer exec --cleanenv --bind /scratch/phyberos:/scratch/phyberos,$SLURM_TMPDIR:$SLURM_TMPDIR \
    --env NXF_HOME=$SLURM_TMPDIR/nxf \
    /scratch/phyberos/mp4/metapathways_4.0.1.sif \
    metapathways run -i $SLURM_TMPDIR/in/$name.fa -o $SLURM_TMPDIR/mp -d "$mpdb" \
        --orf_mode single --max_cpus $SLURM_CPUS_PER_TASK --memory '16 GB' --max_memory '30 GB' \
        --work_dir $SLURM_TMPDIR/work || echo "metapathways exited $?"
echo "mp_seconds $SECONDS"
cat mp/$name/metapathways_steps_log.txt || true
tail -20 mp/$name/errors_warnings_log.txt 2>/dev/null || true
ls -la mp/$name/ptools || true

echo "== pathologic $(date -Is)"
SECONDS=0
cp mp/$name/ptools/0.pf ws/$name.pf
apptainer exec --no-home --cleanenv --pwd /ws \
    --bind $PWD/ws:/ws,$PWD/ws/pgdbs:/data/ptools-local/pgdbs,$PWD/ws/home:/home/phyberos \
    /scratch/phyberos/mp4/private/pathologic.sif pathologic -i /ws/$name.pf -o /ws/cyc --silent \
    || echo "pathologic exited $?"
echo "pathologic_seconds $SECONDS"
# The image's wrapper moves an upper-case <name>cyc, but PathoLogic writes the PGDB lower-case,
# so the PGDB stays under the pgdbs bind.
pgdb=$(ls -d ws/pgdbs/user/*cyc/1.0 2>/dev/null | head -1)
tail -20 ws/cyc/pathologic.log 2>/dev/null || true
if [ -n "$pgdb" ] && [ -s $pgdb/data/reactions.dat ]; then
    for f in reactions pathways enzrxns proteins genes; do
        echo "$f $(grep -c '^UNIQUE-ID' $pgdb/data/$f.dat)"
    done
    mv $pgdb/* ws/cyc/
    rm -r ws/cyc/input
fi

find mp/$name ws/cyc -type f | wc -l | sed 's/^/files /'
du -sh mp/$name ws/cyc
tar -czf "$out/$name.tar.gz" -C mp $name -C $SLURM_TMPDIR/ws cyc
ls -la "$out/$name.tar.gz"
