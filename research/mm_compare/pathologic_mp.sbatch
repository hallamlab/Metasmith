#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 2
#SBATCH --mem=8G
#SBATCH --time=4:00:00
#SBATCH -o /scratch/phyberos/bench/e4_mm/metapathways/pathologic_mp.%j.log
D=/scratch/phyberos/bench/e4_mm/metapathways
cd $SLURM_TMPDIR
for s in k12 w3110 bw25113 dh1 dh10b epi300; do
    rm -rf ws && mkdir -p ws/pgdbs ws/home ws/cyc
    cp $D/out/$s/ptools/0.pf ws/$s.pf
    apptainer exec --no-home --cleanenv --pwd /ws --bind $PWD/ws:/ws,$PWD/ws/pgdbs:/data/ptools-local/pgdbs,$PWD/ws/home:/home/phyberos \
        /scratch/phyberos/bench/e4_mm/ptools/pathologic.sif pathologic -i /ws/$s.pf -o /ws/cyc --silent
    if [ -s ws/cyc/data/reactions.dat ]; then
        mkdir -p $D/pgdb/$s && cp -r ws/cyc/. $D/pgdb/$s/ && echo "$s built"
    else
        echo "$s FAILED"; tail -20 ws/cyc/pathologic.log
    fi
done
