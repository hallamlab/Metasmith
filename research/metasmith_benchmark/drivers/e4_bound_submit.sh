#!/bin/bash
# Submits the lift-bound searches on fir. Run from this drivers/ directory of a checkout.
set -euo pipefail
B=/scratch/phyberos/e4_ablation/evidence/bound
D=$(pwd)
mkdir -p $B/truth/raw $B/samples $B/out $B/logs
python3 -c "import sys; print(open(sys.argv[1]).read().split(\"WRAPPER = r'''\", 1)[1].split(\"'''\", 1)[0])" \
  ../../../src/metasmith_libraries/transforms/functionalAnnotation/clean.py > $B/run_clean.py

# GenBank, not RefSeq, where RefSeq renamed the locus tags the BiGG model's genes carry.
for pair in iML1515:GCF_000005845.2 iYO844:GCF_000009045.1 iJN1463:GCA_000007565.2 iSynCJ816:GCF_000009725.1 \
            iAF987:GCA_000012925.1 iLJ478:GCA_000008545.1; do
  m=${pair%%:*}; a=${pair#*:}; n=${a:4}
  p=https://ftp.ncbi.nlm.nih.gov/genomes/all/${a:0:3}/${n:0:3}/${n:3:3}/${n:6:3}/
  d=$(curl -s $p | grep -o "${a}_[^/\"]*" | head -1)
  [ -s $B/truth/raw/$m.faa.gz ] || curl -s -o $B/truth/raw/$m.faa.gz $p$d/${d}_translated_cds.faa.gz
done
IMG=/scratch/phyberos/cache/apptainer
CARVEME=$IMG/docker..quay.io_hallamlab_external_carveme..1.6.6-py312.sif
KOFAM=$IMG/docker..quay.io_biocontainers_kofamscan..1.3.0--hdfd78af_2.sif
CLEAN=$IMG/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
LIB=/project/6004975/phyberos/lib
PFAM=/scratch/phyberos/refs/vibrant_1.2.1/databases/Pfam-A_v32.HMM
CPU="--account=rrg-shallam-ab --output=$B/logs/%x.%j.out"
ENV="source /scratch/phyberos/bench/checkout/41382ec2/research/metasmith_benchmark/drivers/host_env.sh; export PYTHONPATH=/scratch/phyberos/bench/checkout/41382ec2/src"

prep=$(sbatch --parsable $CPU -J bound_prep -c 2 --mem=16G -t 3:00:00 --wrap "set -euo pipefail; $ENV
  \$BENCH_PYTHON $D/e4_bound_sample.py $B/samples
  \$BENCH_PYTHON $D/e4_bound_truth.py $B
  cat $B/samples/sample.faa $B/truth/queries.faa > $B/all.faa")

sbatch $CPU -J bound_bigg -d afterok:$prep -c 16 --mem=32G -t 12:00:00 --wrap "set -euo pipefail
  for m in iML1515 iYO844 iJN1463 iSynCJ816 iAF987 iLJ478; do
    apptainer exec -B \$SLURM_TMPDIR $CARVEME diamond makedb --in $B/truth/ref_\$m.faa -d \$SLURM_TMPDIR/ref_\$m --quiet
    apptainer exec -B \$SLURM_TMPDIR $CARVEME diamond blastp -d \$SLURM_TMPDIR/ref_\$m -q $B/truth/queries.faa -o $B/out/bigg_\$m.tsv \
      --more-sensitive --max-target-seqs 25 --threads 16 --quiet --outfmt 6 qseqid sseqid pident length evalue bitscore
  done"

# exec_annotation runs one hmmsearch per KO and walled at 12 h on the 23k curated proteins, so the KOs
# are searched as one concatenated profile file and e4_bound_kofam.py applies KofamScan's thresholds.
sbatch $CPU -J bound_kofam -d afterok:$prep -c 32 --mem=32G -t 6:00:00 --wrap "set -euo pipefail; $ENV
  cat $LIB/kofamscan/profiles/*.hmm > \$SLURM_TMPDIR/ko.hmm
  apptainer exec -B \$SLURM_TMPDIR $KOFAM hmmsearch -E 0.01 --cpu 32 --noali -o /dev/null \
    --tblout \$SLURM_TMPDIR/ko.tblout \$SLURM_TMPDIR/ko.hmm $B/truth/queries.faa
  \$BENCH_PYTHON $D/e4_bound_kofam.py \$SLURM_TMPDIR/ko.tblout $LIB/kofamscan/ko_list.tsv $B/out/kofam_truth.tsv"

sbatch $CPU -J bound_uniref -d afterok:$prep -c 32 --mem=120G -t 12:00:00 --wrap "set -euo pipefail
  apptainer exec -B \$SLURM_TMPDIR $CARVEME diamond blastp -d $LIB/diamond/uniref50.dmnd -q $B/all.faa -o $B/out/uniref50.tsv \
    --sensitive --max-target-seqs 5 --evalue 1e-5 --threads 32 -b 6 -c 1 --tmpdir \$SLURM_TMPDIR --quiet \
    --outfmt 6 qseqid sseqid pident length evalue bitscore stitle"

sbatch $CPU -J bound_pfam -d afterok:$prep -c 32 --mem=16G -t 12:00:00 --wrap "set -euo pipefail
  apptainer exec $KOFAM hmmsearch --cut_ga --cpu 32 --noali -o /dev/null --tblout $B/out/pfam.tblout $PFAM $B/all.faa"

sbatch --account=def-shallam --output=$B/logs/%x.%j.out -J bound_clean -d afterok:$prep -c 8 --mem=48G -t 8:00:00 \
  --gres=gpu:nvidia_h100_80gb_hbm3_2g.20gb:1 --wrap "set -euo pipefail
  mkdir -p \$SLURM_TMPDIR/clean_ws
  apptainer exec --nv -B \$SLURM_TMPDIR/clean_ws:/clean_ws -B $B:/bound -B $B/run_clean.py:/work/run_clean.py \
    --env TORCH_HOME=/opt/torch_cache $CLEAN python /work/run_clean.py --fasta /bound/all.faa \
    --out /bound/out/clean.tsv --app /app --workdir /clean_ws"
