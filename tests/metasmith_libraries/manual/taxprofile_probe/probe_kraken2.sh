#!/usr/bin/env bash
# Probe Kraken2 + Bracken using the pre-built Kraken2 standard 16GB DB
# already staged on sockeye at /scratch/st-shallam-1/k2_standard_16_GB_20251015.
# That directory contains hash.k2d, opts.k2d, taxo.k2d + database150mers.kmer_distrib
# (and other read-length variants for Bracken).
source "$(dirname "$(readlink -f "$0")")/_lib.sh"

probe_init kraken2
probe_alloc

DB=${KRAKEN2_DB:-/scratch/st-shallam-1/k2_standard_16_GB_20251015}
echo "[kraken2] using DB: $DB" | tee -a "$LOG"
[[ -f "$DB/hash.k2d" ]] || { echo "$DB missing hash.k2d - cannot proceed" | tee -a "$LOG" >&2; exit 1; }

# 1) kraken2 classify (paired)
probe_run "kraken2 --paired --db $DB --report kraken2.kreport --output kraken2.classifications.tsv --threads $CPUS $R1 $R2"

# Inspect output formats
probe_run "echo '== kraken2.kreport (head) =='; head -8 kraken2.kreport"
probe_run "echo '== kraken2.kreport (column count) =='; awk -F'\\t' 'NR==1{print NF\" cols\"; exit}' kraken2.kreport"
probe_run "echo '== kraken2.classifications.tsv (head) =='; head -3 kraken2.classifications.tsv"
probe_run "echo '== kraken2.classifications.tsv (column count) =='; awk -F'\\t' 'NR==1{print NF\" cols\"; exit}' kraken2.classifications.tsv"

# 2) bracken (separate container, same DB dir; kmer_distrib for 150bp is pre-built)
BRACKEN_IMG="$WORK/containers/bracken.sif"
echo "[bracken] using DB: $DB (database150mers.kmer_distrib expected)" | tee -a "$LOG"
srun --jobid="$JOBID" apptainer exec \
    --bind "$WORK:$WORK" \
    --bind /scratch/st-shallam-1:/scratch/st-shallam-1:ro \
    --pwd "$RUN_DIR" \
    "$BRACKEN_IMG" bash -c \
    "bracken -d $DB -i kraken2.kreport -o bracken.species.tsv -w bracken.kreport -r 150 -l S 2>&1; echo '== bracken.species.tsv =='; head -5 bracken.species.tsv; echo '== bracken.kreport =='; head -8 bracken.kreport" \
    2>&1 | tee -a "$LOG"

probe_record
echo "[probe:kraken2] DONE" | tee -a "$LOG"
