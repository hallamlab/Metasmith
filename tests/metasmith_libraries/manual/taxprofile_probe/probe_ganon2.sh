#!/usr/bin/env bash
# Probe ganon (v2.4.1) on tiny inputs. Uses `build-custom` with a per-file
# taxID map (NCBI taxonomy from the K2 standard DB).
source "$(dirname "$(readlink -f "$0")")/_lib.sh"

probe_init ganon
probe_alloc

DB_DIR="$WORK/dbs/ganon2_mini"
DB_PREFIX="$DB_DIR/tiny"
K2DB=/scratch/st-shallam-1/k2_standard_16_GB_20251015

if [[ ! -f "${DB_PREFIX}.ibf" && ! -f "${DB_PREFIX}.hibf" ]]; then
    echo "[ganon2] building mini IBF with build-custom" | tee -a "$LOG"
    mkdir -p "$DB_DIR"

    # Per-file 3-column input: file <tab> target <tab> taxID.
    # "target" is the unique entry identifier (we use the basename).
    cat > "$DB_DIR/input.tsv" <<EOF
$WORK/fixtures/tiny_refs/ecoli_k12.fna	ecoli_k12	511145
$WORK/fixtures/tiny_refs/saureus_n315.fna	saureus_n315	158879
$WORK/fixtures/tiny_refs/bsubtilis_168.fna	bsubtilis_168	224308
$WORK/fixtures/tiny_refs/ppputida_kt2440.fna	ppputida_kt2440	160488
$WORK/fixtures/tiny_refs/lactobacillus_acidophilus.fna	lactobacillus_acidophilus	272621
EOF
    probe_run "ganon build-custom --input-file $DB_DIR/input.tsv --input-target file --db-prefix $DB_PREFIX --taxonomy ncbi --taxonomy-files $K2DB/nodes.dmp $K2DB/names.dmp --skip-genome-size --threads $CPUS"
fi

probe_run "ganon --version"

# Classify; -p accepts pairs in sequence: R1a R2a [R1b R2b ...]
probe_run "ganon classify --db-prefix $DB_PREFIX --paired-reads $R1 $R2 --output-prefix tiny --threads $CPUS"

# Outputs: <prefix>.rep (raw report), <prefix>.tre (tree-like abundance)
probe_run "echo '== tiny.tre (head) =='; head -10 tiny.tre; awk -F'\\t' 'NR==1{print NF\" cols\"}' tiny.tre"
probe_run "echo '== tiny.rep (head) =='; head -10 tiny.rep; awk -F'\\t' 'NR==1{print NF\" cols\"}' tiny.rep"

probe_record
echo "[probe:ganon2] DONE" | tee -a "$LOG"
