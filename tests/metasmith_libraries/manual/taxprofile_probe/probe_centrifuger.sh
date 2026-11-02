#!/usr/bin/env bash
# Probe Centrifuger on tiny inputs. Reuses the K2 standard DB's NCBI taxonomy
# (names.dmp + nodes.dmp) and builds a tiny FM-index from the 5 fixture refs.
source "$(dirname "$(readlink -f "$0")")/_lib.sh"

probe_init centrifuger
probe_alloc

DB_DIR="$WORK/dbs/centrifuger_mini"
DB_PREFIX="$DB_DIR/idx"
K2DB=/scratch/st-shallam-1/k2_standard_16_GB_20251015

if [[ ! -f "${DB_PREFIX}.1.cfr" ]]; then
    echo "[centrifuger] building mini index from fixtures/tiny_refs/" | tee -a "$LOG"
    mkdir -p "$DB_DIR"

    # Two-column "file taxID" list; taxIDs from NCBI Taxonomy:
    #   ecoli_k12 K-12 MG1655 = 511145
    #   saureus_n315          = 158879
    #   bsubtilis_168         = 224308
    #   ppputida_kt2440       = 160488
    #   lactobacillus_acidophilus NCFM = 272621
    cat > "$DB_DIR/refs.list" <<EOF
$WORK/fixtures/tiny_refs/ecoli_k12.fna	511145
$WORK/fixtures/tiny_refs/saureus_n315.fna	158879
$WORK/fixtures/tiny_refs/bsubtilis_168.fna	224308
$WORK/fixtures/tiny_refs/ppputida_kt2440.fna	160488
$WORK/fixtures/tiny_refs/lactobacillus_acidophilus.fna	272621
EOF
    probe_run "centrifuger-build -t $CPUS --taxonomy-tree $K2DB/nodes.dmp --name-table $K2DB/names.dmp -l $DB_DIR/refs.list -o $DB_PREFIX"
fi

# Classify (paired) - classifications go to STDOUT, redirect to file
probe_run "centrifuger -x $DB_PREFIX -1 $R1 -2 $R2 -t $CPUS > centrifuger.classifications.tsv"
probe_run "echo '== centrifuger.classifications.tsv (head) =='; head -3 centrifuger.classifications.tsv; awk -F'\\t' 'NR==1{print NF\" cols\"}' centrifuger.classifications.tsv"

# Kraken-style report via separate centrifuger-kreport binary
probe_run "centrifuger-kreport -x $DB_PREFIX centrifuger.classifications.tsv > centrifuger.kreport"
probe_run "echo '== centrifuger.kreport (head) =='; head -10 centrifuger.kreport; awk -F'\\t' 'NR==1{print NF\" cols\"}' centrifuger.kreport"

# centrifuger-quant: per-taxon abundance summary
probe_run "centrifuger-quant -x $DB_PREFIX -c centrifuger.classifications.tsv > centrifuger.summary.tsv 2>&1 || echo '(centrifuger-quant exited non-zero; flags may differ)'"
probe_run "echo '== centrifuger.summary.tsv (head) =='; head -5 centrifuger.summary.tsv 2>/dev/null || echo '(no summary produced)'"

probe_record
echo "[probe:centrifuger] DONE" | tee -a "$LOG"
