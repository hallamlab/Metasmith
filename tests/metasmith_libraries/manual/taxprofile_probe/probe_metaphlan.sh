#!/usr/bin/env bash
# Probe MetaPhlAn 4 on tiny inputs.
#
# WARNING: smallest official MetaPhlAn DB is ~5-10 GB. If the cost is
# prohibitive on the probe machine, skip this script and mark the
# MetaPhlAn row in PROBE.md as "docs-only, pending real run" — the
# tool's TSV format is stable across releases.
source "$(dirname "$(readlink -f "$0")")/_lib.sh"

probe_init metaphlan
probe_alloc

DB_DIR="$WORK/dbs/metaphlan_mini"
if [[ -z "$(ls -A "$DB_DIR" 2>/dev/null)" ]]; then
    cat <<EOM | tee -a "$LOG" >&2
ERROR: MetaPhlAn DB not present at $DB_DIR.
Compute nodes lack internet — pre-download from a login node:
  module load gcc/9.4.0 apptainer/1.3.1
  apptainer exec --bind /scratch/st-shallam-1:/scratch/st-shallam-1 \\
    $WORK/containers/metaphlan.sif \\
    metaphlan --install --db_dir $DB_DIR --nproc 8
EOM
    exit 1
fi

# --offline avoids the network check (compute nodes are firewalled).
# MetaPhlAn 4.2 requires either --subsampling_paired with -1/-2, OR --mapout
# with the comma-separated form. Use the comma form so we also capture the
# bowtie2 SAM (useful artifact for downstream QC).
probe_run "metaphlan $R1,$R2 --input_type fastq --offline --db_dir $DB_DIR --nproc $CPUS -o metaphlan.profile.tsv --mapout metaphlan.bowtie2.sam --tmp_dir $RUN_DIR/tmp"
probe_run "echo '== metaphlan.profile.tsv (head) =='; head -8 metaphlan.profile.tsv; awk -F'\\t' '/^[^#]/{print NF\" cols (first non-comment line)\"; exit}' metaphlan.profile.tsv"

probe_record
echo "[probe:metaphlan] DONE — copy profile.tsv to tests/test_data/taxprofile_examples/metaphlan/" | tee -a "$LOG"
