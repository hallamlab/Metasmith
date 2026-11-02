#!/usr/bin/env bash
# Probe sylph on tiny inputs. Builds a sketch DB in seconds.
source "$(dirname "$(readlink -f "$0")")/_lib.sh"

probe_init sylph
probe_alloc

DB="$WORK/dbs/sylph_mini/tiny.syldb"
if [[ ! -f "$DB" ]]; then
    echo "[sylph] sketching fixtures/tiny_refs/ -> $DB" | tee -a "$LOG"
    mkdir -p "$(dirname "$DB")"
    probe_run "sylph sketch -i $WORK/fixtures/tiny_refs/*.fna -o ${DB%.syldb} -t $CPUS"
fi

# `sylph profile` produces relative abundance + containment ANI; `sylph query` is a different mode.
probe_run "sylph profile $DB -1 $R1 -2 $R2 -t $CPUS -o sylph.profile.tsv"
probe_run "head -3 sylph.profile.tsv"
probe_run "awk -F'\\t' 'NR==1{print NF\" columns\"; exit}' sylph.profile.tsv"

probe_record
echo "[probe:sylph] DONE — copy outputs to tests/test_data/taxprofile_examples/sylph/" | tee -a "$LOG"
