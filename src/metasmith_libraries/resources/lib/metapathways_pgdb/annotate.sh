#!/bin/bash
# MetaPathways 4 annotation (no Pathway Tools) of every <name>.fa in a directory,
# in one run, inside the MetaPathways 4 image. Writes <scratch>/mp/<name>/,
# <scratch>/pf/<name>.pf for each genome that reached PATHOLOGIC_INPUT, and one
# stats row per genome on stdout.
#
# `metapathways run` exits 0 when a step fails, so each genome's status comes
# from its metapathways_steps_log.txt.
# usage: annotate.sh <fasta_dir> <mpdb> <scratch> <cpus> <max_mem_gb> <task_mem_gb>
set -uo pipefail
fasta_dir=$(realpath "$1"); mpdb=$(realpath "$2"); s=$(realpath -m "$3")
cpus=$4; max_mem=$5; task_mem=$6
mkdir -p "$s"/mp "$s"/pf
export NXF_HOME=$s/nxf

SECONDS=0
metapathways run -i "$fasta_dir" -o "$s"/mp -d "$mpdb" --orf_mode single \
    --max_cpus "$cpus" --memory "$task_mem GB" --max_memory "$max_mem GB" \
    --work_dir "$s"/work > "$s"/metapathways.log 2>&1 < /dev/null
echo "metapathways exited $? after $SECONDS s" >&2

printf 'genome\tannotation\tfailed_steps\tbatch_mp_seconds\n'
for fa in "$fasta_dir"/*.fa; do
    name=$(basename "$fa" .fa)
    steps=$s/mp/$name/metapathways_steps_log.txt
    failed=$(awk -F'\t' '$2 !~ /^(SUCCESS|SKIPPED)/ {print $1}' "$steps" 2>/dev/null | paste -sd, -)
    status=ok
    if [ ! -s "$steps" ]; then
        status=missing
    elif [ -n "$failed" ]; then
        status=failed
    fi
    if [ -s "$s/mp/$name/ptools/0.pf" ]; then
        cp "$s/mp/$name/ptools/0.pf" "$s/pf/$name.pf"
    fi
    printf '%s\t%s\t%s\t%s\n' "$name" "$status" "${failed:--}" "$SECONDS"
done
