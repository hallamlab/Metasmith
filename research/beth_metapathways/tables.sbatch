#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=beth_tables
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=6:00:00
#SBATCH --output=/scratch/phyberos/beth_mp/logs/tables_%j.log
# Usage: sbatch [--mem=...] tables.sbatch LABEL SAMPLE [PORT_RUN_KEY ...]
# Without run keys, every port run whose given.csv names the sample's assembly is used.
set -euo pipefail
B=/scratch/phyberos/beth_mp
J=$B/jobs/tables
SIF=$B/port/metasmith/container_images/docker..quay.io_hallamlab_metasmith..0.24.2.sif
MIN_FREE_INODES=60000
LABEL=$1 S=$2
shift 2
O=$B/tables/$LABEL/$S
MAGS=$B/mp4/mag_maps/$S.tsv
MP=$B/mp4/runs/$S/$S
[ -d $MP ] || MP=$B/mp4/out/$S
py() { apptainer exec --cleanenv -B $B $SIF python3 "$@"; }

free=$(diskusage_report 2>/dev/null | awk 'function n(x) { return (x ~ /K$/ ? 1e3 : x ~ /M$/ ? 1e6 : 1) * (x + 0) }
    /\/scratch \(user/ { split($NF, a, "/"); print n(a[2]) - n(a[1]) }' || true)
if [ -n "$free" ] && [ "${free%.*}" -lt $MIN_FREE_INODES ]; then
    echo "$(date -Is) HOLD: $free inodes free on /scratch, below $MIN_FREE_INODES"
    exit 3
fi

keys=("$@")
if [ ${#keys[@]} -eq 0 ]; then
    for g in $B/port/metasmith/runs/*/results/given.csv; do
        grep -q "/inputs/$S.fasta," "$g" && keys+=("$(basename "$(dirname "$(dirname "$g")")")")
    done
fi
echo "$(date -Is) $S: ${free:-unknown} inodes free; port runs ${keys[*]:-none}; MetaPathways $([ -d $MP ] && echo $MP || echo none)"
runs=() tables=()
for k in "${keys[@]}"; do
    R=$B/port/metasmith/runs/$k
    runs+=(--port-run "$R")
    if [ ${#tables[@]} -eq 0 ] && compgen -G "$R/results/annotation-pgdb_archive/*" >/dev/null; then
        py $J/export_pgdb.py --sample $S --bins $MAGS --port-run "$R" --out-dir $O/pgdb_port
        tables+=(--pgdb-table $O/pgdb_port/pgdb_genes.parquet)
    fi
done
mp=()
if [ -d $MP ]; then
    mp=(--mp-out $MP)
    if compgen -G "$MP/results/pgdb/community/*cyc.tar.bz2" >/dev/null || compgen -G "$MP/results/pgdb/MAGs/*/*cyc.tar.bz2" >/dev/null; then
        py $J/export_pgdb.py --sample $S --bins $MAGS --mp-out $MP --out-dir $O/pgdb_mp
        tables+=(--pgdb-table $O/pgdb_mp/pgdb_genes.parquet)
    fi
fi
rc=0
py $J/aggregate_annotations.py --sample $S --assembly $B/inputs/$S.fasta --mag-map $MAGS \
    "${runs[@]}" "${mp[@]}" "${tables[@]}" --out-dir $O || rc=$?
echo "$(date -Is) done, exit $rc"
exit $rc
