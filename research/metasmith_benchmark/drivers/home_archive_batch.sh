#!/bin/bash
# Emit a Globus --batch file that archives one metasmith agent home, for every run in it.
#
#   ssh fir 'bash -s' -- <home> <dest> < home_archive_batch.sh > batch.txt
#
# e3_archive_batch.sh is the one-run original. Its exclusions hold here, cut structurally for the
# same reason: a Globus --exclude matches a bare name at any depth.
#   relay/              per-node sockets, except the msm_relay executable
#   runs/*/nxf_work/    nextflow scratch, mostly hardlinks to task_cache
#   runs/*/results/     hardlinks into task_cache, except _metadata/ and given.csv
set -uo pipefail
H=${1:?home}
D=${2:?destination}

emit() {
    if [ -d "$1" ] && [ ! -L "$1" ]; then
        echo "--recursive $1 $D/$2"
    elif [ ! -L "$1" ]; then
        echo "$1 $D/$2"
    fi
}

for e in "$H"/* "$H"/.[!.]*; do
    [ -e "$e" ] || continue
    n=$(basename "$e")
    case "$n" in relay|runs) continue ;; esac
    emit "$e" "$n"
done
[ -f "$H/relay/msm_relay" ] && emit "$H/relay/msm_relay" "relay/msm_relay"

for r in "$H"/runs/*/; do
    r=${r%/}; k=$(basename "$r")
    for e in "$r"/* "$r"/.[!.]*; do
        [ -e "$e" ] || continue
        n=$(basename "$e")
        case "$n" in nxf_work|results) continue ;; esac
        emit "$e" "runs/$k/$n"
    done
    for e in "$r"/results/_metadata "$r"/results/given.csv; do
        [ -e "$e" ] && emit "$e" "runs/$k/results/$(basename "$e")"
    done
done
