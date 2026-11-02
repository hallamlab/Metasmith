#!/bin/bash
# Emit a Globus --batch file for the E3 archive.
#
# The four exclusions are expressed STRUCTURALLY -- by listing each parent's children and
# omitting the ones we drop -- rather than with Globus --exclude patterns, which match a bare
# name at any depth. "results" as a pattern would also strike any directory called results
# inside a cache shard or inside zenodo_17897233_unpacked, and nothing would report that it had.
set -uo pipefail
H=/scratch/phyberos/pratama2026
D=/Workspace_backups/Tony_Liu/fir_bench_e3

emit() {  # emit <abs source> <dest-relative path>
    if [ -d "$1" ] && [ ! -L "$1" ]; then
        echo "--recursive $1 $D/$2"
    else
        echo "$1 $D/$2"
    fi
}

# Level 1: the home, minus .staging (21 truncated partial downloads, every one superseded by a
# larger completed file in reads_2019/ or reads_2022/) and minus metasmith/, which is cut deeper.
for e in "$H"/* "$H"/.[!.]*; do
    [ -e "$e" ] || continue
    n=$(basename "$e")
    case "$n" in
        .staging|metasmith) continue ;;
    esac
    emit "$e" "pratama2026/$n"
done

# Level 2: metasmith/, minus relay/ (per-node unix sockets pointing into /tmp; untransferable
# and meaningless off this host) and minus runs/, which is cut deeper.
for e in "$H"/metasmith/* "$H"/metasmith/.[!.]*; do
    [ -e "$e" ] || continue
    n=$(basename "$e")
    case "$n" in
        relay|runs) continue ;;
    esac
    emit "$e" "pratama2026/metasmith/$n"
done

# Level 3: the run directory, minus nxf_work/ (nextflow scratch; no cache hit ever consults it,
# and its bytes are mostly hardlinks to task_cache content that Globus would write out twice)
# and minus results/ (5,332 hardlinks into task_cache -- a second copy of 554 GB).
for e in "$H"/metasmith/runs/Qt0rbV1R/* "$H"/metasmith/runs/Qt0rbV1R/.[!.]*; do
    [ -e "$e" ] || continue
    n=$(basename "$e")
    case "$n" in
        nxf_work|results) continue ;;
    esac
    emit "$e" "pratama2026/metasmith/runs/Qt0rbV1R/$n"
done

# The metadata and the rescued product travel beside the tree, not inside it, so the manifest
# describes a tree it is not itself a member of.
echo "--recursive /scratch/phyberos/bench/e3_archive_meta $D/meta"
echo "--recursive /scratch/phyberos/bench/e3_rescue $D/rescue"

# Two files the level-2 and level-3 exclusions above drop, and which nothing else replaces.
# msm_relay is the relay executable that SETUP_COMMANDS starts before every run; the 25 sockets
# beside it are the worthless part of relay/. The image is named in _common.py as a mutable remote
# tag and apptainer resolved it through APPTAINER_CACHEDIR to a path outside the home, so no
# container was in the archive set at all.
echo "$H/metasmith/relay/msm_relay $D/pratama2026/metasmith/relay/msm_relay"
echo "/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_metasmith..0.22.1.sif $D/deps/docker..quay.io_hallamlab_metasmith..0.22.1.sif"
