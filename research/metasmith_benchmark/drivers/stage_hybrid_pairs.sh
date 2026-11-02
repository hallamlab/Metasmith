#!/bin/bash
# Hard-link each of Pratama's 17 hybrid Illumina/MinION pairings into its own path.
#
# Run this ON fir -- it only touches local scratch paths, no ssh. e3_pratama.py's hybrid_partners()
# gives each 0.2 um 2022 Illumina replicate its own e3::nanopore_reads given at
# $HYBRID_PAIRS/<illumina_run>__<minion_run>.fastq.gz. A distinct path per pairing is load-bearing,
# not cosmetic: the given library's manifest is keyed by path (pool.py's GivenLibrary, transfer.py's
# Unpack), and importing resolves a symlink to its target before that key is taken (ops.data's
# `Path(path).resolve()`) -- so the shared RAW_2022 source path, or a symlink to it, collapses the
# three Illumina replicates of one well's MinION run back down to a single given. A hard link is a
# second directory entry for the same inode that resolve() does not walk through, so it alone keeps
# the 17 pairings 17 distinct paths.
#
# PAIRS below is illumina_run -> minion_run, generated from e3_pratama.py's hybrid_partners() against
# research/pratama2026/runs.tsv as it stood on 2026-09-26 (17 rows: H14/H32/H41/H52/H53 x 3
# replicates, H51 x 2). tests/metasmith_libraries/test_e3_hybrid_pairs.py checks this table still
# agrees with the driver; regenerate it (and re-run that test) if runs.tsv ever changes.
set -euo pipefail

RAW_2022="${PRATAMA_RAW_2022:-/scratch/phyberos/pratama2026/reads_2022}"
HYBRID_PAIRS="${PRATAMA_HYBRID_PAIRS:-/scratch/phyberos/pratama2026/hybrid_pairs}"

declare -A PAIRS=(
    [SRR32696677]=SRR32696687
    [SRR32696679]=SRR32696687
    [SRR32696681]=SRR32696688
    [SRR32696689]=SRR32696683
    [SRR32696691]=SRR32696683
    [SRR32696693]=SRR32696688
    [SRR32696694]=SRR32696683
    [SRR32696696]=SRR32696684
    [SRR32696698]=SRR32696684
    [SRR32696700]=SRR32696684
    [SRR32696701]=SRR32696685
    [SRR32696703]=SRR32696685
    [SRR32696707]=SRR32696686
    [SRR32696709]=SRR32696686
    [SRR32696711]=SRR32696686
    [SRR32696713]=SRR32696687
    [SRR32696715]=SRR32696688
)

mkdir -p "$HYBRID_PAIRS"
linked=0
for illumina in "${!PAIRS[@]}"; do
    minion="${PAIRS[$illumina]}"
    src="$RAW_2022/$minion/${minion}_1.fastq.gz"
    dst="$HYBRID_PAIRS/${illumina}__${minion}.fastq.gz"
    [ -s "$src" ] || { echo "MISSING source: $src" >&2; exit 1; }
    if [ -e "$dst" ]; then
        [ "$src" -ef "$dst" ] || { echo "$dst already exists and is not a hard link to $src" >&2; exit 1; }
    else
        ln "$src" "$dst"
        linked=$((linked + 1))
    fi
done
echo "${#PAIRS[@]} pairings: $linked linked just now, $(( ${#PAIRS[@]} - linked )) already in place"
