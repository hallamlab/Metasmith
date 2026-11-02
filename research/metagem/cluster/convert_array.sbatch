#!/bin/bash
#SBATCH --job-name=metagem_convert
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=4:00:00
#SBATCH --output=/scratch/phyberos/metagem/_logs/%x.%A_%a.out
# One array task per RUN: .sra -> fastq.gz at the manifest's relpaths. No network, which is the
# point -- fir compute nodes move 0.02-0.25 MB/s to every external host tested, so the fetch
# happens on a login node (fetch_sra.sh) and only this half belongs in the queue.
#
# THE MANIFEST'S MD5 CANNOT GATE THESE FILES and no amount of care here will change that. It is
# ENA's md5 over the fastq ENA generates; this file comes out of NCBI's .sra, and the two differ
# in the read header alone -- ENA writes `.../1`, fasterq-dump writes ` length=82`. Measured on
# ERR969493 the sequence and quality streams are byte-identical (md5 efcb48d6 and 6a406749 on
# both), so the science is the same file and the gzip is not. Three gates replace it:
#
#   1. the published md5 of the .sra, checked by fetch_sra.sh before the object is promoted;
#   2. vdb-validate, which is the .sra's own per-column checksums;
#   3. ENA's published read_count and base_count for the run, checked below against what
#      actually came out. Verified exact on a paired ERR and a paired SRR run.
#
# Expect the products to run 6-8% larger than the manifest's bytes for the same reason;
# measured 106.1% on li2019 and 108.4% on korem2015 over the files promoted so far.
set -uo pipefail

ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
DS=${DS:?set DS}
WORKLIST=${WORKLIST:-$ROOT/_convert.$DS.tsv}
CPUS=${SLURM_CPUS_PER_TASK:-8}

LINE=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$WORKLIST")
test -n "$LINE"
IFS=$'\t' read -r RUN SZ MD5 READS BASES RELPATHS <<<"$LINE"

SRA=$ROOT/.sra/$DS/$RUN.sra
WORK=${SLURM_TMPDIR:-$ROOT/.staging}/conv.$DS.$RUN
mkdir -p "$WORK/tmp"
trap 'rm -rf "$WORK"' EXIT

module --force purge >/dev/null 2>&1
module load StdEnv/2023 sra-toolkit/3.0.9 >/dev/null 2>&1

echo "== $DS/$RUN  sra=$SZ expect spots=$READS bases=$BASES"
if ! vdb-validate "$SRA" 2>&1 | grep -q "is consistent"; then
    echo "VDB-VALIDATE FAILED $RUN -- the staged .sra is not intact, deleting it so the"
    echo "login-node fetch re-pulls it; the published md5 is checked there."
    rm -f "$SRA"
    exit 1
fi

fasterq-dump --split-files -e "$CPUS" -m 8G -t "$WORK/tmp" -O "$WORK" "$SRA" || exit 1

# fasterq-dump names its output after the run, so single-end lands <RUN>.fastq and paired
# lands <RUN>_1/_2 -- which is already the manifest's own naming. A third file would mean an
# orphan split this corpus has not shown, and is a mismatch rather than something to guess at.
mapfile -t PRODUCED < <(cd "$WORK" && ls *.fastq 2>/dev/null | sort)
mapfile -t WANT < <(printf '%s\n' "${RELPATHS//;/$'\n'}" | sort)
if [ "${#PRODUCED[@]}" != "${#WANT[@]}" ]; then
    echo "MISMATCH $RUN produced ${#PRODUCED[@]} files (${PRODUCED[*]}) for ${#WANT[@]} manifest rows"
    exit 1
fi

FILE_SPOTS=(); totbases=0
for f in "${PRODUCED[@]}"; do
    n=$(( $(wc -l < "$WORK/$f") / 4 ))
    b=$(awk 'NR%4==2{n+=length($0)} END{print n+0}' "$WORK/$f")
    echo "   $f reads=$n bases=$b"
    FILE_SPOTS+=("$n")
    totbases=$((totbases + b))
done
# A paired run's two mates must report the same spot count -- fasterq-dump interleaving them
# unevenly is exactly the class of silent split this corpus has not shown but the count check
# below could otherwise miss, since it used to keep only the LAST file's count.
spots=${FILE_SPOTS[0]}
for n in "${FILE_SPOTS[@]}"; do
    if [ "$n" != "$spots" ]; then
        echo "MATE MISMATCH $RUN: per-file spot counts disagree (${FILE_SPOTS[*]})"; exit 1
    fi
done
if [ -n "$READS" ] && [ "$spots" != "$READS" ]; then
    echo "COUNT FAIL $RUN spots=$spots vs ENA read_count=$READS"; exit 1
fi
if [ -n "$BASES" ] && [ "$totbases" != "$BASES" ]; then
    echo "COUNT FAIL $RUN bases=$totbases vs ENA base_count=$BASES"; exit 1
fi

pigz -6 -p "$CPUS" "$WORK"/*.fastq || exit 1

for i in "${!WANT[@]}"; do
    src=$WORK/${PRODUCED[$i]}.gz
    dst=$ROOT/$DS/${WANT[$i]}
    mkdir -p "$(dirname "$dst")"
    mv "$src" "$dst" || exit 1
    echo "   -> ${WANT[$i]} $(stat -c%s "$dst")"
done

# Every relpath is verified on disk at this point -- mate counts agreed with each other and
# with ENA's own published counts, and every move above succeeded. The .sra is now a pure
# duplicate of what is already on disk in fastq form, so delete it. $SRA is built at the top
# of this script from $ROOT (a fixed absolute default), $DS (a required, validated arg) and
# $RUN (one field of the worklist line) -- never taken whole from an unchecked variable.
if [ -n "$SRA" ] && [ "$SRA" != "$ROOT/.sra/$DS/.sra" ]; then
    rm -f "$SRA"
fi
echo "ok $DS/$RUN"
