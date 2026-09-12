#!/bin/bash
#SBATCH --job-name=iphop_unpack
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=8:00:00
#SBATCH --output=/scratch/phyberos/viromics_refs/_logs/%x.%j.out
# Runs after the chunk array. `cat | tar` skips the 170 GB intermediate tarball that
# `iphop download` would write, so peak disk is the chunks plus the unpacked tree rather
# than three copies. The chunks go once the tree is out.
set -euo pipefail

ROOT=${IPHOP_ROOT:-/scratch/phyberos/viromics_refs}
DB=$ROOT/iphop_db
SIF=$ROOT/sif/iphop_1.3.3.sif
DBV=${DBV:-iPHoP_db_Aug23_rw}

cd "$DB"
module load apptainer/1.3.5
export APPTAINER_CACHEDIR=$ROOT/.apptainer_cache

CHUNKS=$(ls "$DBV".tar.gz.[0-9][0-9] | sort)
echo "$CHUNKS" | wc -l
md5sum -c <(grep -F -f <(echo "$CHUNKS") "$DBV.tar.gz.md5")

df -h "$ROOT" | tail -1
# shellcheck disable=SC2086
cat $CHUNKS | tar xzf -
rm -f $CHUNKS
ls -la

FULL=$(dirname "$(find "$DB" -maxdepth 3 -name db_infos -type d | head -1)")
if [ ! -s "$SIF" ]; then
    apptainer pull "$SIF" docker://quay.io/biocontainers/iphop:1.3.3--pyhdfd78af_0
fi
# --cleanenv, or this cluster's SSL_CERT_FILE points the container's TLS at a path that
# does not exist inside it.
apptainer exec --cleanenv --bind "$ROOT":"$ROOT" "$SIF" \
    iphop download --db_dir "$FULL" --full_verify

echo "IPHOP DB DONE $(date -Is) at $FULL"
