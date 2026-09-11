#!/bin/bash
#SBATCH --job-name=gtdbtk_denovo
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=32
#SBATCH --mem=240G
#SBATCH --time=48:00:00
#SBATCH --output=/scratch/phyberos/gtdbtk_denovo/_logs/%x.%j.out
# The exact invocation gtdbtk_de_novo.py builds, run here because the tree it infers
# contains the whole GTDB reference set and will not fit on a workstation. What this
# checks is the invocation, the outgroup names and the staged reference -- not the
# transform's own staging and globbing, which is thin and runs locally.
#
# Both domains, into one output directory, because iPHoP's add_to_db globs for
# gtdbtk.{ar,bac}[0-9]*.decorated.tree under a single --gtdb_dir. A domain with no
# genomes exits non-zero and that is expected, hence `|| true` per domain; success is
# whether a decorated tree exists.
#
# The outgroups are r232 spellings. GTDB renamed its phyla to the -ota suffix, so the
# paper's p__Cyanobacteria and p__Altarchaeota do not resolve against this release and
# de_novo_wf exits on an unknown outgroup.
#
# The reference was extracted without its ANI screening directory, which de_novo_wf does
# not use. `gtdbtk check_install` fails because of that absence; it is expected here and
# is not evidence of a bad extraction.
set -eu

ROOT=${GTDBTK_ROOT:-/scratch/phyberos/gtdbtk_denovo}
REF=${GTDBTK_REF:-/scratch/phyberos/staging/gtdb/release232}
SIF=$ROOT/sif/gtdbtk_2.6.1.sif
GENOMES=$ROOT/genomes
OUT=$ROOT/out

mkdir -p "$ROOT/_logs" "$ROOT/sif" "$GENOMES" "$OUT"
export APPTAINER_CACHEDIR=$ROOT/.apptainer_cache
module load apptainer/1.3.5

if [ ! -s "$SIF" ]; then
    apptainer pull "$SIF" docker://quay.io/biocontainers/gtdbtk:2.6.1--pyh1f0d9b5_2
fi

test -n "$(ls -A "$GENOMES")" || { echo "no genomes in $GENOMES"; exit 1; }

run_domain() {
    # --cleanenv, because this cluster's login environment leaks into the container:
    # SSL_CERT_FILE and CURL_CA_BUNDLE name a RHEL path that does not exist in a Debian
    # image, and PYTHONPATH names a tree the container's interpreter must not import.
    apptainer exec --cleanenv --bind "$ROOT":"$ROOT" --bind "$REF":/ref \
        --env GTDBTK_DATA_PATH=/ref "$SIF" \
        gtdbtk de_novo_wf --genome_dir "$GENOMES" --"$1" \
            --outgroup_taxon "$2" \
            --extension fna --cpus 32 \
            --force --out_dir "$OUT" || true
}
run_domain bacteria p__Cyanobacteriota
run_domain archaea  p__Altiarchaeota

ls -la "$OUT"
find "$OUT" -name "gtdbtk.*.decorated.tree*" | sed 's/^/tree: /'
echo "GTDBTK DE NOVO DONE $(date -Is)"
