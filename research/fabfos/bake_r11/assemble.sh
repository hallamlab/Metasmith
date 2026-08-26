#!/usr/bin/env bash
# r11, assembled by hand from the deployed caches -- because the pipeline cannot run.
#
# WHY THIS EXISTS. `aam_redox` fails every run with `MissingInstanceError` on the
# `interm::aam_stack` product. The cause is in the engine and is now fixed and filed
# (`bootstrap.resolve_step_inputs`, commit c8e460e), but the agent runs a PINNED image
# keyed on the engine build hash, so taking the fix into a run means rebuilding and
# pushing a container. r11's payload does not need one.
#
# WHY IT IS SOUND TO DO THIS AT ALL. Everything r11 changes is downstream of the mappers
# and upstream of nothing that was not already on this disk. `atom_pairs extract` is
# arithmetic over a cache of mapped SMILES -- it runs no mapper -- so the whole payload
# is a re-extraction and a re-fusion of tables the deployed chunk already carries. There
# is precedent: `research/fabfos/benchmarks/direction_rescue/reassemble.py` reproduces the
# deployed direction annotation frame-identically the same way.
#
# WHAT IS NOT REPRODUCED. The provenance. A chunk built here is pinned by the commit that
# carries this script, not by a run's lineage, and the journal says so. The member
# TABLES are rebuilt but the member CACHES are not touched, so the next generation stages
# exactly what this one did.
#
#   PYTHONPATH=src mamba run -n rdkit-scratch bash research/fabfos/bake_r11/assemble.sh
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO"

MNX="$REPO/data/fabfos/originals/metanetx/4.5"
MC="$REPO/data/fabfos/originals/metacyc/26/data"
DEPLOYED="$REPO/data/fabfos/processed/metabolism_bake"
LOOK="$REPO/data/fabfos/processed/lookups"
SEAMS="$DEPLOYED/seams"
OUT="${OUT:-$REPO/data/fabfos/scratch/r11}"
PY="PYTHONPATH=$REPO/src OMP_NUM_THREADS=1 python3"

CURATED_DAT="atom-mappings-smiles.dat"
[ -s "$MC/$CURATED_DAT" ] || { echo "no $CURATED_DAT under $MC" >&2; exit 1; }

mkdir -p "$OUT"/{L1,L2,L3,L5,members,redox,ref,seams}
cd "$OUT"

echo "=== L1: the curated map ==="
eval $PY -m ecspr.bake.aam.metacyc_member \
    --smiles-dat "$MC/$CURATED_DAT" --reac-xref "$MNX/reac_xref.tsv" \
    --out L1/metacyc.tsv --out-report L1/refused_residues.tsv
eval $PY -m ecspr.bake.atom_pairs extract \
    --aam L1/metacyc.tsv --align structural --connectivity-fallback \
    --reac-prop "$MNX/reac_prop.tsv" --chem-prop "$MNX/chem_prop.tsv" \
    --out L1/pairs.parquet --out-status L1/status.tsv

# The three members. `cache.tsv` is the merged member table under another name -- same
# four columns, cumulative, latest row winning -- so extracting from it is what the lane
# does after its shards merge, minus the inference.
R="$SEAMS/aam_rescue"
for M in indigo rxnmapper localmapper; do
    echo "=== member $M: re-extract over the deployed cache ==="
    eval $PY -m ecspr.bake.atom_pairs extract \
        --aam "$DEPLOYED/aam_cache/$M/cache.tsv" --align strict --fallback-forced \
        --universe "$SEAMS/aam_universe.parquet" \
        --resolved "$R/crosswalk.tsv" --placeholders "$R/placeholders.tsv" \
        --balance "$R/balance.tsv" \
        --reac-prop "$MNX/reac_prop.tsv" --chem-prop "$MNX/chem_prop.tsv" \
        --out "members/aam_member_$M.parquet" --out-status "members/${M}_status.tsv"
done

MEMBERS="--member rxnmapper=members/aam_member_rxnmapper.parquet \
         --member localmapper=members/aam_member_localmapper.parquet \
         --member indigo=members/aam_member_indigo.parquet"

echo "=== L2 / L3 / L5: one pass, three submission classes ==="
for pair in whole:L2 completed:L3 reduced:L5; do
    CLASS="${pair%%:*}"; DIR="${pair##*:}"
    eval $PY -m ecspr.bake.aam.layers fuse --submission-class "$CLASS" \
        $MEMBERS --reac-prop "$MNX/reac_prop.tsv" --out "$DIR/stack.parquet"
done

echo "=== the stack ==="
eval $PY -m ecspr.bake.aam.layers stack \
    --layer metacyc=L1/pairs.parquet,curated,1.0 \
    --layer ensemble=L2/stack.parquet \
    --layer curation=L3/stack.parquet \
    --layer algebra="$SEAMS/aam_algebra/forced_pairs.parquet",conservation_algebra,1.0 \
    --layer partial="$SEAMS/aam_partial/partial_forced.parquet",partial_forced,1.0 \
    --layer partial=L5/stack.parquet,partial_reduced,1.0 \
    --out aam_stack.parquet

eval $PY -m ecspr.bake.aam.layers audit \
    --pairs aam_stack.parquet --reac-prop "$MNX/reac_prop.tsv" --out injectivity.tsv

echo "=== redox repair ==="
mkdir -p _lookups
ln -sfn "$LOOK/reactions.parquet"   _lookups/reactions.parquet
ln -sfn "$LOOK/metabolites.parquet" _lookups/metabolites.parquet
ln -sfn "$LOOK/atom_ranks.parquet"  _lookups/atom_ranks.parquet
eval $PY -m ecspr.bake.aam.redox repair \
    --pairs aam_stack.parquet --lookups _lookups \
    --out redox/aam_pairs.parquet \
    --out-refusals redox/refusals.parquet \
    --out-cofactors redox/cofactors.tsv \
    --out-emptied redox/emptied.txt \
    --out-summary redox/summary.tsv

echo "=== close the ledger ==="
eval $PY -m ecspr.bake.aam.worklist close \
    --worklist "$SEAMS/aam_worklist.parquet" --pairs redox/aam_pairs.parquet \
    --rescued "$R/rescued.parquet" \
    --forecast "$SEAMS/aam_forecast.parquet" \
    --redox-emptied redox/emptied.txt \
    --out ref/ledger.parquet --out-summary ref/ledger_summary.tsv

echo "=== the bake: vocabulary + encoded pairs ==="
eval $PY -m ecspr.bake.metabolism pairs \
    --aam-pairs redox/aam_pairs.parquet --reactions "$LOOK/reactions.parquet" \
    --out-vocab vocab.parquet --out-pairs atom_pairs.parquet

# MANDATORY even though no direction science changes. The identity block travels with
# the atom pairs, so `direction.parquet` must be re-encoded against the vocabulary minted
# above or `assert_same_bake` refuses the trio. This is NOT the r9/r10 case, where the
# atom identity was inherited byte for byte and the direction lane was the only thing
# that moved.
echo "=== re-encode the direction lane against the new vocabulary ==="
eval $PY -m ecspr.bake.metabolism direction \
    --direction "$SEAMS/direction_annotation.parquet" \
    --vocab vocab.parquet \
    --out direction.parquet

echo
echo "=== r11 assembled at $OUT ==="
ls -la "$OUT"/*.parquet
