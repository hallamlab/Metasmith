#!/usr/bin/env bash
# The r10 pricing ladder: six runs, each adding ONE change in a fixed order.
#
# WHY A LADDER AND NOT A CROSS. r10 moves five things at once, and a single before/after
# number would make every one of them a caveat on the others. Each rung below adds exactly
# one mechanism to the rung under it, so every delta belongs to exactly one cause.
#
# ARM 1 IS THE INSTRUMENT'S OWN CHECK and it must come out IDENTICAL. Three flags are needed
# for that and none is a nicety: `--supplementary-crosswalk` is what r9 was baked with,
# `--clamp 100` is what r9 was baked with before canon moved to three decades without a
# re-bake, and `--sigma0 23.489` is r9's fitted value. Held fixed through arms 2-5 so each
# delta is the mechanism alone and arm 6's delta is the constants alone.
#
# The curated table is built once and reused: it is the same table on every rung, and
# rebuilding it costs nine seconds a rung and risks the arms differing by something other
# than their flag.
#
# Run: bash research/fabfos/benchmarks/direction_rescue/ladder.sh <outdir>
set -euo pipefail

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
OUT=${1:?usage: ladder.sh <outdir>}
PY=${LADDER_PYTHON:-python}
MNX=$REPO/data/fabfos/originals/metanetx/4.5
CORR=$REPO/research/fabfos/bake/work/correction.parquet
R9=$REPO/data/fabfos/processed/metabolism_bake/seams/direction_annotation.parquet

mkdir -p "$OUT"
export PYTHONPATH=$REPO/src

CURATED=$OUT/_curated_per_mnxr.parquet
if [ ! -s "$CURATED" ]; then
    $PY -m ecspr.bake.direction.curated \
        --metacyc-reactions "$(find "$REPO/data/fabfos/originals/metacyc" -name reactions.dat | head -1)" \
        --reac-xref "$MNX/reac_xref.tsv" --reac-prop "$MNX/reac_prop.tsv" \
        --chem-xref "$MNX/chem_xref.tsv" --supplementary-crosswalk \
        --out "$CURATED" --out-per-reaction "$OUT/_curated_per_reaction.parquet"
fi

# r9's constants, held through arms 1-5.
R9CONST=(--sigma0 23.489 --clamp 100 --no-widen-suspect)
arm () {
    local n=$1; shift
    echo "=================== arm $n ==================="
    $PY "$REPO/research/fabfos/benchmarks/direction_rescue/reassemble.py" \
        --out "$OUT/arm$n" --curated "$CURATED" --expect "$R9" "$@" \
        2>&1 | tee "$OUT/arm$n.log" | grep -E "^\[reassemble\]|^\[combine\]|^\[check\]|^  " || true
}

arm 1 "${R9CONST[@]}" --balance-gate before_member --prior-width tau  --prior-quantity standard
arm 2 "${R9CONST[@]}" --balance-gate after_member  --prior-width tau  --prior-quantity standard
arm 3 "${R9CONST[@]}" --balance-gate after_member  --prior-width robust --prior-quantity standard
arm 4 "${R9CONST[@]}" --balance-gate after_member  --prior-width robust --prior-quantity standard \
      --quotient "$CORR"
arm 5 "${R9CONST[@]}" --balance-gate after_member  --prior-width robust \
      --prior-quantity physiological --quotient "$CORR"
# Arm 6 drops every held constant: sigma_0, the clamp and the suspect widening all come from
# canon, which is what the bake itself will run with.
arm 6 --balance-gate after_member --prior-width robust \
      --prior-quantity physiological --quotient "$CORR"

$PY "$REPO/research/fabfos/benchmarks/direction_rescue/ladder_table.py" "$OUT"
