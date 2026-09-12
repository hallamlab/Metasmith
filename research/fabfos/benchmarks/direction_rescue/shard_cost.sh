#!/usr/bin/env bash
# Run the eQuilibrator member over one crc32 shard, before and after the water fix.
#
# WHAT THIS MEASURES AND WHY IT IS A SHARD. crc32 sharding makes a shard a uniform sample
# of the universe, so a reason histogram taken on one extrapolates and a wall clock taken
# on one prices the whole lane. The deferred re-bake needs both: the deployed lane declares
# twelve hours on no measurement at all (r7's step log shows about 35 minutes), and it is
# about to stop short-circuiting on ~14,000 reactions it used to refuse for free.
#
# THE CACHE PRESENTATION IS THE LANE'S, NOT AN APPROXIMATION OF IT.
# `equilibrator_cache.zenodo.get_cached_filepath` resolves through
# `pooch.os_cache("equilibrator")` -- i.e. `$XDG_CACHE_HOME/equilibrator/<file>` -- and
# reads nothing else. `EQUILIBRATOR_CACHE_DIR` is read by no part of pooch,
# equilibrator_cache or component_contribution; exporting it is a no-op and a trap.
# Symlinks rather than copies: compounds.sqlite is 1.34 GB.
#
# THE BEFORE ARM RUNS THE OLD LOADER, not a flag. `$WORK/prefix_src` is a copy of
# `src/ecspr` with `direction/refdata.py` taken from the last commit before the fix, so the
# two arms differ in exactly the one function under test and in nothing else.
#
# THE REF IS PINNED AND THEN CHECKED, because `HEAD~1` is not a fact about the fix -- it
# moves every time anything else lands, and the first run of this script measured the FIXED
# loader against itself and reported it as a before/after. The extracted file must still
# carry the namespace guard, and that is asserted below rather than assumed.
#
# Usage: research/fabfos/benchmarks/direction_rescue/shard_cost.sh <work-dir> [shard-spec]
set -euo pipefail

WORK="${1:?usage: shard_cost.sh <work-dir> [i/n]}"
SHARD="${2:-0/20}"
PREFIX_REF="${PREFIX_REF:-f26fe10}"   # last commit before `T1: water was never a compound here`
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
MNX="$REPO/data/fabfos/originals/metanetx/4.5"
EQD="$REPO/data/fabfos/originals/equilibrator/0.7.1"
ENVPY=/home/tony/lib/miniforge3/envs/build-refs-equilibrator/bin/python

mkdir -p "$WORK/_eqcache/equilibrator"
for f in compounds.sqlite cc_params.npz; do
    [ -s "$EQD/$f" ] || { echo "the pinned cache at $EQD is missing $f" >&2; exit 1; }
    ln -sfn "$EQD/$f" "$WORK/_eqcache/equilibrator/$f"
done
export XDG_CACHE_HOME="$WORK/_eqcache"
export OMP_NUM_THREADS=1

if [ ! -s "$WORK/universe.json" ]; then
    PYTHONPATH="$REPO/src" "$ENVPY" -m ecspr.bake.direction.drive universe \
        --reac-prop "$MNX/reac_prop.tsv" --out "$WORK/universe.json"
fi

# The BEFORE tree: this scope's src with refdata.py rolled back to the pinned ref.
if [ ! -d "$WORK/prefix_src" ]; then
    mkdir -p "$WORK/prefix_src"
    cp -r "$REPO/src/ecspr" "$WORK/prefix_src/"
    git -C "$REPO" show "$PREFIX_REF:src/ecspr/bake/direction/refdata.py" \
        > "$WORK/prefix_src/ecspr/bake/direction/refdata.py"
fi
grep -q 'startswith("MNXM")' "$WORK/prefix_src/ecspr/bake/direction/refdata.py" || {
    echo "the tree at $PREFIX_REF does not carry the MNXM namespace guard, so the" >&2
    echo "'before' arm would measure the fixed loader against itself." >&2; exit 1; }

run () {  # run <label> <pythonpath>
    local label="$1" pp="$2"
    echo "=== $label · shard $SHARD ==="
    PYTHONPATH="$pp" /usr/bin/time -v "$ENVPY" \
        -m ecspr.bake.direction.drive eval --member eq --require \
        --universe "$WORK/universe.json" \
        --reac-prop "$MNX/reac_prop.tsv" --chem-prop "$MNX/chem_prop.tsv" \
        --shard "$SHARD" --out "$WORK/eq_${label}.parquet" \
        2>&1 | tee "$WORK/eq_${label}.log" | grep -E \
        "^\[eval|^(no_props|unresolved|uninformative|ok|error)|Elapsed \(wall|Maximum resident"
}

# SEQUENTIALLY, not in parallel: each process re-hashes the 1.34 GB cache through pooch on
# construction, so two at once measures the disk.
run before "$WORK/prefix_src"
run after  "$REPO/src"
