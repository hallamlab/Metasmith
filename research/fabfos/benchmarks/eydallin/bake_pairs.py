"""Decode `data/fabfos/processed/metabolism_bake` into the schema ecspr's loaders read.

**tier4 is retired.** The `data/fabfos/benchmark/reference_tier4` pin is dropped;
nothing here may read it. The bytes are not gone -- the chunk stays reachable from
the last commit that carried the pin -- but the bake is the atom-pair basis now.

The bake stores `(rxn, tail_met, head_met, element, method, source)` as integer vocab
codes, while `ecspr.model.build.load_pairs` wants the string schema the frozen reference used
(`mnxr/element/substrate/product/sub_idx/prod_idx`). Handing `load_pairs` the encoded
table does not raise -- `df[df.element == "C"]` compares ints to a string and returns
zero rows, so the graph comes back empty rather than wrong-looking. Hence this decode,
in one place, rather than inline per script.

All three parts are read from the SAME bake directory in one call, which is what keeps
the bake-identity invariant (`vocab`/`atom_pairs`/`direction` are one artifact) true by
construction here -- there is no path through this module that mixes two bakes.

The decoded tables are cached under `cache/`, keyed on the bake they were decoded from.
That key, and the reason the shared identity block alone is not enough for it, live in
`bake_identity` one directory up -- every study in this tree shares them.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_identity                                                          # noqa: E402

ROOT = Path(__file__).resolve().parents[4]
BAKE = bake_identity.DEPLOYED
CACHE = Path(__file__).resolve().parent / "cache"


def atom_pairs() -> Path:
    """Path to the atom-transfer table in the substrate/product schema."""
    return bake_identity.build_atom_pairs(CACHE / "atom_pairs_bake.parquet", BAKE)


def direction_ratios() -> Path:
    """Path to `{mnxr: ratio}` with reaction codes resolved to MNXR ids."""
    return bake_identity.build_direction_ratios(CACHE / "direction_ratios.parquet", BAKE)
