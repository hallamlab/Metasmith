"""Decode `data/fabfos/processed/metabolism_bake` into the schema ecspr's loaders read.

**tier4 is retired.** The `data/fabfos/benchmark/reference_tier4` pin is dropped;
nothing here may read it. The bytes are not gone -- the chunk stays reachable from
the last commit that carried the pin -- but the bake is the atom-pair basis now.

The bake stores `(rxn, tail_met, head_met, element, method, source)` as integer vocab
codes, while `ecspr.build.load_pairs` wants the string schema the frozen reference used
(`mnxr/element/substrate/product/sub_idx/prod_idx`). Handing `load_pairs` the encoded
table does not raise -- `df[df.element == "C"]` compares ints to a string and returns
zero rows, so the graph comes back empty rather than wrong-looking. Hence this decode,
in one place, rather than inline per script.

All three parts are read from the SAME bake directory in one call, which is what keeps
the bake-identity invariant (`vocab`/`atom_pairs`/`direction` are one artifact) true by
construction here -- there is no path through this module that mixes two bakes.

The decoded tables are cached under `cache/`; delete that directory after a repin.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
CACHE = Path(__file__).resolve().parent / "cache"


def _vocab() -> dict:
    v = pd.read_parquet(BAKE / "vocab.parquet")
    return {k: g.set_index("code").symbol for k, g in v.groupby("kind")}


def atom_pairs() -> Path:
    """Path to the atom-transfer table in the substrate/product schema."""
    out = CACHE / "atom_pairs_bake.parquet"
    if out.exists():
        return out
    sym = _vocab()
    ap = pd.read_parquet(BAKE / "atom_pairs.parquet")
    df = pd.DataFrame({
        "mnxr": ap.rxn.map(sym["rxn"]),
        "element": ap.element.map(sym["element"]),
        "substrate": ap.tail_met.map(sym["met"]),
        "product": ap.head_met.map(sym["met"]),
        "sub_idx": ap.tail_rank,
        "prod_idx": ap.head_rank,
        "pair_w": ap.pair_w,
        "method": ap.method.map(sym["method"]),
        "source": ap.source.map(sym["source"]),
        "confidence": ap.confidence,
    })
    # MetaNetX's EMPTY sentinel is a real code in the bake, not a null; it must not
    # become a reaction id.
    df = df[(df.mnxr != "EMPTY") & df.substrate.notna() & df["product"].notna()]
    CACHE.mkdir(exist_ok=True)
    df.to_parquet(out)
    return out


def direction_ratios() -> Path:
    """Path to `{mnxr: ratio}` with reaction codes resolved to MNXR ids."""
    out = CACHE / "direction_ratios.parquet"
    if out.exists():
        return out
    sym = _vocab()
    d = pd.read_parquet(BAKE / "direction.parquet")
    d = d.assign(mnxr=d.rxn.map(sym["rxn"]))
    CACHE.mkdir(exist_ok=True)
    d[d.mnxr != "EMPTY"][["mnxr", "ratio"]].to_parquet(out)
    return out
