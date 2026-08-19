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
    df = df[(df.mnxr != "EMPTY") & df.substrate.notna() & df["product"].notna()]
    CACHE.mkdir(exist_ok=True)
    df.to_parquet(out)
    return out


def direction_ratios() -> Path:
    out = CACHE / "direction_ratios.parquet"
    if out.exists():
        return out
    sym = _vocab()
    d = pd.read_parquet(BAKE / "direction.parquet")
    d = d.assign(mnxr=d.rxn.map(sym["rxn"]))
    CACHE.mkdir(exist_ok=True)
    d[d.mnxr != "EMPTY"][["mnxr", "ratio"]].to_parquet(out)
    return out
