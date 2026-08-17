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

The decoded tables are cached under `cache/`, keyed on the bake they were decoded from --
see `_identity`. A cache keyed on the file merely existing outlived the r7->r8 repin in
five sibling worktrees, each serving r7's ratios to a script that believed it was reading
the current bake.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[4]
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
CACHE = Path(__file__).resolve().parent / "cache"


def _identity() -> str:
    """What bake the decode came from, as a cache key.

    THE SHARED IDENTITY BLOCK IS NOT ENOUGH. `vocab_sha256` is a fact about the node
    space, and a direction re-bake leaves the node space alone -- r8 inherits r7's
    identity byte for byte, by construction. The field that moves is the per-file
    `src_direction_sha256`, which lives under a SEPARATE footer key precisely so
    `assert_same_bake` does not compare it across the trio. Both are in the key, so a
    cache survives neither a new node space nor a new direction table.
    """
    md = pq.read_schema(BAKE / "direction.parquet").metadata or {}
    ident = json.loads(md[b"ecspr_bake"])
    per_file = json.loads(md[b"ecspr_bake_file"])
    return f"{ident['vocab_sha256']}:{per_file['src_direction_sha256']}"


def _fresh(name: str) -> Path | None:
    """The cached decode of `name`, if the bake on disk is the one that produced it."""
    out, stamp = CACHE / name, CACHE / f"{name}.from"
    if out.exists() and stamp.exists() and stamp.read_text().strip() == _identity():
        return out
    return None


def _keep(name: str) -> Path:
    """Stamp a freshly written cache entry with the bake it came from."""
    (CACHE / f"{name}.from").write_text(_identity())
    return CACHE / name


def _vocab() -> dict:
    v = pd.read_parquet(BAKE / "vocab.parquet")
    return {k: g.set_index("code").symbol for k, g in v.groupby("kind")}


def atom_pairs() -> Path:
    """Path to the atom-transfer table in the substrate/product schema."""
    name = "atom_pairs_bake.parquet"
    if (hit := _fresh(name)) is not None:
        return hit
    out = CACHE / name
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
    CACHE.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out)
    return _keep(name)


def direction_ratios() -> Path:
    """Path to `{mnxr: ratio}` with reaction codes resolved to MNXR ids."""
    name = "direction_ratios.parquet"
    if (hit := _fresh(name)) is not None:
        return hit
    sym = _vocab()
    d = pd.read_parquet(BAKE / "direction.parquet")
    d = d.assign(mnxr=d.rxn.map(sym["rxn"]))
    CACHE.mkdir(parents=True, exist_ok=True)
    d[d.mnxr != "EMPTY"][["mnxr", "ratio"]].to_parquet(CACHE / name)
    return _keep(name)
