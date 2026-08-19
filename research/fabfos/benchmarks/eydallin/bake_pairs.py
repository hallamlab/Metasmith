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

The decoded tables are cached under `cache/`, and a cache older than the bake part it
decodes is rebuilt rather than served. That check is not a nicety: a repin rewrites
`direction.parquet` in place under an unchanged directory name, so a cache keyed only by
name goes on answering with the SUPERSEDED ratios, and nothing downstream can tell --
the figure still draws, the numbers are still plausible, and they are the old bake's.
That happened here: a cache from before the direction repin disagreed with the pinned
table on 47% of reactions.

`FABFOS_BAKE` points the decode at a different bake directory, for comparing a re-baked
reference against the pinned one. It moves all three parts together, so the identity
invariant above still holds by construction, and it keys the cache by the bake's
directory name -- serving one bake's ratios against another's pairs is exactly the mix
this module exists to prevent, and a fixed cache name would do it silently.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
BAKE = Path(os.environ.get("FABFOS_BAKE") or DEFAULT_BAKE).resolve()
CACHE = Path(__file__).resolve().parent / "cache"


def tag() -> str:
    """Filename suffix naming the bake in use; empty for the pinned one.

    Callers append it to their own outputs so two bakes' figures and solve caches coexist
    instead of overwriting each other.
    """
    return "" if BAKE == DEFAULT_BAKE.resolve() else f"_{BAKE.name}"


def _fresh(cached: Path, *sources: Path) -> bool:
    """Is `cached` usable -- present, and no older than every bake part it decodes?"""
    if not cached.exists():
        return False
    return cached.stat().st_mtime >= max(s.stat().st_mtime for s in sources)


def _vocab() -> dict:
    v = pd.read_parquet(BAKE / "vocab.parquet")
    return {k: g.set_index("code").symbol for k, g in v.groupby("kind")}


def atom_pairs() -> Path:
    """Path to the atom-transfer table in the substrate/product schema."""
    out = CACHE / f"atom_pairs_bake{tag()}.parquet"
    if _fresh(out, BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet"):
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
    out = CACHE / f"direction_ratios{tag()}.parquet"
    if _fresh(out, BAKE / "vocab.parquet", BAKE / "direction.parquet"):
        return out
    sym = _vocab()
    d = pd.read_parquet(BAKE / "direction.parquet")
    d = d.assign(mnxr=d.rxn.map(sym["rxn"]))
    CACHE.mkdir(exist_ok=True)
    d[d.mnxr != "EMPTY"][["mnxr", "ratio"]].to_parquet(out)
    return out
