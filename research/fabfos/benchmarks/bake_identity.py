# Which bake a derived table came from, recorded beside it.
#
# Every study here decodes `data/fabfos/processed/metabolism_bake` into its own cache and
# then reads that cache for the rest of the campaign. A cache keyed on the file merely
# existing outlived the r7->r8 repin in five sibling worktrees, each serving r7's ratios to
# a script that believed it was reading the current bake. The failure is silent by
# construction: the consumer gets a plausible table and no error.
#
# THE SHARED IDENTITY BLOCK IS NOT ENOUGH. `vocab_sha256` is a fact about the node space,
# and a direction-only re-bake leaves the node space alone -- r8 inherits r7's identity byte
# for byte. The field that moves is the per-file `src_direction_sha256`, which lives under a
# SEPARATE footer key precisely so `assert_same_bake` does not compare it across the trio.
# The stamp pairs them, so a cache survives neither a new node space nor a new direction
# table.
#
# Every function takes the bake directory, so an instrument can be aimed at a chunk staged
# beside the deployed one. `research/fabfos/benchmarks/direction_rescue/REBAKE.md` explains
# why that matters: a suffixed chunk is read by nothing, so verifying at the deployed path
# before the promote proves only that the old bake still works.
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
DEPLOYED = ROOT / "data/fabfos/processed/metabolism_bake"


def identity(bake: Path = DEPLOYED) -> str:
    md = pq.read_schema(Path(bake) / "direction.parquet").metadata or {}
    ident = json.loads(md[b"ecspr_bake"])
    per_file = json.loads(md[b"ecspr_bake_file"])
    return f"{ident['vocab_sha256']}:{per_file['src_direction_sha256']}"


def stamp_path(out: Path) -> Path:
    return Path(out).with_name(Path(out).name + ".from")


def stamp_of(out: Path) -> str | None:
    p = stamp_path(out)
    return p.read_text().strip() if p.exists() else None


def fresh(out: Path, bake: Path = DEPLOYED) -> Path | None:
    out = Path(out)
    return out if out.exists() and stamp_of(out) == identity(bake) else None


def keep(out: Path, bake: Path = DEPLOYED) -> Path:
    stamp_path(out).write_text(identity(bake))
    return Path(out)


def require_fresh(out: Path, bake: Path = DEPLOYED) -> Path:
    out = Path(out)
    if not out.exists():
        raise SystemExit(f"[bake_identity] {out} does not exist; build it first")
    have, want = stamp_of(out), identity(bake)
    if have is None:
        raise SystemExit(f"[bake_identity] {out} carries no bake stamp -- rebuild it "
                         f"through bake_identity so it records one")
    if have != want:
        raise SystemExit(f"[bake_identity] {out} was decoded from {have}\n"
                         f"                 {bake} is {want}\n"
                         f"                 delete the cache and rebuild")
    return out


def _vocab(bake: Path) -> dict:
    v = pd.read_parquet(Path(bake) / "vocab.parquet")
    return {k: g.set_index("code").symbol for k, g in v.groupby("kind")}


def decode_atom_pairs(bake: Path = DEPLOYED) -> pd.DataFrame:
    sym = _vocab(bake)
    ap = pd.read_parquet(Path(bake) / "atom_pairs.parquet")
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
    return df[(df.mnxr != "EMPTY") & df.substrate.notna() & df["product"].notna()]


def build_atom_pairs(out: Path, bake: Path = DEPLOYED) -> Path:
    out, bake = Path(out), Path(bake)
    if (hit := fresh(out, bake)) is not None:
        return hit
    out.parent.mkdir(parents=True, exist_ok=True)
    decode_atom_pairs(bake).to_parquet(out)
    return keep(out, bake)


def decode_direction_ratios(bake: Path = DEPLOYED) -> pd.DataFrame:
    bake = Path(bake)
    d = pd.read_parquet(bake / "direction.parquet")
    v = pd.read_parquet(bake / "vocab.parquet")
    rv = v[v.kind == "rxn"][["code", "symbol"]].rename(
        columns={"code": "rxn", "symbol": "mnxr"})
    d = d.merge(rv, on="rxn", how="inner")
    return d[d.mnxr != "EMPTY"][["mnxr", "ratio"]]


def build_direction_ratios(out: Path, bake: Path = DEPLOYED) -> Path:
    out, bake = Path(out), Path(bake)
    if (hit := fresh(out, bake)) is not None:
        return hit
    out.parent.mkdir(parents=True, exist_ok=True)
    decode_direction_ratios(bake).to_parquet(out, index=False)
    return keep(out, bake)
