#!/usr/bin/env python3
"""Orientation gate for the v1/v2 bake mix, plus the run manifest's identity block.

`vocab` / `atom_pairs` / `direction` are ONE artifact. This benchmark is forced to
mix versions: the answer key was built on bake **v1**
(`data/benchmark/reference_tier4/atom_pairs_tier4.parquet`), and the only
`direction` table on disk is bake **v2** (`data/processed/metabolism_bake/`).

The hazard is not vocabulary drift, which would be loud -- it is **orientation**.
A direction ratio above 1 reverses an edge, so if v1 and v2 disagree about which
side of a reaction is the substrate, the directed model inverts silently and
nothing downstream can tell. This gate decodes both tables to molecule-level
triples and counts v1 pairs whose reverse, not whose forward form, appears in v2.

Writes vs_gem/out/bake_manifest.json. Non-zero exit if the gate fails.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402

MAX_REVERSED_FRACTION = 0.01   # 1% of the shared-reaction v1-only pairs


def sha256(path: Path, chunk: int = 1 << 22) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while (b := fh.read(chunk)):
            h.update(b)
    return h.hexdigest()


def frozen_sha() -> str | None:
    txt = C.TIER4_FREEZE.read_text()
    m = re.search(r"atom_pairs_tier4\.parquet\s*\n\s*sha256\s+([0-9a-f]{64})", txt)
    return m.group(1) if m else None


def decode_v2() -> pd.DataFrame:
    vocab = pd.read_parquet(C.BAKE / "vocab.parquet")
    mol = vocab[vocab.kind == "met"].set_index("code").symbol.to_dict()
    rxn = vocab[vocab.kind == "rxn"].set_index("code").symbol.to_dict()
    ele = vocab[vocab.kind == "element"].set_index("code").symbol.to_dict()
    ap = pd.read_parquet(C.BAKE / "atom_pairs.parquet",
                         columns=["element", "rxn", "tail_met", "head_met"])
    ap = ap.drop_duplicates()
    out = pd.DataFrame({
        "mnxr": ap.rxn.map(rxn),
        "element": ap.element.map(ele),
        "substrate": ap.tail_met.map(mol),
        "product": ap.head_met.map(mol),
    }).dropna()
    return out[out.mnxr != "EMPTY"].drop_duplicates()


def decode_v1() -> pd.DataFrame:
    v1 = pd.read_parquet(C.ATOM_PAIRS,
                         columns=["mnxr", "element", "substrate", "product"])
    return v1.drop_duplicates()


def direction_ratios() -> dict:
    """metabolism_bake direction.parquet joined through vocab onto MNXR -- the
    same shape examples/scadc_ecspr_t1_refs.py builds it in."""
    vocab = pd.read_parquet(C.BAKE / "vocab.parquet")
    rxn = vocab[vocab.kind == "rxn"][["code", "symbol"]].rename(
        columns={"code": "rxn", "symbol": "mnxr"})
    d = pd.read_parquet(C.BAKE / "direction.parquet").merge(rxn, on="rxn", how="inner")
    d = d[d.mnxr != "EMPTY"]
    return dict(zip(d.mnxr, d.ratio.astype(float)))


def main() -> int:
    v1, v2 = decode_v1(), decode_v2()
    shared = set(v1.mnxr.unique()) & set(v2.mnxr.unique())
    a = v1[v1.mnxr.isin(shared)]
    b = v2[v2.mnxr.isin(shared)]

    fwd = set(map(tuple, b.itertuples(index=False, name=None)))
    rev = {(r, e, p, s) for (r, e, s, p) in fwd}

    a_t = list(map(tuple, a.itertuples(index=False, name=None)))
    v1_only = [t for t in a_t if t not in fwd]
    reversed_in_v2 = [t for t in v1_only if t in rev]

    frac = len(reversed_in_v2) / max(1, len(v1_only))
    ratios = direction_ratios()

    man = {
        "atom_pairs_v1": str(C.ATOM_PAIRS.relative_to(C.ROOT)),
        "atom_pairs_v1_sha256": sha256(C.ATOM_PAIRS),
        "atom_pairs_v1_sha256_frozen": frozen_sha(),
        "bake_v2_dir": str(C.BAKE.relative_to(C.ROOT)),
        "n_shared_reactions": len(shared),
        "n_v1_pairs_shared_rxn": len(a_t),
        "n_v1_only_pairs": len(v1_only),
        "n_reversed_in_v2": len(reversed_in_v2),
        "reversed_fraction": frac,
        "max_reversed_fraction": MAX_REVERSED_FRACTION,
        "n_direction_ratios": len(ratios),
        "n_ratio_gt_1": int(sum(1 for v in ratios.values() if v > 1)),
    }
    (C.OUT / "bake_manifest.json").write_text(json.dumps(man, indent=1))
    print(json.dumps(man, indent=1))

    ok = True
    if man["atom_pairs_v1_sha256_frozen"] and \
            man["atom_pairs_v1_sha256"] != man["atom_pairs_v1_sha256_frozen"]:
        print("FAIL: tier4 atom_pairs sha256 does not match TIER4_FREEZE.md")
        ok = False
    if frac > MAX_REVERSED_FRACTION:
        print(f"FAIL: {len(reversed_in_v2)} of {len(v1_only)} v1-only pairs "
              f"({frac:.4%}) appear reversed in v2 -- the direction table cannot "
              f"be trusted against these pairs")
        ok = False
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
