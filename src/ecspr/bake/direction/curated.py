from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .metacyc_flatfile import load_reactions
from .refdata import (
    load_source_to_mnxr,
    load_mnxr_sides,
    load_metacyc_compound_to_mnxm,
    parse_col3_sides,
)

FLIP = {
    "LEFT-TO-RIGHT": "RIGHT-TO-LEFT",
    "RIGHT-TO-LEFT": "LEFT-TO-RIGHT",
    "PHYSIOL-LEFT-TO-RIGHT": "PHYSIOL-RIGHT-TO-LEFT",
    "PHYSIOL-RIGHT-TO-LEFT": "PHYSIOL-LEFT-TO-RIGHT",
    "REVERSIBLE": "REVERSIBLE",
}


def flip_verdict(left_ids, right_ids, sides_lr, cmap):
    if sides_lr is None:
        return None, 0, 0
    xl, xr = sides_lr
    ml = {cmap[c] for c in left_ids if c in cmap}
    mr = {cmap[c] for c in right_ids if c in cmap}
    agree = len(ml & xl) + len(mr & xr)
    flip = len(ml & xr) + len(mr & xl)
    if agree + flip == 0 or agree == flip:
        return None, agree, flip
    return (flip > agree), agree, flip


def align_one(direction, left_ids, right_ids, sides_lr, cmap):
    if not direction:
        return None, "no_direction"
    if direction not in FLIP:
        raise ValueError(f"unknown REACTION-DIRECTION {direction!r}")
    if sides_lr is None:
        return None, "no_mnxr_sides"
    flipped, agree, flip = flip_verdict(left_ids, right_ids, sides_lr, cmap)
    if flipped is None:
        return None, ("no_overlap" if agree + flip == 0 else "tie")
    return (FLIP[direction] if flipped else direction), ("flipped" if flipped else "same")


def read_source(reactions_dat: Path, source: str, id2mnxr, sides, cmap, col3):
    rows = []
    for rec in load_reactions(Path(reactions_dat)):
        d = rec["direction"]
        if not d:
            continue
        key = rec["unique_id"]
        mnxr = id2mnxr.get(key)
        if mnxr is None:
            continue
        left, right = rec["left"], rec["right"]
        sides_lr = sides.get(mnxr)
        aligned, reason = align_one(d, left, right, sides_lr, cmap)
        c3 = col3.get(key)
        c3_agree = None
        if c3 is not None and reason in ("same", "flipped"):
            _, c3l, c3r = c3
            c3_flip, ca, cf = flip_verdict(c3l, c3r, sides_lr, cmap)
            if c3_flip is not None:
                c3_agree = bool(c3_flip == (reason == "flipped"))
        rows.append(dict(source=source, mc_id=key, mnxr=mnxr, raw_direction=d,
                         aligned=aligned, reason=reason, col3_agree=c3_agree))
    return pd.DataFrame(rows)


def collapse_to_mnxr(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for mnxr, g in df.groupby("mnxr", sort=False):
        decided = g[g["aligned"].notna()]
        srcs = tuple(sorted(g["source"].unique()))
        source = "both" if len(srcs) > 1 else srcs[0]
        if decided.empty:
            reason = g["reason"].mode().iat[0]
            out.append(dict(mnxr=mnxr, aligned=None, source=source,
                            n_reactions=len(g), n_decided=0, reason=reason))
            continue
        vals = set(decided["aligned"])
        if len(vals) == 1:
            aligned = next(iter(vals))
            reason = "agreed"
        else:
            aligned = None
            source = "disagree"
            reason = "conflict:" + "|".join(sorted(vals))
        out.append(dict(mnxr=mnxr, aligned=aligned, source=source,
                        n_reactions=len(g), n_decided=len(decided), reason=reason))
    return pd.DataFrame(out)


def build(metacyc_reactions, reac_xref, reac_prop, chem_xref):
    id2mnxr = load_source_to_mnxr(reac_xref, "metacyc.reaction")
    sides = load_mnxr_sides(reac_prop)
    cmap = load_metacyc_compound_to_mnxm(chem_xref)
    col3 = parse_col3_sides(reac_xref)
    per_rxn = read_source(metacyc_reactions, "metacyc", id2mnxr, sides, cmap, col3)
    decided = per_rxn["reason"].isin(("same", "flipped")).mean()
    assert decided > 0.5, (
        f"curated: only {decided:.1%} of source reactions decided -- the MNXR "
        f"crosswalk or compound map is likely broken, not the data")
    per_mnxr = collapse_to_mnxr(per_rxn)
    return per_rxn, per_mnxr


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--metacyc-reactions", required=True,
                    help="the licensed drop-in's reactions.dat")
    ap.add_argument("--reac-xref", required=True)
    ap.add_argument("--reac-prop", required=True)
    ap.add_argument("--chem-xref", required=True)
    ap.add_argument("--out", required=True, help="per-MNXR parquet")
    ap.add_argument("--out-per-reaction", default=None)
    a = ap.parse_args(argv)
    per_rxn, per_mnxr = build(Path(a.metacyc_reactions), Path(a.reac_xref),
                              Path(a.reac_prop), Path(a.chem_xref))
    per_mnxr.to_parquet(a.out, index=False)
    if a.out_per_reaction:
        per_rxn.to_parquet(a.out_per_reaction, index=False)
    dec = per_rxn["reason"].value_counts()
    print(f"[curated] {len(per_rxn):,} source reactions -> {len(per_mnxr):,} MNXRs")
    print(f"[curated] per-reaction decidability:\n{dec.to_string()}")
    flipped = int((per_rxn['reason'] == 'flipped').sum())
    same = int((per_rxn['reason'] == 'same').sum())
    if same + flipped:
        print(f"[curated] flip rate among decided: {flipped/(same+flipped):.1%} "
              f"({flipped:,}/{same+flipped:,})")


if __name__ == "__main__":
    main()
