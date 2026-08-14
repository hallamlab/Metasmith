"""Curated member: BioCyc REACTION-DIRECTION, aligned to MNXR orientation.

The hazard this module exists for: REACTION-DIRECTION is stated relative to
MetaCyc's own equation orientation, but MNXref re-canonicalises orientation on
import, so a naive metacyc-reaction -> MNXR join inverts the curated direction on
~60% of reactions (measured). Direction cannot be made orientation-agnostic the
way the AAM lane was, because orientation IS the signal. So every reaction's
curated call is re-expressed in MNXR orientation by comparing the MetaCyc
LEFT/RIGHT compound sets (mapped to MNXM) against the MNXR substrate/product sets.

Anchor = the flat file's LEFT/RIGHT slots, which are self-consistent with its own
REACTION-DIRECTION. The reac_xref col-3 equation (a newer MetaCyc) is an
independent cross-check, recorded but not used to decide.

SOURCE. The deployed member read a staged sqlite pgdb; this one reads the licensed
drop-in's own `reactions.dat` through `dir_metacyc_flatfile`. Same three slots
(REACTION-DIRECTION, LEFT, RIGHT), same alignment, one less derived artifact in the
acquisition tier. EcoCyc is NOT a second source here: it carries no dedicated MNXref
prefix and joins through the same `metacyc.reaction` map, so on this crosswalk it adds
reaction ids MetaCyc already supplies rather than independent evidence -- and the drop-in
is MetaCyc. The `source` column is kept so a second pgdb could be added back without a
schema change.

Standalone: `python dir_curated.py --out <parquet>` (run under an env with pandas).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from dir_metacyc_flatfile import load_reactions
from dir_refdata import (
    load_source_to_mnxr,
    load_mnxr_sides,
    load_metacyc_compound_to_mnxm,
    parse_col3_sides,
)

# All five curated values, and their orientation-flip images. A KeyError here on
# an unseen value is deliberate: a new direction token must be handled, not
# silently defaulted (the failure mode that inverts the ~7% right-to-left corpus).
FLIP = {
    "LEFT-TO-RIGHT": "RIGHT-TO-LEFT",
    "RIGHT-TO-LEFT": "LEFT-TO-RIGHT",
    "PHYSIOL-LEFT-TO-RIGHT": "PHYSIOL-RIGHT-TO-LEFT",
    "PHYSIOL-RIGHT-TO-LEFT": "PHYSIOL-LEFT-TO-RIGHT",
    "REVERSIBLE": "REVERSIBLE",
}


def flip_verdict(left_ids, right_ids, sides_lr, cmap):
    """Is this MetaCyc LEFT/RIGHT orientation flipped relative to the MNXR sides?

    Returns (flipped, agree, flip): flipped is True/False, or None when there is
    no overlap (nothing compared -> not a decision) or an exact tie (~transport,
    same compound both sides). left/right are bare MetaCyc compound ids mapped to
    MNXM via cmap, then overlapped against the MNXR substrate/product sets.
    """
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
    """Return (aligned_direction, reason). aligned is None when undecidable.

    reason in {same, flipped, no_direction, unknown_value, no_mnxr_sides,
    no_overlap, tie}.
    """
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
    """One row per source reaction that carries a REACTION-DIRECTION and an MNXR."""
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
        # Independent cross-check: col-3 is a NEWER MetaCyc's own equation. Give it
        # the same flip test against the same MNXR sides that the flat file's
        # LEFT/RIGHT got. Both are MetaCyc orientation, so the verdicts should match; a
        # mismatch flags version skew on that reaction.
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
    """One row per MNXR. aligned = the agreed direction across all contributing
    reactions/sources, else None with source='disagree'. Many metacyc reactions
    can share one MNXR (max ~167); a genuine conflict is recorded, never
    first-wins-resolved.
    """
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
    # Loud floor: if the crosswalk breaks (wrong prefix, stale reac_prop, empty
    # cmap) nearly everything lands 'no_overlap' and the table is silently empty
    # of direction. A run that decides almost nothing is not a curated member.
    decided = per_rxn["reason"].isin(("same", "flipped")).mean()
    assert decided > 0.5, (
        f"curated: only {decided:.1%} of source reactions decided -- the MNXR "
        f"crosswalk or compound map is likely broken, not the data")
    per_mnxr = collapse_to_mnxr(per_rxn)
    return per_rxn, per_mnxr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metacyc-reactions", required=True,
                    help="the licensed drop-in's reactions.dat")
    ap.add_argument("--reac-xref", required=True)
    ap.add_argument("--reac-prop", required=True)
    ap.add_argument("--chem-xref", required=True)
    ap.add_argument("--out", required=True, help="per-MNXR parquet")
    ap.add_argument("--out-per-reaction", default=None)
    a = ap.parse_args()
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
