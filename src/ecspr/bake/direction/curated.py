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

THE SUPPLEMENTARY CROSSWALK (`--supplementary-crosswalk`, off by default). 545 of the
18,591 directed MetaCyc reactions carry no `metacyc.reaction` row in reac_xref, so the
primary join never sees them -- and read as a list rather than as a count, they are
overwhelmingly GENERIC-POLYMER chemistry (glucans, amylopectin, levan, maltodextrins,
peptides, mRNA fragments), which is the same MetaNetX weakness the polymer substitution
lane exists for. Those records are matched to an MNXR on the unordered set of their
mapped MNXM participants, then re-expressed through `align_one` like any other -- never
around it, which is the inversion this module is organised against.

Two things make that safe rather than merely plausible. The key is PRICED, not argued:
run over the 18,046 records reac_xref DOES crosswalk, where reac_xref is ground truth,
it fires on 14,860 and is right on 14,801, and `SUPP_PRECISION_FLOOR` fails the build if
that ever degrades. And the arm is ADDITIVE ONLY -- it matches only onto an MNXR no
reac_xref row already claims -- so on every reaction it reaches there is no primary call
for it to contradict. The orientation hazard is closed by construction; the precision
figure is the backstop, not the argument.

Standalone: `python -m ecspr.bake.direction.curated --out <parquet>` (any env with pandas).
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from .metacyc_flatfile import load_reactions
from .refdata import (
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


# The measured precision of the supplementary key against reac_xref's own answers.
# A floor, not a target: it fails the build if a MetaNetX or MetaCyc release ever makes
# the participant-set key stop identifying reactions, rather than shipping the drift.
SUPP_PRECISION_FLOOR = 0.99


def _participant_index(sides):
    """frozenset(MNXM participants) -> [MNXR]. The match key, and it is orientation-BLIND
    on purpose: which side a compound sits on is what `align_one` decides afterwards, and
    a key that encoded it would bake in the very flip this module exists to detect."""
    idx = defaultdict(list)
    for mnxr, (xl, xr) in sides.items():
        idx[frozenset(xl | xr)].append(mnxr)
    return idx


def _match_one(rec, idx, sides, cmap):
    """(mnxr, None) for the one MNXR this record is, else (None, refusal_reason).

    Every refusal is a whole-record refusal. A partially mapped equation is declined
    rather than matched on the compounds that happened to resolve: a subset of the
    participants is a different reaction, and it would match a different MNXR.
    """
    left, right = list(rec["left"]), list(rec["right"])
    if not left or not right:
        return None, "empty_side"
    if any(c not in cmap for c in left + right):
        return None, "compound_unmapped"
    ml = {cmap[c] for c in left}
    mr = {cmap[c] for c in right}
    cands = idx.get(frozenset(ml | mr), ())
    if not cands:
        return None, "no_mnxr_has_those_participants"
    if len(cands) > 1:
        return None, "ambiguous"
    mnxr = cands[0]
    xl, xr = sides[mnxr]
    # A shared compound INVENTORY is not identity -- A+B=C+D and A+C=B+D have the same
    # one. Demand that the two mapped sides ARE the MNXR's two sides, in either order;
    # either order, because MNXref re-canonicalises orientation and a flip here is
    # normal and is `align_one`'s to resolve.
    if not ((ml == set(xl) and mr == set(xr)) or (ml == set(xr) and mr == set(xl))):
        return None, "sides_do_not_correspond"
    return mnxr, None


def crosswalk_precision(records, id2mnxr, idx, sides, cmap):
    """(correct, fired) for the key run over the records reac_xref DOES crosswalk.

    A held-out check of thousands rather than an argument: on those records reac_xref is
    the answer, so the key can simply be asked whether it agrees.
    """
    correct = fired = 0
    for rec in records:
        truth = id2mnxr.get(rec["unique_id"])
        if truth is None:
            continue
        mnxr, _ = _match_one(rec, idx, sides, cmap)
        if mnxr is None:
            continue
        fired += 1
        correct += (mnxr == truth)
    return correct, fired


def supplementary_map(records, id2mnxr, sides, cmap):
    """(mc_id -> MNXR, refusal ledger, (correct, fired)) for the reac_xref gap.

    Refuses to return anything at all if the key fails `SUPP_PRECISION_FLOOR` on the
    held-out corpus, because a key that no longer identifies reactions would otherwise
    hand confident directions to the wrong ones.
    """
    idx = _participant_index(sides)
    correct, fired = crosswalk_precision(records, id2mnxr, idx, sides, cmap)
    prec = correct / fired if fired else 0.0
    assert prec >= SUPP_PRECISION_FLOOR, (
        f"curated: the supplementary key reproduces reac_xref on only {prec:.2%} of "
        f"{fired:,} held-out records (floor {SUPP_PRECISION_FLOOR:.0%}) -- it is no "
        f"longer identifying reactions, so it must not be used to direct them")
    claimed = set(id2mnxr.values())
    out, ledger = {}, Counter()
    for rec in records:
        if rec["unique_id"] in id2mnxr:
            continue
        mnxr, why = _match_one(rec, idx, sides, cmap)
        if mnxr is None:
            ledger[why] += 1
            continue
        # ADDITIVE ONLY. An MNXR reac_xref already crosswalks has a primary call, and a
        # second opinion arriving by a weaker key could only contradict it. Declining
        # here is what lets this arm carry no orientation risk of its own.
        if mnxr in claimed:
            ledger["primary_already_claims_it"] += 1
            continue
        out[rec["unique_id"]] = mnxr
        ledger["matched"] += 1
    return out, ledger, (correct, fired)


def read_source(records, source: str, id2mnxr, sides, cmap, col3):
    """One row per source reaction that carries a REACTION-DIRECTION and an MNXR.

    Takes already-parsed records rather than a path because `build` runs it twice over
    the one file -- once against reac_xref's map, once against the supplementary one.
    """
    rows = []
    for rec in records:
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


def build(metacyc_reactions, reac_xref, reac_prop, chem_xref, supplementary=False):
    id2mnxr = load_source_to_mnxr(reac_xref, "metacyc.reaction")
    sides = load_mnxr_sides(reac_prop)
    cmap = load_metacyc_compound_to_mnxm(chem_xref)
    col3 = parse_col3_sides(reac_xref)
    records = [r for r in load_reactions(Path(metacyc_reactions)) if r["direction"]]
    per_rxn = read_source(records, "metacyc", id2mnxr, sides, cmap, col3)
    # Loud floor: if the crosswalk breaks (wrong prefix, stale reac_prop, empty
    # cmap) nearly everything lands 'no_overlap' and the table is silently empty
    # of direction. A run that decides almost nothing is not a curated member.
    decided = per_rxn["reason"].isin(("same", "flipped")).mean()
    assert decided > 0.5, (
        f"curated: only {decided:.1%} of source reactions decided -- the MNXR "
        f"crosswalk or compound map is likely broken, not the data")
    supp_ledger, supp_prec = None, None
    if supplementary:
        supp, supp_ledger, supp_prec = supplementary_map(records, id2mnxr, sides, cmap)
        # Same function, same orientation check, a different id->MNXR map. The rows are
        # tagged `metacyc_supp` so a consumer can tell how a call arrived; nothing
        # downstream branches on it, and the additive gate guarantees no MNXR carries
        # both tags.
        per_rxn = pd.concat(
            [per_rxn, read_source(records, "metacyc_supp", supp, sides, cmap, col3)],
            ignore_index=True)
    per_mnxr = collapse_to_mnxr(per_rxn)
    return per_rxn, per_mnxr, supp_ledger, supp_prec


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--metacyc-reactions", required=True,
                    help="the licensed drop-in's reactions.dat")
    ap.add_argument("--reac-xref", required=True)
    ap.add_argument("--reac-prop", required=True)
    ap.add_argument("--chem-xref", required=True)
    ap.add_argument("--out", required=True, help="per-MNXR parquet")
    ap.add_argument("--out-per-reaction", default=None)
    ap.add_argument("--supplementary-crosswalk", action="store_true",
                    help="also match the reactions reac_xref never crosswalked, on "
                         "their participant set, additively (see the module docstring)")
    a = ap.parse_args(argv)
    per_rxn, per_mnxr, supp_ledger, supp_prec = build(
        Path(a.metacyc_reactions), Path(a.reac_xref), Path(a.reac_prop),
        Path(a.chem_xref), supplementary=a.supplementary_crosswalk)
    per_mnxr.to_parquet(a.out, index=False)
    if a.out_per_reaction:
        per_rxn.to_parquet(a.out_per_reaction, index=False)
    dec = per_rxn["reason"].value_counts()
    print(f"[curated] supplementary crosswalk: "
          f"{'ON' if a.supplementary_crosswalk else 'off'}")
    if supp_prec is not None:
        correct, fired = supp_prec
        print(f"[curated] supplementary key vs reac_xref on {fired:,} held-out "
              f"records: {correct:,} correct ({correct / fired:.2%})")
        # The refusals are printed beside the matches for the reason `decisions.tsv`
        # exists in the substitution lane: an arm that reports only what it admitted
        # cannot be told apart from one that admitted everything it saw.
        for k, v in sorted(supp_ledger.items(), key=lambda kv: -kv[1]):
            print(f"[curated]   {k:<32} {v:,}")
    print(f"[curated] {len(per_rxn):,} source reactions -> {len(per_mnxr):,} MNXRs")
    print(f"[curated] per-reaction decidability:\n{dec.to_string()}")
    flipped = int((per_rxn['reason'] == 'flipped').sum())
    same = int((per_rxn['reason'] == 'same').sum())
    if same + flipped:
        print(f"[curated] flip rate among decided: {flipped/(same+flipped):.1%} "
              f"({flipped:,}/{same+flipped:,})")


if __name__ == "__main__":
    main()
