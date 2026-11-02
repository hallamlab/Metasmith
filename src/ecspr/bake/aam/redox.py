"""The redox invariance repair: refuse the impossible arm, keep the atom's claim.

WHAT IS WRONG, AND WHY A MAPPER CANNOT SEE IT
---------------------------------------------
NADH and NAD(+) differ by one hydrogen. Hydrogen is not in this graph's element
vocabulary -- `atom_pairs.ELEMENTS` is C, N, S, P -- so to a maximum-common-substructure
mapper the two 21-carbon skeletons are indistinguishable, and it will happily route a
substrate's carbon into the cofactor or a cofactor's nitrogen into the substrate. A
hydride transfer does no such thing: it moves two electrons and a proton and leaves both
skeletons intact. Every C/N/P correspondence running between a NAD(P)/FAD/FMN couple and
a substrate is therefore physically impossible, and the measured population is large --
5,457 reactions, concentrated in `disagree_diluted` and `<member>_only`, with `curated`
and `consensus` clean.

IT IS A REPAIR, NOT A FILTER, AND THAT IS THE WHOLE POINT
---------------------------------------------------------
The proposal this lane comes from deletes the impossible rows and reports the deletion as
its cost. Two things make that avoidable, and both use machinery already here.

  RENORMALISE. A pair row carries a weight because an ambiguous source atom spreads a
  total of 1.0 over its candidate destinations -- that is what `layers.fuse_members`
  writes for `disagree_diluted`. In GAPDH, BPG's carbon is spread over G3P (correct) and
  NAD(+) (impossible). Refusing the impossible arm and rescaling the rest back to the
  total the atom started with does not remove the atom's claim; it CONCENTRATES it on the
  destination that survives the invariant. The reaction keeps its row and the row gets
  more confident. `atom_pairs.multiplicity_dilution` is the same rescaling under a
  different predicate.

  RE-DERIVE WHERE NOTHING SURVIVES. The residue is small -- an (mnxr, element) key where
  EVERY pair ran between the couple and a substrate, so the mapper's whole answer for that
  element was the artifact and a filter leaves nothing behind. But the invariant that
  condemns those pairs also constrains the replacement: with the couple removed, what is
  left may be a reaction conservation settles by itself, and `atom_pairs.forced_pairs` is
  exactly that computation. No mapper runs here and none can -- this stage is downstream
  of every member -- so what conservation settles is re-derived and what it does not is
  recorded as a loss with its own ledger outcome. A key that goes empty is never allowed
  to read as `mapped_nothing`.

THE SCOPE PREDICATE, AND WHY IT IS PER FAMILY
----------------------------------------------
A reaction is in scope for a family only when that family appears OXIDISED on one side
and REDUCED on the other -- which is what a hydride transfer looks like in the equation.
NAD as a genuine substrate (an NAD glycohydrolase, an NAD kinase) has no reduced partner
across the arrow, the family is not a couple there, and nothing about that reaction is
touched. That is the scope guard, and it is a property of the equation rather than a list
of reaction ids.

Within an in-scope reaction the rule is SAME FAMILY OR NOTHING: a correspondence is
refused when its two endpoints do not belong to the same cofactor family and at least one
of them belongs to a family that is a couple here. That covers cofactor <-> substrate,
and it also covers cofactor <-> OTHER cofactor, which the one-sided reading would miss:
transhydrogenase moves a hydride from NADH to NADP(+), and the NAD skeleton still does not
become the NADP skeleton.

C, N AND P ONLY. Sulfur is untouched because NAD, NADP, FAD and FMN carry no sulfur --
0.0% of S rows are affected -- so refusing S here would be refusing on a coincidence of
the predicate rather than on the invariant.

THE COFACTOR SET IS RESOLVED, NOT HARDCODED. Names are matched against a closed
name -> (family, state) map and then CROSS-CHECKED against the family's C/N/P signature,
so a record whose name says NADH and whose formula is not C21N7P2 is refused rather than
admitted. The resolution ships as a table beside the refusals: which ids were treated as
which family and state, and -- as a check that the name map has not gone stale against a
new MNXref -- which participants carry a family's exact signature under a name the map
does not know.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from .. import atom_pairs as AP
from . import layers as L

FAMILY_SIGNATURE = {
    "nad":  (21, 7, 2),
    "nadp": (21, 7, 3),
    "fad":  (27, 9, 2),
    "fmn":  (17, 4, 1),
}

STATE_BY_NAME = {
    "nad": ("nad", "ox"), "nad(+)": ("nad", "ox"), "nad+": ("nad", "ox"),
    "nadh": ("nad", "red"),
    "nadp": ("nadp", "ox"), "nadp(+)": ("nadp", "ox"), "nadp+": ("nadp", "ox"),
    "nadph": ("nadp", "red"),
    "fad": ("fad", "ox"), "fadh2": ("fad", "red"),
    "fmn": ("fmn", "ox"), "fmnh2": ("fmn", "red"),
}

REPAIR_ELEMENTS = ("C", "N", "P")

PREDICATE = "cofactor_skeleton_crossing"

REDERIVED_METHOD = "redox_forced"
REDOX_SOURCE = "redox"

REFUSAL_COLS = ("mnxr", "element", "substrate", "product", "sub_idx", "prod_idx",
                "pair_w", "method", "source", "family_substrate", "family_product",
                "predicate")
COFACTOR_COLS = ("mnxm", "name", "formula", "family", "state", "decision", "evidence")


def _norm(name) -> str:
    return str(name).strip().lower() if isinstance(name, str) else ""


def resolve_cofactors(mets: pd.DataFrame, want: set | None = None):
    family_of, state_of, rows = {}, {}, []
    for r in mets.itertuples(index=False):
        if want is not None and r.mnxm not in want:
            continue
        sig = tuple(AP.count_element(r.formula, X) for X in ("C", "N", "P"))
        named = STATE_BY_NAME.get(_norm(r.name))
        if named is None:
            for fam, want_sig in FAMILY_SIGNATURE.items():
                if sig == want_sig:
                    rows.append((r.mnxm, r.name, r.formula, fam, "",
                                 "report:signature_matches_unknown_name",
                                 f"C/N/P {sig} is {fam}'s signature"))
                    break
            continue
        fam, state = named
        if sig != FAMILY_SIGNATURE[fam]:
            rows.append((r.mnxm, r.name, r.formula, fam, state,
                         "refuse:formula_contradicts_family",
                         f"C/N/P {sig} against {fam}'s {FAMILY_SIGNATURE[fam]}"))
            continue
        family_of[r.mnxm] = fam
        state_of[r.mnxm] = state
        rows.append((r.mnxm, r.name, r.formula, fam, state, "accept",
                     f"name and C/N/P {sig} agree"))
    return family_of, state_of, rows


def couples_of(subs, prods, family_of: dict, state_of: dict) -> frozenset:
    side = (defaultdict(set), defaultdict(set))
    for i, ms in enumerate((subs, prods)):
        for m in ms:
            f = family_of.get(m)
            if f:
                side[i][f].add(state_of[m])
    out = set()
    for f in set(side[0]) & set(side[1]):
        s, p = side[0][f], side[1][f]
        if ("ox" in s and "red" in p) or ("red" in s and "ox" in p):
            out.add(f)
    return frozenset(out)


def scope(equations: dict, family_of: dict, state_of: dict, mnxrs) -> dict:
    out = {}
    for r in mnxrs:
        eq = equations.get(r)
        if not isinstance(eq, str):
            continue
        pe = AP.parse_equation(eq)
        if not pe:
            continue
        c = couples_of(pe[0], pe[1], family_of, state_of)
        if c:
            out[r] = c
    return out


SRC_KEY = ("mnxr", "element", "substrate", "sub_idx")


def repair(pairs: pd.DataFrame, in_scope: dict, family_of: dict):
    tally = Counter()
    if not len(pairs):
        return pairs, pd.DataFrame(columns=list(REFUSAL_COLS)), tally

    aff = pairs["mnxr"].isin(in_scope)
    rest, work = pairs[~aff], pairs[aff].copy()
    tally["rows in scope"] = len(work)
    if not len(work):
        return pairs, pd.DataFrame(columns=list(REFUSAL_COLS)), tally

    fam_s, fam_p, refuse = [], [], []
    for r in work.itertuples(index=False):
        here = in_scope[r.mnxr]
        fs = family_of.get(r.substrate)
        fp = family_of.get(r.product)
        fs = fs if fs in here else None
        fp = fp if fp in here else None
        fam_s.append(fs or "")
        fam_p.append(fp or "")
        refuse.append(r.element in REPAIR_ELEMENTS
                      and (fs is not None or fp is not None) and fs != fp)
    work["family_substrate"] = fam_s
    work["family_product"] = fam_p
    work["_refused"] = refuse

    refused = work[work["_refused"]].copy()
    kept = work[~work["_refused"]].copy()
    tally["rows refused"] = len(refused)
    tally["reactions in scope"] = len(in_scope)
    tally["reactions with a refusal"] = int(refused["mnxr"].nunique()) if len(refused) else 0

    if len(refused) and len(kept):
        hit = set(map(tuple, refused[list(SRC_KEY)].itertuples(index=False, name=None)))
        before = work.groupby(list(SRC_KEY))["pair_w"].sum()
        after = kept.groupby(list(SRC_KEY))["pair_w"].sum()
        keys = list(zip(*[kept[c] for c in SRC_KEY]))
        w = []
        n_scaled = 0
        for k, old in zip(keys, kept["pair_w"]):
            if k in hit:
                a = float(after.get(k, 0.0))
                b = float(before.get(k, 0.0))
                if a > 0 and b > 0:
                    w.append(float(old) * b / a)
                    n_scaled += 1
                    continue
            w.append(float(old))
        kept["pair_w"] = w
        tally["arms rescaled"] = n_scaled

    kept = kept.drop(columns=["_refused", "family_substrate", "family_product"])
    out = pd.concat([rest, kept], ignore_index=True) if len(rest) else kept
    refused["predicate"] = PREDICATE
    return (out.reset_index(drop=True),
            refused[list(REFUSAL_COLS)].reset_index(drop=True),
            tally)


def emptied_keys(before: pd.DataFrame, after: pd.DataFrame) -> list:
    had = set(zip(before["mnxr"], before["element"]))
    have = set(zip(after["mnxr"], after["element"]))
    return sorted(had - have)


def rederive(keys, in_scope: dict, equations: dict, formulas: dict, ranks_of: dict,
             family_of: dict):
    rows, tally = [], Counter()
    for mnxr, X in keys:
        here = in_scope.get(mnxr)
        if not here:
            tally["emptied outside the repair's scope"] += 1
            continue
        pe = AP.parse_equation(equations.get(mnxr) or "")
        if not pe:
            tally["no parseable equation"] += 1
            continue
        subs = [m for m in pe[0] if family_of.get(m) not in here]
        prods = [m for m in pe[1] if family_of.get(m) not in here]
        if not subs or not prods:
            tally["nothing left once the couple is removed"] += 1
            continue
        fp = AP.forced_pairs(subs, prods, formulas, ranks_of)
        got = [(sm, pm, idxs) for (el, sm, pm), idxs in fp.items() if el == X]
        if not got:
            tally["conservation does not settle the remainder"] += 1
            continue
        for sm, pm, idxs in got:
            for a, b, w in idxs:
                rows.append((mnxr, X, sm, pm, int(a), int(b), float(w),
                             REDERIVED_METHOD, REDOX_SOURCE, 1.0))
        tally["re-derived from conservation"] += 1
    return pd.DataFrame(rows, columns=list(L.ATOM_COLS)), tally


def cmd_repair(args):
    pairs = pd.read_parquet(args.pairs)
    missing = [c for c in L.ATOM_COLS if c not in pairs.columns]
    if missing:
        raise SystemExit(f"[redox] {args.pairs} is missing {missing} -- this stage reads "
                         f"the stacked table in `layers.ATOM_COLS` shape")
    pairs = pairs[list(L.ATOM_COLS)]

    rx = pd.read_parquet(args.lookups / "reactions.parquet",
                         columns=["mnxr", "equation"])
    equations = dict(zip(rx["mnxr"], rx["equation"]))

    have = set(pairs["mnxr"])
    want = set()
    for r in have:
        pe = AP.parse_equation(equations.get(r) or "")
        if pe:
            want |= set(pe[0]) | set(pe[1])

    mets = pd.read_parquet(args.lookups / "metabolites.parquet",
                           columns=["mnxm", "name", "formula"])
    family_of, state_of, cof_rows = resolve_cofactors(mets, want=want)
    formulas = dict(zip(mets["mnxm"], mets["formula"]))
    ar = pd.read_parquet(args.lookups / "atom_ranks.parquet",
                         columns=["mnxm", "element", "ranks"])
    ranks_of = {(r.mnxm, r.element): list(r.ranks) for r in ar.itertuples(index=False)
                if len(r.ranks)}

    n_acc = sum(1 for r in cof_rows if r[5] == "accept")
    print(f"[redox] {len(pairs):,} pair rows over {len(have):,} reactions; "
          f"{n_acc} cofactor records resolved over {len(FAMILY_SIGNATURE)} families",
          flush=True)

    in_scope = scope(equations, family_of, state_of, have)
    print(f"[redox] {len(in_scope):,} reactions carry a couple on both sides", flush=True)

    kept, refused, tally = repair(pairs, in_scope, family_of)

    empt = emptied_keys(pairs, kept)
    red, rtally = rederive(empt, in_scope, equations, formulas, ranks_of, family_of)
    if len(red):
        kept = pd.concat([kept, red], ignore_index=True)
    lost = emptied_keys(pairs, kept)
    lost_rxn = sorted({m for m, _X in lost})
    still = set(kept["mnxr"]) if len(kept) else set()
    emptied_rxn = [m for m in lost_rxn if m not in still]

    kept.to_parquet(args.out, index=False)
    refused.to_parquet(args.out_refusals, index=False)
    pd.DataFrame(cof_rows, columns=list(COFACTOR_COLS)).to_csv(
        args.out_cofactors, sep="\t", index=False)
    Path(args.out_emptied).write_text("".join(f"{m}\n" for m in emptied_rxn))

    lines = ["kind\tkey\tn"]
    for k, v in sorted(tally.items()):
        lines.append(f"repair\t{k}\t{v}")
    for k, v in sorted(rtally.items()):
        lines.append(f"rederive\t{k}\t{v}")
    lines.append(f"product\tpair_rows_before\t{len(pairs)}")
    lines.append(f"product\tpair_rows_after\t{len(kept)}")
    lines.append(f"product\trederived_rows\t{len(red)}")
    for label, d in (("before", pairs), ("after", kept)):
        lines.append(f"keys\t{label}\t{len(set(zip(d['mnxr'], d['element'])))}")
        lines.append(f"reactions\t{label}\t{int(d['mnxr'].nunique()) if len(d) else 0}")
    for X in AP.ELEMENTS:
        b = int((pairs["element"] == X).sum())
        a = int((kept["element"] == X).sum())
        lines.append(f"element_rows\t{X}\t{b}->{a}")
    lines.append(f"keys\temptied_after_rederivation\t{len(lost)}")
    lines.append(f"reactions\temptied\t{len(emptied_rxn)}")
    lines.append(f"predicate\t{PREDICATE}\t{len(refused)}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 64)
    print("REDOX REPAIR -- refuse the crossing arm, keep the atom's claim")
    print("=" * 64)
    for k, v in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<44} {v:>9,}")
    for k, v in sorted(rtally.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<44} {v:>9,}")
    print(f"\n  {'pair rows':<44} {len(pairs):>9,} -> {len(kept):,}")
    print(f"  {'(reaction, element) keys':<44} "
          f"{len(set(zip(pairs['mnxr'], pairs['element']))):>9,} -> "
          f"{len(set(zip(kept['mnxr'], kept['element']))):,}")
    print(f"  {'reactions emptied':<44} {len(emptied_rxn):>9,}")
    s_b = int((pairs["element"] == "S").sum())
    s_a = int((kept["element"] == "S").sum())
    if s_b != s_a:
        raise SystemExit(f"[redox] {s_b:,} sulfur rows before and {s_a:,} after. The "
                         f"cofactors this lane scopes to carry no sulfur, so an S row "
                         f"cannot be a crossing arm -- this is a bug in the predicate, "
                         f"not a repair")
    print(f"\nwrote {args.out}, {args.out_refusals}, {args.out_cofactors}, "
          f"{args.out_emptied} and {args.out_summary}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("repair"); p.set_defaults(fn=cmd_repair)
    p.add_argument("--pairs", required=True, help="interm::aam_stack, the stacked table")
    p.add_argument("--lookups", required=True, type=Path)
    p.add_argument("--out", required=True, help="the corrected pairs, parquet")
    p.add_argument("--out-refusals", required=True,
                   help="every refused correspondence, with the named predicate")
    p.add_argument("--out-cofactors", required=True,
                   help="which ids were treated as which family and state, and why")
    p.add_argument("--out-emptied", required=True,
                   help="reactions the repair left with no pair at all, one per line. "
                        "The ledger reads this to give them an outcome of their own")
    p.add_argument("--out-summary", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
