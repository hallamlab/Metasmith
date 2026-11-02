"""The additive layer stack, and the gates that make "additive" a checkable claim.

THE ARCHITECTURE, AND WHY IT IS LAYERED RATHER THAN VOTED
---------------------------------------------------------
Three sources of atom correspondence, laid down in order of how much they are worth,
each claiming only what the layer below left unclaimed:

  L1  metacyc     the curated map. Expert-assigned, and it balances per element at
                  99.8% or better. It goes FIRST.
  L2  ensemble    RXNMapper + LocalMapper + Indigo, contributing where they agree over
                  the reactions L1 did not reach. The neural increment balances carbon
                  in 12.5% of the candidates it proposes, which is the whole argument
                  for it being second rather than first.
  L3  curation    structures proposed for structure-less metabolites, accepted only
                  when the completed reaction maps AND an element balances.

ADDITIVE MEANS ADDITIVE, AND THE CLAIM IS TESTED. A layer may only add (mnxr, element)
combinations absent from everything below it. It may never overwrite one, because
overwriting changes a reaction that existing results already depend on -- and because
"we replaced 15,539 reactions' worth of predictions with curation" is a different and
much larger claim than "we filled the gaps". `additive_gates` refuses rather than warns:
a gate that warns is a gate that gets read once.

WHAT THE PREVIOUS GENERATION DECLINED, AND WHAT PUTTING METACYC FIRST COSTS
--------------------------------------------------------------------------
scadc kept the neural universe as tier B and appended MetaCyc as a small tier-4
increment, explicitly declining to let curation REPLACE prediction on the 15,539
reactions where both had an answer. Laying MetaCyc down first takes that path by
construction. It is better founded -- on the shared reactions the two agree 98.6% at
molecule level, and where they differ the curated map is the one that balances -- but
the resulting table is NOT the deployed table, and the deployed reaction count is a
floor to clear rather than a number to reproduce.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ELEMENTS = ("C", "N", "S", "P")

ATOM_COLS = ("mnxr", "element", "substrate", "product", "sub_idx", "prod_idx",
             "pair_w", "method", "source", "confidence")

PAIR_KEY = ("mnxr", "element", "substrate", "product", "sub_idx", "prod_idx")

SINGLE_MEMBER_CREDIT = 0.5


def explode(path, method: str, source: str, confidence: float = 1.0,
            only_class: str | None = None) -> pd.DataFrame:
    d = pd.read_parquet(path)
    if only_class is not None:
        if "submission_class" not in d.columns:
            raise SystemExit(
                f"[layers] {path} has no `submission_class` column, so its rows cannot be "
                f"partitioned by layer -- it was extracted without `--universe`. Fusing "
                f"it as one layer would let a partial map sit where a full map belongs.")
        d = d[d["submission_class"].astype(str) == only_class]
    if not len(d):
        return pd.DataFrame(columns=list(ATOM_COLS))
    out = defaultdict(list)
    for r in d.itertuples(index=False):
        si = str(r.sub_idx).split(",")
        pi = str(r.prod_idx).split(",")
        pw = str(r.pair_w).split(",")
        if not (len(si) == len(pi) == len(pw)):
            raise SystemExit(
                f"[layers] {path}: {r.mnxr}/{r.element} has {len(si)} sub_idx, "
                f"{len(pi)} prod_idx and {len(pw)} pair_w -- the three lists are one "
                f"list written three ways and must not disagree")
        for a, b, w in zip(si, pi, pw):
            out["mnxr"].append(r.mnxr)
            out["element"].append(r.element)
            out["substrate"].append(r.substrate)
            out["product"].append(r.product)
            out["sub_idx"].append(int(a))
            out["prod_idx"].append(int(b))
            out["pair_w"].append(float(w))
    df = pd.DataFrame(out)
    df["method"] = method
    df["source"] = source
    df["confidence"] = float(confidence)
    return df[list(ATOM_COLS)]


def fuse_members(members: dict, single_credit: float = SINGLE_MEMBER_CREDIT,
                 coefficients: dict | None = None):
    key = ["mnxr", "element", "substrate", "sub_idx"]
    speak = defaultdict(dict)
    for name, d in members.items():
        if d is None or not len(d):
            continue
        for r in d.itertuples(index=False):
            k = (r.mnxr, r.element, r.substrate, r.sub_idx)
            speak[k].setdefault(name, []).append(
                (r.product, r.prod_idx, float(r.pair_w), float(r.confidence)))

    rows = []
    tally = defaultdict(int)
    for k, per_member in speak.items():
        mnxr, el, sm, s_idx = k
        # A participant written `c` times is `c` copies sharing one atom identity, and
        # their atoms may end up in different products -- `2 A = B + C` is one atom of A
        # to B and one to C. The member is allowed `c` destinations for that reason and
        # no more. Keeping one was a lost edge; the cap is what keeps it from becoming a
        # licence to fan out.
        cap = 1
        if coefficients is not None:
            cap = max(1, coefficients.get(mnxr, ({}, {}))[0].get(sm, 1))
        picks = {}
        for name, lst in per_member.items():
            # Ties on pair_w break on the destination, never on row order.
            best = {}
            for p, pi, w, c in lst:
                cur = best.get((p, pi))
                if cur is None or w > cur[0]:
                    best[(p, pi)] = (w, c)
            ordered = sorted(best.items(),
                             key=lambda kv: (kv[1][0], kv[0][0], kv[0][1]), reverse=True)
            picks[name] = [(p, pi, w, c) for (p, pi), (w, c) in ordered[:cap]]
            if len({p for p, _i, _w, _c in lst}) > 1:
                tally["member_offered_two_molecules"] += 1
        srcs = "+".join(sorted(picks))
        offered = {n: frozenset((p, pi) for p, pi, _w, _c in v) for n, v in picks.items()}

        # WHY THE WEIGHT IS A SHARE AND NOT A COUNT. `graph_from_pairs` folds `pair_w`
        # straight into conductance BECAUSE it is margin-conserving -- a source atom's
        # candidate weights sum to one. Two copies of a participant are still one atom
        # node here, so the margin divides across the destinations rather than doubling;
        # letting it reach 2.0 would inflate conductance out of that node and break the
        # solve's conservation. At one destination this is 1.0 and nothing moves.
        share = defaultdict(float)
        denom = 0.0
        for name, v in picks.items():
            for p, pi, _w, c in v:
                if c != c:
                    c = 1.0
                share[(p, pi)] += c
                denom += c
        if not (denom > 0):
            share = defaultdict(float)
            denom = float(sum(len(v) for v in picks.values()))
            for name, v in picks.items():
                for p, pi, _w, _c in v:
                    share[(p, pi)] += 1.0

        if len(picks) == 1:
            name = next(iter(picks))
            for (pm, p_idx), s in share.items():
                w = single_credit * s / denom
                rows.append((mnxr, el, sm, pm, s_idx, p_idx, w,
                             f"{name}_only", name, w))
            tally[f"{name}_only"] += 1
        elif len(set(offered.values())) == 1:
            cbar = (sum(c for v in picks.values() for *_x, c in v)
                    / max(1, sum(len(v) for v in picks.values())))
            for (pm, p_idx), s in share.items():
                rows.append((mnxr, el, sm, pm, s_idx, p_idx, s / denom,
                             "consensus", srcs, cbar))
            tally["consensus"] += 1
        else:
            for (pm, p_idx), s in share.items():
                rows.append((mnxr, el, sm, pm, s_idx, p_idx, s / denom,
                             "disagree_diluted", srcs, s / denom))
            tally["disagree_diluted"] += 1

    df = pd.DataFrame(rows, columns=list(ATOM_COLS))
    return df, dict(tally)


def injectivity_violations(df: pd.DataFrame, coefficients: dict | None = None):
    a = df.groupby(["mnxr", "element", "substrate", "sub_idx"])["product"].nunique()
    b = df.groupby(["mnxr", "element", "product", "prod_idx"])["substrate"].nunique()
    mA = pd.MultiIndex.from_frame(
        df[["mnxr", "element", "substrate", "sub_idx"]]).isin(a[a > 1].index)
    mB = pd.MultiIndex.from_frame(
        df[["mnxr", "element", "product", "prod_idx"]]).isin(b[b > 1].index)
    if coefficients is None:
        return mA, mB, mA | mB

    # THE BOUND IS THE COEFFICIENT, not a blanket exemption. A participant written `c`
    # times is genuinely `c` copies, and the atom identity `(metabolite, canonical rank)`
    # cannot tell them apart -- glutamate synthase writes `2 L-glutamate`, so both
    # substrates correctly reach one node. But that buys the participant `c` destinations
    # and no more: exempting it outright let one atom of a doubled participant fan out to
    # five molecules unreported. An unknown coefficient takes 1, so ignorance stays
    # conservative rather than permissive.
    def cap(idx: pd.MultiIndex, side: int):
        return np.array([coefficients.get(r, ({}, {}))[side].get(m, 1)
                         for r, m in zip(idx.get_level_values(0),
                                         idx.get_level_values(2))])

    over_a = a.index[a.to_numpy() > cap(a.index, 0)]
    over_b = b.index[b.to_numpy() > cap(b.index, 1)]
    bad = (pd.MultiIndex.from_frame(
               df[["mnxr", "element", "substrate", "sub_idx"]]).isin(over_a)
           | pd.MultiIndex.from_frame(
               df[["mnxr", "element", "product", "prod_idx"]]).isin(over_b))
    return mA, mB, bad


def additive_gates(below: pd.DataFrame, added: pd.DataFrame, name: str):
    problems = []

    if len(below) and len(added):
        have = set(zip(below["mnxr"], below["element"]))
        clash = {(m, e) for m, e in zip(added["mnxr"], added["element"])} & have
        if clash:
            problems.append(
                f"{len(clash):,} (reaction, element) combinations are already claimed by "
                f"a lower layer. A layer may only ADD -- overwriting changes reactions "
                f"existing results depend on. First few: {sorted(clash)[:5]}")

    if len(below) and len(added):
        b = below.set_index(list(PAIR_KEY)).index
        a = added.set_index(list(PAIR_KEY)).index
        n = len(a.intersection(b))
        if n:
            problems.append(f"{n:,} atom correspondences collide with a lower layer on "
                            f"the 6-tuple pair key")

    if len(below):
        after = pd.concat([below, added], ignore_index=True) if len(added) else below
        for X in ELEMENTS:
            b = below[below.element == X].mnxr.nunique()
            t = after[after.element == X].mnxr.nunique()
            if t < b:
                problems.append(f"element {X}: {b:,} reactions before, {t:,} after -- "
                                f"an append cannot lose coverage")

    if len(added):
        if (added["sub_idx"] < 0).any() or (added["prod_idx"] < 0).any():
            problems.append("negative canonical ranks present -- a sentinel leaked into "
                            "the atom identity space; the bake packs these unsigned")

    if problems:
        for p in problems:
            print(f"[layers] GATE FAILED ({name}): {p}", flush=True)
        raise SystemExit(f"[layers] layer {name!r} is not additive over the stack below "
                         f"it; refusing to write")
    return True


def stack(layers: list):
    acc = pd.DataFrame(columns=list(ATOM_COLS))
    report = []
    for name, df in layers:
        if df is None or not len(df):
            report.append(dict(layer=name, offered=0, added=0, rxn=0))
            continue
        if len(acc):
            claimed = set(zip(acc["mnxr"], acc["element"]))
            mask = [(m, e) not in claimed
                    for m, e in zip(df["mnxr"], df["element"])]
            add = df[pd.Series(mask, index=df.index)]
        else:
            add = df
        additive_gates(acc, add, name)
        report.append(dict(layer=name, offered=len(df), added=len(add),
                           rxn=int(add.mnxr.nunique()) if len(add) else 0))
        acc = pd.concat([acc, add], ignore_index=True) if len(acc) else add.copy()
    return acc.reset_index(drop=True), report


def load_coefficients(reac_prop: Path) -> dict:
    term = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@")
    out = {}
    for line in open(reac_prop):
        if line.startswith("#"):
            continue
        f = line.rstrip("\n").split("\t")
        if len(f) < 2 or not f[0].startswith("MNXR") or "=" not in f[1]:
            continue
        # Floored at one. 2,729 terms over 86 reactions carry a sub-1 coefficient
        # (`0.000503 MNXM1101285@`), and rounding those to zero would say the reaction
        # does not contain a participant it does contain -- which reads downstream as a
        # fanout cap of zero and deletes every row the participant has.
        out[f[0]] = tuple({m: max(1, int(round(float(c)))) for c, m in term.findall(side)}
                          for side in f[1].split("=", 1))
    return out


def report_collapsed_splits(worklist: Path, df: pd.DataFrame, coefficients: dict):
    # A COLLAPSED SUBMISSION CANNOT CARRY A SPLIT, and the fusion cannot tell that from a
    # mapper simply not finding one. `worklist.collapse` writes each distinct molecule
    # once, so a mapper handed `2 A = B + C` sees one A and has one atom to place -- the
    # second destination is absent from the input, not lost in the arithmetic. The one
    # way it could still be expressed is by reusing a map number, which the extractor's
    # injectivity gate now drops, so raising the fanout cap buys these reactions nothing.
    #
    # The two collapsed populations are NOT the same bargain and are counted apart:
    # a reaction the expanded caps refused is one collapse RESCUED (there would be no map
    # at all otherwise), while an `oversize` reaction goes to Indigo either way and was
    # collapsed only to shrink it. The second is a choice that could be revisited by
    # handing Indigo the expanded string; the first is not.
    wl = pd.read_parquet(worklist, columns=["mnxr", "verdict", "collapsed"])
    wl = wl[wl["collapsed"].astype(bool)]
    dbl = {r for r, (s, _p) in coefficients.items() if any(c > 1 for c in s.values())}
    have = set(df["mnxr"])
    rescued = set(wl.loc[wl["verdict"] == "mappable", "mnxr"]) & dbl & have
    oversize = set(wl.loc[wl["verdict"] == "oversize", "mnxr"]) & dbl & have
    print(f"\n  collapsed submissions that write a substrate more than once:")
    print(f"    rescued by the collapse (no map without it) : {len(rescued):>6,}")
    print(f"    oversize, collapsed only to shrink it       : {len(oversize):>6,}")
    print(f"    -- their duplicated participants cannot reach a second destination, "
          f"because the copy is absent from the submission.")


def cmd_audit(args):
    df = pd.read_parquet(args.pairs)
    coef = load_coefficients(Path(args.reac_prop)) if args.reac_prop else None
    mA, mB, bad = injectivity_violations(df, coef)
    n_rxn = df.mnxr.nunique()
    hit = df.loc[bad, "mnxr"].nunique()

    print("=" * 68)
    print("ATOM-MAP INJECTIVITY")
    print("=" * 68)
    print(f"  A  substrate atom split across molecules : {int(mA.sum()):>9,} rows")
    print(f"  B  product atom claimed by two molecules : {int(mB.sum()):>9,} rows")
    if coef is not None:
        print(f"     stoichiometric multiplicity explains : "
              f"{int(((mA | mB) & ~bad).sum()):>9,} rows")
    print(f"  UNEXPLAINED                              : {int(bad.sum()):>9,} rows "
          f"({100*bad.sum()/max(len(df),1):.2f}%), {hit:,} of {n_rxn:,} reactions "
          f"({100*hit/max(n_rxn,1):.2f}%)")
    if int(bad.sum()):
        print("\n  by method:")
        print(df[bad].groupby("method").size().sort_values(ascending=False).to_string())
        print("\n  molecules receiving the contested weight:")
        top = (df[bad & mA].groupby("product")["pair_w"].sum()
               .sort_values(ascending=False).head(10))
        for m, w in top.items():
            print(f"    {m:<16} {w:>10,.1f}")
    if args.worklist and coef is not None:
        report_collapsed_splits(Path(args.worklist), df, coef)

    if args.out:
        (df[bad].groupby("mnxr")
         .agg(rows=("pair_w", "size"), weight=("pair_w", "sum"))
         .sort_values("weight", ascending=False)
         .to_csv(args.out, sep="\t"))
        print(f"\nwrote {args.out}")

    if args.max_reactions is not None and hit > args.max_reactions:
        raise SystemExit(
            f"[layers] GATE FAILED (injectivity): {hit:,} reactions carry an atom "
            f"correspondence that contradicts another, over a ceiling of "
            f"{args.max_reactions:,}. A cross-MOLECULE split is not an abstention -- "
            f"`compile_atom_graph` sums the diluted rows into one edge set, so a solve "
            f"enters on one member's map and leaves on the other's. What survives "
            f"`atom_pairs.duplicate_map_numbers` is members with valid maps that disagree "
            f"about the destination MOLECULE, so raising the ceiling is a statement about "
            f"how much of that the graph may carry -- not a way past a broken member.")
    return 0


def _spec(s: str):
    name, _, rest = s.partition("=")
    parts = rest.split(",")
    path = parts[0]
    method = parts[1] if len(parts) > 1 and parts[1] else name
    conf = float(parts[2]) if len(parts) > 2 and parts[2] else 1.0
    return name, path, method, conf


def cmd_fuse(args):
    members = {}
    for s in args.member:
        name, path, method, conf = _spec(s)
        members[name] = explode(path, method=method, source=name, confidence=conf,
                                only_class=args.submission_class)
        print(f"[layers] member {name:<12} {len(members[name]):,} atom "
              f"correspondences over {members[name].mnxr.nunique():,} reactions"
              + (f"  [{args.submission_class}]" if args.submission_class else ""),
              flush=True)
    coef = load_coefficients(Path(args.reac_prop)) if args.reac_prop else None
    if coef is not None:
        n = sum(1 for s, _p in coef.values() if any(c > 1 for c in s.values()))
        print(f"[layers] coefficients for {len(coef):,} reactions, {n:,} of which write a "
              f"substrate more than once", flush=True)
    df, tally = fuse_members(members, coefficients=coef)
    df.to_parquet(args.out, index=False)
    print(f"\n[layers] fused -> {args.out}  ({len(df):,} rows, "
          f"{df.mnxr.nunique():,} reactions)")
    for k, v in sorted(tally.items(), key=lambda x: -x[1]):
        print(f"    {k:<22} {v:>9,}")
    return 0


def cmd_stack(args):
    layers = []
    for s in args.layer:
        name, path, method, conf = _spec(s)
        p = Path(path)
        if not p.exists():
            print(f"[layers] layer {name}: {p} absent -- treated as an empty layer, "
                  f"which is a coverage gap and not a failure", flush=True)
            layers.append((name, None))
            continue
        d = pd.read_parquet(p)
        # A packed layer writes its three lists as comma-joined STRINGS and a fused one
        # writes one row per correspondence. Asked as "is this integer yet", never as "is
        # this dtype `object`": pandas stopped backing string columns with `object` and
        # the old test silently stopped exploding, which loads a packed layer with its
        # indices still strings and only fails several gates later.
        if "sub_idx" in d.columns and not pd.api.types.is_integer_dtype(d["sub_idx"]):
            d = explode(p, method=method, source=name, confidence=conf)
        elif "method" not in d.columns:
            d["method"], d["source"], d["confidence"] = method, name, conf
        layers.append((name, d[list(ATOM_COLS)]))
        print(f"[layers] layer {name:<12} {len(d):,} rows over "
              f"{d.mnxr.nunique():,} reactions", flush=True)

    df, report = stack(layers)
    df.to_parquet(args.out, index=False)

    print("\n" + "=" * 68)
    print("ADDITIVE LAYER STACK")
    print("=" * 68)
    for r in report:
        print(f"  {r['layer']:<12} offered {r['offered']:>9,}  added {r['added']:>9,}"
              f"  (+{r['rxn']:,} reactions)")
    print(f"\n  TOTAL       {len(df):,} atom correspondences over "
          f"{df.mnxr.nunique():,} reactions")
    print("\n  per element:")
    for X in ELEMENTS:
        g = df[df.element == X]
        print(f"    {X}: {len(g):>9,} correspondences  {g.mnxr.nunique():>7,} reactions")
    print("\n  provenance:")
    print(df.method.value_counts().to_string())
    print(f"\nwrote {args.out}")
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("fuse"); p.set_defaults(fn=cmd_fuse)
    p.add_argument("--member", action="append", required=True,
                   help="name=path[,method[,confidence]] -- an extractor pairs parquet")
    p.add_argument("--reac-prop", default=None,
                   help="MetaNetX reac_prop.tsv, read for stoichiometric coefficients "
                        "ONLY. A participant written c times is c copies sharing one atom "
                        "identity, so one of its atoms may reach up to c destinations. "
                        "Omit and every coefficient is 1, which is what the fusion did "
                        "before this existed -- a doubled participant then keeps one "
                        "destination and the other real edge is dropped.")
    p.add_argument("--submission-class", default=None,
                   choices=("whole", "completed", "reduced"),
                   help="keep only the rows this class of submission produced. One pass "
                        "puts all three in every member's table, so this is what a layer "
                        "IS now -- and it is a better key than 'which pass wrote the "
                        "file', because it stops layer membership depending on "
                        "scheduling")
    p.add_argument("--out", required=True)

    p = sub.add_parser("audit"); p.set_defaults(fn=cmd_audit)
    p.add_argument("--pairs", required=True,
                   help="a fused or stacked correspondence table")
    p.add_argument("--reac-prop", default=None,
                   help="MetaNetX reac_prop.tsv. Without it a duplicated participant "
                        "reads as a violation, because nothing else says a reaction "
                        "writes one metabolite twice")
    p.add_argument("--worklist", default=None,
                   help="the aam_worklist seam. Reports how many reactions writing a "
                        "substrate more than once were submitted COLLAPSED, and so "
                        "cannot express a split whatever the fusion allows. Needs "
                        "--reac-prop.")
    p.add_argument("--out", default=None, help="per-reaction violation counts")
    p.add_argument("--max-reactions", type=int, default=None,
                   help="refuse when more reactions than this carry an unexplained "
                        "violation. Omit to report without gating")

    p = sub.add_parser("stack"); p.set_defaults(fn=cmd_stack)
    p.add_argument("--layer", action="append", required=True,
                   help="name=path[,method[,confidence]] -- IN ORDER, most trusted first")
    p.add_argument("--out", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
