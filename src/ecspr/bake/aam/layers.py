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
import sys
from collections import defaultdict
from pathlib import Path

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


def fuse_members(members: dict, single_credit: float = SINGLE_MEMBER_CREDIT):
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
        picks = {}
        for name, lst in per_member.items():
            best = max(lst, key=lambda t: t[2])
            picks[name] = best
        srcs = "+".join(sorted(picks))
        distinct = {(p, pi) for p, pi, _w, _c in picks.values()}

        if len(picks) == 1:
            name = next(iter(picks))
            pm, p_idx, _w, c = picks[name]
            rows.append((mnxr, el, sm, pm, s_idx, p_idx, single_credit,
                         f"{name}_only", name, c * single_credit))
            tally[f"{name}_only"] += 1
        elif len(distinct) == 1:
            pm, p_idx = distinct.pop()
            cbar = sum(c for *_x, c in picks.values()) / len(picks)
            rows.append((mnxr, el, sm, pm, s_idx, p_idx, 1.0,
                         "consensus", srcs, cbar))
            tally["consensus"] += 1
        else:
            confs = {n: c for n, (*_x, c) in picks.items()}
            wsum = sum(v for v in confs.values() if v == v)
            if not (wsum > 0):
                wsum = float(len(picks))
                confs = {n: 1.0 for n in picks}
            for name, (pm, p_idx, _w, _c) in picks.items():
                c = confs.get(name, 1.0)
                if c != c:
                    c = 1.0
                rows.append((mnxr, el, sm, pm, s_idx, p_idx, c / wsum,
                             "disagree_diluted", srcs, c / wsum))
            tally["disagree_diluted"] += 1

    df = pd.DataFrame(rows, columns=list(ATOM_COLS))
    return df, dict(tally)


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
    df, tally = fuse_members(members)
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
        if "sub_idx" in d.columns and d["sub_idx"].dtype == object:
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
    p.add_argument("--submission-class", default=None,
                   choices=("whole", "completed", "reduced"),
                   help="keep only the rows this class of submission produced. One pass "
                        "puts all three in every member's table, so this is what a layer "
                        "IS now -- and it is a better key than 'which pass wrote the "
                        "file', because it stops layer membership depending on "
                        "scheduling")
    p.add_argument("--out", required=True)

    p = sub.add_parser("stack"); p.set_defaults(fn=cmd_stack)
    p.add_argument("--layer", action="append", required=True,
                   help="name=path[,method[,confidence]] -- IN ORDER, most trusted first")
    p.add_argument("--out", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
