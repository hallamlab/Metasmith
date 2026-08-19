"""Adjudicate the whole reaction universe BEFORE any mapper runs.

WHY THIS STEP EXISTS
--------------------
Until now the three AAM members each derived their own universe from
`lookup::reactions` -- "every row whose rxn_smiles is not null and is under 8,000
characters" -- and everything else simply did not appear. A reaction that produced no
pairs and a reaction that was never attempted came out of the build looking identical,
so "did we cover it" was only answerable by re-running the thing that dropped it. The
deployed chain this tree ports did not work that way: it classified the whole 83,796
reaction universe first and mapped only the partition that could bank, which is the
only reason its accounting adds up reaction by reaction.

So: one row per MNXR, always, with a VERDICT from a closed set and, where the reaction
is blocked, the FAMILY of each blocker. Members read this table instead of deriving a
universe, so the three provably see one list; the rescue lanes read the families, so
each lever's target set is a number rather than a guess; and the tier-4 gate reads the
verdict, so a reaction we miss carries the reason we missed it.

THE TWO SIZE BOUNDS ARE DIFFERENT BOUNDS
----------------------------------------
`too_long` is the 8,000-CHARACTER cap the neural members already had; `oversize` is a
cap on the reaction's ATOM count. They refuse different reactions and exist for
different reasons, so merging them would make the atom threshold unmovable -- you could
not raise it without also raising a limit that is about transformer context rather than
about cost.

Each threshold's warrant is a measured yield curve, taken by joining reaction size to
the reactions the deployed bake banked. Under the expanded count: 98-99% up to 600, then
12.8% at 600-800, 1.3% at 800-1,200 and zero above. Cutting at 600 removes 0.9% of the
buildable universe while removing the reactions that cost minutes each and OOM-killed
the LocalMapper lane twice. It is a named constant because it is a judgement call, not
a fact.

AND THE COUNT ITSELF WAS WRONG, WHICH IS WHY THERE ARE NOW THREE CONSTANTS
--------------------------------------------------------------------------
`rxn_smiles` is a stoichiometric EXPANSION: a coefficient of 16 writes the metabolite
sixteen times. So the atom cap has been measuring how many times a molecule APPEARS
rather than how much distinct chemistry a mapper must attend to -- and nitrogenase,
which hydrolyses 16 ATP, was refused at 1,236 atoms while the acetylene-reduction proxy
for exactly that chemistry sailed through.

`collapse` writes each distinct molecule once per side, and a reaction the expanded
measure refuses gets a second reading under `COLLAPSED_ATOM_LIMIT`. A reaction the
expanded measure ADMITS is untouched, byte for byte, which is what makes "coverage may
only go up" a property of the construction: 57,061 mappable before, 57,515 after, none
lost. Both counts are kept on every row so either curve stays recomputable.

VERDICTS ARE ASSIGNED IN GATE ORDER and only `oversize` / `too_long` remove anything
that the current graph would otherwise map. Everything else names a reaction no mapper
could ever have seen. That asymmetry is deliberate: this step is meant to make the
build's coverage legible, not to shrink it.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SMILES_LEN_LIMIT = 8000

ATOM_LIMIT = 600

NEURAL_ADMITS = ("mappable",)
INDIGO_ADMITS = ("mappable", "oversize")

COLLAPSED_ATOM_LIMIT = 600

VERDICTS = (
    "mappable",
    "oversize",
    "too_long",
    "blocked_no_structure",
    "non_molecule",
    "no_transfer",
    "pseudo_reaction",
    "unparseable_equation",
)

FAMILIES = ("non_molecule", "electron_carrier", "acyl_carrier", "trna_holo",
            "polymer", "generic_rgroup", "other_structureless")

_F = re.compile
_FAMILY_RX = [
    ("non_molecule", _F(
        r"^(unknown|carbon|e\(-\)|e-|hnu|hn|h\N{GREEK SMALL LETTER NU}|photon|light"
        r"|biomass|.*\bbiomass\b.*)$", re.I)),
    ("electron_carrier", _F(
        r"ferredoxin|flavodoxin|cytochrome|rubredoxin|adrenodoxin|thioredoxin"
        r"|glutaredoxin|electron[- ]transfer flavoprotein|hemoprotein reductase"
        r"|^an? (ubi|mena|demethylmena)quino[ln]$|plastoquino[ln]|plastocyanin", re.I)),
    ("acyl_carrier", _F(r"\bacp\b|acyl.?carrier|\bcoa\b|coenzyme a", re.I)),
    ("trna_holo", _F(r"\btrna\b|\bholo-|\bapo-|charged .*rna", re.I)),
    ("polymer", _F(
        r"starch|chitin|glycogen|cellulose|dextran|amylose|amylopectin|glucan|mannan"
        r"|xylan|peptidoglycan|polymer|oligosaccharide|polysaccharide|\(n\)|\(n\+1\)"
        r"|\bn\+1\b|glycoconjugate|lipopolysaccharide|teichoic", re.I)),
    ("generic_rgroup", _F(
        r"^(a|ah2|d|dh2|nad|nadh|idh\d*)$|acceptor|donor|\bprotein\b|enzyme-\w+ complex"
        r"|^an? alcohol$|^an? aldehyde$|^an? carboxylate$|r-group|residue", re.I)),
]


def family_of(name) -> str:
    n = str(name or "").strip()
    if not n:
        return "other_structureless"
    for fam, rx in _FAMILY_RX:
        if rx.search(n):
            return fam
    return "other_structureless"


def count_atoms(rxn_smiles: str):
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    m = Chem.MolFromSmiles(rxn_smiles.replace(">>", "."), sanitize=False)
    return None if m is None else m.GetNumAtoms()


def collapse(rxn_smiles: str):
    if not rxn_smiles or ">>" not in rxn_smiles:
        return None
    lhs, _, rhs = rxn_smiles.partition(">>")
    return ".".join(_uniq(_components(lhs))) + ">>" + ".".join(_uniq(_components(rhs)))


def _components(side: str):
    out, start, depth = [], 0, 0
    for i, ch in enumerate(side):
        if ch in "[(":
            depth += 1
        elif ch in "])":
            depth -= 1
        elif ch == "." and depth == 0:
            out.append(side[start:i])
            start = i + 1
    out.append(side[start:])
    return [c for c in out if c]


def _uniq(items):
    seen, out = set(), []
    for c in items:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def adjudicate(reactions: pd.DataFrame, name_of: dict, atom_limit: int,
               char_limit: int,
               collapsed_atom_limit: int | None = None) -> pd.DataFrame:
    if collapsed_atom_limit is None:
        collapsed_atom_limit = COLLAPSED_ATOM_LIMIT
    rows = []
    for i, r in enumerate(reactions.itertuples(index=False)):
        subs = list(r.substrates) if r.substrates is not None else []
        prods = list(r.products) if r.products is not None else []
        blockers = list(r.blockers) if r.blockers is not None else []
        fams = sorted({family_of(name_of.get(m)) for m in blockers})
        smi = r.rxn_smiles if isinstance(r.rxn_smiles, str) and r.rxn_smiles else None

        atoms, chars = None, (len(smi) if smi else None)
        if smi and chars <= char_limit:
            atoms = count_atoms(smi)

        smi_c = atoms_c = chars_c = None
        collapsed = False
        would_refuse = smi is not None and (
            chars > char_limit or atoms is None or atoms > atom_limit)
        if would_refuse:
            smi_c = collapse(smi)
            if smi_c is not None:
                chars_c = len(smi_c)
                if chars_c <= char_limit:
                    atoms_c = count_atoms(smi_c)

        if int(r.n_blockers) < 0:
            verdict = "unparseable_equation"
        elif not subs or not prods:
            verdict = "pseudo_reaction"
        elif blockers:
            if "non_molecule" in fams:
                verdict = "non_molecule"
            elif Counter(subs) == Counter(prods):
                verdict = "no_transfer"
            else:
                verdict = "blocked_no_structure"
        elif smi is None:
            verdict = "blocked_no_structure"
        elif chars <= char_limit and atoms is not None and atoms <= atom_limit:
            verdict = "mappable"
        elif (chars_c is not None and chars_c <= char_limit
                and atoms_c is not None and atoms_c <= collapsed_atom_limit):
            verdict, collapsed = "mappable", True
        elif chars > char_limit:
            verdict = "too_long"
        else:
            verdict = "oversize"

        if (verdict == "oversize" and not collapsed and smi_c is not None
                and chars_c is not None and chars_c <= char_limit):
            collapsed = True

        rows.append(dict(
            mnxr=r.mnxr, verdict=verdict, atoms=atoms, chars=chars,
            atoms_collapsed=atoms_c, chars_collapsed=chars_c, collapsed=collapsed,
            n_blockers=int(r.n_blockers), blockers=blockers, blocker_families=fams,
            is_transport=str(r.is_transport), is_balanced=str(r.is_balanced),
            classifs=str(r.classifs),
            rxn_smiles=(smi_c if collapsed else smi),
        ))
        if (i + 1) % 20000 == 0:
            print(f"[worklist]   adjudicated {i + 1:,}/{len(reactions):,}", flush=True)
    return pd.DataFrame(rows)


SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("verdict", pa.string()),
    ("atoms", pa.int32()), ("chars", pa.int32()),
    ("atoms_collapsed", pa.int32()), ("chars_collapsed", pa.int32()),
    ("collapsed", pa.bool_()),
    ("n_blockers", pa.int32()),
    ("blockers", pa.list_(pa.string())), ("blocker_families", pa.list_(pa.string())),
    ("is_transport", pa.string()), ("is_balanced", pa.string()),
    ("classifs", pa.string()), ("rxn_smiles", pa.string()),
])


def load(path) -> pd.DataFrame:
    return pd.read_parquet(path)


def mappable(path, admits=NEURAL_ADMITS) -> dict:
    d = pd.read_parquet(path, columns=["mnxr", "verdict", "rxn_smiles"])
    d = d[d["verdict"].isin(admits) & d["rxn_smiles"].notna()]
    return dict(zip(d["mnxr"], d["rxn_smiles"]))


def cmd_build(args):
    rx = pd.read_parquet(args.reactions)
    mets = pd.read_parquet(args.metabolites, columns=["mnxm", "name"])
    name_of = dict(zip(mets["mnxm"], mets["name"]))
    print(f"[worklist] {len(rx):,} reactions, {len(name_of):,} metabolite names",
          flush=True)

    wl = adjudicate(rx, name_of, args.atom_limit, args.char_limit,
                    args.collapsed_atom_limit)
    pq.write_table(pa.Table.from_pandas(wl, schema=SCHEMA, preserve_index=False),
                   args.out, compression="zstd")

    vc = wl["verdict"].value_counts()
    fam = Counter(f for fs in wl["blocker_families"] for f in fs)
    lines = ["kind\tkey\tn"]
    for v in VERDICTS:
        lines.append(f"verdict\t{v}\t{int(vc.get(v, 0))}")
    for f in FAMILIES:
        lines.append(f"blocker_family\t{f}\t{int(fam.get(f, 0))}")
    built = wl[wl["atoms"].notna()]
    for lo, hi in ((0, 300), (300, 600), (600, 1200), (1200, 100000)):
        n = int(((built["atoms"] >= lo) & (built["atoms"] < hi)).sum())
        lines.append(f"atom_bin\t{lo}-{hi}\t{n}")
    coll = wl[wl["atoms_collapsed"].notna()]
    for lo, hi in ((0, 300), (300, 600), (600, 1200), (1200, 100000)):
        n = int(((coll["atoms_collapsed"] >= lo) & (coll["atoms_collapsed"] < hi)).sum())
        lines.append(f"collapsed_atom_bin\t{lo}-{hi}\t{n}")
    recovered = int(((wl["verdict"] == "mappable") & wl["collapsed"]).sum())
    lines.append(f"collapse\trecovered\t{recovered}")
    lines.append(f"collapse\twritten_collapsed\t{int(wl['collapsed'].sum())}")
    lines.append(f"collapse\tattempted\t{int(wl['atoms_collapsed'].notna().sum())}")
    lines.append(f"route\tindigo_only\t{int((wl['verdict'] == 'oversize').sum())}")
    lines.append(f"limit\tatom_limit\t{args.atom_limit}")
    lines.append(f"limit\tchar_limit\t{args.char_limit}")
    lines.append(f"limit\tcollapsed_atom_limit\t{args.collapsed_atom_limit}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("WORKLIST -- one verdict per reaction, closed set")
    print("=" * 60)
    for v in VERDICTS:
        print(f"  {v:<24} {int(vc.get(v, 0)):>8,}")
    unknown = set(wl["verdict"]) - set(VERDICTS)
    if unknown:
        raise SystemExit(f"[worklist] verdict outside the closed set: {sorted(unknown)}")
    print(f"  {'TOTAL':<24} {len(wl):>8,}")
    print(f"\n  of which mappable only after a stoichiometric collapse: {recovered:,}")
    print(f"  over the atom cap, so Indigo alone: "
          f"{int((wl['verdict'] == 'oversize').sum()):,}")
    print("\nblocker families (metabolite-blocked reactions may carry several):")
    for f in FAMILIES:
        print(f"  {f:<24} {int(fam.get(f, 0)):>8,}")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


OUTCOMES = (
    "banked",
    "banked_partial",
    "partial_declined",
    "redox_emptied",
    "mapped_nothing",
    "rescued_nothing",
    "rescue_declined",
    "oversize", "too_long", "non_molecule", "no_transfer", "pseudo_reaction",
    "unparseable_equation",
)

PARTIAL_SOURCE = "partial"


def cmd_close(args):
    wl = pd.read_parquet(args.worklist, columns=["mnxr", "verdict", "blocker_families"])
    pairs = pd.read_parquet(args.pairs, columns=["mnxr", "element", "method", "source"])

    rescued = set()
    if args.rescued:
        rescued = set(pd.read_parquet(args.rescued, columns=["mnxr"])["mnxr"])

    offered = set()
    if args.forecast:
        f = pd.read_parquet(args.forecast, columns=["base_mnxr", "offer"])
        offered = set(f.loc[f["offer"].astype(bool), "base_mnxr"])

    redox_emptied = set()
    if args.redox_emptied and Path(args.redox_emptied).exists():
        redox_emptied = {l.strip() for l in Path(args.redox_emptied).read_text().splitlines()
                         if l.strip()}

    full = pairs[pairs["source"] != PARTIAL_SOURCE]
    banked_full = set(full["mnxr"])

    by_rxn = pairs.groupby("mnxr")
    n_pairs = by_rxn.size()
    els = by_rxn["element"].apply(lambda s: ",".join(sorted(set(s))))
    meths = by_rxn["method"].apply(lambda s: ",".join(sorted(set(s))))
    srcs = by_rxn["source"].apply(lambda s: ",".join(sorted(set(s))))
    banked = set(n_pairs.index)

    def outcome(mnxr, verdict):
        if mnxr in banked:
            return "banked" if mnxr in banked_full else "banked_partial"
        if mnxr in redox_emptied:
            return "redox_emptied"
        if mnxr in offered:
            return "partial_declined"
        if verdict == "mappable":
            return "mapped_nothing"
        if verdict == "blocked_no_structure":
            return "rescued_nothing" if mnxr in rescued else "rescue_declined"
        return verdict

    wl["rescue_completed"] = wl["mnxr"].isin(rescued)
    wl["outcome"] = [outcome(m, v) for m, v in zip(wl["mnxr"], wl["verdict"])]
    wl["n_pairs"] = wl["mnxr"].map(n_pairs).fillna(0).astype("int64")
    wl["elements"] = wl["mnxr"].map(els).fillna("")
    wl["methods"] = wl["mnxr"].map(meths).fillna("")
    wl["sources"] = wl["mnxr"].map(srcs).fillna("")
    wl.drop(columns=["blocker_families"]).to_parquet(args.out, index=False)

    unknown = set(wl["outcome"]) - set(OUTCOMES)
    if unknown:
        raise SystemExit(f"[worklist] outcome outside the closed set: {sorted(unknown)}")

    resc = wl[wl["rescue_completed"] & (wl["outcome"] == "banked")]
    n_consensus = int(resc["methods"].str.contains("consensus").sum())

    lines = ["kind\tkey\tn"]
    oc = wl["outcome"].value_counts()
    for o in OUTCOMES:
        lines.append(f"outcome\t{o}\t{int(oc.get(o, 0))}")
    for X in ("C", "N", "S", "P"):
        lines.append(f"element_reactions\t{X}\t"
                     f"{int(pairs.loc[pairs.element == X, 'mnxr'].nunique())}")
    lines.append(f"partial\toffered\t{len(offered)}")
    lines.append(f"partial\tpair_rows\t{int((pairs['source'] == PARTIAL_SOURCE).sum())}")
    lines.append(f"redox\temptied\t{len(redox_emptied)}")
    lines.append(f"rescue\tcompleted\t{len(rescued)}")
    lines.append(f"rescue\tbanked\t{len(resc)}")
    lines.append(f"rescue\tbanked_with_consensus\t{n_consensus}")
    lines.append(f"total\treactions\t{len(wl)}")
    lines.append(f"total\tpairs\t{len(pairs)}")
    Path(args.out_summary).write_text("\n".join(lines) + "\n")

    print("\n" + "=" * 60)
    print("LEDGER -- one outcome per reaction, closed set")
    print("=" * 60)
    for o in OUTCOMES:
        print(f"  {o:<24} {int(oc.get(o, 0)):>8,}")
    print(f"  {'TOTAL':<24} {len(wl):>8,}")
    print(f"\nrescue: {len(rescued):,} completed, {len(resc):,} banked, "
          f"{n_consensus:,} of those carry a consensus correspondence "
          f"(the deployed table has zero -- every rescued reaction there is one "
          f"member at half weight)")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("close"); p.set_defaults(fn=cmd_close)
    p.add_argument("--worklist", required=True)
    p.add_argument("--pairs", required=True, help="the stacked aam_pairs parquet")
    p.add_argument("--rescued", default=None, help="the rescued universe parquet")
    p.add_argument("--forecast", default=None,
                   help="interm::aam_forecast. Its offered `base_mnxr` set is what the "
                        "partial lane was ASKED to reach, which is what distinguishes a "
                        "reaction the lane declined from one nothing tried. The lane's "
                        "own universe is the wrong table: it holds only what the lane "
                        "managed to build.")
    p.add_argument("--redox-emptied", default=None,
                   help="the redox repair's emptied-reaction list, one MNXR per line")
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)

    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--reactions", required=True, help="lookup::reactions parquet")
    p.add_argument("--metabolites", required=True, help="lookup::metabolites parquet")
    p.add_argument("--atom-limit", type=int, default=ATOM_LIMIT)
    p.add_argument("--char-limit", type=int, default=SMILES_LEN_LIMIT)
    p.add_argument("--collapsed-atom-limit", type=int, default=COLLAPSED_ATOM_LIMIT,
                   help="the cap on the DISTINCT-molecule count, applied only to "
                        "reactions the expanded measure would refuse")
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
