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
`too_long` is the 8,000-CHARACTER cap the neural members already had. `oversize` is a
cap on the reaction's ATOM count, and it is new. They refuse different reactions and
they exist for different reasons, so merging them would make the atom threshold
unmovable -- you could not raise it without also raising a limit that is about
transformer context rather than about cost.

The atom threshold's warrant is a measured yield curve, taken by joining reaction size
to the deployed tier-4 table: reactions under 300 atoms bank at 91-99%, 300-400 at
40.4%, 400-600 at 28.4%, 600-800 at 5.3%, 1,200-1,600 at 1.4%, and above 1,600 atoms
exactly zero of 124 banked. Cutting at 600 removes 0.80% of the buildable universe and
12 of the 54,382 reactions the deployed table banked -- 0.022% -- while removing the
reactions that cost minutes each and OOM-killed the LocalMapper lane twice. It is a
named constant because it is a judgement call, not a fact.

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

# The character cap the neural members have always applied. It is stated in both places
# rather than imported, because importing it here from a member would make the
# adjudication depend on a mapper module it is meant to precede. The two are kept honest
# at RUNTIME instead: `aam_neural_members.universe_from_worklist` re-checks every
# reaction it is handed and refuses if any exceeds its own constant, so a drift is a
# loud failure at the top of the lane rather than a quiet difference in coverage.
SMILES_LEN_LIMIT = 8000

# The atom cap. See the header for the yield curve that warrants 600.
ATOM_LIMIT = 600

VERDICTS = (
    "mappable",              # goes to the mapper lanes
    "oversize",              # buildable, but over ATOM_LIMIT atoms
    "too_long",              # buildable, but over SMILES_LEN_LIMIT characters
    "blocked_no_structure",  # >=1 participant has no structure; the rescue lanes' target
    "non_molecule",          # >=1 participant is not a molecule; nothing can stand in
    "no_transfer",           # blocked AND the two sides are the same multiset
    "pseudo_reaction",       # a side is empty -- an exchange/sink, not chemistry
    "unparseable_equation",  # reac_prop's equation did not parse
)

# Blocker families. Assigned per structureless participant, first match wins, and the
# order is the specificity order rather than an alphabet. These are what the rescue
# lanes aim at, so the names match the lanes: `carrier` takes acyl_carrier, the acceptor
# lane takes generic_rgroup, the ladder takes polymer, and `lane_carrier`'s widened arm
# takes trna_holo. `electron_carrier` is what the deterministic placeholder library
# already covers; `other_structureless` is the ordinary case the twin/transform/
# fragment/supplier lanes work on.
FAMILIES = ("non_molecule", "electron_carrier", "acyl_carrier", "trna_holo",
            "polymer", "generic_rgroup", "other_structureless")

_F = re.compile
_FAMILY_RX = [
    ("non_molecule", _F(
        r"^(unknown|carbon|e\(-\)|e-|hnu|hn|h\N{GREEK SMALL LETTER NU}|photon|light"
        r"|biomass|.*\bbiomass\b.*)$", re.I)),
    # The deterministic placeholder library's subjects: redox carriers whose body is
    # conserved and whose atoms never transit.
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
    """Heavy-plus-explicit atoms across the WHOLE reaction, both sides.

    `sanitize=False`: this is a size measurement, not a chemistry check, and refusing to
    count an unsanitizable reaction would silently hand it to the mapper as if it were
    small. `>>` becomes `.` so one parse covers both sides -- the mappers attend over the
    whole string, so the whole string is what costs.
    """
    from rdkit import Chem, RDLogger
    RDLogger.DisableLog("rdApp.*")
    m = Chem.MolFromSmiles(rxn_smiles.replace(">>", "."), sanitize=False)
    return None if m is None else m.GetNumAtoms()


def adjudicate(reactions: pd.DataFrame, name_of: dict, atom_limit: int,
               char_limit: int) -> pd.DataFrame:
    rows = []
    for i, r in enumerate(reactions.itertuples(index=False)):
        subs = list(r.substrates) if r.substrates is not None else []
        prods = list(r.products) if r.products is not None else []
        blockers = list(r.blockers) if r.blockers is not None else []
        fams = sorted({family_of(name_of.get(m)) for m in blockers})
        smi = r.rxn_smiles if isinstance(r.rxn_smiles, str) and r.rxn_smiles else None

        # ATOMS ARE COUNTED ONLY UNDER THE CHARACTER CAP, and that ordering is not an
        # optimisation. `rxn_smiles` is a stoichiometric EXPANSION -- a coefficient of
        # 1,000 writes the metabolite a thousand times -- so while no metabolite's SMILES
        # exceeds 3,199 characters, MNXR144749's reaction string is 80.7 MB. RDKit does
        # not return from parsing that in any useful time, and a run died on it: the lane
        # walked 20,000 reactions in three seconds and then spent an hour inside one call.
        # The char gate refuses those reactions anyway, so the parse was never needed;
        # counting first merely put the unbounded work ahead of the bound that excludes it.
        atoms, chars = None, (len(smi) if smi else None)
        if smi and chars <= char_limit:
            atoms = count_atoms(smi)

        if int(r.n_blockers) < 0:
            verdict = "unparseable_equation"
        elif not subs or not prods:
            verdict = "pseudo_reaction"
        elif blockers:
            # A non-molecule participant is terminal: there is nothing to stand in FOR,
            # so no lane and no threshold change the answer. It outranks the rest.
            if "non_molecule" in fams:
                verdict = "non_molecule"
            elif Counter(subs) == Counter(prods):
                verdict = "no_transfer"
            else:
                verdict = "blocked_no_structure"
        elif smi is None:
            # blockers empty but no SMILES: the lookup builder declined for some other
            # reason. Recorded rather than folded into a neighbouring class.
            verdict = "blocked_no_structure"
        elif chars > char_limit:
            verdict = "too_long"
        elif atoms is None or atoms > atom_limit:
            verdict = "oversize"
        else:
            verdict = "mappable"

        rows.append(dict(
            mnxr=r.mnxr, verdict=verdict, atoms=atoms, chars=chars,
            n_blockers=int(r.n_blockers), blockers=blockers, blocker_families=fams,
            is_transport=str(r.is_transport), is_balanced=str(r.is_balanced),
            classifs=str(r.classifs), rxn_smiles=smi,
        ))
        if (i + 1) % 20000 == 0:
            print(f"[worklist]   adjudicated {i + 1:,}/{len(reactions):,}", flush=True)
    return pd.DataFrame(rows)


SCHEMA = pa.schema([
    ("mnxr", pa.string()), ("verdict", pa.string()),
    ("atoms", pa.int32()), ("chars", pa.int32()), ("n_blockers", pa.int32()),
    ("blockers", pa.list_(pa.string())), ("blocker_families", pa.list_(pa.string())),
    ("is_transport", pa.string()), ("is_balanced", pa.string()),
    ("classifs", pa.string()), ("rxn_smiles", pa.string()),
])


def load(path) -> pd.DataFrame:
    return pd.read_parquet(path)


def mappable(path) -> dict:
    """`{mnxr -> rxn_smiles}` for the reactions the lanes are allowed to attempt.

    THE ONE READER EVERY MEMBER USES. A member that derived its own universe could drift
    from its siblings by one filter and the ensemble's disagreement rate would stop
    measuring disagreement between mappers.
    """
    d = pd.read_parquet(path, columns=["mnxr", "verdict", "rxn_smiles"])
    d = d[(d["verdict"] == "mappable") & d["rxn_smiles"].notna()]
    return dict(zip(d["mnxr"], d["rxn_smiles"]))


def cmd_build(args):
    rx = pd.read_parquet(args.reactions)
    mets = pd.read_parquet(args.metabolites, columns=["mnxm", "name"])
    name_of = dict(zip(mets["mnxm"], mets["name"]))
    print(f"[worklist] {len(rx):,} reactions, {len(name_of):,} metabolite names",
          flush=True)

    wl = adjudicate(rx, name_of, args.atom_limit, args.char_limit)
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
    lines.append(f"limit\tatom_limit\t{args.atom_limit}")
    lines.append(f"limit\tchar_limit\t{args.char_limit}")
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
    print("\nblocker families (metabolite-blocked reactions may carry several):")
    for f in FAMILIES:
        print(f"  {f:<24} {int(fam.get(f, 0)):>8,}")
    print(f"\nwrote {args.out} and {args.out_summary}", flush=True)
    return 0


# =====================================================================
# closing the ledger
# =====================================================================
# The spine says what each reaction was ALLOWED to do. The close says what it DID. Every
# MNXR keeps its row, so the three answers that used to look identical -- produced
# nothing, was never attempted, was refused for a named reason -- stay distinguishable
# after the build as well as before it.

OUTCOMES = (
    "banked",                  # the reaction contributed pairs to the final table
    "mapped_nothing",          # mappable, went to the lanes, no pair survived
    "rescued_nothing",         # completed by the rescue, still no pair survived
    "rescue_declined",         # blocked, and no lane could complete it
    "oversize", "too_long", "non_molecule", "no_transfer", "pseudo_reaction",
    "unparseable_equation",
)


def cmd_close(args):
    wl = pd.read_parquet(args.worklist, columns=["mnxr", "verdict", "blocker_families"])
    pairs = pd.read_parquet(args.pairs, columns=["mnxr", "element", "method", "source"])

    rescued = set()
    if args.rescued:
        rescued = set(pd.read_parquet(args.rescued, columns=["mnxr"])["mnxr"])

    by_rxn = pairs.groupby("mnxr")
    n_pairs = by_rxn.size()
    els = by_rxn["element"].apply(lambda s: ",".join(sorted(set(s))))
    meths = by_rxn["method"].apply(lambda s: ",".join(sorted(set(s))))
    srcs = by_rxn["source"].apply(lambda s: ",".join(sorted(set(s))))
    banked = set(n_pairs.index)

    def outcome(mnxr, verdict):
        if mnxr in banked:
            return "banked"
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

    # THE NUMBER THIS BUILD EXISTS TO MOVE. In the deployed table every rescue-derived
    # reaction is `<member>_only` at half weight, because only one mapper ever saw the
    # completed reaction. Here three do, so the ones they agree on are consensus -- and
    # that count is the improvement, stated rather than assumed.
    resc = wl[wl["rescue_completed"] & (wl["outcome"] == "banked")]
    n_consensus = int(resc["methods"].str.contains("consensus").sum())

    lines = ["kind\tkey\tn"]
    oc = wl["outcome"].value_counts()
    for o in OUTCOMES:
        lines.append(f"outcome\t{o}\t{int(oc.get(o, 0))}")
    for X in ("C", "N", "S", "P"):
        lines.append(f"element_reactions\t{X}\t"
                     f"{int(pairs.loc[pairs.element == X, 'mnxr'].nunique())}")
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


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("close"); p.set_defaults(fn=cmd_close)
    p.add_argument("--worklist", required=True)
    p.add_argument("--pairs", required=True, help="the stacked aam_pairs parquet")
    p.add_argument("--rescued", default=None, help="the rescued universe parquet")
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)

    p = sub.add_parser("build"); p.set_defaults(fn=cmd_build)
    p.add_argument("--reactions", required=True, help="lookup::reactions parquet")
    p.add_argument("--metabolites", required=True, help="lookup::metabolites parquet")
    p.add_argument("--atom-limit", type=int, default=ATOM_LIMIT)
    p.add_argument("--char-limit", type=int, default=SMILES_LEN_LIMIT)
    p.add_argument("--out", required=True)
    p.add_argument("--out-summary", required=True)
    return ap.parse_args()


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
