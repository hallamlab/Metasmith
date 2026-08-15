#!/usr/bin/env python3
"""Read the composed community measurement: who exchanges what, and is it measurable.

    python research/fabfos/examples/nostoc_ecspr_query.py corrinoid     # B12 pathway coverage per organism
    python research/fabfos/examples/nostoc_ecspr_query.py exchange      # what the bridges carry
    python research/fabfos/examples/nostoc_ecspr_query.py polarity      # logP split, for MS method choice

Three questions the products answer, kept together because they share one awkward join
(bridge table -> MetaNetX names -> chemistry) and one recurring correction.

THE CORRECTION, since every ranking here is exposed to it: bridge conductance is
``g0 * D * min(G_A,G_B) / k``, so ``min(G)`` makes a raw ranking substantially a DEGREE
ranking. Sorting by ``g_bridge`` returns central carbon metabolism from any pair of
organisms and says nothing about them. The asymmetry ``D`` is the size-free half and is
where a difference between two genomes actually shows; a mass floor keeps it from
surfacing metabolites that are merely rare. Report both or neither.

``polarity`` needs RDKit, which is deliberately NOT in the msm-fabfos env -- it is a
read-side convenience, not a pipeline dependency. Run that subcommand under an env that
has it. It also needs MetaNetX ``chem_prop.tsv`` for SMILES: 1.5M rows, and ``--chem-prop``
defaults to this repo's pinned copy under ``data/fabfos/originals/metanetx/``, which must
be materialised (``dvc checkout data/fabfos/originals/metanetx.dvc``). It used to name a
sibling worktree's copy; that repository is archived and the pinned one is the same bytes.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "data/fabfos/nostoc/ecspr"
NETS = OUT / "networks"
CHEM_PROP = REPO / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"

# Each organism appears in two of the three pairwise tables; M must agree between them.
PAIRS = ("NOS-ERY_bl-on", "NOS-RHI_bl-on", "ERY-RHI_bl-on")
ORFS = {"NOS": 5921, "ERY": 3184, "RHI": 4396}

# The corrin ring, split from the trunk it shares with siroheme. Everything upstream of
# precorrin-3 is also sirohaem/coenzyme-F430 chemistry, so an organism can carry the whole
# trunk with no capacity to build a corrin ring at all -- which is exactly what
# Erythrobacter does. Counting the trunk as B12 evidence turns an auxotroph into a producer.
RING = re.compile(r"precorrin-4|precorrin-5|precorrin-6|precorrin-7|precorrin-8|"
                  r"Co-precorrin|Cobalt-precorrin|Cobalt-factor|cobalt-dihydrosiro|"
                  r"Cobalt-siro|cobyrinate|cobyrate|cobinamide|cobalamin|cobamide", re.I)
TRUNK = re.compile(r"sirohydrochlorin|precorrin-1|precorrin-2|precorrin-3", re.I)
BOUND = re.compile(r"protein\]", re.I)


def _bridges(pair: str) -> pd.DataFrame:
    return pd.read_parquet(NETS / pair / "bridges.parquet")


def _per_organism(element: str = "C") -> pd.DataFrame:
    """M for every organism on one index, cross-checked across the tables that carry it."""
    M: dict[str, pd.Series] = {}
    for p in PAIRS:
        b = _bridges(p)
        b = b[b.element == element]
        for org, col in ((b.org_a.iloc[0], "M_a"), (b.org_b.iloc[0], "M_b")):
            s = b.set_index("metabolite")[col]
            if org in M:
                x, y = M[org].align(s, join="inner")
                assert np.allclose(x, y), f"{org} disagrees between pairwise tables"
                s = M[org].combine_first(s)
            M[org] = s
    df = pd.DataFrame(M).fillna(0.0)
    names = _bridges(PAIRS[0]).drop_duplicates("metabolite").set_index("metabolite")["name"]
    df.insert(0, "name", names.reindex(df.index))
    return df


def corrinoid(_args) -> int:
    df = _per_organism("C")
    cob = df[df.name.fillna("").str.contains(RING) & ~df.name.fillna("").str.contains(BOUND)]
    trunk = df[df.name.fillna("").str.contains(TRUNK)]
    print(f"corrin-ring + salvage metabolites: {len(cob)}   shared trunk: {len(trunk)}\n")
    for o in ("NOS", "RHI", "ERY"):
        print(f"  {o}: ring {int((cob[o] > 0).sum()):2d}/{len(cob)}   M={cob[o].sum():7.3f}"
              f"   per-1k-ORF={1000 * cob[o].sum() / ORFS[o]:.3f}"
              f"   | trunk {int((trunk[o] > 0).sum())}/{len(trunk)}")

    # The control that decides whether a gap is a pathway or just a smaller genome.
    both = df[(df.NOS > 0) & (df.RHI > 0)]
    base = (both.ERY == 0).mean()
    cob_both = cob[(cob.NOS > 0) & (cob.RHI > 0)]
    print(f"\n  ERY absent from {100 * base:.1f}% of the {len(both)} metabolites both others "
          f"carry (genome-wide baseline)")
    print(f"  ERY absent from {100 * (cob_both.ERY == 0).mean():.1f}% of the "
          f"{len(cob_both)} corrinoid metabolites both others carry")
    gap = cob[(cob.ERY == 0) & (cob.NOS > 0) & (cob.RHI > 0)]
    print(f"\n  the {len(gap)} ring steps ERY lacks that both others have:")
    print(gap[["name", "NOS", "RHI"]].sort_values("NOS", ascending=False).to_string())
    return 0


def exchange(args) -> int:
    for p in PAIRS:
        b = _bridges(p)
        live = b[b.bridged == 1]
        a, bb = b.org_a.iloc[0], b.org_b.iloc[0]
        print(f"\n{'=' * 96}\n{p}  ({a} vs {bb})   {len(live)} live bridges, "
              f"total g={live.g_bridge.sum():.1f}\n{'=' * 96}")
        for el in ("C", "N", "P", "S"):
            s = live[live.element == el]
            if not len(s):
                continue
            top = s.nlargest(args.top, "g_bridge")
            print(f"\n  --- {el}: {len(s)} bridges, g={s.g_bridge.sum():.1f}; "
                  f"top {args.top} carry {100 * top.g_bridge.sum() / s.g_bridge.sum():.1f}% "
                  f"(degree-driven -- see module docstring) ---")
            print(top[["name", "M_a", "M_b", "asymmetry", "g_bridge"]].to_string(index=False))
        s = live[(live.element == "C") & (live[["M_a", "M_b"]].max(axis=1) >= args.floor)]
        print(f"\n  --- C by ASYMMETRY, mass floor max(M) >= {args.floor} "
              f"({len(s)} bridges): what the genomes disagree about ---")
        print(s.nlargest(args.top, "asymmetry")[
            ["name", "M_a", "M_b", "asymmetry", "g_bridge"]].to_string(index=False))
    return 0


def polarity(args) -> int:
    try:
        from rdkit import Chem, RDLogger
        from rdkit.Chem import Crippen, Descriptors, rdMolDescriptors
    except ImportError:
        print("polarity needs RDKit; run this subcommand under an env that has it "
              "(see the module docstring)", file=sys.stderr)
        return 2
    RDLogger.DisableLog("rdApp.*")

    if not args.chem_prop.exists():
        print(f"missing {args.chem_prop} -- pass --chem-prop", file=sys.stderr)
        return 2
    cp = pd.read_csv(args.chem_prop, sep="\t", skiprows=388, low_memory=False)
    cp.columns = ["mnxm", "name", "reference", "formula", "charge", "mass", "InChI",
                  "InChIKey", "SMILES"]

    parts = []
    for p in PAIRS:
        b = _bridges(p)
        b = b[b.bridged == 1]
        g = b.groupby("metabolite").agg(g=("g_bridge", "sum"), name=("name", "first"))
        parts.append(g.assign(pair=p).reset_index())
    m = pd.concat(parts).merge(cp[["mnxm", "SMILES", "charge"]],
                               left_on="metabolite", right_on="mnxm", how="left")

    # A third of the bridges are not discrete small molecules -- protein-bound residues,
    # polymers, MetaNetX generic classes. They are legitimate NODES (two organisms really do
    # use them differently) and impossible MS TARGETS, so they get their own bucket rather
    # than being silently averaged into the polar one.
    notmol = re.compile(r"protein|\[.*subunit|residue|polysialic|starch|amylose|glycogen|"
                        r"cellulose|peptidoglycan|lipopolysaccharide|tRNA|mRNA|DNA|RNA|"
                        r"^a |^an |^fragments", re.I)
    m["measurable"] = ~(m.name.fillna("").str.contains(notmol)
                        | m.SMILES.isna() | m.SMILES.fillna("").str.contains(r"\*"))

    def props(s):
        mol = Chem.MolFromSmiles(s)
        if mol is None:
            return (np.nan, np.nan, np.nan)
        return (Crippen.MolLogP(mol), rdMolDescriptors.CalcTPSA(mol), Descriptors.MolWt(mol))

    uniq = m.loc[m.measurable, "SMILES"].dropna().drop_duplicates()
    print(f"computing descriptors for {len(uniq)} unique structures ...", flush=True)
    m = m.join(pd.DataFrame([props(s) for s in uniq], index=uniq.values,
                            columns=["logP", "TPSA", "MW"]), on="SMILES")
    m.loc[~m.measurable, ["logP", "TPSA", "MW"]] = np.nan

    def bucket(r):
        if np.isnan(r.logP):
            return "not-a-small-molecule"
        return ("polar (HILIC)" if r.logP < 0 else
                "mid (RP-C18 ok)" if r.logP < 3 else "non-polar (RP / lipid)")
    m["bucket"] = m.apply(bucket, axis=1)

    t = m.groupby("bucket").agg(n=("metabolite", "size"), g=("g", "sum"))
    t["% conductance"] = 100 * t.g / m.g.sum()
    print("\n=== all bridges, weighted by conductance ===")
    print(t[["n", "% conductance"]].sort_values("% conductance", ascending=False)
          .to_string(float_format=lambda x: f"{x:6.1f}"))

    mm = m[m.bucket != "not-a-small-molecule"]
    t = mm.groupby("bucket").agg(n=("metabolite", "size"), g=("g", "sum"))
    t["% conductance"] = 100 * t.g / mm.g.sum()
    print("\n=== measurable small molecules only ===")
    print(t[["n", "% conductance"]].to_string(float_format=lambda x: f"{x:6.1f}"))
    for cut in (0, 1, 3, 5):
        print(f"  conductance with logP < {cut}: "
              f"{100 * mm.loc[mm.logP < cut, 'g'].sum() / mm.g.sum():.1f}%")

    # Charge at pH 7 picks the ESI mode, which in practice constrains the assay more than
    # the column does.
    chg = pd.to_numeric(mm.charge, errors="coerce")
    b = pd.cut(chg, [-99, -2.5, -1.5, -0.5, 0.5, 99],
               labels=["<= -3", "-2", "-1", "0 (neutral)", "positive"])
    t = mm.groupby(b, observed=True).agg(n=("metabolite", "size"), g=("g", "sum"))
    t["% conductance"] = 100 * t.g / mm.g.sum()
    print("\n=== charge at pH 7 (drives ESI mode) ===")
    print(t[["n", "% conductance"]].to_string(float_format=lambda x: f"{x:6.1f}"))

    if args.out:
        m.to_parquet(args.out)
        print(f"\nwrote {args.out}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("corrinoid")
    e = sub.add_parser("exchange")
    e.add_argument("--top", type=int, default=15)
    e.add_argument("--floor", type=float, default=1.0,
                   help="minimum max(M_a,M_b) for the asymmetry ranking")
    q = sub.add_parser("polarity")
    q.add_argument("--chem-prop", type=Path, default=CHEM_PROP)
    q.add_argument("--out", type=Path, default=None)
    a = p.parse_args(argv)
    return {"corrinoid": corrinoid, "exchange": exchange, "polarity": polarity}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
