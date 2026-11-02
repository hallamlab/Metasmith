#!/usr/bin/env python3
"""Why ECSPr cannot call the direction of a glycogen-module overexpression.

Three findings, each printed as its own section, on the five glycogen-module genes of the
eydallin cohort measured against `e_coli_ag1`:

1. EVIDENCE. The three reactions that own the glycogen node carry `dir_tier=0`
   (`no_evidence`, ratio 1.0). Not a coverage accident -- MetaNetX writes glycogen as a
   fixed-formula molecule with no polymer acceptor, so the equations are unbalanced and
   both thermo members structurally refuse them. Where evidence DOES exist (malP's
   maltodextrin arm) it is standard-state dG', which favours chain elongation and is
   therefore physiologically backwards for a phosphorylase.

2. MONOTONICITY. `gm = ratio * gp`, so an E_r rescale multiplies both diode branches by
   the same factor and the diode's asymmetry is scale-invariant. Effective conductance is
   then non-decreasing in E_r whatever the ratios are (Rayleigh). Curating the module's
   directions takes malP's two-point response from +0.484 to +0.036 -- it mutes the false
   positive and cannot produce the negative a 60.8%-of-WT phenotype needs.

3. WHAT IS TWO-SIDED -- nothing here. Gross throughput at the glycogen node is the only
   readout that produces a negative at all (malP, under the curated reference), and
   section 4 shows that negative is an upstream competition artifact rather than a
   direction call. Five readouts were tried (two-point conductance, leak draw, gross
   throughput, biomass-precursor share, flux partition) x two references x two node
   definitions: agreement ranges 1/5 to 3/5 and WHICH genes are right changes with the
   choice, which on n=5 is noise, not an instrument.

4. WHY glgP AND glgB STAY WRONG, and they are different failures.
   glgP owns exactly the two glycogen-terminal reactions with no direction evidence. Give
   it the correct direction and its edges carry 1.9e-6 of the glucose->glycogen current
   against glgA's 1.0 -- so the two-point response collapses to exactly 0.000. Correct
   direction makes glgP INVISIBLE, not negative: a gene whose only role is to drain the
   target cannot register on a probe that asks how well carbon reaches it.
   glgB is not a direction problem. MNXR145021 has good evidence (tier 2, dG'=-6.37) and
   it is right. MetaNetX carries exactly two glucan species with no chain length, so a
   branching enzyme moves carbon between two nodes that are one biological pool: pool them
   and glgB's effect falls to +0.014 (the physically correct answer for a transferase that
   neither makes nor destroys glucan), separate them and every readout scores the transfer
   as activity. The 26.4%-of-WT phenotype is a chain-length effect and no atom-transfer
   network has a variable for it.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/glycogen_direction_probe.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ecspr.model.build import (load_pairs, load_direction_ratios,                  # noqa: E402
                               graph_from_pairs, reaction_currents)
from ecspr.model.graph import Terminal, attach_leak, solve, OMEGA                  # noqa: E402
import bake_pairs                                                                  # noqa: E402

BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
LOOKUPS = ROOT / "data/fabfos/processed/lookups"
RUNS = ROOT / "data/fabfos/runs"

SOURCE_MNXM = "MNXM1364061"      # D-glucose
GLYCOGEN_MNXM = "MNXM738130"
BRANCHED_MNXM = "MNXM8348"       # "Branching glycogen" -- the same biological pool

# gene -> (its MNXRs in the host, measured glycogen as % of wild type)
GENES = {
    "glgC": (["MNXR145050"], 453.3),
    "glgA": (["MNXR145046"], 327.9),
    "glgB": (["MNXR145021"], 26.4),
    "glgP": (["MNXR145036", "MNXR145038"], 59.8),
    "malP": (["MNXR145036", "MNXR145038", "MNXR145632", "MNXR145636", "MNXR145639"], 60.8),
}

HARD = 1e6
CURATED = {
    "MNXR145036": HARD,
    "MNXR145038": HARD,
    "MNXR145639": HARD,
    "MNXR145632": 1 / HARD,
    "MNXR145636": 1 / HARD,
    "MNXR145046": 1 / HARD,
}


def evidence_table(mnxrs) -> pd.DataFrame:
    ann = pd.read_parquet(BAKE / "seams/direction_annotation.parquet")
    eq = pd.read_parquet(BAKE / "seams/direction_member_eq.parquet")
    db = pd.read_parquet(BAKE / "seams/direction_member_dgbyg.parquet")
    rx = pd.read_parquet(LOOKUPS / "reactions.parquet")[["mnxr", "equation", "is_balanced"]]
    out = (ann[ann.mnxr.isin(mnxrs)][["mnxr", "ratio", "dir_tier", "dir_method", "dG_prime"]]
           .merge(eq[["mnxr", "reason"]].rename(columns={"reason": "eq"}), on="mnxr")
           .merge(db[["mnxr", "reason"]].rename(columns={"reason": "dgbyg"}), on="mnxr")
           .merge(rx, on="mnxr", how="left"))
    return out


def resolve(eqn: str, names: dict) -> str:
    import re
    return re.sub(r"(MNXM[0-9A-Za-z]+)", lambda m: names.get(m.group(1), m.group(1)), eqn)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--leak", type=float, default=1e-6)
    args = p.parse_args()
    pd.set_option("display.width", 250)

    module = sorted({r for rs, _ in GENES.values() for r in rs})

    print("=" * 100)
    print("1. DIRECTION EVIDENCE for the glycogen module")
    print("=" * 100)
    ev = evidence_table(module)
    mets = pd.read_parquet(LOOKUPS / "metabolites.parquet", columns=["mnxm", "name"])
    names = dict(zip(mets.mnxm, mets.name))
    for r in ev.itertuples():
        print(f"\n{r.mnxr}  ratio={r.ratio:<12.6g} tier={r.dir_tier} {r.dir_method:<16}"
              f" dG'={r.dG_prime:+.3f}  balanced={r.is_balanced or '-'}")
        print(f"    eq={r.eq}  dgbyg={r.dgbyg}")
        print(f"    {resolve(r.equation, names)}")

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    baked = load_direction_ratios(bake_pairs.direction_ratios())
    curated = dict(baked, **CURATED)
    host = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}

    def graph(weights, ratios):
        return graph_from_pairs(pairs, args.element, weights, ratios)

    def two_point(weights, ratios) -> float:
        g = graph(weights, ratios)
        src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
        snk = Terminal.metabolite(g, GLYCOGEN_MNXM, label="glycogen")
        if src.missing or snk.missing:
            raise SystemExit("terminal missing")
        return float(solve(g, src, snk).total)

    def throughput(weights, ratios, mnxms=(GLYCOGEN_MNXM,)) -> float:
        g = graph(weights, ratios)
        src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
        g2, _ = attach_leak(g, None, leak=args.leak)
        sol = solve(g2, Terminal(src.label, frozenset(src.nodes), src.metabolites, src.missing),
                    Terminal.of_nodes("OMEGA", [OMEGA]))
        i, o = sol._boundary_flux([a for m in mnxms for a in g.atoms_of(m)])
        return max(i, o)

    print("\n" + "=" * 100)
    print(f"2. TWO-POINT Ieff(glucose -> glycogen), fold {args.fold} -- monotone by Rayleigh")
    print("=" * 100)
    print("   Curating direction mutes the false positives toward zero and never past it.\n")
    rows = []
    for label, ratios in [("baked", baked), ("curated", curated)]:
        b = two_point(base_w, ratios)
        for gene, (rs, pct) in GENES.items():
            v = two_point(dict(base_w, **{r: args.fold for r in rs}), ratios)
            rows.append(dict(ref=label, gene=gene, pct_wt=pct, log2fc=np.log2(v / b)))
        print(f"   base Ieff ({label}) = {b:.6f}")
    print()
    _report(pd.DataFrame(rows))

    print("\n" + "=" * 100)
    print(f"3. THROUGHPUT at the glycogen node, fold {args.fold} -- two-sided")
    print("=" * 100)
    rows = []
    for label, ratios in [("baked", baked), ("curated", curated)]:
        b = throughput(base_w, ratios)
        for gene, (rs, pct) in GENES.items():
            v = throughput(dict(base_w, **{r: args.fold for r in rs}), ratios)
            rows.append(dict(ref=label, gene=gene, pct_wt=pct, log2fc=np.log2(v / b)))
        print(f"   base throughput ({label}) = {b:.6e}")
    print()
    _report(pd.DataFrame(rows))

    print("\n" + "=" * 100)
    print("4a. glgP -- correct direction makes it INVISIBLE, not negative")
    print("=" * 100)
    print("   Carbon current per module reaction in the CURATED two-point solve.")
    print("   glgP's two edges carry the diode's reverse leak; glgA and glgC carry all of it.\n")
    g = graph_from_pairs(pairs, args.element, base_w, curated, with_provenance=True)
    sol = solve(g, Terminal.metabolite(g, SOURCE_MNXM), Terminal.metabolite(g, GLYCOGEN_MNXM))
    cur = reaction_currents(g, sol).reindex(module)
    for r, v in cur.items():
        owners = ",".join(k for k, (rs, _) in GENES.items() if r in rs)
        print(f"   {r}  I = {v:.4e}   {owners}")

    print("\n" + "=" * 100)
    print("4b. glgB -- a chain-length gap, not a direction gap")
    print("=" * 100)
    print("   Throughput log2FC with Glycogen and 'Branching glycogen' kept apart vs pooled.")
    print("   Pooling is what a branching enzyme actually does; its effect then ~vanishes.\n")
    rows = []
    for label, mnxms in [("separate", (GLYCOGEN_MNXM,)),
                         ("pooled", (GLYCOGEN_MNXM, BRANCHED_MNXM))]:
        b = throughput(base_w, curated, mnxms)
        for gene, (rs, pct) in GENES.items():
            v = throughput(dict(base_w, **{r: args.fold for r in rs}), curated, mnxms)
            rows.append(dict(ref=label, gene=gene, pct_wt=pct, log2fc=np.log2(v / b)))
    _report(pd.DataFrame(rows))


def _report(df: pd.DataFrame):
    df["want"] = np.where(df.pct_wt >= 100, "+", "-")
    df["sign"] = np.where(df.log2fc > 0, "+", "-")
    df["ok"] = df["sign"] == df["want"]
    piv = df.pivot_table(index=["gene", "pct_wt", "want"], columns="ref", values="log2fc")
    print(piv.to_string(float_format=lambda x: f"{x:+.5f}"))
    for ref, g in df.groupby("ref"):
        print(f"   {ref:8} sign agreement {g.ok.sum()}/{len(g)}")


if __name__ == "__main__":
    main()
