"""How much of the deployed bake would a concentration term move?

Decomposes the correction that turns dG'o into a physiological dG' into its two parts,
because they have completely different data requirements and the cheap one is often
mistaken for the whole thing:

  MOLECULARITY  RT * dn * ln(1 mM)          dn = sum(nu). Needs NO data at all -- it is
                                            eQuilibrator's `physiological_dg_prime`, and it
                                            is a constant 17.1 kJ/mol per net solute.
  SKEW          RT * sum(nu_i * ln(c_i/1mM)) Needs a measured concentration per participant.
                                            Zero by construction under uniform 1 mM.

Reported against the bake's own `dG_raw` (the posterior BEFORE shrinkage), because that is
the quantity a concentration term adds to. Movement is counted in DECADES of conductance
ratio, the unit the ratio is consumed in.

Water and the proton are excluded: eQuilibrator's prime potentials already account for them.
Dissolved gases are excluded and counted separately -- their activity is set by a partial
pressure, which is a different measurement from an intracellular pool.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from ecspr.bake.direction.refdata import load_mnxm_names, load_mnxr_stoich

RT = 8.314e-3 * 298.15
DECADE = RT * math.log(10.0)
DEFAULT_mM = 1.0

IMPLICIT = {"WATER", "MNXM1"}
GASES = {"MNXM735438": "O2", "MNXM13": "CO2", "MNXM1098": "N2", "MNXM1101872": "H2",
         "MNXM10917": "CO", "MNXM732448": "CH4"}
# A polymer or an unspecified acceptor has no free-solute concentration. Excluding it is
# the same assertion `substitute.py` makes about its standard term, and it is why the
# phosphorylase family needs the polymer budget balanced BEFORE a concentration term
# means anything there.
UNIT_ACTIVITY = {"MNXM738130", "MNXM8348", "MNXM727735", "MNXM725902", "BIOMASS",
                 "MNXM01", "MNXM8975"}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--reac-prop", required=True)
    ap.add_argument("--chem-prop", required=True)
    ap.add_argument("--conc", required=True)
    ap.add_argument("--annotation", required=True)
    ap.add_argument("--atom-pairs", required=True)
    ap.add_argument("--vocab", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)

    conc = {}
    with open(a.conc) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            conc[r["mnxm"]] = (float(r["conc_mM"]), float(r["log10_spread"]))
    stoich = load_mnxr_stoich(a.reac_prop)
    names = load_mnxm_names(a.chem_prop)
    ann = pd.read_parquet(a.annotation)
    # atom_pairs stores reactions as integer codes; vocab.parquet is the only thing that
    # knows which MNXR a code is. Joining on the raw code silently yields an empty
    # intersection with an accession column, which reads as "no reaction is in the graph".
    pairs = pd.read_parquet(a.atom_pairs)
    vocab = pd.read_parquet(a.vocab)
    rxn_symbol = dict(zip(vocab.loc[vocab["kind"] == "rxn", "code"],
                          vocab.loc[vocab["kind"] == "rxn", "symbol"]))
    assert rxn_symbol, "vocab.parquet carries no kind=rxn rows"
    in_graph = {rxn_symbol[c] for c in pairs["rxn"].unique() if c in rxn_symbol}
    print(f"[blast] {len(ann):,} annotated reactions, {len(in_graph):,} carry atom pairs")

    rows = []
    for r in ann.itertuples():
        s = stoich.get(r.mnxr)
        if s is None:
            continue
        st, _bal, _tr = s
        dn = 0.0
        skew = 0.0
        n_meas = n_def = n_skip = 0
        var_log = 0.0
        for mnxm, coeff in st.items():
            if mnxm in IMPLICIT:
                continue
            if mnxm in GASES or mnxm in UNIT_ACTIVITY:
                n_skip += 1
                continue
            dn += coeff
            hit = conc.get(mnxm)
            if hit is None:
                n_def += 1
                # log-uniform over the 1 uM .. 10 mM window MDF analyses use: sigma of a
                # log-uniform over 4 decades is 4/sqrt(12) = 1.155 decades.
                var_log += (coeff * 1.155) ** 2
                continue
            n_meas += 1
            c, spread = hit
            skew += coeff * math.log10(c / DEFAULT_mM)
            # A metabolite measured under one condition has no spread of its own; give it
            # the median spread of those that do rather than a confident zero.
            var_log += (coeff * max(spread, 0.30)) ** 2
        molec = RT * dn * math.log(1e-3)
        skew_kj = RT * skew * math.log(10.0)
        sigma_conc = DECADE * math.sqrt(var_log)
        rows.append(dict(mnxr=r.mnxr, dir_tier=r.dir_tier, dir_method=r.dir_method,
                         dG_raw=r.dG_raw, dG_prime=r.dG_prime, ratio=r.ratio,
                         sigma=r.sigma, in_graph=r.mnxr in in_graph,
                         dn=dn, molec=molec, skew=skew_kj,
                         correction=molec + skew_kj, sigma_conc=sigma_conc,
                         n_meas=n_meas, n_default=n_def, n_skip=n_skip))
    df = pd.DataFrame(rows)
    df["dG_conc"] = df["dG_raw"] + df["correction"]
    df.to_parquet(a.out, index=False)

    spoke = df[df["dir_tier"] > 0].copy()
    ig = spoke[spoke["in_graph"]]

    def sgn(x):
        return (x > DECADE).astype(int) - (x < -DECADE).astype(int)

    print(f"\n[blast] over the {len(spoke):,} reactions the ensemble spoke on "
          f"({len(ig):,} in-graph)\n")
    for label, d in (("all", spoke), ("in-graph", ig)):
        flip = (sgn(d["dG_raw"]) != sgn(d["dG_conc"])) & (sgn(d["dG_raw"]) != 0)
        rev = (sgn(d["dG_raw"]) * sgn(d["dG_conc"]) == -1)
        big = d["correction"].abs() > DECADE
        huge = d["correction"].abs() > 3 * DECADE
        skewbig = d["skew"].abs() > DECADE
        print(f"  {label:9s} n={len(d):6,}"
              f"  |corr|>1 decade {big.sum():6,} ({big.mean():5.1%})"
              f"  >3 decades {huge.sum():6,} ({huge.mean():5.1%})")
        print(f"  {'':9s}         "
              f"  call changes {flip.sum():6,} ({flip.mean():5.1%})"
              f"  OUTRIGHT REVERSAL {rev.sum():6,} ({rev.mean():5.1%})")
        print(f"  {'':9s}         "
              f"  skew alone >1 decade {skewbig.sum():6,} ({skewbig.mean():5.1%})"
              f"  -- what uniform 1 mM cannot see")
        print()

    print("[blast] the correction is dominated by molecularity or by skew?")
    m = spoke["molec"].abs()
    s = spoke["skew"].abs()
    print(f"  median |molecularity| {m.median():6.2f} kJ/mol   "
          f"median |skew| {s.median():6.2f} kJ/mol")
    print(f"  skew larger than molecularity on {int((s > m).sum()):,} of {len(spoke):,} "
          f"({(s > m).mean():.1%})")

    print("\n[blast] how measured is the correction?")
    full = spoke["n_default"] == 0
    print(f"  every participant measured: {int(full.sum()):,} ({full.mean():.1%})")
    for k in (1, 2, 3):
        sel = spoke["n_default"] == k
        print(f"  {k} defaulted: {int(sel.sum()):,} ({sel.mean():.1%})")
    print(f"  4+ defaulted: {int((spoke['n_default'] >= 4).sum()):,} "
          f"({(spoke['n_default'] >= 4).mean():.1%})")

    print("\n[blast] largest reversals in-graph, by |correction| (measured only):")
    cand = ig[(ig["n_default"] == 0) &
              (sgn(ig["dG_raw"]) * sgn(ig["dG_conc"]) == -1)]
    for r in cand.reindex(cand["correction"].abs().sort_values(ascending=False).index)[:12].itertuples():
        st, _b, _t = stoich[r.mnxr]
        eq = " = ".join(
            " + ".join(f"{abs(c):g} {names.get(k, k)}" for k, c in st.items()
                       if (c < 0) == (side == 0))
            for side in (0, 1))
        print(f"  {r.mnxr}  tier{r.dir_tier} {r.dir_method:18s} "
              f"dG {r.dG_raw:8.2f} -> {r.dG_conc:8.2f}  (skew {r.skew:7.2f}) "
              f"sigma_conc {r.sigma_conc:5.2f}")
        print(f"      {eq[:150]}")
    print(f"\n-> {a.out}")


if __name__ == "__main__":
    main()
