#!/usr/bin/env python3
r"""One source, two grounds: the smallest circuit in which a diversion is visible.

`monotonicity_ladder.py` established the mechanism and `glycogen_share.py` measured it over
the whole cohort. This is the demonstration in between -- the minimum apparatus that shows
current being taken AWAY from glycogen rather than merely failing to arrive.

    glucose  --[ the AG1 carbon network ]-->  glycogen        (ground A, the target)
                                          \-->  diversion     (ground B, the competitor)

Both grounds are real ports (`attach_leak` with `port=1.0`); every other metabolite gets
only the background `leak`, so at a small leak the injected current has exactly two ways
out and `I_A + I_B` is the whole of it. The SAME two ports are used for all three networks,
which is what makes the four deltas comparable -- moving the ports between conditions would
be measuring three different instruments.

Three networks, one fold each, from the Eydallin (2010) overexpression screen:

    host        AG1 wild type
    + glgC      453.3% of WT glycogen -- the positive case, ADP-glucose pyrophosphorylase
    + talA       49.7% of WT glycogen -- the negative case, transaldolase

talA is the case that matters. It is a real glycogen-DEFICIENT hit, and it is deficient by
competition rather than by damage: transaldolase does not touch the glycogen route, it
widens a different exit from the hexose-phosphate pool. A two-point conductance to glycogen
cannot express that -- Rayleigh makes every fold non-negative -- so the single-ground probe
scored it +0.00125, right gene and unusable sign. With somewhere else to go, the same fold
on the same network reads negative.

Ground B is chosen by MEASUREMENT, not by hand: under the full biomass grounding the ports
that GAIN current when talA is doubled are the E4P-derived branch (pyridoxal 5'-phosphate,
L-tryptophan, L-tyrosine, L-phenylalanine, thiamine diphosphate). `--ground-b` sweeps the
candidates plus the aggregate, because a signature that appears at one choice is a knob and
one that holds across them is a result.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/two_ground_panel.py
    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/two_ground_panel.py --leak 1e-3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import (load_pairs, load_direction_ratios,  # noqa: E402
                               graph_from_pairs)
from ecspr.model.graph import Terminal, measure_leak, solve         # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glycogen"))
import bake_pairs                                                   # noqa: E402
from glycogen_share import biomass_precursors                       # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"          # D-glucose
GLYCOGEN_MNXM = "MNXM738130"

CASES = [
    ("glgC", "positive", ["MNXR145050"]),
    ("talA", "negative", ["MNXR146501"]),
]

GROUND_B = {
    "pyridoxal5P": ["MNXM161"],
    "L-tryptophan": ["MNXM741553"],
    "L-tyrosine": ["MNXM76"],
    "thiamine-PP": ["MNXM256"],
    "E4P": ["MNXM258"],
    "aromatics": ["MNXM741553", "MNXM76", "MNXM741664"],
    "biomass": None,
}


def chem_names() -> dict:
    c = pd.read_csv(CHEM_PROP, sep="\t", comment="#", header=None,
                    names=["id", "name", "ref", "formula", "charge", "mass",
                           "InChI", "InChIKey", "SMILES"], dtype=str, low_memory=False)
    return dict(zip(c.id, c.name))


def measure(pairs, element, weights, ratios, ports, *, leak, port=1.0) -> dict:
    g = graph_from_pairs(pairs, element, weights, ratios)
    src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
    if src.missing:
        raise SystemExit("[2g] source missing from the graph")
    return measure_leak(g, src, ports, leak=leak, port=port)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--leak", type=float, default=1e-6,
                   help="background conductance of every non-port metabolite. Small makes "
                        "it a true two-ground circuit; 1e-3 is what the cohort ran at.")
    p.add_argument("--ground-b", default="", help="one key of GROUND_B; default sweeps all")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = p.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(RUNS / a.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    g0 = graph_from_pairs(pairs, a.element, base_w, ratios)
    universe = set(g0.metabolites())
    GROUND_B["biomass"] = sorted(set(biomass_precursors(universe)) - {GLYCOGEN_MNXM})
    nm = chem_names()

    meas = pd.read_csv(MEASURED, sep="\t")
    pct = {str(g).lower(): v for g, v in zip(meas.gene, meas.pct_wt)}

    print(f"[2g] {g0.meta['n_reactions_used']} reactions, {g0.n} atom nodes; "
          f"source {nm.get(SOURCE_MNXM)} -> ground A {nm.get(GLYCOGEN_MNXM)}; "
          f"leak={a.leak:g} fold={a.fold:g}", file=sys.stderr)

    print("\n=== control: one ground (glucose -> glycogen), the two-point conductance ===",
          file=sys.stderr)
    onep = []
    gb = graph_from_pairs(pairs, a.element, base_w, ratios)
    c0 = solve(gb, Terminal.metabolite(gb, SOURCE_MNXM, label="glucose"),
               Terminal.metabolite(gb, GLYCOGEN_MNXM, label="glycogen")).total
    onep.append(dict(ground_b="(one ground)", n_b=0, case="host", label="wild type",
                     pct_wt=100.0, I_glycogen=c0, I_diversion=np.nan,
                     d_glycogen=0.0, d_diversion=np.nan, rel_glycogen=0.0,
                     rel_diversion=np.nan, frac_glycogen=1.0, two_point_total=c0))
    print(f"  host       C_eff={c0:.6f}", file=sys.stderr)
    for gene, sign, rxns in CASES:
        gw = graph_from_pairs(pairs, a.element,
                              dict(base_w, **{r: base_w[r] * a.fold for r in rxns}), ratios)
        c = solve(gw, Terminal.metabolite(gw, SOURCE_MNXM, label="glucose"),
                  Terminal.metabolite(gw, GLYCOGEN_MNXM, label="glycogen")).total
        onep.append(dict(ground_b="(one ground)", n_b=0, case=gene, label=f"{sign} case",
                         pct_wt=pct.get(gene.lower()), I_glycogen=c, I_diversion=np.nan,
                         d_glycogen=c - c0, d_diversion=np.nan,
                         rel_glycogen=(c - c0) / c0, rel_diversion=np.nan,
                         frac_glycogen=1.0, two_point_total=c))
        print(f"  +{gene:<9} C_eff={c:.6f}  dC={c - c0:+.6e}  "
              f"({pct.get(gene.lower()):.1f}% WT measured)", file=sys.stderr)

    keys = [a.ground_b] if a.ground_b else list(GROUND_B)
    rows = list(onep)
    for key in keys:
        bmets = [m for m in GROUND_B[key] if m in universe]
        if not bmets:
            print(f"[2g] {key}: no ground-B metabolite in the carbon universe -- skipped",
                  file=sys.stderr)
            continue
        ports = sorted({GLYCOGEN_MNXM, *bmets})

        def split(draw):
            ia = float(draw.get(GLYCOGEN_MNXM, 0.0))
            ib = float(sum(draw.get(m, 0.0) for m in bmets))
            return ia, ib

        r0 = measure(pairs, a.element, base_w, ratios, ports, leak=a.leak)
        a0, b0 = split(r0["draw"])
        rows.append(dict(ground_b=key, n_b=len(bmets), case="host", label="wild type",
                         pct_wt=100.0, I_glycogen=a0, I_diversion=b0,
                         d_glycogen=0.0, d_diversion=0.0,
                         rel_glycogen=0.0, rel_diversion=0.0,
                         frac_glycogen=a0 / (a0 + b0) if (a0 + b0) else np.nan,
                         two_point_total=r0["total"]))

        print(f"\n=== ground B = {key} ({len(bmets)} metabolite(s)) ===", file=sys.stderr)
        print(f"  host       I_glyc={a0:.6e}  I_div={b0:.6e}  "
              f"frac_glyc={a0 / (a0 + b0):.6f}", file=sys.stderr)

        for gene, sign, rxns in CASES:
            w = dict(base_w, **{r: base_w[r] * a.fold for r in rxns if r in base_w})
            missing = [r for r in rxns if r not in base_w]
            if missing:
                raise SystemExit(f"[2g] {gene}: {missing} not in the host GPR")
            r = measure(pairs, a.element, w, ratios, ports, leak=a.leak)
            ia, ib = split(r["draw"])
            rows.append(dict(ground_b=key, n_b=len(bmets), case=gene, label=f"{sign} case",
                             pct_wt=pct.get(gene.lower()), I_glycogen=ia, I_diversion=ib,
                             d_glycogen=ia - a0, d_diversion=ib - b0,
                             rel_glycogen=(ia - a0) / a0 if a0 else np.nan,
                             rel_diversion=(ib - b0) / b0 if b0 else np.nan,
                             frac_glycogen=ia / (ia + ib) if (ia + ib) else np.nan,
                             two_point_total=r["total"]))
            print(f"  +{gene:<9} I_glyc={ia:.6e}  I_div={ib:.6e}  "
                  f"dI_glyc={ia - a0:+.3e}  dI_div={ib - b0:+.3e}  "
                  f"({pct.get(gene.lower()):.1f}% WT measured)", file=sys.stderr)

    df = pd.DataFrame(rows)
    out = a.out_dir / f"two_ground_panel_{a.host}_{a.element}_leak{a.leak:g}.tsv"
    df.to_csv(out, sep="\t", index=False)

    pd.set_option("display.width", 220)
    print("\n" + "=" * 100, file=sys.stderr)
    print("Ieff at each ground, unit injection at glucose "
          f"(leak={a.leak:g}, fold={a.fold:g})", file=sys.stderr)
    print("=" * 100, file=sys.stderr)
    show = df[["ground_b", "case", "pct_wt", "I_glycogen", "I_diversion",
               "d_glycogen", "d_diversion", "frac_glycogen"]]
    print(show.to_string(index=False, float_format="%+.6e"), file=sys.stderr)

    ok = []
    for key, sub in df.groupby("ground_b", sort=False):
        s = sub.set_index("case")
        if "glgC" not in s.index or "talA" not in s.index:
            continue
        ok.append(dict(ground_b=key,
                       glgC_glyc_up=bool(s.loc["glgC", "d_glycogen"] > 0),
                       talA_glyc_down=bool(s.loc["talA", "d_glycogen"] < 0),
                       talA_div_up=bool(s.loc["talA", "d_diversion"] > 0),
                       ratio=abs(s.loc["talA", "d_glycogen"] /
                                 s.loc["glgC", "d_glycogen"])))
    chk = pd.DataFrame(ok)
    print("\nthe signature, per ground-B choice "
          "(positive raises the target; negative lowers it AND raises the competitor):",
          file=sys.stderr)
    print(chk.to_string(index=False, float_format="%.3f"), file=sys.stderr)
    print(f"\n[2g] wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
