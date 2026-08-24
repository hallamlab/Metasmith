"""The reaction the concern was raised about, and the bake's own arithmetic re-derived.

MNXR145036 is glycogen phosphorylase. MetaNetX writes it `G1P = Glycogen + Pi`, the
substitution lane restages it as `alpha-maltotriose + G1P = alpha-maltotetraose + Pi`, and
r9 shipped ratio 0.204 -- polymer synthesis favoured about 5:1, which is backwards from what
glgP and malP do.

Nothing about that number was wrong as thermodynamics. dG'o = -4.18 kJ/mol is the standard
state, every participant at 1 M, and a phosphorylase's equilibrium constant genuinely sits
near unity. What decides its direction in a cell is that [Pi] runs two orders above [G1P],
because phosphoglucomutase drains G1P as fast as it appears. That is a Q term, and r9 had
no Q term.

r10 DOES, so this script's job changed. It recomputes the correction from the pinned
concentration table and the restaged equation, INDEPENDENTLY of the lane, and checks it
against the `dG_correction` the deployed annotation carries. Agreement is the check; the
walk-through below is what makes the number readable.

The two glucans cancel: they are the same polymer at n and n+1, so their activity RATIO is
1 whatever the polymer's concentration is. That is the whole reason the restaged form is the
one a concentration term can be applied to at all -- MetaNetX's one-sided form leaves a
glucosyl unit unaccounted for, and a correction applied to it prices a molecule that is not
there.
"""
from __future__ import annotations

import csv
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

import pandas as pd

from ecspr.bake.direction import substitute
from ecspr.bake.direction.refdata import (load_mnxm_formulas, load_mnxm_names,
                                          load_mnxm_props, load_mnxr_stoich)

RT = 8.314e-3 * 298.15
# The substitution lane's stand-in glucans: the same polymer at n and n+1, so their
# activity ratio is 1 whatever the polymer concentration is.
POLYMER = {"MODEL:maltotriose", "MODEL:maltotetraose", "MODEL:maltopentaose",
           "MODEL:maltohexaose"}
DECADE = RT * math.log(10.0)
MNXR = "MNXR145036"
CP = "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
RP = "data/fabfos/originals/metanetx/4.5/reac_prop.tsv"
BAKE = "data/fabfos/processed/metabolism_bake"

conc = {r["mnxm"]: (float(r["conc_mM"]), r["name"], int(r["n_all"]))
        for r in csv.DictReader(open("research/fabfos/bake/work/conc_mnxm.tsv"),
                                delimiter="\t")}
stoich = load_mnxr_stoich(RP)
names = load_mnxm_names(CP)
props = load_mnxm_props(CP)
subs = substitute.load(Path("src/ecspr/bake/direction"), props, names,
                       formulas=load_mnxm_formulas(CP), member="eq")

st, bal, _tr = stoich[MNXR]
rewritten = subs.rewrite(st)
ann = pd.read_parquet(f"{BAKE}/seams/direction_annotation.parquet")
row = ann[ann.mnxr == MNXR].iloc[0]

print(f"{MNXR}  balanced_in_reac_prop={bal}")
print("  as MetaNetX writes it :  " + " = ".join(
    " + ".join(f"{abs(c):g} {names.get(k, k)}" for k, c in st.items() if (c < 0) == (s == 0))
    for s in (0, 1)))
print("  as r9 restages it     :  " + " = ".join(
    " + ".join(f"{abs(c):g} {names.get(k, k)}" for k, c in rewritten.items()
               if (c < 0) == (s == 0))
    for s in (0, 1)))
print(f"\n  deployed: dG_standard {row.dG_standard:+.3f}  + correction "
      f"{row.dG_correction:+.3f}  = dG_raw {row.dG_raw:+.3f}")
print(f"            dG_prime {row.dG_prime:+.3f}  ratio {row.ratio:.4f}  "
      f"tier {row.dir_tier} {row.dir_method}  lambda {row.lambda_shrink:.3f}")

print("\n  concentration term, over the restaged equation:")
corr, missing = 0.0, []
for mnxm, coeff in sorted(rewritten.items(), key=lambda kv: kv[0]):
    nm = names.get(mnxm, mnxm)
    if mnxm in ("WATER", "MNXM1"):
        print(f"    {coeff:+g}  {nm:34s} implicit in the prime transform")
        continue
    if mnxm in POLYMER:
        print(f"    {coeff:+g}  {nm:34s} cancels against its own partner "
              f"(same polymer at n and n+1)")
        continue
    hit = conc.get(mnxm)
    if hit is None:
        # 1 mM, eQuilibrator's own physiological default. Skipping the term instead
        # leaves the participant at 1 M and makes the correction ONE-SIDED, which
        # invents a pool skew rather than declining to state one.
        missing.append(nm)
        c, cname, n = 1.0, "defaulted", 0
    else:
        c, cname, n = hit
    term = RT * coeff * math.log(c / 1000.0)   # mM -> M
    corr += term
    tag = f"(ECMDB {cname}, n={n})" if n else "(no measurement -- 1 mM default)"
    print(f"    {coeff:+g}  {nm:34s} {c:9.4f} mM  {tag}  -> {term:+8.3f} kJ/mol")

# THE CHECK. `corr` is computed here from the pinned table and the restaged equation, with
# no reference to the lane; `row.dG_correction` is what the lane applied. They are the same
# arithmetic reached by two paths, so a disagreement means one of them is wrong.
agree = abs(corr - float(row.dG_correction)) < 1e-6
print(f"\n  correction re-derived here             : {corr:+.3f} kJ/mol "
      f"({corr / DECADE:+.2f} decades)")
print(f"  correction the bake applied            : {float(row.dG_correction):+.3f} kJ/mol"
      f"   {'AGREE' if agree else 'DISAGREE -- one of the two is wrong'}")
print(f"  dG'o standard state                    : {row.dG_standard:+.3f} kJ/mol   "
      f"ratio {math.exp(row.dG_standard / RT):.4f}")
print(f"  dG'  with measured [Pi] and [G1P]      : {row.dG_raw:+.3f} kJ/mol   "
      f"ratio {math.exp(row.dG_raw / RT):.4f}")
print(f"  shipped, after shrinkage               : {row.dG_prime:+.3f} kJ/mol   "
      f"ratio {row.ratio:.4f}")
print(f"\n  the raw ratio moves by a factor of {math.exp(corr / RT):.1f}")
pi = conc["MNXM9"][0]
g1p = conc["MNXM1364212"][0]
print(f"  [Pi] = {pi} mM, [G1P] = {g1p:.4f} mM  -- and the G1P row reaches "
      f"MNXM1364212 only\n  through the ALIAS expansion; the accession join "
      f"lands on MNXM1364214.")
print(f"\n  [Pi]/[G1P] = {pi/g1p:.1f}.  A phosphorylase's Keq is near unity, so a "
      f"two-order-of-magnitude\n  pool skew is not a correction to the direction call, "
      f"it IS the direction call.")
