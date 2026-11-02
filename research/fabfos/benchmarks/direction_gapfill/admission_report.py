import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, "src")
from ecspr.bake.direction import substitute
from ecspr.bake.direction.refdata import (load_mnxm_props, load_mnxm_names,
                                          load_mnxm_formulas, load_mnxr_stoich)

CP = Path("data/fabfos/originals/metanetx/4.5/chem_prop.tsv")
RP = Path("data/fabfos/originals/metanetx/4.5/reac_prop.tsv")
D = Path("src/ecspr/bake/direction")

props = load_mnxm_props(CP); names = load_mnxm_names(CP); forms = load_mnxm_formulas(CP)
for member in ("dgbyg", "eq"):
    subs = substitute.load(D, props, names, formulas=forms, collect=True, member=member)
    dec = subs.decisions
    print(f"\n=== member={member}: {len(dec)} rows, "
          f"{int((dec.verdict=='admitted').sum())} admitted, "
          f"{int((dec.verdict=='refused').sum())} refused")
    r = dec[dec.verdict == "refused"]
    if len(r):
        print(r.groupby("predicate").size().sort_values(ascending=False).to_string())
        print("\n  refused rows (kind, mnxm, name, predicate):")
        for x in r.itertuples(index=False):
            print(f"    {x.kind:<10} {x.mnxm:<14} {str(x.mnx_name)[:30]:<30} {x.predicate}")

# how many reactions does the ADMITTED set actually touch?
subs = substitute.load(D, props, names, formulas=forms, member="dgbyg")
stoich = load_mnxr_stoich(RP)
covered = {m for m in subs._by_mnxm}
print(f"\nadmitted substitutable metabolites (dgbyg): {len(covered)}")
hits = [r for r, st in stoich.items() if covered & set(st)]
print(f"reactions containing at least one of them : {len(hits):,}")
