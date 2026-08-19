"""Simulate per-member admission: where dgbyg's substitution is now REFUSED, dgbyg sees the
unsubstituted equation -- which is exactly what r8's member table already holds."""
import sys; sys.path.insert(0, "src")
import pandas as pd
from pathlib import Path
from ecspr.bake.direction import substitute as S
from ecspr.bake.direction.refdata import (load_mnxr_stoich, load_mnxm_names,
                                          load_mnxm_props, load_mnxm_formulas)
MNX = Path("data/fabfos/originals/metanetx/4.5")
props = load_mnxm_props(MNX/"chem_prop.tsv"); names = load_mnxm_names(MNX/"chem_prop.tsv")
formulas = load_mnxm_formulas(MNX/"chem_prop.tsv")
stoich = load_mnxr_stoich(MNX/"reac_prop.tsv")
sub_db = S.load(Path("src/ecspr/bake/direction"), props, names, formulas=formulas,
                member="dgbyg")
print(f"dgbyg admitted rows: {len(sub_db)}")

r9 = pd.read_parquet("data/fabfos/temp/_seams/direction_member_dgbyg.parquet")
r8 = pd.read_parquet("data/fabfos/processed/metabolism_bake/seams/direction_member_dgbyg.parquet")
r8i = r8.set_index("mnxr")

still = {m for m, (st, _b, _t) in stoich.items() if sub_db.covers(st)}
print(f"reactions dgbyg is STILL substituted on: {len(still):,}")

cols = [c for c in r9.columns]
out, swapped = [], 0
for row in r9.to_dict("records"):
    m = str(row["mnxr"])
    if m in still or m not in r8i.index:
        out.append(row); continue
    b = r8i.loc[m]
    swapped += 1
    out.append({**row, "dg": b["dg"], "sigma": b["sigma"], "flag": b["flag"],
                "reason": b["reason"], "sigma_sub": 0.0})
df = pd.DataFrame(out, columns=cols)
dst = "/home/tony/.claude/jobs/b0777582/tmp/t6/dgbyg_spliced.parquet"
df.to_parquet(dst, index=False)
print(f"spliced {swapped:,} rows back to their unsubstituted r8 values -> {dst}")
print(f"  r9 answered {r9['dg'].notna().sum():,} -> spliced answers {df['dg'].notna().sum():,}")
print(f"  rows carrying sigma_sub>0: {(r9['sigma_sub']>0).sum():,} -> {(df['sigma_sub']>0).sum():,}")
