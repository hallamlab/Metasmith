import json, sys
from pathlib import Path
import pandas as pd

TMP = Path(sys.argv[1])
RP = Path("data/fabfos/originals/metanetx/4.5/reac_prop.tsv")
CP = Path("data/fabfos/originals/metanetx/4.5/chem_prop.tsv")

def load(name):
    return {r["ec"]: r for r in (json.loads(l) for l in open(TMP / name))}

hard = load("ec_split_none.jsonl")
soft = load("ec_split_0.05.jsonl")
hard_split = {e for e, r in hard.items() if r["n_components"] >= 2}
soft_split = {e for e, r in soft.items() if r["n_components"] >= 2}
multi      = set(soft)

eq   = {}
for line in open(RP):
    if line.startswith("#"): continue
    p = line.rstrip("\n").split("\t")
    if len(p) >= 2 and p[0].startswith("MNXR"): eq[p[0]] = p[1]
name = {}
for line in open(CP):
    if line.startswith("#"): continue
    p = line.rstrip("\n").split("\t")
    if len(p) >= 2: name[p[0]] = p[1]

def pretty(e):
    out = []
    for tok in e.split():
        base = tok.split("@")[0]
        out.append(name.get(base, base) if base.startswith(("MNXM", "WATER", "BIOMASS")) else tok)
    return " ".join(out)

for label, path in [("scadc_metagenome (4-lane)", "data/fabfos/runs/scadc_metagenome/gpr/gpr_4lane.parquet"),
                    ("scadc_fosmids (7-lane)",    "data/fabfos/runs/scadc_fosmids/gpr/gpr_7lane.parquet")]:
    df = pd.read_parquet(path, columns=["channel", "projection_via", "intermediate_id", "orf", "mnxr"])
    df = df[df.projection_via == "ec"]
    used = set(df.intermediate_id.astype(str))
    u_multi, u_soft, u_hard = used & multi, used & soft_split, used & hard_split
    rows = len(df)
    r_soft = int(df.intermediate_id.isin(u_soft).sum())
    r_hard = int(df.intermediate_id.isin(u_hard).sum())
    print(f"\n=== {label} ===")
    print(f"  EC-routed rows              {rows:>12,}")
    print(f"  distinct ECs emitted        {len(used):>12,}")
    print(f"    ... with >=2 MNXR         {len(u_multi):>12,}  ({100*len(u_multi)/len(used):.1f}%)")
    print(f"    ... SPLIT, currency-tol   {len(u_soft):>12,}  ({100*len(u_soft)/len(used):.1f}% of emitted)")
    print(f"    ... SPLIT, share nothing  {len(u_hard):>12,}  ({100*len(u_hard)/len(used):.1f}% of emitted)")
    print(f"  rows through a split EC     {r_soft:>12,}  ({100*r_soft/rows:.1f}%)")
    print(f"  rows through a share-nothing EC {r_hard:>8,}  ({100*r_hard/rows:.1f}%)")
    o = df[df.intermediate_id.isin(u_hard)]
    print(f"  ORFs touched by a share-nothing EC {o.orf.nunique():>7,} of {df.orf.nunique():,}")
    if label.startswith("scadc_metagenome"):
        top = (df[df.intermediate_id.isin(u_hard)]
               .groupby("intermediate_id").agg(rows=("orf","size"), orfs=("orf","nunique"))
               .sort_values("rows", ascending=False).head(8))
        print("\n  --- worst share-nothing ECs by evidence volume ---")
        for ec, r in top.iterrows():
            rec = hard[ec]
            print(f"\n  EC {ec}  {r['rows']:,} rows / {r['orfs']:,} ORFs / "
                  f"{rec['n_mnxr']} MNXR in {rec['n_components']} disjoint groups "
                  f"{rec['comp_sizes']}")
            for ci, comp in enumerate(rec["components"][:4]):
                head = comp[0]
                print(f"     group {ci+1} (n={len(comp)}): {head}  {pretty(eq.get(head,''))[:150]}")
