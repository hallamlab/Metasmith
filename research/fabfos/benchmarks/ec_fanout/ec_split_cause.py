"""Why do the share-nothing ECs split? Test the hypothesis that the odd component is
a non-small-molecule pseudo-species (protein, complex, polymer) rather than real
chemical disagreement."""
import json, sys
from pathlib import Path
from collections import Counter
import pandas as pd

TMP = Path(sys.argv[1])
CP = Path("data/fabfos/originals/metanetx/4.5/chem_prop.tsv")
RP = Path("data/fabfos/originals/metanetx/4.5/reac_prop.tsv")

# chem_prop: id, name, reference, formula, charge, mass, InChI, InChIKey, SMILES
smiles, cname, formula = {}, {}, {}
for line in open(CP):
    if line.startswith("#"): continue
    p = line.rstrip("\n").split("\t")
    if not p or not p[0]: continue
    cname[p[0]] = p[1] if len(p) > 1 else ""
    formula[p[0]] = p[3] if len(p) > 3 else ""
    smiles[p[0]] = p[8] if len(p) > 8 else ""

eq = {}
for line in open(RP):
    if line.startswith("#"): continue
    p = line.rstrip("\n").split("\t")
    if len(p) >= 2 and p[0].startswith("MNXR"): eq[p[0]] = p[1]

def parts(r):
    return {t.split("@")[0] for t in eq.get(r, "").replace("=", " ").split() if "@" in t}

def is_pseudo(m):
    """No SMILES and no formula -> MetaNetX has no chemical structure for it:
    a protein, a complex, a generic polymer, an abstract entity."""
    return not smiles.get(m, "") and not formula.get(m, "")

hard = [json.loads(l) for l in open(TMP / "ec_split_none.jsonl")]
hard = [r for r in hard if r["n_components"] >= 2]

cause = Counter()
genuine = []
for r in hard:
    comps = sorted(r["components"], key=len)
    minor = comps[:-1]                      # everything but the largest group
    minor_mets = {m for c in minor for rx in c for m in parts(rx)}
    if minor_mets and all(is_pseudo(m) for m in minor_mets):
        cause["pseudo-species only (protein/complex/polymer)"] += 1
    elif minor_mets and any(is_pseudo(m) for m in minor_mets):
        cause["mixed: some pseudo-species"] += 1
    else:
        cause["all real small molecules -- genuine chemical disagreement"] += 1
        genuine.append((r, minor))

print(f"share-nothing level-4 ECs: {len(hard)}")
for k, v in cause.most_common():
    print(f"  {v:>4}  {100*v/len(hard):5.1f}%  {k}")

def pretty(r):
    return " ".join(cname.get(t.split("@")[0], t.split("@")[0]) if "@" in t else t
                    for t in eq.get(r, "").split())

# rank the genuine ones by evidence volume in the metagenome
df = pd.read_parquet("data/fabfos/runs/scadc_metagenome/gpr/gpr_4lane.parquet",
                     columns=["projection_via", "intermediate_id", "orf"])
df = df[df.projection_via == "ec"]
vol = df.groupby("intermediate_id").agg(rows=("orf", "size"), orfs=("orf", "nunique"))
genuine.sort(key=lambda g: -int(vol.rows.get(g[0]["ec"], 0)))

print(f"\n=== genuine chemical disagreement, ranked by metagenome evidence ===")
for r, minor in genuine[:12]:
    ec = r["ec"]
    v = vol.loc[ec] if ec in vol.index else None
    tag = f"{int(v.rows):,} rows / {int(v.orfs):,} ORFs" if v is not None else "not emitted here"
    print(f"\nEC {ec}  [{tag}]  {r['n_mnxr']} MNXR, groups {r['comp_sizes']}")
    major = max(r["components"], key=len)
    print(f"   major (n={len(major)}): {pretty(major[0])[:130]}")
    for c in minor[:3]:
        print(f"   minor (n={len(c)}): {pretty(c[0])[:130]}")
