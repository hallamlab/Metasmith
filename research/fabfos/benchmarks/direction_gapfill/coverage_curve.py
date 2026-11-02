import re, sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, "src")
from ecspr.bake.direction import substitute
from ecspr.bake.direction.refdata import load_mnxm_props, load_mnxm_names, load_mnxm_formulas

CP = Path("data/fabfos/originals/metanetx/4.5/chem_prop.tsv")
D = Path("src/ecspr/bake/direction")
props = load_mnxm_props(CP); names = load_mnxm_names(CP); forms = load_mnxm_formulas(CP)
adm = substitute.load(D, props, names, formulas=forms, member="dgbyg")
admitted = set(adm._by_mnxm)

TERM = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@")
parts = {}
for line in open("data/fabfos/originals/metanetx/4.5/reac_prop.tsv"):
    if line.startswith("#"): continue
    f = line.rstrip().split("\t")
    if len(f) < 2 or not f[0].startswith("MNXR") or "=" not in f[1]: continue
    parts[f[0]] = {m for _c, m in TERM.findall(f[1])}

cp = pd.read_csv(CP, sep="\t", comment="#", header=None, usecols=[0,1,8],
                 names=["mnxm","name","smiles"], dtype=str)
wild = {m for m, s in zip(cp.mnxm, cp.smiles) if isinstance(s, str) and "*" in s}
nm = dict(zip(cp.mnxm, cp.name))
ann = pd.read_parquet("data/fabfos/processed/metabolism_bake/seams/direction_annotation.parquet",
                      columns=["mnxr","dir_method"])
refused = set(ann.loc[ann.dir_method == "refused", "mnxr"])

need = {r: (parts[r] & wild) - admitted for r in refused if r in parts}
need = {r: s for r, s in need.items() if s}
print(f"refused reactions with at least one uncovered wildcard: {len(need):,}")

cov, picked = set(admitted), []
for step in range(20):
    from collections import Counter
    gain = Counter()
    for r, s in need.items():
        left = s - cov
        if len(left) == 1:
            gain[next(iter(left))] += 1
    if not gain:
        break
    m, g = gain.most_common(1)[0]
    cov.add(m); picked.append((m, g))
    done = sum(1 for r, s in need.items() if not (s - cov))
    print(f"  +{len(picked):>2}  {str(nm.get(m))[:42]:<42} unlocks {g:>4}  "
          f"cumulative reactions fully covered: {done:,}")
