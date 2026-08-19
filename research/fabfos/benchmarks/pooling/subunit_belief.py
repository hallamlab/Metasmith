"""Does belief conservation over-credit multi-subunit enzymes? K-12, both lanes."""
import ast
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_R = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_R / "src"))
from ecspr.model.gpr import weights_from_rows

ROOT = _R / "data/fabfos/runs/e_coli_k12/gpr"

# ---- GPR rule -> DNF (list of AND-groups = candidate enzymes/complexes) ----
def dnf(node):
    if isinstance(node, ast.Name):
        return [frozenset([node.id])]
    if isinstance(node, ast.BoolOp):
        parts = [dnf(v) for v in node.values]
        if isinstance(node.op, ast.Or):
            out = []
            for p in parts: out.extend(p)
            return out
        acc = [frozenset()]
        for p in parts:
            acc = [a | b for a in acc for b in p]
        return acc
    if isinstance(node, ast.Expression):
        return dnf(node.body)
    if isinstance(node, ast.Module):
        return dnf(node.body[0])
    if isinstance(node, ast.Expr):
        return dnf(node.value)
    raise TypeError(ast.dump(node))

def parse_rule(rule):
    if not rule or not str(rule).strip():
        return []
    r = str(rule).replace(" and ", " and ").replace(" or ", " or ")
    try:
        return dnf(ast.parse(r, mode="eval"))
    except Exception:
        return []

# =====================================================================
gem = pd.read_parquet(ROOT / "gpr_gem.parquet")
w_gem = weights_from_rows(gem, "belief")
E = pd.Series(w_gem, name="E_full")

n_orf = gem.groupby("mnxr")["feature_id"].nunique().rename("n_orf")

# per mnxr: union of DNF groups over all GEM reactions mapping to it
rows = []
for mnxr, g in gem.groupby("mnxr"):
    groups = set()
    for rule in g.gpr_rule.dropna().unique():
        for grp in parse_rule(rule):
            if grp: groups.add(grp)
    if not groups:
        rows.append((mnxr, 0, 0, 0.0)); continue
    n_enz = len(groups)                       # OR-arity: distinct enzymes/isozymes
    sizes = [len(x) for x in groups]
    rows.append((mnxr, n_enz, max(sizes), float(np.mean(sizes))))
st = pd.DataFrame(rows, columns=["mnxr", "n_enzymes", "max_subunits", "mean_subunits"]).set_index("mnxr")

df = st.join(E).join(n_orf).dropna(subset=["E_full"])
df["in_universe"] = df.index.isin(set(gem[gem.in_atom_universe].mnxr))

def cls(r):
    if r.n_enzymes == 0: return "no rule"
    if r.n_enzymes == 1 and r.max_subunits == 1: return "monomer, single gene"
    if r.n_enzymes == 1: return f"single complex"
    if r.max_subunits == 1: return "isozymes only"
    return "isozymes of complexes"
df["class"] = df.apply(cls, axis=1)

print(f"iML1515 lane: {len(df)} MNXR, {df.in_universe.sum()} in atom universe")
print("\n-- E_full by rule class (in-universe only) --")
u = df[df.in_universe]
print(u.groupby("class").agg(n=("E_full", "size"), E_med=("E_full", "median"),
                             E_mean=("E_full", "mean"), E_p95=("E_full", lambda s: s.quantile(.95)),
                             n_orf_med=("n_orf", "median"), n_orf_max=("n_orf", "max")).to_string())

print("\n-- E_full vs max_subunits, for single-complex reactions --")
sc = u[(u.n_enzymes == 1)]
print(sc.groupby("max_subunits").agg(n=("E_full", "size"), E_med=("E_full", "median"),
                                     E_mean=("E_full", "mean"), n_orf_med=("n_orf", "median")).to_string())

print("\n-- E_full vs n_enzymes, for monomeric isozymes (max_subunits==1) --")
iso = u[(u.max_subunits == 1)]
print(iso.groupby("n_enzymes").agg(n=("E_full", "size"), E_med=("E_full", "median"),
                                   E_mean=("E_full", "mean"), n_orf_med=("n_orf", "median")).head(10).to_string())

print("\n-- top 15 E_full (in universe) --")
print(u.sort_values("E_full", ascending=False).head(15).to_string())

print("\ncorr(E_full, n_orf) =", u.E_full.corr(u.n_orf).round(3),
      " corr(E_full, max_subunits) =", u.E_full.corr(u.max_subunits).round(3),
      " corr(E_full, n_enzymes) =", u.E_full.corr(u.n_enzymes).round(3))

# =====================================================================
dn = pd.read_parquet(ROOT / "gpr_denovo.parquet")
w_dn = weights_from_rows(dn, "belief")
Ed = pd.Series(w_dn, name="E_full")
nd = dn.groupby("mnxr")["feature_id"].nunique().rename("n_orf")
d = pd.concat([Ed, nd], axis=1).dropna()
d["in_universe"] = d.index.isin(set(dn[dn.in_atom_universe].mnxr))
du = d[d.in_universe]
print(f"\n\nde novo lane: {len(d)} MNXR ({du.shape[0]} in universe), "
      f"{dn.feature_id.nunique()} ORFs, sum(E)={Ed.sum():.3f}")
print("\n-- E_full and n_orf quantiles (in universe) --")
print(du[["E_full", "n_orf"]].describe(percentiles=[.5, .9, .99]).to_string())
print("corr(E_full, n_orf) =", du.E_full.corr(du.n_orf).round(3))
print("\n-- top 15 E_full --")
print(du.sort_values("E_full", ascending=False).head(15).to_string())

# how much of E's spread is n_orf vs per-ORF dilution?
du = du.copy()
du["E_per_orf"] = du.E_full / du.n_orf
print("\nE_full spread (p99/p50): %.2f ; E_per_orf spread: %.2f"
      % (du.E_full.quantile(.99) / du.E_full.median(),
         du.E_per_orf.quantile(.99) / du.E_per_orf.median()))
