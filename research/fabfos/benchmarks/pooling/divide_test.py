"""Should E_full be divided by n_orf? Two tests on real K-12 data.

T1  Does n_orf carry real presence evidence, or is it an artifact?
    Rank de-novo-nominated MNXR by E, E/n_orf, n_orf; score against iML1515's
    reactome as the truth set (AUROC + precision@k).

T2  Holding truth constant, does the de novo lane's E track SUBUNIT count?
    Join de novo E onto iML1515's rule structure (AND-arity vs OR-arity).
"""
import ast
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_R = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_R / "src"))
from ecspr.model.gpr import weights_from_rows

R = str(_R / "data/fabfos/runs/e_coli_k12/gpr") + "/"
gem = pd.read_parquet(R + "gpr_gem.parquet")
dn = pd.read_parquet(R + "gpr_denovo.parquet")

def dnf(n):
    if isinstance(n, ast.Expression): return dnf(n.body)
    if isinstance(n, ast.Name): return [frozenset([n.id])]
    if isinstance(n, ast.BoolOp):
        ps = [dnf(v) for v in n.values]
        if isinstance(n.op, ast.Or):
            return [g for p in ps for g in p]
        acc = [frozenset()]
        for p in ps: acc = [a | b for a in acc for b in p]
        return acc
    raise TypeError

def rule_struct(g):
    groups = set()
    for rule in g.gpr_rule.dropna().unique():
        s = str(rule).strip()
        if not s: continue
        try:
            for grp in dnf(ast.parse(s, mode="eval")):
                if grp: groups.add(grp)
        except Exception: pass
    if not groups: return pd.Series(dict(n_enzymes=0, max_subunits=0))
    return pd.Series(dict(n_enzymes=len(groups), max_subunits=max(len(x) for x in groups)))

struct = gem.groupby("mnxr").apply(rule_struct, include_groups=False)

# ---- de novo weights ----
E = pd.Series(weights_from_rows(dn, "belief"), name="E")
n_orf = dn.groupby("mnxr")["feature_id"].nunique().rename("n_orf")
inuni = dn.groupby("mnxr")["in_atom_universe"].any().rename("in_universe")
d = pd.concat([E, n_orf, inuni], axis=1).dropna(subset=["E"])
d = d[d.in_universe]
d["E_div"] = d.E / d.n_orf
d["truth"] = d.index.isin(set(gem.mnxr))

print(f"de novo, in universe: {len(d)} MNXR; {d.truth.sum()} also in iML1515 "
      f"({d.truth.mean():.1%}); iML1515 has {gem.mnxr.nunique()} MNXR total")

def auroc(y, s):
    r = pd.Series(s).rank().to_numpy()
    y = np.asarray(y, bool); n1, n0 = y.sum(), (~y).sum()
    return (r[y].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)

print("\n== T1: ranking de-novo nominations against the iML1515 reactome ==")
for name, s in [("E (as built)", d.E), ("E / n_orf", d.E_div), ("n_orf alone", d.n_orf),
                ("random-ish (1.0)", pd.Series(1.0, index=d.index))]:
    order = s.sort_values(ascending=False)
    p100 = d.truth.reindex(order.index[:100]).mean()
    p500 = d.truth.reindex(order.index[:500]).mean()
    print(f"  {name:18s} AUROC {auroc(d.truth, s):.3f}   P@100 {p100:.2f}   P@500 {p500:.2f}")

print("\n== T2: does de novo E track subunit count, holding truth constant? ==")
j = d.join(struct, how="inner")
j = j[j.n_enzymes > 0]
print(f"  {len(j)} MNXR with a parsed iML1515 rule")
print("\n  -- by max_subunits (AND-arity: one enzyme, k subunits) --")
print(j.groupby(j.max_subunits.clip(upper=6)).agg(
    n=("E", "size"), E_med=("E", "median"), E_mean=("E", "mean"),
    Ediv_med=("E_div", "median"), n_orf_med=("n_orf", "median")).to_string())
print("\n  -- by n_enzymes (OR-arity: k independent isozymes) --")
print(j.groupby(j.n_enzymes.clip(upper=6)).agg(
    n=("E", "size"), E_med=("E", "median"), E_mean=("E", "mean"),
    Ediv_med=("E_div", "median"), n_orf_med=("n_orf", "median")).to_string())

# partial: within single-enzyme reactions only, so OR-arity is held at 1
s1 = j[j.n_enzymes == 1]
print(f"\n  -- single-enzyme reactions only (n={len(s1)}), by subunit count --")
print(s1.groupby(s1.max_subunits.clip(upper=6)).agg(
    n=("E", "size"), E_med=("E", "median"), Ediv_med=("E_div", "median"),
    n_orf_med=("n_orf", "median")).to_string())

print("\n  spearman(E, max_subunits) = %.3f ; spearman(E, n_enzymes) = %.3f"
      % (j.E.corr(j.max_subunits, method="spearman"),
         j.E.corr(j.n_enzymes, method="spearman")))
print("  spearman(E_div, max_subunits) = %.3f ; spearman(E_div, n_enzymes) = %.3f"
      % (j.E_div.corr(j.max_subunits, method="spearman"),
         j.E_div.corr(j.n_enzymes, method="spearman")))
print("  spearman(n_orf, max_subunits) = %.3f ; spearman(n_orf, n_enzymes) = %.3f"
      % (j.n_orf.corr(j.max_subunits, method="spearman"),
         j.n_orf.corr(j.n_enzymes, method="spearman")))
