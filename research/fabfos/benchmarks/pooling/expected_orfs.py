"""Is high E just "many ORFs hit it"? And what expected-ORF reference do we have?

Observed (de novo K-12) vs two expectations for the same MNXR:
  * iML1515's own gene count -- same organism, curated: the per-genome expectation
  * distinct UniProt proteins carrying the MNXR -- the family-size prior, all life
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_R = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_R / "src"))
from ecspr.model.gpr import weights_from_rows

R = str(_R / "data/fabfos/runs/e_coli_k12/gpr") + "/"
dn = pd.read_parquet(R + "gpr_denovo.parquet")
gem = pd.read_parquet(R + "gpr_gem.parquet")
uni = pd.read_parquet(Path(__file__).resolve().parent / "uniprot_per_mnxr.parquet")["n_uniprot"]

E = pd.Series(weights_from_rows(dn, "belief"), name="E")
obs = dn.groupby("mnxr")["feature_id"].nunique().rename("n_orf_obs")
exp_gem = gem.groupby("mnxr")["feature_id"].nunique().rename("n_gene_gem")
inuni = dn.groupby("mnxr")["in_atom_universe"].any().rename("in_universe")

d = pd.concat([E, obs, exp_gem, inuni], axis=1).join(uni)
d = d[d.in_universe.fillna(False) & d.E.notna()]
d["n_uniprot"] = d.n_uniprot.fillna(0)

print(f"{len(d)} in-universe MNXR nominated by the de novo lane; "
      f"{d.n_gene_gem.notna().sum()} also in iML1515; "
      f"{(d.n_uniprot>0).sum()} have UniProt evidence")

tot = d.E.sum()
top = d.sort_values("E", ascending=False)
for k in (10, 20, 50, 100):
    print(f"  top {k:3d} reactions carry {top.E.head(k).sum()/tot:6.1%} of total conductance "
          f"({top.E.head(k).sum():.1f} of {tot:.1f})")

print("\n== the top 20 by E: observed ORFs vs both expectations ==")
t = top.head(20).copy()
t["obs/gem"] = (t.n_orf_obs / t.n_gene_gem).round(1)
print(t[["E", "n_orf_obs", "n_gene_gem", "obs/gem", "n_uniprot"]].to_string())

print("\n== E vs UniProt family size (the prior) ==")
d2 = d[d.n_uniprot > 0].copy()
d2["fam_bin"] = pd.cut(d2.n_uniprot, [0, 10, 100, 1000, 10000, 1e7],
                       labels=["1-10", "10-100", "100-1k", "1k-10k", ">10k"])
print(d2.groupby("fam_bin", observed=True).agg(
    n=("E", "size"), E_med=("E", "median"), E_mean=("E", "mean"),
    n_orf_med=("n_orf_obs", "median"), n_orf_max=("n_orf_obs", "max")).to_string())
print("  spearman(E, n_uniprot) = %.3f ; spearman(n_orf_obs, n_uniprot) = %.3f"
      % (d2.E.corr(d2.n_uniprot, method="spearman"),
         d2.n_orf_obs.corr(d2.n_uniprot, method="spearman")))

print("\n== observed vs iML1515's own gene count, for the 1063 shared reactions ==")
s = d[d.n_gene_gem.notna()].copy()
s["ratio"] = s.n_orf_obs / s.n_gene_gem
print(s[["n_orf_obs", "n_gene_gem", "ratio"]].describe(percentiles=[.5, .9, .99]).round(2).to_string())
print(f"  reactions where the de novo lane finds >3x the curated gene count: "
      f"{(s.ratio > 3).sum()} of {len(s)} ({(s.ratio>3).mean():.1%}), "
      f"carrying {s[s.ratio>3].E.sum()/s.E.sum():.1%} of the shared-set conductance")

print("\n== what an expected-count normalisation would do to the truth ranking ==")
s_all = d.copy()
s_all["truth"] = s_all.index.isin(set(gem.mnxr))
s_all["E_over_uniprot"] = s_all.E / np.log10(s_all.n_uniprot.clip(lower=1) + 10)
s_all["E_over_obs"] = s_all.E / s_all.n_orf_obs

def auroc(y, sc):
    r = pd.Series(sc).rank().to_numpy(); y = np.asarray(y, bool)
    n1, n0 = y.sum(), (~y).sum()
    return (r[y].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)

for name, sc in [("E", s_all.E), ("E / n_orf", s_all.E_over_obs),
                 ("E / log10(uniprot family)", s_all.E_over_uniprot),
                 ("n_uniprot alone", s_all.n_uniprot)]:
    o = sc.sort_values(ascending=False)
    print(f"  {name:28s} AUROC {auroc(s_all.truth, sc):.3f}  "
          f"P@100 {s_all.truth.reindex(o.index[:100]).mean():.2f}  "
          f"P@500 {s_all.truth.reindex(o.index[:500]).mean():.2f}")
