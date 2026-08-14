from metasmith.python_api import *
from pathlib import Path

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image   = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
feat_ft = model.AddRequirement(lib.GetType("metabolomics::metabolomics_feature_table"))
out_dir = model.AddProduct(lib.GetType("metabolomics::metabolomics_differential"))


def protocol(context: ExecutionContext):
    ift  = context.Input(feat_ft)
    iout = context.Output(out_dir)

    script = Path("diff_analysis_script.py")
    script.write_text('''
import sys
import json
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
from statsmodels.stats.multitest import multipletests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

feature_csv = Path(sys.argv[1])
out_dir     = Path(sys.argv[2])
out_dir.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(feature_csv)

# Detect sample columns and metadata columns
sample_cols = [c for c in df.columns if "161-pe" in c.lower()]
meta_cols   = [c for c in df.columns if c not in sample_cols]

# Parse condition from column name
def parse_condition(name):
    n = name.lower()
    if "0h" in n:
        return "0h"
    for cond in ["8", "p", "s"]:
        if f"-{cond}-" in n:
            return cond
    return "unknown"

def parse_timepoint(name):
    for tp in [24, 72, 144]:
        if f"{tp}h" in name.lower():
            return tp
    return 0

col_meta = pd.DataFrame({
    "sample":    sample_cols,
    "condition": [parse_condition(c) for c in sample_cols],
    "timepoint": [parse_timepoint(c) for c in sample_cols],
})

baseline_cols = col_meta[col_meta["condition"] == "0h"]["sample"].tolist()
conditions    = [c for c in ["8", "p", "s"] if c in col_meta["condition"].values]
timepoints    = sorted(col_meta[col_meta["condition"] != "0h"]["timepoint"].unique())

all_results = []

for cond in conditions:
    cond_meta = col_meta[col_meta["condition"] == cond]
    for tp in timepoints:
        tp_cols  = cond_meta[cond_meta["timepoint"] == tp]["sample"].tolist()
        if not tp_cols or not baseline_cols:
            continue

        comparison = f"{cond}_vs_0h_t{tp}h"
        records = []
        for _, row in df.iterrows():
            feat_id = row.get("compound_name", row.get("feature_id", str(_)))
            g1 = row[tp_cols].dropna().values
            g2 = row[baseline_cols].dropna().values
            if len(g1) < 2 or len(g2) < 2:
                continue

            mean1, mean2 = g1.mean(), g2.mean()
            log2fc = mean1 - mean2  # already log2 in input

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                _, pval = stats.ttest_ind(g1, g2, equal_var=False)

            records.append({
                "feature":     feat_id,
                "compound_name": row.get("compound_name", ""),
                "kegg_id":     row.get("KEGG_ID", row.get("kegg_id", "")),
                "inchi_key":   row.get("inchi_key", ""),
                "log2FC":      log2fc,
                "pval":        pval,
                "condition":   cond,
                "timepoint_h": tp,
            })

        if not records:
            continue

        res_df = pd.DataFrame(records).dropna(subset=["pval"])
        _, adj_p, _, _ = multipletests(res_df["pval"].values, method="fdr_bh")
        res_df["adj_pval"]     = adj_p
        res_df["significant"]  = (res_df["adj_pval"] < 0.05) & (res_df["log2FC"].abs() > 1)

        out_csv = out_dir / f"{comparison}.csv"
        res_df.to_csv(out_csv, index=False)
        all_results.append(res_df.assign(comparison=comparison))

        # Volcano plot
        fig, ax = plt.subplots(figsize=(7, 5))
        ns = res_df[~res_df["significant"]]
        sg = res_df[res_df["significant"]]
        ax.scatter(ns["log2FC"], -np.log10(ns["pval"] + 1e-300), color="grey", alpha=0.4, s=10, label="n.s.")
        ax.scatter(sg["log2FC"], -np.log10(sg["pval"] + 1e-300), color="tomato", s=15, label="significant")
        ax.axvline(1,  color="steelblue", linestyle="--", linewidth=0.8)
        ax.axvline(-1, color="steelblue", linestyle="--", linewidth=0.8)
        ax.axhline(-np.log10(0.05), color="orange", linestyle=":", linewidth=0.8)
        ax.set_xlabel("log2 Fold Change")
        ax.set_ylabel("-log10(p-value)")
        ax.set_title(comparison)
        ax.legend(fontsize=8)
        plt.tight_layout()
        fig.savefig(out_dir / f"{comparison}_volcano.png", dpi=150)
        plt.close()

# One-way ANOVA across all 3 conditions per feature per timepoint
anova_records = []
for tp in timepoints:
    for _, row in df.iterrows():
        feat_id = row.get("compound_name", row.get("feature_id", str(_)))
        groups  = []
        for cond in conditions:
            tp_cols = col_meta[(col_meta["condition"] == cond) & (col_meta["timepoint"] == tp)]["sample"].tolist()
            g = row[tp_cols].dropna().values if tp_cols else np.array([])
            groups.append(g)
        groups = [g for g in groups if len(g) >= 2]
        if len(groups) < 2:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fstat, pval = stats.f_oneway(*groups)
        anova_records.append({
            "feature":     feat_id,
            "compound_name": row.get("compound_name", ""),
            "timepoint_h": tp,
            "F_stat":      fstat,
            "pval":        pval,
        })

if anova_records:
    anova_df = pd.DataFrame(anova_records).dropna(subset=["pval"])
    _, adj_p, _, _ = multipletests(anova_df["pval"].values, method="fdr_bh")
    anova_df["adj_pval"] = adj_p
    anova_df.to_csv(out_dir / "anova_all_conditions.csv", index=False)

# Summary of all comparisons
if all_results:
    summary = pd.concat(all_results, ignore_index=True)
    summary.to_csv(out_dir / "all_comparisons_summary.csv", index=False)

print(f"Differential analysis complete. Outputs in {out_dir}")
''')

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python {script} {ift.container} {iout.container}",
    )

    return ExecutionResult(
        manifest=[{out_dir: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=feat_ft,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
