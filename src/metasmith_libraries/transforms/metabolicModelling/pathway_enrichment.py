from metasmith.python_api import *
from pathlib import Path

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image    = model.AddRequirement(lib.GetType("env::metabolomics-python.env"))
diff_dir = model.AddRequirement(lib.GetType("metabolomics::metabolomics_differential"))
out_dir  = model.AddProduct(lib.GetType("metabolomics::metabolomics_pathway_enrichment"))


def protocol(context: ExecutionContext):
    idiff = context.Input(diff_dir)
    iout  = context.Output(out_dir)

    script = Path("pathway_enrichment_script.py")
    script.write_text('''
import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path
from scipy.stats import fisher_exact
from statsmodels.stats.multitest import multipletests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

diff_dir = Path(sys.argv[1])
out_dir  = Path(sys.argv[2])
out_dir.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# 1. Collect all detected KEGG IDs (background) and significant sets
# -------------------------------------------------------------------
all_kegg     = set()
sig_sets     = {}  # comparison -> set of KEGG IDs

for csv_path in sorted(diff_dir.glob("*_vs_*.csv")):
    if csv_path.name == "all_comparisons_summary.csv":
        continue
    df = pd.read_csv(csv_path)
    kegg_col = next((c for c in df.columns if c.lower() in {"kegg_id", "kegg"}), None)
    if kegg_col is None:
        continue
    detected = df[df[kegg_col].notna() & (df[kegg_col] != "")][kegg_col].astype(str)
    all_kegg.update(detected)
    sig_df = df[df.get("significant", False) == True]
    sig_ids = set(sig_df[sig_df[kegg_col].notna()][kegg_col].astype(str))
    if sig_ids:
        sig_sets[csv_path.stem] = sig_ids

if not all_kegg:
    print("No KEGG IDs found in differential results; writing empty output.")
    pd.DataFrame(columns=["pathway", "comparison", "pval", "adj_pval",
                           "overlap_ratio", "metabolites"]).to_csv(out_dir / "enrichment.csv", index=False)
    sys.exit(0)

# -------------------------------------------------------------------
# 2. Load KEGG pathway-compound mapping via sspa or REST fallback
# -------------------------------------------------------------------
pathway_compounds = {}  # pathway_id -> set(KEGG compound IDs)
pathway_names     = {}  # pathway_id -> name

try:
    import sspa
    pw = sspa.process_kegg("map01100")  # global metabolic overview
    if hasattr(pw, "pathways"):
        for pid, info in pw.pathways.items():
            cpds = set(info.get("compounds", []))
            if cpds:
                pathway_compounds[pid] = cpds
                pathway_names[pid] = info.get("name", pid)
except Exception:
    pass

# Fallback: KEGG REST API
if not pathway_compounds:
    import requests, time
    # Get list of all metabolic pathways for reference organism (map = generic)
    try:
        resp = requests.get("https://rest.kegg.jp/list/pathway/map", timeout=30)
        if resp.ok:
            for line in resp.text.strip().split("\\n"):
                parts = line.split("\\t")
                if len(parts) >= 2:
                    pid = parts[0].replace("path:", "")
                    pathway_names[pid] = parts[1]
        time.sleep(0.5)

        for pid in list(pathway_names.keys()):
            try:
                r = requests.get(f"https://rest.kegg.jp/link/compound/{pid}", timeout=15)
                if r.ok and r.text.strip():
                    cpds = set()
                    for line in r.text.strip().split("\\n"):
                        p = line.split("\\t")
                        if len(p) >= 2:
                            cpds.add(p[1].replace("cpd:", ""))
                    if cpds:
                        pathway_compounds[pid] = cpds
                time.sleep(0.35)  # KEGG rate limit
            except Exception:
                continue
    except Exception as e:
        print(f"KEGG REST API error: {e}")

if not pathway_compounds:
    print("Could not load KEGG pathway data; writing empty output.")
    pd.DataFrame(columns=["pathway", "comparison", "pval", "adj_pval",
                           "overlap_ratio", "metabolites"]).to_csv(out_dir / "enrichment.csv", index=False)
    sys.exit(0)

# -------------------------------------------------------------------
# 3. ORA (Fisher exact test) per comparison
# -------------------------------------------------------------------
background = all_kegg
N = len(background)
all_enrichment = []

for comp_name, sig_ids in sig_sets.items():
    n_sig = len(sig_ids)
    if n_sig == 0:
        continue
    records = []
    for pid, cpds in pathway_compounds.items():
        cpds_in_bg  = cpds & background
        K = len(cpds_in_bg)
        if K < 2:
            continue
        overlap = sig_ids & cpds_in_bg
        x = len(overlap)
        if x == 0:
            continue
        # 2x2 table for Fisher exact
        a = x
        b = n_sig - x
        c = K - x
        d = N - n_sig - K + x
        _, pval = fisher_exact([[a, b], [c, d]], alternative="greater")
        records.append({
            "pathway":       pid,
            "pathway_name":  pathway_names.get(pid, pid),
            "comparison":    comp_name,
            "overlap":       x,
            "pathway_size":  K,
            "sig_size":      n_sig,
            "background":    N,
            "overlap_ratio": x / K if K > 0 else 0,
            "pval":          pval,
            "metabolites":   ";".join(sorted(overlap)),
        })
    if records:
        rdf = pd.DataFrame(records)
        _, adj_p, _, _ = multipletests(rdf["pval"].values, method="fdr_bh")
        rdf["adj_pval"] = adj_p
        all_enrichment.append(rdf)

if all_enrichment:
    enrich_df = pd.concat(all_enrichment, ignore_index=True)
    enrich_df.to_csv(out_dir / "enrichment.csv", index=False)

    # -------------------------------------------------------------------
    # 4. Dot plot: pathway vs comparison, size=overlap_ratio, color=-log10(q)
    # -------------------------------------------------------------------
    sig_enrich = enrich_df[enrich_df["adj_pval"] < 0.1].copy()
    if not sig_enrich.empty:
        sig_enrich["-log10q"] = -np.log10(sig_enrich["adj_pval"].clip(lower=1e-20))
        top_pw = sig_enrich.groupby("pathway_name")["-log10q"].max().nlargest(20).index
        plot_df = sig_enrich[sig_enrich["pathway_name"].isin(top_pw)]

        comparisons = sorted(plot_df["comparison"].unique())
        pathways    = sorted(plot_df["pathway_name"].unique(), key=lambda x: plot_df[plot_df["pathway_name"] == x]["-log10q"].max(), reverse=True)

        fig, ax = plt.subplots(figsize=(max(6, len(comparisons) * 1.5), max(4, len(pathways) * 0.4)))
        for _, row in plot_df.iterrows():
            xi = comparisons.index(row["comparison"])
            yi = pathways.index(row["pathway_name"])
            ax.scatter(xi, yi,
                       s=row["overlap_ratio"] * 300 + 20,
                       c=row["-log10q"],
                       cmap="YlOrRd", vmin=0, vmax=plot_df["-log10q"].max(),
                       edgecolors="k", linewidths=0.5)
        ax.set_xticks(range(len(comparisons)))
        ax.set_xticklabels(comparisons, rotation=45, ha="right", fontsize=8)
        ax.set_yticks(range(len(pathways)))
        ax.set_yticklabels(pathways, fontsize=8)
        ax.set_xlabel("Comparison")
        ax.set_ylabel("KEGG Pathway")
        ax.set_title("Pathway Enrichment (ORA)")
        plt.colorbar(ax.collections[0], ax=ax, label="-log10(q-value)", shrink=0.6)
        plt.tight_layout()
        fig.savefig(out_dir / "enrichment_dotplot.png", dpi=150)
        plt.close()
else:
    pd.DataFrame(columns=["pathway", "comparison", "pval", "adj_pval",
                           "overlap_ratio", "metabolites"]).to_csv(out_dir / "enrichment.csv", index=False)

print(f"Pathway enrichment complete. Outputs in {out_dir}")
''')

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idiff.external, "/diff_data")],
        cmd=f"python {script} /diff_data {iout.container}",
    )

    return ExecutionResult(
        manifest=[{out_dir: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=diff_dir,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
