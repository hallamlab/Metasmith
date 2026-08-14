from pathlib import Path
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
exp    = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
de     = model.AddRequirement(lib.GetType("transcriptomics::deseq2_results"), parents={exp})
image  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out    = model.AddProduct(lib.GetType("transcriptomics::volcano_plot"))
outpng = model.AddProduct(lib.GetType("transcriptomics::volcano_plot_png"))

def protocol(context: ExecutionContext):
    ide    = context.Input(de)
    iout   = context.Output(out)
    ioutpng = context.Output(outpng)

    script = Path("volcano_plot.py")
    with open(script, "w") as f:
        f.write("""\
import sys
import subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "plotly", "kaleido", "-q", "--no-warn-script-location"])

import math
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

de_file     = sys.argv[1]
output_file = sys.argv[2]
png_file    = sys.argv[3]

de = pd.read_csv(de_file, index_col="gene_id")

# Discover comparisons from column prefixes (pattern: {alt}_vs_{ref}_log2FoldChange)
comp_map = {}
for col in de.columns:
    if col.endswith("_log2FoldChange"):
        comp = col[: -len("_log2FoldChange")]
        alt, ref = comp.split("_vs_")
        comp_map[(alt, ref)] = comp
print(f"Comparisons: {list(comp_map.values())}")

# Extract sorted unique condition names
conds = sorted({c for pair in comp_map for c in pair})
print(f"Conditions (sorted): {conds}")

# 2x2 grid: rows = conds[1:], cols = conds[:-1]
row_conds = conds[1:]   # e.g. [POR-8, POR-S]
col_conds = conds[:-1]  # e.g. [POR-0, POR-8]
nrows, ncols = len(row_conds), len(col_conds)

fig = make_subplots(
    rows=nrows, cols=ncols,
    subplot_titles=[None] * (nrows * ncols),
    column_titles=col_conds,
    row_titles=row_conds,
    shared_xaxes=True,
    shared_yaxes=True,
    horizontal_spacing=0.06,
    vertical_spacing=0.08,
)

for ri, row_cond in enumerate(row_conds):
    for ci, col_cond in enumerate(col_conds):
        r, c = ri + 1, ci + 1
        # Skip upper triangle: col condition index >= row condition index
        row_idx = conds.index(row_cond)
        col_idx = conds.index(col_cond)
        if col_idx >= row_idx:
            # Hide axes for empty cell
            fig.update_xaxes(visible=False, row=r, col=c)
            fig.update_yaxes(visible=False, row=r, col=c)
            continue

        # Look up comparison: row_cond vs col_cond
        comp = comp_map.get((row_cond, col_cond))
        if comp is None:
            fig.update_xaxes(visible=False, row=r, col=c)
            fig.update_yaxes(visible=False, row=r, col=c)
            continue

        log2fc = de[f"{comp}_log2FoldChange"]
        padj   = de[f"{comp}_padj"].fillna(1.0)
        neg_log10p = -np.log10(np.clip(padj, 1e-300, 1.0))

        sig_up   = (padj < 0.05) & (log2fc > 1)
        sig_down = (padj < 0.05) & (log2fc < -1)
        ns       = ~(sig_up | sig_down)

        label = comp.replace("_vs_", " vs ")

        for mask, color, name in [
            (sig_up,   "#d62728", f"{label} up"),
            (sig_down, "#1f77b4", f"{label} down"),
            (ns,       "#999999", f"{label} n.s."),
        ]:
            if mask.sum() == 0:
                continue
            fig.add_trace(go.Scatter(
                x=log2fc[mask],
                y=neg_log10p[mask],
                mode="markers",
                name=name,
                showlegend=False,
                marker=dict(
                    size=5 if color == "#999999" else 6,
                    color=color,
                    opacity=0.3 if color == "#999999" else 0.8,
                    line=dict(width=0.5 if color != "#999999" else 0, color="black"),
                ),
                hovertemplate=(
                    f"<b>{label}</b><br>"
                    "Gene: %{customdata}<br>"
                    "log2FC: %{x:.3f}<br>"
                    "-log10(padj): %{y:.2f}<extra></extra>"
                ),
                customdata=de.index[mask].tolist(),
            ), row=r, col=c)

        # Threshold lines
        padj_y = -math.log10(0.05)
        fig.add_hline(y=padj_y, line_dash="dash", line_color="gray",
                      line_width=0.8, row=r, col=c)
        fig.add_vline(x=-1, line_dash="dash", line_color="lightgray",
                      line_width=0.8, row=r, col=c)
        fig.add_vline(x=1, line_dash="dash", line_color="lightgray",
                      line_width=0.8, row=r, col=c)

        n_up   = sig_up.sum()
        n_down = sig_down.sum()
        print(f"  {label}: {n_up} up, {n_down} down (padj<0.05, |log2FC|>1)")

fig.update_layout(
    title="Volcano Plots — DESeq2 Pairwise Differential Expression",
    template="plotly_white",
    width=900,
    height=800,
    margin=dict(l=80, r=40, t=80, b=60),
)

# Axis labels on outer edges only
for ci in range(ncols):
    fig.update_xaxes(title_text="log2FC", row=nrows, col=ci + 1)
for ri in range(nrows):
    fig.update_yaxes(title_text="-log10(padj)", row=ri + 1, col=1)

fig.write_html(output_file, include_plotlyjs="cdn")
print(f"Wrote {output_file}")
fig.write_image(png_file, width=900, height=800, scale=2)
print(f"Wrote {png_file}")
""")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python volcano_plot.py {ide.container} {iout.container} {ioutpng.container}",
    )
    return ExecutionResult(
        manifest=[{out: iout.local}, {outpng: ioutpng.local}],
        success=iout.local.exists() and ioutpng.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
