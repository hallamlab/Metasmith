import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

HERE = Path(__file__).resolve().parent
E4 = HERE.parent

STUDIES = [
    ("sunagawa2015", "Sunagawa 2015"),
    ("karlsson2013", "Karlsson 2013"),
    ("bissett_base", "Bissett"),
    ("li2019", "Li 2019"),
    ("korem2015", "Korem 2015"),
]
LANES = [
    ("repro", "Reproduction (CarveMe 1.2.2)", "#2a78d6"),
    ("modern", "Modern (CarveMe 1.6.6)", "#eb6834"),
]
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
FONT = dict(family="Arial, Helvetica, sans-serif", size=13, color=INK)


def reaction_jaccard(lane):
    df = pd.read_csv(E4 / f"parity_{lane}.tsv", sep="\t")
    df = df[df.reactions_ours != "no published GEM"]
    df = df.astype({c: int for c in ("reactions_ours", "reactions_theirs", "reactions_shared")})
    union = df.reactions_ours + df.reactions_theirs - df.reactions_shared
    return df.assign(jaccard=df.reactions_shared / union)


def fig1():
    fig = go.Figure()
    half = 0.17
    for i, (study, _) in enumerate(STUDIES):
        for j, (lane, name, color) in enumerate(LANES):
            y = reaction_jaccard(lane).query("study == @study").jaccard
            x = i + (j - 0.5) * 0.42
            fig.add_trace(go.Violin(
                x=[x] * len(y), y=y, side="negative", width=2 * half, spanmode="hard",
                points=False, line=dict(color=color, width=1), fillcolor=color, opacity=0.55,
                name=name, legendgroup=lane, showlegend=i == 0, hoverinfo="skip",
            ))
            fig.add_trace(go.Box(
                x=[x + 0.05] * len(y), y=y, width=0.07, boxpoints=False,
                line=dict(color=color, width=1.5), fillcolor="white",
                legendgroup=lane, showlegend=False, hoverinfo="skip",
            ))
            fig.add_annotation(x=x, y=1.0, yshift=10, text=f"{len(y):,}", showarrow=False,
                               font=dict(size=11, color=MUTED))
    fig.add_annotation(x=-0.62, y=1.0, yshift=10, text="N", showarrow=False, xanchor="right",
                       font=dict(size=11, color=MUTED))
    fig.update_layout(
        width=900, height=480, font=FONT, plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(l=70, r=20, t=40, b=60), violinmode="overlay", boxmode="overlay",
        legend=dict(orientation="h", x=0, y=-0.14, xanchor="left", font=dict(size=12)),
    )
    fig.update_xaxes(tickvals=list(range(len(STUDIES))), ticktext=[s[1] for s in STUDIES],
                     range=[-0.7, len(STUDIES) - 0.4], showgrid=False, zeroline=False,
                     showline=True, linecolor=MUTED, ticks="")
    fig.update_yaxes(title="Reaction Jaccard vs metaGEM", range=[0, 1.06], dtick=0.2,
                     gridcolor=GRID, zeroline=False, showline=True, linecolor=MUTED,
                     ticks="outside", tickcolor=MUTED)
    return fig


SOURCES = ["metagem", "R0", "R1", "B", "U", "V", "G", "D", "S", "M"]
SOURCE_LABELS = ["meta<br>GEM"] + SOURCES[1:]
OLD_CODE = {"R0", "R1", "B", "U"}
PANELS = [
    ("total_score", "Total"),
    ("consistency", "Consistency"),
    ("annotation_met", "Metabolite annotation"),
    ("annotation_rxn", "Reaction annotation"),
    ("annotation_gene", "Gene annotation"),
    ("annotation_sbo", "SBO annotation"),
]
REFERENCE = "#9a9994"


def source_color(source):
    if source == "metagem":
        return REFERENCE
    return LANES[0][2] if source in OLD_CODE else LANES[1][2]


def fig2():
    df = pd.read_csv(E4 / "ablation_memote.tsv", sep="\t")
    stats = df.groupby("source")[[c for c, _ in PANELS]].quantile([0.25, 0.5, 0.75]).unstack()
    fig = make_subplots(rows=2, cols=3, subplot_titles=[t for _, t in PANELS],
                        shared_xaxes=True, vertical_spacing=0.14, horizontal_spacing=0.06)
    colors = [source_color(s) for s in SOURCES]
    for k, (col, _) in enumerate(PANELS):
        row, column = k // 3 + 1, k % 3 + 1
        q1, med, q3 = (stats[(col, q)].reindex(SOURCES) for q in (0.25, 0.5, 0.75))
        fig.add_trace(go.Bar(
            x=SOURCE_LABELS, y=med, marker=dict(color=colors, line=dict(width=0)), width=0.72,
            error_y=dict(type="data", symmetric=False, array=q3 - med, arrayminus=med - q1,
                         color=MUTED, thickness=1, width=3),
            showlegend=False, hovertemplate="%{x}: %{y:.3f}<extra></extra>",
        ), row=row, col=column)
        fig.add_trace(go.Scatter(
            x=SOURCE_LABELS, y=med, mode="markers", showlegend=False, hoverinfo="skip",
            marker=dict(symbol="line-ew", size=14, line=dict(color=colors, width=2.5)),
        ), row=row, col=column)
    for name, color in [("metaGEM (published)", REFERENCE), ("CarveMe 1.2.2 code", LANES[0][2]),
                        ("CarveMe 1.6.6 code", LANES[1][2])]:
        fig.add_trace(go.Bar(x=[None], y=[None], name=name, marker_color=color))
    fig.update_layout(
        width=1000, height=600, font=FONT, plot_bgcolor="white", paper_bgcolor="white", bargap=0.2,
        margin=dict(l=60, r=20, t=40, b=70),
        legend=dict(orientation="h", x=0, y=-0.1, xanchor="left", font=dict(size=12)),
    )
    fig.update_annotations(font=dict(size=13, color=INK))
    fig.update_xaxes(showgrid=False, zeroline=False, showline=True, linecolor=MUTED, ticks="",
                     tickfont=dict(size=10), tickangle=0)
    fig.update_yaxes(range=[0, 1.02], dtick=0.25, gridcolor=GRID, zeroline=False, showline=True,
                     linecolor=MUTED, ticks="outside", tickcolor=MUTED, tickfont=dict(size=11))
    fig.update_yaxes(title="MEMOTE score", row=1, col=1)
    fig.update_yaxes(title="MEMOTE score", row=2, col=1)
    return fig


FIGURES = {"fig1_reproduction": fig1, "fig2_ablation_memote": fig2}

if __name__ == "__main__":
    for name in sys.argv[1:] or FIGURES:
        FIGURES[name]().write_image(HERE / f"{name}.svg")
        print(HERE / f"{name}.svg")
