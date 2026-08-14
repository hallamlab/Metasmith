"""Histogram of cross-genome BLAST Score Ratio (BSR) distances.

Input : pooled all-vs-all diamond blastp, outfmt 6, with genome-tagged ids of
        the form  <genome>__<protein_id>  (qseqid, sseqid).
Output: SVG histogram.

BSR(query, subject) = bitscore(query, subject) / bitscore(query, query-self).
We keep, per (query protein, target genome), the single best bitscore, then plot
distance = 1 - BSR over cross-genome (query genome != subject genome) pairs.
"""
import sys
import pandas as pd
import numpy as np
import plotly.graph_objects as go

path_blast, path_out = sys.argv[1:]

COLS = ["qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
        "qstart", "qend", "sstart", "send", "evalue", "bitscore"]
df = pd.read_csv(path_blast, sep="\t", names=COLS)
print("blast rows:", len(df))

# genome tag is the prefix before the '__' separator
df["qg"] = df["qseqid"].str.split("__").str[0]
df["sg"] = df["sseqid"].str.split("__").str[0]
print("genomes:", sorted(df["qg"].unique()))

# self bitscore per query protein (query aligned to itself = max possible score)
self_hits = df[df["qseqid"] == df["sseqid"]]
self_score = self_hits.groupby("qseqid")["bitscore"].max()
print("queries with a self score:", len(self_score))

# cross-genome hits only
cross = df[df["qg"] != df["sg"]].copy()
print("cross-genome hits:", len(cross))

# best hit per (query protein, target genome)
best = cross.groupby(["qseqid", "sg"], sort=False)["bitscore"].max().reset_index()

# normalize by the query's self score
best["self"] = best["qseqid"].map(self_score)
dropped = int(best["self"].isna().sum())
if dropped:
    print(f"dropped {dropped} obs with no self score")
best = best.dropna(subset=["self"])
best["bsr"] = best["bitscore"] / best["self"]
best["dist"] = (1.0 - best["bsr"]).clip(0.0, 1.0)

dist = best["dist"].to_numpy()
print("observations:", len(dist))
if len(dist):
    print("dist  min/median/max: %.3f / %.3f / %.3f"
          % (dist.min(), float(np.median(dist)), dist.max()))

fig = go.Figure()
fig.add_trace(go.Histogram(
    x=dist,
    xbins=dict(start=0.0, end=1.0, size=0.02),
    marker=dict(color="#1f4e79", line=dict(color="#0d2840", width=0.5)),
))
fig.update_layout(
    width=900, height=600,
    template="simple_white",
    bargap=0.02,
    title=dict(
        text=f"Cross-genome BSR distance — best hit per (query protein × target genome), "
             f"N={len(dist):,}",
        x=0.5, xanchor="center", font=dict(size=15),
    ),
    xaxis=dict(title="BSR distance  (1 − BSR);  0 = strong homolog, 1 = no similarity",
               range=[0, 1], dtick=0.1),
    yaxis=dict(title="count"),
    margin=dict(l=80, r=30, t=70, b=70),
)
fig.write_image(path_out)
print("wrote", path_out)
