import os
from pathlib import Path
import pandas as pd
import numpy as np
import sys
from umap import UMAP
from sklearn.metrics.pairwise import pairwise_distances
# ----------------------------------------------------------------------------

from local.figures.base.layout import Canvas, Panel, Transform
from local.figures.base.geometry import Brush
from local.figures.base.text import TextPlotter
from local.figures.template import BaseFigure, ApplyTemplate, go, SubplotSize
from local.figures.categorical_bars import CategoricalBar
from local.figures.colors import Color, Palettes, COLORS
from hierarchical_clustering import HierarchicalCluster, Deduplicate
# ----------------------------------------------------------------------------

path_matrix, path_out = sys.argv[1:]
df = pd.read_csv(path_matrix)
stability = df["Non-unique Gene name"].to_numpy()
_left = 14
xlabels = list(df.columns[_left:])
for c in xlabels:
    col = df[c]
    df[c] = col.apply(lambda v: len(v.split(' "')) if isinstance(v, str) else 0)
mat = df.iloc[:, _left:].to_numpy()
print("mat.shape, len(stability), len(xlabels)")
print(mat.shape, len(stability), len(xlabels))
# ----------------------------------------------------------------------------

bmat = mat.astype("bool")
ylabels = list(df["Gene"])
gmat, gylabels = Deduplicate(bmat, ylabels)
print("gmat.shape")
print(gmat.shape)
# ----------------------------------------------------------------------------

metric="cosine"
seed = 42
model = UMAP(n_components=1, n_neighbors= min(15, len(gmat)-1), metric=metric, transform_seed=seed)
_emb = model.fit_transform(gmat)
print("_emb.shape")
print(_emb.shape)
# ----------------------------------------------------------------------------

gclust = HierarchicalCluster(gmat, gylabels, method="complete", sort_order=_emb[:, 0])
print("len(gclust.labels), gclust.mat.shape")
print(len(gclust.labels), gclust.mat.shape)
# ----------------------------------------------------------------------------

pdist = pairwise_distances(bmat.T, metric="jaccard")
# display names: `genus species PCC NNNN` (manifest kept them hyphen-joined)
dlabels = [x.replace('-', ' ') for x in xlabels]
clust = HierarchicalCluster(pdist, labels=dlabels, method="complete", metric="precomputed", distance_sort=False)
print("clust.labels")
print(clust.labels)
# ----------------------------------------------------------------------------

_gorder = np.array([i for g in gylabels for i in g.groupi])
_gstability = stability[_gorder]
_new = []
for i, c in enumerate(["cloud", "shell", "persistent"][::-1]):
    _f = _gstability==c
    _new.append(_gorder[_f])
gi = np.hstack(_new)
gstability = stability[gi]
# ----------------------------------------------------------------------------

row_heights=[1, 3]
column_widths=[2*len(xlabels), 1, 5]
COLS, ROWS = len(column_widths), len(row_heights)
fig = BaseFigure(
    shape=(COLS, ROWS),
    row_heights=row_heights,
    column_widths=column_widths,
    shared_xaxes=False, shared_yaxes=False,
    horizontal_spacing=0, vertical_spacing=0,
    specs=[
        [{}, {}, {"rowspan": 2, "colspan": 1}],
        [{}, {}, None],
    ]
)
WIDTH, HEIGHT = 800, 800

_black = Color.Hex("212121")
_colorscale = [
    [0, COLORS.WHITE],
    [1/2, _black.color_value],
    [1, COLORS.RED],
]

# anti aliasing
z = mat.clip(0, 2)[gi][:, clust.order]
seg = 10
n_seg = len(z)//seg
new_len = seg*n_seg
print(seg, n_seg, new_len)
z = z[:new_len].reshape((n_seg, z.shape[1], -1))
z = z.mean(axis=2)

fig.add_trace(
    go.Heatmap(
        z = z,
        zmin=0, zmax=2,
        x = clust.labels,
        colorscale=_colorscale,
        showscale=False,
    ),
    row=2, col=1,
)

BORDER=5
TSO = 30
invis = dict(showticklabels=False, linecolor=COLORS.TRANSPARENT, ticks=None)
fig = ApplyTemplate(
    fig,
    default_yaxis=invis,
    axis = {
        "1 1 x": invis,
        "1 1 y": dict(title="Jaccard", linecolor=COLORS.BLACK, showticklabels=True, ticks="outside", tickvals=[-0.5, 0, 0.5], ticktext=[f"{v*clust.root_distance:.02}" for v in [0, 0.5, 1]]),
        "1 2 x": dict(title="Genomes"),
        "1 2 y": invis|dict(title=f"Gene families N={len(mat)}", title_standoff=50, linecolor=COLORS.BLACK),
        "2 2 x": invis,
        "3 1 x": invis|dict(range=[0, 1]),
        "3 1 y": invis|dict(range=[0, 1]),
    },
    layout=dict(
        width=WIDTH, height=HEIGHT,
        margin={'l': 90, 'r': 0, 'b': BORDER, 't': 15},
    )
)

cvs = Canvas(
    row=1, col=1,
)

cvsb = Canvas(
    row=2, col=2,
)
bar_panel = cvsb.NewPanel(Transform(rotation=np.pi/2))
color_map = {'cloud': COLORS.CYAN, 'shell': COLORS.GREEN, 'persistent': COLORS.ORANGE}
CategoricalBar(
    gstability, bar_panel, circular=False, thickness=1,
    color_map=color_map,
)

relh = row_heights[0]/sum(row_heights)
relw = column_widths[0]/sum(column_widths)
relw, relh = SubplotSize(fig, 1, 1, 2) # must be called after drawn (set_layout()?)
sx = relw/relh * (len(xlabels)/(len(xlabels)+1))
ptree = cvs.NewPanel(transform=Transform(dx=-0.5, dy=-0.5, sx=(len(xlabels)/(len(xlabels)+1)), sy=0.95))
btree = Brush(_black)
ptree.AddElement(btree)

w = 0.01
for n in clust.tree.Traverse():
    if n.IsLeaf(): continue
    btree.Line(n.left.x+w/2/sx, n.y, n.right.x-w/2/sx, n.y, w=w)
    btree.Line(n.left.x, n.y+w/2, n.left.x, n.left.y-w/2, w=w/sx)
    btree.Line(n.right.x, n.y+w/2, n.right.x, n.right.y-w/2, w=w/sx)

fig = cvs.Render(
    fig=fig,
    # debug=True
)
fig = cvsb.Render(
    fig=fig,
    # debug=True
)

_legend = []
for k, c in list(color_map.items())[::-1]:
    _legend.append((k, c))
_legend.append(("", COLORS.TRANSPARENT))
for k, (_, c) in zip("absent, present, multicopy".split(", "), _colorscale):
    _legend.append((k, c))
def _ly(i):
    r = i/len(_legend)
    return 0.5+r/2
fig.add_trace(
    go.Scatter(
        x = [0.15]*len(_legend),
        y = [_ly(i) for i in range(len(_legend))],
        text = [t for t, c in _legend],
        mode = "markers+text",
        textposition="middle right",
        marker = dict(
            color=[c for t, c in _legend], 
            symbol = "square", size=15, 
            line=dict(color=[COLORS.BLACK if c==COLORS.WHITE else COLORS.TRANSPARENT for t, c in _legend], width=1),
        ),
        name="",
    ),
    row=1, col=3,
)

print("writing image")
fig.write_image(path_out)
print("done")
