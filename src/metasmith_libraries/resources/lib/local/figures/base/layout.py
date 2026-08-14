from __future__ import annotations
from typing import Any
import numpy as np
from plotly import graph_objects as go, subplots as sp

from ...common import NotImplementedException
from ..colors import XColor, COLORS, ColorValue
from .coordinates import Transform, Xywh2lrbt

# abstract
class Element:
    def _render(self, fig: go.Figure, parent: Panel, kwargs:dict=dict()) -> dict:
        raise NotImplementedException(f"{self} did not implement _render()")

class DebugBox(Element):
    def __init__(self) -> None:
        super().__init__()

    def _render(self, fig: go.Figure, parent: Panel, kwargs:dict=dict()):
        l, r, b, t = Xywh2lrbt(0, 0, 1, 1)
        box = parent.ApplyTransforms(np.array([
            [l, b], [l, t], [r, t], [r, b]
        ]))
        return {
            "type": "path",
            "line": {"color": ColorValue(COLORS.RED),"width": 1},
            "path": " ".join(f"{cmd} {','.join(str(v) for v in pt)}" for cmd, pt in zip("MLLL", box)) + " Z",
        }

class DebugOrigin(Element):
    def __init__(self) -> None:
        super().__init__()

    def _render(self, fig: go.Figure, parent: Panel, kwargs:dict=dict()):
        r = 0.1
        return {
            "type": "path",
            "line": {"color": ColorValue(COLORS.RED),"width": 1},
            "path": f"M -{r},0 L {r},0 M 0,-{r} L 0,{r}",
        } 

class Panel:
    def __init__(self, transform: Transform=Transform(), z: int=0, parent: Panel|None=None) -> None:
        self.transform = transform
        self.z = z
        self._panels: list[Panel] = []
        self._elements: list[Element] = []
        self.hidden = False
        self.parent = parent

    def _render(self, fig: go.Figure, kwargs:dict=dict()):
        for e in self._elements:
            yield e._render(fig, self, kwargs)

    def _add_debug_box(self):
        self._elements.append(DebugBox())

    def AddElement(self, e: Element):
        self._elements.append(e)

    def RemoveElement(self, e: Element):
        self._elements.remove(e)

    def _get_transform(self):
        if self.parent is None: return self.transform
        p = self.parent._get_transform()
        return self.transform + p

    def ApplyTransforms(self, points: np.ndarray):
        t = self._get_transform()
        return t.Apply(points)

    def NewPanel(self, transform: Transform=Transform()):
        panel = Panel(transform, z=len(self._panels))
        panel.parent = self
        self._panels.append(panel)
        return panel

    def RemovePanel(self, panel: Panel):
        self._panels.remove(panel)

class Canvas(Panel):
    def __init__(self, transform: Transform=Transform(), row=1, col=1, bg_col: XColor="white") -> None:
        super().__init__(transform, 0)
        self.bg_col = bg_col
        self._last_render: go.Figure|None = None
        self._row_col = (row, col)

    def _add_origin(self):
        self.AddElement(DebugOrigin())

    def Render(self, fig: go.Figure, debug=False):
        l, r, b, t = Xywh2lrbt(0, 0, 1, 1)
        box = self.ApplyTransforms(np.array([
            [l, b], [l, t], [r, t], [r, b]
        ]))
        x, y = box[:, 0], box[:, 1]
        xr = [x.min(), x.max()]
        yr = [y.min(), y.max()]

        panels: list[Panel] = []
        todo = self._panels.copy()
        while len(todo)>0:
            p = todo.pop()
            if not p.hidden: panels.append(p)
            todo += p._panels
        panels.append(self)
        panels = sorted(panels, key=lambda p: p.z)
        _rows, _cols = fig._get_subplot_rows_columns()
        nrows, ncols = [len(x) for x in [_rows, _cols]]
        row, col = self._row_col
        axis_index = (row-1)*ncols+col
        if axis_index==1: axis_index=""
        def _yield_shapes():
            origin = DebugOrigin()
            if debug: self.AddElement(origin)
            for p in panels:
                box = DebugBox()
                if debug: p.AddElement(box)
                for shape in p._render(fig):
                    yield shape|dict(yref=f"y{axis_index}", xref=f"x{axis_index}")
                if debug: p.RemoveElement(box)
            if debug: self.RemoveElement(origin)
        prev_shapes = list(fig.layout["shapes"])

        _layout = {}
        for k, r in zip([f"xaxis{axis_index}", f"yaxis{axis_index}"], [xr, yr]):
            if k in fig.layout:
                axis = fig.layout[k]
            else:
                axis = {
                    'showticklabels': False,
                    'showgrid': False,
                    'zeroline': False,
                }
            axis["range"] = r
            _layout[k] = axis
        fig = fig.update_layout(_layout, shapes=prev_shapes+list(_yield_shapes()))
            # margin={'l': BORDER, 'r': BORDER, 'b': BORDER, 't': BORDER},
            # paper_bgcolor=self.bg_col,
            # plot_bgcolor=self.bg_col,
            # dragmode='pan',
            # showlegend=False,
        self._last_render = fig
        return fig

    def ShowPlot(self, scroll_zoom=False):
        assert self._last_render is not None, f"render me first"
        self._last_render.show(config=dict(
            scrollZoom=scroll_zoom
        ))

# ---------------------------------------------------------------------------------------

color_axis = 'rgba(0, 0, 0, 0.15)'
color_black = 'rgba(0, 0, 0, 0)'
AXIS: dict = dict(linecolor=color_black, gridcolor=color_axis, zerolinecolor=color_axis, zerolinewidth=1)
LAYOUT = dict(
    autosize=False,
    width=1400,
    height=650,
    margin=dict(
        l=155, r=25, b=25, t=25, pad=5
    ),
    # paper_bgcolor="white",
    font_family="Times New Roman",
    font_color="black",
    font_size=20,
    plot_bgcolor='white',
    xaxis=AXIS,
    yaxis=AXIS,
    xaxis2=AXIS,
    yaxis2=AXIS,
)