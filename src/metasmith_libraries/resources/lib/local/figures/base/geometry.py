from typing import Any
import numpy as np
from plotly import graph_objects as go

from .layout import Element, Panel
from ..colors import XColor, ColorValue

class Brush(Element):
    def __init__(self, color: XColor, below_traces=False, union_fill=True) -> None:
        self._pts: list[np.ndarray] = []
        self._cmds: list[str] = []
        self._color = color
        self._below_traces = below_traces
        self._union_fill = union_fill
        self._line_width = 0
        self._line_col = "black"

    def SetLineWidth(self, w: float):
        self._line_width = w

    def SetLineColour(self, col: XColor):
        self._line_col = ColorValue(col)

    def _render(self, fig: go.Figure, parent: Panel, kwargs:dict=dict()):
        # SHAPES = "shapes"
        # lay: Any = fig.layout
        # shapes = list(lay[SHAPES]) if hasattr(lay, SHAPES) else []
        def _draw_path(cmds: str, pts:np.ndarray):
            _path = []
            for c, (x, y) in zip(cmds, pts):
                _path.append(f"{c} {x},{y}")
            _path.append("Z")
            return " ".join(_path)

        data = {
            "type": "path",
            "fillcolor": ColorValue(self._color),
            "line": {
                "color": self._line_col,
                "width": self._line_width,
            },
            "path": " ".join(_draw_path(c, parent.ApplyTransforms(p)) for c, p in zip(self._cmds, self._pts)), # type: ignore
            "layer": "below" if self._below_traces else "above",
            "fillrule": "nonzero" if self._union_fill else "evenodd"
        }
        data.update(kwargs)
        # shapes.append(data)
        return data
        # fig.update_layout(shapes=shapes)

    def Line(self, sx, sy, ex, ey, w=0.01):
        dy, dx = ey-sy, ex-sx
        epsilon = 1e-6 # used for zero
        if abs(dx) > epsilon and abs(dy) > epsilon:
            slope = dy/dx
            perp = -1/slope
            scale = w/2/np.sqrt(1+(perp*perp))
            ox, oy = 1*scale, perp*scale
        else:
            if dx <= epsilon:
                ox, oy = w/2, 0
            else:
                ox, oy = 0, w/2

        self._pts.append(np.array([
            [sx-ox, sy-oy],
            [sx+ox, sy+oy],
            [ex+ox, ey+oy],
            [ex-ox, ey-oy],
        ]))
        self._cmds.append("MLLL")

    def EllipticalArc(
            self,
            x_rad: float=1/2, y_rad: float|None=None,
            width: float = 0.01,
            start_angle:float=0, end_angle=np.pi/2,
            x_center:float=0, y_center:float=0,
            pie: bool=False,
            resolution=128,
        ):
        while end_angle < start_angle:
            end_angle += 2*np.pi
        if y_rad is None: y_rad = x_rad
        n = int(abs(start_angle-end_angle)/(2*np.pi) * resolution) + 1
        n = max(n, 2)
        start_angle, end_angle = [-r+np.pi/2 for r in [start_angle, end_angle]]
        t = np.linspace(start_angle, end_angle, n)
        xri, xro, yri, yro = x_rad-width/2, x_rad+width/2, y_rad-width/2, y_rad+width/2
        xi, yi = x_center + xri*np.cos(t), y_center + yri*np.sin(t)
        xo, yo = x_center + xro*np.cos(t), y_center + yro*np.sin(t)
        xo, yo = xo[::-1], yo[::-1] # reverse arrays

        if pie:
            points = np.hstack((
                np.vstack((xo, yo)),
                np.array([[x_center], [y_center]]),
            )).T
        else:
            points = np.vstack((
                np.hstack((xi, xo)),
                np.hstack((yi, yo)),
            )).T

        self._pts.append(points)
        self._cmds.append("M"+"L"*(len(points)-1))
