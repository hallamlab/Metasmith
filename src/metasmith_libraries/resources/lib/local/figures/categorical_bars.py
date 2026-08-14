from typing import Iterable
import numpy as np

from .base.layout import Panel
from .base.geometry import Brush
from .colors import XColor, ListOfXColor, Palettes, COLORS

def CategoricalBar(
    # df: pd.DataFrame, column_to_map: str, panel: Panel, 
    # radius: float=0.5, thickness: float=0.03, circular=1.0,
    # colors: XColor|ListOfXColor=Palettes.PLOTLY, binary_value: Any=None):
    assignments: Iterable[str],  panel: Panel, 
    position: float=0, width: float=1, thickness: float=0.03, circular: bool=True,
    color_map: dict[str, XColor]|None=None):
    
    if color_map is not None:
        for c in color_map.values():
            assert isinstance(c, XColor), f"[{c}] is not of type color"
    segments = {}
    def _mark_segment(label, l, r):
        segments[label] = segments.get(label, []) + [(l, r)]
    _last_val, _last_i = None, 0
    len_assignments = 0
    for i, val in enumerate(assignments):
        len_assignments += 1
        if val == _last_val: continue
        if _last_val is not None:_mark_segment(_last_val, _last_i, i)
        _last_val, _last_i = val, i
    _mark_segment(_last_val, _last_i, len_assignments) # close the last segment
    
    if color_map is None:
        color_map = {k:v for k, v in zip(segments.keys(), Palettes.PLOTLY)}
    assert len(color_map)>=len(segments), f"[{len(segments)}] classes but the current (parhaps default) @color_map only has [{len(color_map)}]"
    for k in segments:
        assert k in color_map, f"[{k}] not in @colormap"
    color_order = [l for l, c in sorted(segments.items(), key=lambda t: len(t[1]))]

    c2brush: dict[str, Brush] = {}
    for k, c in color_map.items():
        e = Brush(c)
        panel.AddElement(e)
        c2brush[k] = e

    def _to_pos(pos):
        mid = 0.5/len_assignments
        return (pos/len_assignments + mid)*width
    MAX_R = circular
    def _draw_radial(b, l, r):
        l, r = (v*MAX_R*2*np.pi for v in (l, r))
        b.EllipticalArc(position, width=thickness, start_angle=l, end_angle=r)

    def _draw_linear(b, l, r):
        l, r = (v-0.5 for v in (l, r))
        sx, sy = l, position
        ex, ey = r, position
        b.Line(sx, sy, ex, ey, w=thickness)

    for i, label in enumerate(color_order):
        brush = c2brush[label]
        for bounds in segments[label]:
            data = [_to_pos(v) for v in bounds]
            if circular:
                _draw_radial(brush, *data)
            else:
                _draw_linear(brush, *data)
    return color_order
