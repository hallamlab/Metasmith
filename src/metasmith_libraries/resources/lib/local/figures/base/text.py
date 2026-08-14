import os
import numpy as np
import plotly.graph_objects as go
from PIL import Image

from ...caching import save_exists, save, load
from .coordinates import rectify_angle, to_cart, rad2deg

class TextPlotter:
    def __init__(self, fig: go.Figure, font_family: str="default") -> None:
        # self.font_widths = GetFontWidths(font_family)
        self.font_widths = dict()
        self.fig = fig

    def _rotate_text(self, r:float, mode=0):
        # if mode == 0: # inline with spoke
        #     r = np.pi-r if r < np.pi*3/2 and r > np.pi/2 else -r
        if r >= np.pi: r += np.pi
        r -= np.pi/2
        r = rectify_angle(r)
        return r

    def Write(self, text, x, y, size, rot:float=0, color="black", **kwargs):
        self.fig.add_annotation(
            x=x, y=y,
            text=text,
            font=dict(
                size=size+6,
                color=color,
            ),
            textangle=rad2deg(rot),
            showarrow=False,
            **kwargs,
        )

    def WriteRadial(self, text, rot, radius, font_size, dx=0.0, dy=0.0, color="black"):
        rot = rectify_angle(rot)
        x, y = to_cart(rot, radius)
        x, y = x+dx, y+dy
        # buf_len = sum([self.font_widths.get(c, 22) for c in text])
        # print(text, radius, font_size, buf_len)
        # buf_len = int(round(buf_len / 22.5))
        # buf = "".join(" " for _ in range(buf_len))
        # text = text.strip()
        # if rot >= np.pi:
        #     text = text + buf
        # else:
        #     text = buf + text
        self.fig.add_annotation(
            x=x, y=y,
            text=text,
            font=dict(
                size=font_size+6,
                color=color,
            ),
            showarrow=False,
            textangle=rad2deg(self._rotate_text(rot)),
            xanchor = "center", yanchor="middle",
        )

def _render(fig: go.Figure, bounds=None, bg_col="white"):
    BORDER=0
    if bounds is None:
        xr = [0, 10]
        yr = [0, 10]
    elif isinstance(bounds, float) or isinstance(bounds, int):
        xr, yr = ((-bounds, bounds), (-bounds, bounds))
    else:
        xr, yr = bounds[:2], bounds[2:]

    fig.update_layout(
        xaxis={
            'range': xr,
            'showticklabels': False,
            'showgrid': False,
            'zeroline': False,
        },
        yaxis={
            'range': yr,
            'showticklabels': False,
            'showgrid': False,
            'zeroline': False,
            'scaleanchor': 'x',
            'scaleratio': 1,
        },
        margin={'l': BORDER, 'r': BORDER, 'b': BORDER, 't': BORDER},
        paper_bgcolor=bg_col,
        plot_bgcolor=bg_col,
        dragmode='pan',
        showlegend=False
    )
    return fig

def GetFontWidths(font_family: str = "default"):
    def _get():
        fig = go.Figure()

        # ascii codes
        start = 33
        end = 130
        _max = end-start
        font = dict(
            size=100,
            color="black",
        )
        if font_family != "default":
            font["family"] = font_family
        for i in range(_max):
            i += start
            fig.add_annotation(
                x=0, y=5 + (i-start)*10,
                text=chr(i),
                # text=f"{text}",
                font=font,
                showarrow=False,
                xanchor="left",
            )

        fig = _render(fig, bounds=(0, 10, 0, _max*10))
        fig.update_layout(
            width = 100,
            height = _max*100,
        )

        font_calibration = f"./cache/{font_family}_calibration.png"
        os.makedirs("./cache", exist_ok=True)
        fig.write_image(font_calibration)
        img = Image.open(font_calibration)

        w, h = img.size
        _delta = h/_max

        def scan(top, bot, r2l = False):
            _w = w - 1
            if r2l:
                dx, x, end = -1, _w, 0
            else:
                dx, x, end = 1, 0, _w
            while True:
                for y in range(bot, top):
                    pix = img.getpixel((x, y))
                    if np.mean(pix) < 125: return x
                if x == end: return None
                x += dx
        widths = {}
        for i in range(_max):
            j = _max-i-1
            char = chr(j+start)
            bot = int(round(i*_delta))
            top = int(bot + _delta)
            l, r = scan(top, bot), scan(top, bot, True)
            if r is None or l is None:
                continue 
            widths[char] = abs(r-l)
        widths[" "] = widths["I"] # guess
        return widths

    SAVE_NAME = f"{font_family}_widths"
    if save_exists(SAVE_NAME):
        return load(SAVE_NAME)
    else:
        map = _get()
        save(SAVE_NAME, map)
        return map