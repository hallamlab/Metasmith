from __future__ import annotations
from enum import Enum
import colorsys

from ..common import PrivateInit
import re

class Color(PrivateInit):
    def __init__(self, _key=None) -> None:
        super().__init__(_key)
        self.color_value: str = ""
        self.rgba: list[float] = [0, 0, 0, 0]

    def __repr__(self) -> str:
        return f"Color<{self.color_value}"

    @classmethod
    def RGB(cls, r: float, g: float, b: float, a:float=1):
        c = Color(cls._initializer_key)
        vals = [r, g, b]
        for v in vals:
            assert v<=255 and v >= 0, f"value {v} not between 0-255"
        assert a>= 0 and a <= 1, f"alpha {a} not between 0.0-1.0"
        vals += [a]
        def _str(v):
            return f"{v:0.4f}"
        c.color_value = f"rgba({','.join([_str(v) for v in vals])})"
        c.rgba = vals
        return c
    
    @classmethod
    def _hex2rgb(cls, hex):
        hex = hex.replace("#", "")
        return [int(hex[i:i+2], 16) for i in (0, 2, 4)]
    
    @classmethod
    def Hex(cls, val: str):
        assert re.match(r'^\#?[\dabcdefABCDEF]{6}|[\dabcdefABCDEF]{8}$', val), f"{val} is not a valid 6 or 8 digit hex value"
        if val[0] != "#": val = "#"+val
        alpha = 1
        if len(val) == 9:
            alpha = int(val[-2:], 16)/255
            val = val[:-2]
        vals = cls._hex2rgb(val)+[alpha]
        return cls.RGB(*vals)
    
    @classmethod
    def HSV(cls, h: float, s: float, v: float, a: float=1):
        vals = (h, s, v, a)
        for v in vals:
            assert v>=0 and v<=1, f"values must be between 0-1, got [{v}]"
        rgb_vals = [255*v for v in colorsys.hsv_to_rgb(*vals[:-1])]+[a]
        return cls.RGB(*rgb_vals)
    
    def Fade(self, alpha: float) -> Color:
        r, g, b = self.rgba[:-1]
        return Color.RGB(r, g, b, alpha)

    def AsHsv(self):
        return list(colorsys.rgb_to_hsv(*[v/255 for v in self.rgba[:-1]]))+[self.rgba[-1]]
    
    def AsHex(self):
        rgb = [int(round(v)) for v in self.rgba[:-1]]
        return '#{:02x}{:02x}{:02x}'.format(*rgb)

class COLORS:
    INDIANRED =               "#CD5C5C"
    LIGHTCORAL =              "#F08080"
    SALMON =                  "#FA8072"
    DARKSALMON =              "#E9967A"
    LIGHTSALMON =             "#FFA07A"
    CRIMSON =                 "#DC143C"
    RED =                     "#FF0000"
    DARKRED =                 "#8B0000"
    PINK =                    "#FFC0CB"
    LIGHTPINK =               "#FFB6C1"
    HOTPINK =                 "#FF69B4"
    DEEPPINK =                "#FF1493"
    MEDIUMVIOLETRED =         "#C71585"
    PALEVIOLETRED =           "#DB7093"
    CORAL =                   "#FF7F50"
    TOMATO =                  "#FF6347"
    ORANGERED =               "#FF4500"
    DARKORANGE =              "#FF8C00"
    ORANGE =                  "#FFA500"
    GOLD =                    "#FFD700"
    YELLOW =                  "#FFFF00"
    LIGHTYELLOW =             "#FFFFE0"
    LEMONCHIFFON =            "#FFFACD"
    LIGHTGOLDENRODYELLOW =    "#FAFAD2"
    PAPAYAWHIP =              "#FFEFD5"
    MOCCASIN =                "#FFE4B5"
    PEACHPUFF =               "#FFDAB9"
    PALEGOLDENROD =           "#EEE8AA"
    KHAKI =                   "#F0E68C"
    DARKKHAKI =               "#BDB76B"
    LAVENDER =                "#E6E6FA"
    THISTLE =                 "#D8BFD8"
    PLUM =                    "#DDA0DD"
    VIOLET =                  "#EE82EE"
    ORCHID =                  "#DA70D6"
    FUCHSIA =                 "#FF00FF"
    MAGENTA =                 "#FF00FF"
    MEDIUMORCHID =            "#BA55D3"
    MEDIUMPURPLE =            "#9370DB"
    REBECCAPURPLE =           "#663399"
    BLUEVIOLET =              "#8A2BE2"
    DARKVIOLET =              "#9400D3"
    DARKORCHID =              "#9932CC"
    DARKMAGENTA =             "#8B008B"
    PURPLE =                  "#800080"
    INDIGO =                  "#4B0082"
    SLATEBLUE =               "#6A5ACD"
    DARKSLATEBLUE =           "#483D8B"
    MEDIUMSLATEBLUE =         "#7B68EE"
    GREENYELLOW =             "#ADFF2F"
    CHARTREUSE =              "#7FFF00"
    LAWNGREEN =               "#7CFC00"
    LIME =                    "#00FF00"
    LIMEGREEN =               "#32CD32"
    PALEGREEN =               "#98FB98"
    LIGHTGREEN =              "#90EE90"
    MEDIUMSPRINGGREEN =       "#00FA9A"
    SPRINGGREEN =             "#00FF7F"
    MEDIUMSEAGREEN =          "#3CB371"
    SEAGREEN =                "#2E8B57"
    FORESTGREEN =             "#228B22"
    GREEN =                   "#008000"
    DARKGREEN =               "#006400"
    YELLOWGREEN =             "#9ACD32"
    OLIVEDRAB =               "#6B8E23"
    OLIVE =                   "#6B8E23"
    DARKOLIVEGREEN =          "#556B2F"
    MEDIUMAQUAMARINE =        "#66CDAA"
    DARKSEAGREEN =            "#8FBC8B"
    LIGHTSEAGREEN =           "#20B2AA"
    DARKCYAN =                "#008B8B"
    TEAL =                    "#008080"
    AQUA =                    "#00FFFF"
    CYAN =                    "#00FFFF"
    LIGHTCYAN =               "#E0FFFF"
    PALETURQUOISE =           "#AFEEEE"
    AQUAMARINE =              "#7FFFD4"
    TURQUOISE =               "#40E0D0"
    MEDIUMTURQUOISE =         "#48D1CC"
    DARKTURQUOISE =           "#00CED1"
    CADETBLUE =               "#5F9EA0"
    STEELBLUE =               "#4682B4"
    LIGHTSTEELBLUE =          "#B0C4DE"
    POWDERBLUE =              "#B0E0E6"
    LIGHTBLUE =               "#ADD8E6"
    SKYBLUE =                 "#87CEEB"
    LIGHTSKYBLUE =            "#87CEFA"
    DEEPSKYBLUE =             "#00BFFF"
    DODGERBLUE =              "#1E90FF"
    CORNFLOWERBLUE =          "#6495ED"
    ROYALBLUE =               "#4169E1"
    BLUE =                    "#0000FF"
    MEDIUMBLUE =              "#0000CD"
    DARKBLUE =                "#00008B"
    NAVY =                    "#00008B"
    MIDNIGHTBLUE =            "#191970"
    CORNSILK =                "#FFF8DC"
    BLANCHEDALMOND =          "#FFEBCD"
    BISQUE =                  "#FFE4C4"
    NAVAJOWHITE =             "#FFDEAD"
    WHEAT =                   "#F5DEB3"
    BURLYWOOD =               "#DEB887"
    TAN =                     "#D2B48C"
    ROSYBROWN =               "#BC8F8F"
    SANDYBROWN =              "#F4A460"
    GOLDENROD =               "#DAA520"
    DARKGOLDENROD =           "#B8860B"
    PERU =                    "#CD853F"
    CHOCOLATE =               "#D2691E"
    SADDLEBROWN =             "#8B4513"
    SIENNA =                  "#A0522D"
    BROWN =                   "#A52A2A"
    MAROON =                  "#800000"
    WHITE =                   "#FFFFFF"
    SNOW =                    "#FFFAFA"
    HONEYDEW =                "#F0FFF0"
    MINTCREAM =               "#F5FFFA"
    AZURE =                   "#F0FFFF"
    ALICEBLUE =               "#F0F8FF"
    GHOSTWHITE =              "#F8F8FF"
    WHITESMOKE =              "#F5F5F5"
    SEASHELL =                "#FFF5EE"
    BEIGE =                   "#F5F5DC"
    OLDLACE =                 "#FDF5E6"
    FLORALWHITE =             "#FDF5E6"
    IVORY =                   "#FFFFF0"
    ANTIQUEWHITE =            "#FAEBD7"
    LINEN =                   "#FAF0E6"
    LAVENDERBLUSH =           "#FFF0F5"
    MISTYROSE =               "#FFE4E1"
    GAINSBORO =               "#DCDCDC"
    LIGHTGRAY =               "#D3D3D3"
    SILVER =                  "#C0C0C0"
    DARKGRAY =                "#A9A9A9"
    GRAY =                    "#808080"
    DIMGRAY =                 "#696969"
    LIGHTSLATEGRAY =          "#778899"
    SLATEGRAY =               "#708090"
    DARKSLATEGRAY =           "#2F4F4F"
    BLACK =                   "#000000"
    TRANSPARENT =             "rgba(0,0,0,0)"

XColor = str | Color
ListOfXColor = list[str]|list[Color]|list[XColor]

def ColorObj(col: XColor):
    if isinstance(col, str): return Color.Hex(col)
    return col

def ColorValue(col: XColor):
    return ColorObj(col).color_value

class Palettes:
    # https://plotly.com/python/discrete-color/
    PLOTLY = [Color.Hex(c) for c in ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']]
    # https://www.heavy.ai/blog/12-color-palettes-for-telling-better-stories-with-your-data
    SPRING_PASTEL = [Color.Hex(c) for c in ["#b2e061", "#7eb0d5", "#fd7f6f", "#bd7ebe", "#ffb55a", "#ffee65", "#beb9db", "#fdcce5", "#8bd3c7"]]
    DUTCH_FIELD = [Color.Hex(c) for c in ["#e60049", "#0bb4ff", "#50e991", "#e6d800", "#9b19f5", "#ffa300", "#dc0ab4", "#b3d4ff", "#00bfa0"]]


# ########################################################################
# custom themes may be overkill for now

# class Theme:
#     def __init__(self, palette: list[tuple[int, int, int]]) -> None:
#         assert len(palette) >= 2
#         self.__palette = palette

#     @classmethod
#     def rgb_str(cls, r, g, b, a):
#         """r g b between 0 - 255, a between 0.0 - 1.0"""
#         return f'rgba({r},{g},{b},{a})'

#     def get_color(self, i:int, w: float):
#         rgb = self.__palette[i%len(self.__palette)]
#         s = 1000
#         w = round(w*s)/s
#         return self.rgb_str(*(rgb+(w,)))

#     def background(self, w: float=1):
#         return self.get_color(0, w)

#     def primary(self, w:float=1):
#         return self.get_color(1, w)

#     def secondary(self, w:float=1):
#         return self.get_color(2, w)

# THEME_BLACK = Theme(palette=[
#     (0, 0, 0),
#     (255, 255, 255),
#     (200, 200, 200)
# ])

# THEME_WHITE = Theme(palette=[
#     (255, 255, 255),
#     (0, 0, 0),
#     (0, 15, 100)
# ])

# THEME_DARK = Theme(palette=[
#     (30, 30, 30),
#     (225, 225, 225),
#     (0, 84, 225)
# ])

# _theme: Theme = THEME_WHITE

# def set_theme(theme: Theme):
#     global _theme
#     _theme = theme

# def get_theme():
#     return _theme