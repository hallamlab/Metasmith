from __future__ import annotations
from typing import TypeVar, Any
import numpy as np

def to_cart(rad, len):
    rad = -rad + np.pi/2
    x = np.cos(rad) * len
    y = np.sin(rad) * len
    return x, y

def to_rad(x, y):
    return np.arctan2(y, x)

def rad2deg(r: float):
    return r/np.pi*180

T = TypeVar('T')
def rectify_angle(x: T) -> T:
    if isinstance(x, np.ndarray):
        return np.where(x<0 , 2*np.pi+x, x)
    elif type(x) is float:
        _x: float = x # to deal with typing
        while _x<0:
            _x+=2*np.pi
        ret: Any = _x # to deal with typing
        return ret
    else:
        return x

class Transform:
    def __init__(self, dx: float=0, dy: float=0, rotation: float=0, sx: float=1, sy: float|None=None) -> None:
        dt = float
        def Trans():
            return np.array([
                [1, 0, dx],
                [0, 1, dy],
                [0, 0, 1],
            ], dtype=dt).T

        def Scale():
            nonlocal sy
            if sy is None: sy = sx
            return np.array([
                [sx, 0, 0],
                [0, sy, 0],
                [0, 0, 1],
            ], dtype=dt).T

        def Rot():
            r = rectify_angle(rotation)
            cos_r = np.cos(r)
            sin_r = np.sin(r)
            return np.array([
                [cos_r, -sin_r, 0],
                [sin_r, cos_r,  0],
                [0,     0,      1],
            ], dtype=dt).T
        self._mat = Rot() @ Trans() @ Scale()

    def __add__(self, other: Transform) -> Transform:
        assert isinstance(other, Transform), f'can only add Transforms, but given [{type(other)}]'
        new = Transform()
        new._mat = self._mat @ other._mat
        return new

    def _apply(self, points: np.ndarray):
        
        return points

    def Apply(self, points: np.ndarray) -> np.ndarray:
        assert points.shape[1] == 2, f"expected array of shape [n, 2], got {points.shape}"
        # https://en.wikipedia.org/wiki/Transformation_matrix#Affine_transformations
        homo = np.hstack((points, np.ones(shape=(len(points), 1))))
        return (homo @ self._mat).T[:-1].T

def Xywh2lrbt(x, y, w, h):
    l, r = x-w/2, x+w/2
    b, t = y-h/2, y+h/2
    return l, r, b, t
