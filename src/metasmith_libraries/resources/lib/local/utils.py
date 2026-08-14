import pandas as pd
import numpy as np
import re
import sys
from pathlib import Path

def safe_log10(x):
    if isinstance(x, np.ndarray):
        negs = np.ones_like(x)
        negs[x<0] = -1
        return  negs * np.log10(negs*x+1)
    else:
        x = np.log10(x+1) if x>0 else -np.log10(1-x)
        return x

def pd_set_type(cols: list|str, t, df: pd.DataFrame):
    if isinstance(cols, str): cols = cols.split(', ')
    for col in cols: df[col] = df[col].astype(t)

def regex(r, s):
    for m in re.finditer(r, s):
        yield s[m.start():m.end()]

# https://stackoverflow.com/questions/8290397/how-to-split-an-iterable-in-constant-size-chunks
def batchify(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def add_to_python_path(paths: list[Path|str]):
    if not isinstance(paths, list): paths = [paths]
    sys.path = list(set(sys.path + [str(p) for p in paths]))
