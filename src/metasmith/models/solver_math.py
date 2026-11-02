from __future__ import annotations

from math import log2
from typing import Sequence

def entropy(counts: Sequence[int]) -> float:
    total = sum(counts)
    if total <= 0: return 0.0
    acc = 0.0
    for c in counts:
        p = c/total
        if p <= 0: continue
        acc += p*log2(p)
    return acc
