"""The float rules the solver's score depends on, stated rather than inherited.

`solver_rng` fixes which index a decision picks; this fixes the numbers those
decisions are made *about*. Both are contracts with the Rust port and neither is
portable by accident.

The one function here used to be four lines of numpy, and both of them were a
divergence:

- **`ndarray.sum` is pairwise, not sequential.** It sums in blocks and combines
  the blocks, which is more accurate and is not what any other implementation
  does. It matches a left-to-right sum for n < 9 and stops matching at n = 9.
- **`np.log2` is not the platform's `log2`.** numpy carries its own vectorised
  implementation; over 20,000 random values in (0,1) it disagreed with libm's
  `log2` in the last bit **52** times. Anything calling the C library -- CPython's
  `math.log2`, Rust's `f64::log2` -- would have disagreed with the solver that
  often, per value, forever.

A last-bit disagreement matters here because the entropy feeds `score_node`, and
the selection rules compare scores and break ties by index. One flipped bit is
one different comparison is one different plan.

So the summation order is stated (left to right, in the caller's order) and the
logarithm is the platform's, which is the one thing both languages can agree to
call. That the two `log2`s really are the same function is checked rather than
assumed -- see the `log2` op in `metasmith.testing.rng_trace`, which compares
them bit for bit across the process boundary.
"""

from __future__ import annotations

from math import log2
from typing import Sequence

def entropy(counts: Sequence[int]) -> float:
    """Negative Shannon entropy in bits, of counts read as a distribution.

    Negative because that is what the solver's score wants and what the numpy
    expression it replaces returned: the sum of `p*log2(p)` is already <= 0.

    The total is summed as an integer, so it is exact and orderless; only the
    `p*log2(p)` terms need a stated order, and theirs is the caller's.
    """
    total = sum(counts)
    if total <= 0: return 0.0
    acc = 0.0
    for c in counts:
        p = c/total
        if p <= 0: continue # `p[p>0]`, kept: log2(0) is a domain error, not -inf
        acc += p*log2(p)
    return acc
