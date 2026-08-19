from __future__ import annotations

import sys

# Every heavy import lives INSIDE a check. `ecspr.cli` reaches this module for the
# `selftest` verb, and `--help`/`--where` have to keep working on the env that is
# missing the very things this asserts.

CHECKS = []


def _check(fn):
    CHECKS.append(fn)
    return fn


@_check
def numpy_honours_the_pin():
    import numpy as np
    if int(np.__version__.split(".")[0]) >= 2:
        raise AssertionError(
            f"numpy {np.__version__}: env.yml pins numpy<2 because scikit-sparse's "
            "CHOLMOD binding and cobra jointly require it")
    return f"numpy {np.__version__}"


@_check
def cholmod_is_present_and_solves():
    import numpy as np
    import scipy.sparse as sp
    from sksparse.cholmod import cho_factor

    from .model.directed import _HAVE_CHOLMOD
    if not _HAVE_CHOLMOD:
        raise AssertionError("sksparse imports here but ecspr.model.directed did not "
                             "see it; the Newton solve would run on the splu fallback")
    A = sp.csc_matrix(np.array([[4.0, 1.0], [1.0, 3.0]]))
    b = np.array([1.0, 2.0])
    x = cho_factor(A).solve(b.reshape(-1, 1)).ravel()
    want = np.linalg.solve(A.toarray(), b)
    if not np.allclose(x, want):
        raise AssertionError(f"CHOLMOD solved a 2x2 SPD system as {x}, not {want}")
    return "cholmod factorises and solves"


@_check
def the_lazy_runtime_deps_are_here():
    import cobra
    import pandas
    import pyarrow
    import scipy
    return (f"cobra {cobra.__version__}, pandas {pandas.__version__}, "
            f"pyarrow {pyarrow.__version__}, scipy {scipy.__version__}")


def run(out=None) -> int:
    out = out or sys.stdout
    from . import __version__
    failed = 0
    for fn in CHECKS:
        try:
            print(f"  ok   {fn.__name__}: {fn()}", file=out)
        except Exception as e:
            failed += 1
            print(f"  FAIL {fn.__name__}: {type(e).__name__}: {e}", file=out)
    if failed:
        raise SystemExit(
            f"ecspr {__version__}: {failed} of {len(CHECKS)} install checks failed. "
            "This env resolves and imports but cannot measure correctly; fix the env "
            "rather than the caller.")
    print(f"ecspr {__version__}: install OK", file=out)
    return 0


if __name__ == "__main__":
    sys.exit(run())
