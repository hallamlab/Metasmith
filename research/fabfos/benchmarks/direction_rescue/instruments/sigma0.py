"""Re-derive DIR_SIGMA_0 by hand from a calibration run's own points table.

The estimator is committed as prose in `canon.py` and computed by no code: the robust
marginal spread (1.4826*MAD) of measured dG' on the eQuilibrator REACTANT-contribution
arm, over the rows clearing DIR_SIGMA_FLOOR. This reimplements it, reproduces r8's
committed value from r8's own points as the check that the reading is right, and then
applies it to r9 -- twice, once over everything and once over the UNSUBSTITUTED subset,
because asserted chemistry should not set the prior width that shrinks every row.
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, "src")
from ecspr.bake.direction import canon


def fit(points, label, exclude=frozenset()):
    ok = points[(points["reason"] == "ok")
                & points["dg"].notna()
                & (points["sigma"] > canon.DIR_SIGMA_FLOOR)
                & (~points["uses_gc"].fillna(False).astype(bool))]
    if exclude:
        ok = ok[~ok["mnxr"].isin(exclude)]
    dg = ok["dg"].to_numpy(float)
    med = float(np.median(dg))
    mad = float(np.median(np.abs(dg - med)))
    s = 1.4826 * mad
    lo, hi = canon.DIR_SIGMA_0_BAND
    print(f"  {label:<28} n={len(dg):>5,}  median={med:>8.4f}  "
          f"MAD={mad:>8.4f}  sigma0={s:>8.4f}  "
          f"{'IN' if lo <= s <= hi else 'OUT OF'} band {canon.DIR_SIGMA_0_BAND}")
    return s


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        path, _, label = arg.partition("=")
        print(f"{label or path}:")
        pts = pd.read_parquet(path)
        fit(pts, "all measured rc rows")
        subs = Path("src/ecspr/bake/direction")
        cov = set()
        try:
            from ecspr.bake.direction import substitute as S
            from ecspr.bake.direction.refdata import (load_mnxr_stoich, load_mnxm_names,
                                                      load_mnxm_props, load_mnxm_formulas)
            MNX = Path("data/fabfos/originals/metanetx/4.5")
            props = load_mnxm_props(MNX / "chem_prop.tsv")
            # member="eq" AND NOT THE UNION. The points are the eQuilibrator
            # reactant-contribution arm, so the subset to exclude is what eQuilibrator's
            # OWN admitted rows touched. A row refused for eq and kept for dGbyG asserted
            # nothing about these points, and excluding it would shrink the fit's
            # denominator over chemistry this arm never saw.
            tabs = S.load(subs, props, load_mnxm_names(MNX / "chem_prop.tsv"),
                          formulas=load_mnxm_formulas(MNX / "chem_prop.tsv"),
                          member="eq")
            allst = load_mnxr_stoich(MNX / "reac_prop.tsv")
            cov = {m for m, v in allst.items() if tabs.covers(v[0])}
            print(f"  (eq substitution tables cover {len(cov):,} reactions)")
        # NARROW, because the committed fit is the UNSUBSTITUTED one: a broad except here
        # turns a refused table into a missing subset and a printed line nobody reads,
        # and the number that gets committed is then the wrong one of the two.
        except (ImportError, OSError) as e:
            print(f"  (substituted subset unavailable: {type(e).__name__}: {e})")
        if cov:
            fit(pts, "UNSUBSTITUTED subset only", exclude=cov)
        print(f"  committed DIR_SIGMA_0 = {canon.DIR_SIGMA_0}\n")
