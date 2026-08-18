#!/usr/bin/env python3
"""The controls a direction re-bake has to survive, run against two annotations.

    check_acceptance.py --baseline <r8 seam> --candidate <r9 seam> \
        --forecast <forecast built with the SAME substitution configuration> \
        --curated-per-reaction <the candidate's curated table>

THE CONTROL SET IS SCREENED, NOT NAMED. The previous one was a name predicate -- three
glycogen accessions asserted to gain nothing -- and r9 made them the target, so it would
have read backwards. This one is derived: the reactions the forecast says BOTH members
are silent on, minus the reactions the curated crosswalk reaches. Those must come through
a re-bake at `dir_tier == 0`, `ratio == 1.0` and an unchanged `dir_method`.

WHY THE CROSSWALK HAS TO BE SUBTRACTED, and it is the correction r9 forced: the forecast
predicts THERMO member behaviour and nothing else. A curated prior is not a thermo vote,
so a reaction can be forecast-silent for both members and still legitimately leave tier 0
for tier 3 on a MetaCyc category. Screening only against the substitution tables would
therefore report 37 correct rescues as control violations.

The complementary claim -- that nothing gains a thermo vote the forecast did not predict
-- is `forecast backtest`'s and is not duplicated here.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
from ecspr.bake.direction import canon                                  # noqa: E402

SILENT = {"eq": {"no_stoich", "no_props", "unresolved"},
          "dgbyg": {"no_stoich", "no_smiles", "unparseable", "wildcard", "unbalanced"}}


def forecast_silent_both(forecast: pd.DataFrame) -> set[str]:
    """MNXRs the forecast predicts no thermo vote for, on either member."""
    spoke = set()
    for member, silent in SILENT.items():
        m = forecast[forecast["member"] == member]
        spoke |= set(m.loc[~m["mechanism"].isin(silent), "mnxr"])
    return set(forecast["mnxr"]) - spoke


def crosswalk_reached(curated_per_reaction: pd.DataFrame) -> set[str]:
    """MNXRs reached by a source other than the primary MetaCyc join."""
    df = curated_per_reaction
    return set(df.loc[df["source"] != "metacyc", "mnxr"].dropna())


def report(name: str, ok: bool, detail: str = "") -> bool:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", required=True, help="the deployed annotation seam")
    ap.add_argument("--candidate", required=True, help="the annotation under test")
    ap.add_argument("--forecast", required=True)
    ap.add_argument("--curated-per-reaction", required=True)
    a = ap.parse_args(argv)

    base = pd.read_parquet(a.baseline).set_index("mnxr").sort_index()
    cand = pd.read_parquet(a.candidate).set_index("mnxr").sort_index()
    fc = pd.read_parquet(a.forecast)
    cur = pd.read_parquet(a.curated_per_reaction)

    assert base.index.equals(cand.index), "the two annotations do not cover one universe"
    print(f"{len(base):,} reactions\n")

    ok = True

    # --- the control set ---------------------------------------------------
    silent = forecast_silent_both(fc)
    reached = crosswalk_reached(cur)
    control = sorted((silent & set(base.index[base["dir_tier"] == 0])) - reached)
    print(f"control set: {len(control):,} reactions "
          f"({len(silent):,} forecast-silent for both, "
          f"{len(reached):,} reached by the curated crosswalk and screened out)")
    c = cand.loc[control]
    b = base.loc[control]
    bad_tier = c.index[c["dir_tier"] != 0]
    bad_ratio = c.index[c["ratio"] != 1.0]
    bad_method = c.index[(c["dir_method"].fillna("") != b["dir_method"].fillna(""))]
    ok &= report("C13 control set stays tier 0", len(bad_tier) == 0,
                 f"{len(bad_tier)} moved: {list(bad_tier[:5])}")
    ok &= report("C13 control set stays ratio 1.0", len(bad_ratio) == 0,
                 f"{len(bad_ratio)} moved: {list(bad_ratio[:5])}")
    ok &= report("C13 control set keeps dir_method", len(bad_method) == 0,
                 f"{len(bad_method)} moved: {list(bad_method[:5])}")

    # --- no sign flip on a measured vote -----------------------------------
    both1 = base.index[(base["dir_tier"] == 1) & (cand["dir_tier"] == 1)]
    x, y = base.loc[both1, "dG_prime"], cand.loc[both1, "dG_prime"]
    flips = both1[(np.sign(x) != np.sign(y)) & (x != 0) & (y != 0)]
    ok &= report(f"C14 no sign flip among {len(both1):,} tier-1-in-both", len(flips) == 0,
                 f"{len(flips)} flipped: {list(flips[:5])}")

    # --- the prior width is the committed one, everywhere it applies -------
    t0 = cand[cand["dir_tier"] == 0]
    off = t0.index[t0["sigma"] != canon.DIR_SIGMA_0]
    ok &= report(f"C18 all {len(t0):,} tier-0 rows at DIR_SIGMA_0={canon.DIR_SIGMA_0}",
                 len(off) == 0, f"{len(off)} off: {list(off[:5])}")

    # --- what moved, so the deltas are attributable rather than asserted ---
    print("\ntier histogram")
    hb, hc = base["dir_tier"].value_counts(), cand["dir_tier"].value_counts()
    for t in sorted(set(hb.index) | set(hc.index)):
        print(f"  tier {t}  {hb.get(t,0):>7,} -> {hc.get(t,0):>7,}  {hc.get(t,0)-hb.get(t,0):+,}")
    moved = base.index[(base["ratio"] != cand["ratio"])
                       | (base["dir_tier"] != cand["dir_tier"])]
    gained = base.index[(base["dir_tier"] == 0) & (cand["dir_tier"] != 0)]
    lost = base.index[(base["dir_tier"] != 0) & (cand["dir_tier"] == 0)]
    print(f"\n{len(moved):,} reactions move ratio or tier; {len(gained):,} leave tier 0; "
          f"{len(lost):,} FALL BACK to tier 0")
    if len(lost):
        print(f"  fell back: {list(lost[:20])}")
        ok &= report("nothing that had a vote loses one", False)

    print("\n" + ("ALL CONTROLS PASS" if ok else "CONTROLS FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
