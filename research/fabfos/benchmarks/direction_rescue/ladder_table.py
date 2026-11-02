# The ladder's six annotations, read as one table of attributable deltas.
#
# WHAT EACH COLUMN CAN AND CANNOT SETTLE. `differs` is the honest one: it counts the rows
# this rung moved relative to the rung below it, and every rung differs from its predecessor
# by exactly one flag, so a delta belongs to exactly one mechanism.
#
# `accuracy` IS NOT A SCORE OF THE PRIOR, and reading it as one is the trap this ladder is
# built to avoid. It is decided accuracy of `dG_raw` against MetaCyc's own directional
# categories, and `dG_raw` contains the curated prior on every row that carries a category --
# which is every row in the scored population. It scores the QUOTIENT cleanly, because the
# quotient never touches the labels. For arms 3 and 5, which move the prior's width and its
# scale, it is a fit against its own training data and the number is printed with that said.
# The prior's evidence is `poc_prior_holdout.py`.
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
from ecspr.bake.direction import canon                       # noqa: E402

RT = canon.DIR_RT
DECADE = canon.DIR_DECADE
FORWARD = {"LEFT-TO-RIGHT", "PHYSIOL-LEFT-TO-RIGHT"}
REVERSE = {"RIGHT-TO-LEFT", "PHYSIOL-RIGHT-TO-LEFT"}

ARMS = [
    (1, "r9 reproduced", "instrument check -- must be IDENTICAL"),
    (2, "+ balance gate", "the fit population"),
    (3, "+ robust width", "the prior's width  (prior arm)"),
    (4, "+ quotient", "the thermodynamic number"),
    (5, "+ prior on dG'", "the prior's scale  (prior arm)"),
    (6, "+ constants", "sigma_0, clamp, suspect widening"),
]
PRIOR_ARMS = {3, 5}
RUN_LABELS = ("prior_width_kind", "prior_quantity")


def score(d: pd.DataFrame) -> tuple:
    lab = d[d["biocyc_category"].isin(FORWARD | REVERSE) & d["dir_tier"].isin([1, 2])]
    truth = np.where(lab["biocyc_category"].isin(REVERSE), 1, -1)
    call = ((lab["dG_raw"] > DECADE).astype(int) - (lab["dG_raw"] < -DECADE).astype(int))
    dec = call != 0
    right = int((call[dec] == truth[dec]).sum())
    return right, int(dec.sum()) - right, int((~dec).sum())


def main(out: Path):
    frames = {}
    for n, _, _ in ARMS:
        p = out / f"arm{n}" / "direction_annotation.parquet"
        if p.exists():
            frames[n] = pd.read_parquet(p).sort_values("mnxr").reset_index(drop=True)
    if not frames:
        raise SystemExit(f"[ladder] no arms under {out}")

    hdr = (f"{'arm':>3} {'change':<16} {'differs':>9} {'clamped':>8} {'suspect':>8} "
           f"{'tier1':>6} {'tier2':>6} {'tier3':>6} {'right':>6} {'wrong':>6} {'hedge':>6} "
           f"{'accuracy':>9} {'MNXR145036':>11}")
    print(hdr)
    print("-" * len(hdr))
    prev = None
    for n, label, _why in ARMS:
        d = frames.get(n)
        if d is None:
            continue
        if prev is None:
            differs = "-"
        else:
            # RUN-LEVEL LABELS ARE NOT ANSWERS. `prior_width_kind` and `prior_quantity` hold
            # one value for the whole table, so a rung that renames them would otherwise
            # report every row as moved and drown the rung that actually moved rows.
            common = [c for c in d.columns
                      if c in prev.columns and c not in RUN_LABELS]
            moved = (~(d[common] == prev[common])
                     & ~(d[common].isna() & prev[common].isna())).any(axis=1)
            differs = "IDENTICAL" if not moved.any() else f"{int(moved.sum()):,}"
        r, w, h = score(d)
        acc = f"{r / (r + w):.2%}" + ("*" if n in PRIOR_ARMS else "")
        sus = int((d["dG_suspect"].astype(str) != "").sum()) if "dG_suspect" in d else 0
        g = float(d.loc[d.mnxr == "MNXR145036", "ratio"].iat[0])
        print(f"{n:>3} {label:<16} {differs:>9} {int(d.clamped.sum()):>8,} {sus:>8,} "
              + " ".join(f"{int((d.dir_tier == t).sum()):>6,}" for t in (1, 2, 3))
              + f" {r:>6,} {w:>6,} {h:>6,} {acc:>9} {g:>11.4f}")
        prev = d

    print("\n* accuracy contains the curated prior on every scored row, so for the two prior "
          "arms it is\n  a fit against its own labels. It scores the QUOTIENT cleanly. The "
          "prior's evidence is\n  poc_prior_holdout.py.")

    first, last = frames[min(frames)], frames[max(frames)]
    print(f"\n[ladder] tier transition r9 -> r10:")
    print(pd.crosstab(first.dir_tier, last.dir_tier).to_string())
    lost = int(((first.dir_tier > 0) & (last.dir_tier == 0)).sum())
    print(f"[ladder] reactions that carried a vote and no longer do: {lost}"
          + ("  -- STOP" if lost else "  (a drop is a stop)"))
    if "dG_suspect" in last:
        s = last["dG_suspect"].astype(str)
        print("[ladder] suspect reasons: "
              + ", ".join(f"{k}={v:,}" for k, v in s[s != ""].value_counts().items()))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
