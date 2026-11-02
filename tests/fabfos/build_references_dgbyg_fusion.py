"""Settle the dGbyG fusion rule from the two member tables, with numbers.

WHY THIS EXISTS. `dir_combine.thermo_vote` has three regimes. When eQuilibrator has
MEASURED a reaction (`uses_gc == False`) the measurement dominates and dGbyG is discarded
outright -- that branch is not in question. When only one member answers, its number is
used -- also not in question. The regime under review is the middle one: BOTH members
answer and eQuilibrator's answer came from the GROUP-CONTRIBUTION arm. There the combiner
averages the two and floors the fused sigma by their disagreement.

THE SUSPICION. Group contribution decomposes a compound into groups and sums their
energies, so for chemistry that CONSERVES groups -- isomerases, most transferases -- the
two sides cancel and the arm returns a number indistinguishable from zero. It is not a
weak estimate of the free energy; it is a structural property of the decomposition, and
`dir_calibrate` already excludes the whole GC arm for exactly this reason (the arm
restriction at `dir_calibrate.calibrate`). If those zeros are numerous, `thermo_vote`
averages a real dGbyG estimate against a structural zero -- halving it toward zero -- and
then WIDENS the uncertainty because the two "disagree", which is the opposite of what the
disagreement floor is there for.

WHAT THIS DECIDES. Three numbers, and the rule follows from them:

  1. COVERAGE dGbyG ADDS. Reactions dGbyG answers that eQuilibrator does not. This is the
     lane's whole justification -- if it is small, the fusion question is moot.
  2. HOW MUCH OF THE CONTESTED REGIME IS STRUCTURAL ZERO. Of the both-answered rows on the
     GC arm, the fraction where |eq_dg| is at or below the near-zero band.
  3. WHAT AVERAGING COSTS THERE. On those rows: how far the fused mean is dragged from
     dGbyG's estimate, and how much the disagreement floor inflates sigma over what
     dGbyG alone would have reported.

It reports; it does not edit the combiner. The rule is a judgement about which of two
predictors to trust in a named regime, and it should be made once, in writing, against
these numbers rather than re-derived from a plot.

    python tests/build_references_dgbyg_fusion.py \
        --eq    data/fabfos/temp/_seams/direction_member_eq.parquet \
        --dgbyg data/fabfos/temp/_seams/direction_member_dgbyg.parquet
"""
import argparse
import math
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]

NEAR_ZERO_KJ = 1.0

TAU_SHARED = 10.0


def load_member(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"{name} member table not found: {path}\n"
                 f"Run the `members` part first; this reads its two seam tables.")
    df = pd.read_parquet(path)
    missing = {"mnxr", "dg", "sigma", "flag", "reason"} - set(df.columns)
    if missing:
        sys.exit(f"{name} table is missing columns {sorted(missing)} -- not a member table.")
    return df


def pct(n: int, d: int) -> str:
    return f"{n:,} ({n / d:.1%})" if d else f"{n:,} (n/a)"


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--eq", type=Path,
                    default=REPO / "data/fabfos/temp/_seams/direction_member_eq.parquet")
    ap.add_argument("--dgbyg", type=Path,
                    default=REPO / "data/fabfos/temp/_seams/direction_member_dgbyg.parquet")
    ap.add_argument("--near-zero", type=float, default=NEAR_ZERO_KJ,
                    help="kJ/mol band treated as the GC arm cancelling (default 1.0)")
    a = ap.parse_args()

    eq = load_member(a.eq, "eQuilibrator")
    db = load_member(a.dgbyg, "dGbyG")

    print(f"universe            eq={len(eq):,} rows   dgbyg={len(db):,} rows")
    if len(eq) != len(db):
        print("  note -- the two members were asked about different universes. Both are "
              "built from the same reac_prop, so a mismatch is a bug worth chasing before "
              "reading anything below.")

    eq_ok = eq[(eq["reason"] == "ok") & eq["dg"].notna()]
    db_ok = db[(db["reason"] == "ok") & db["dg"].notna()]
    print(f"answered            eq={pct(len(eq_ok), len(eq))}   "
          f"dgbyg={pct(len(db_ok), len(db))}")

    print("\n--- abstention reasons ---")
    for name, df in (("eq", eq), ("dgbyg", db)):
        counts = df["reason"].value_counts()
        print(f"  {name}: " + "  ".join(f"{k}={v:,}" for k, v in counts.items()))

    eq_set, db_set = set(eq_ok["mnxr"]), set(db_ok["mnxr"])
    both = eq_set & db_set
    only_db = db_set - eq_set
    only_eq = eq_set - db_set
    union = eq_set | db_set
    print("\n=== 1. COVERAGE ===")
    print(f"  both members answer     {pct(len(both), len(union))} of the union")
    print(f"  dGbyG only              {pct(len(only_db), len(union))}   <- what the lane adds")
    print(f"  eQuilibrator only       {pct(len(only_eq), len(union))}")
    print(f"  union answered          {len(union):,} reactions")

    j = eq_ok.merge(db_ok, on="mnxr", suffixes=("_eq", "_db"))
    uses_gc = j["flag_eq"].astype("boolean")
    measured = j[uses_gc == False]  # noqa: E712
    contested = j[uses_gc == True]  # noqa: E712
    print("\n=== 2. THE CONTESTED REGIME ===")
    print(f"  both-answered rows      {len(j):,}")
    print(f"    eq MEASURED arm       {pct(len(measured), len(j))}  "
          f"-- measurement precedence already discards dGbyG here")
    print(f"    eq GC arm             {pct(len(contested), len(j))}  "
          f"-- averaged against dGbyG today")
    if uses_gc.isna().any():
        print(f"    flag null             {int(uses_gc.isna().sum()):,}  "
              f"-- treated as neither arm; investigate if non-trivial")

    if contested.empty:
        print("\nThe contested regime is empty: the GC arm never coincides with a dGbyG "
              "answer, so the averaging branch is unreachable and the rule needs no "
              "change on this evidence.")
        return 0

    near_zero = contested[contested["dg_eq"].abs() <= a.near_zero]
    print(f"  of the GC arm, |dg| <= {a.near_zero} kJ/mol: "
          f"{pct(len(near_zero), len(contested))}   <- structural cancellation")

    print("\n=== 3. WHAT AVERAGING COSTS ON THE STRUCTURAL ZEROS ===")
    if near_zero.empty:
        print("  none -- the GC arm is not returning cancelled numbers on this universe, "
              "so averaging is a genuine contest between two predictors and the current "
              "rule stands.")
    else:
        mu_fused = 0.5 * (near_zero["dg_eq"] + near_zero["dg_db"])
        drag = (near_zero["dg_db"] - mu_fused).abs()
        s_ind = ((near_zero["sigma_eq"] ** 2 + near_zero["sigma_db"] ** 2) / 4.0) ** 0.5
        spread = ((near_zero["dg_eq"] - near_zero["dg_db"]) / 2.0).abs()
        s_fused = (pd.concat([s_ind, spread], axis=1).max(axis=1) ** 2
                   + TAU_SHARED ** 2) ** 0.5
        s_alone = (near_zero["sigma_db"] ** 2 + TAU_SHARED ** 2) ** 0.5
        print(f"  |dGbyG estimate| median {near_zero['dg_db'].abs().median():.2f} kJ/mol")
        print(f"  drag toward zero        median {drag.median():.2f}, "
              f"p90 {drag.quantile(0.90):.2f} kJ/mol")
        print(f"  sigma if fused          median {s_fused.median():.2f} kJ/mol")
        print(f"  sigma if dGbyG alone    median {s_alone.median():.2f} kJ/mol")
        infl = (s_fused / s_alone)
        print(f"  inflation factor        median {infl.median():.2f}x, "
              f"p90 {infl.quantile(0.90):.2f}x")
        big = int((drag > 5.0).sum())
        print(f"  rows dragged > 5 kJ/mol {pct(big, len(near_zero))}  "
              f"-- these are direction calls, not just widened error bars")

    real_gc = contested[contested["dg_eq"].abs() > a.near_zero]
    if not real_gc.empty:
        d = (real_gc["dg_eq"] - real_gc["dg_db"]).abs()
        print("\n--- for contrast, the GC arm where it did NOT cancel ---")
        print(f"  rows {len(real_gc):,}   |eq - dgbyg| median {d.median():.2f}, "
              f"p90 {d.quantile(0.90):.2f} kJ/mol")
        print("  A disagreement of the same order here and on the zeros would mean the "
              "zeros are not special and the floor is doing its job; a much smaller one "
              "here is the signal that the zeros are structural.")

    print("\n=== READ IT LIKE THIS ===")
    print("  If the structural-zero share of the GC arm is large AND the drag moves "
          "direction calls, exclude the GC arm from the averaging branch the way "
          "dir_calibrate already excludes it -- fall through to 'dGbyG alone' rather "
          "than averaging. If the share is small, leave thermo_vote as it is: the extra "
          "code path costs more than it buys.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
