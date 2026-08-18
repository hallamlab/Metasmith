"""Step (a): PROVE END TO END that the retrieved member tables carry substitutions.

This is the check `f4642fc` earned. Neither member lane passed `--substitutions` until
that commit, and the failure was invisible: the job runs, exits 0, writes a full-size
table, and the only difference is that the chemistry is r8's under an r9 DIRVER. So the
question is not "did the run succeed" -- it did either way -- but "is the chemistry in
this table the chemistry the tables describe".

Four checks, in increasing order of how hard they are to pass by accident:

  1. NOT EMPTY / FULL UNIVERSE. `drive eval` catches an ImportError and writes an EMPTY
     member table, returning 0 ("the ensemble loses a vote, not the build"). A green run
     with an absent member is the loudest silent failure here, so it is checked first.
  2. sigma_sub > 0 EXISTS. The substituted-width column is written by nothing else. Zero
     such rows means the tables did not reach the member.
  3. COVERED REACTIONS SPOKE. Reactions the tables cover that were no_props / no_smiles /
     wildcard in r8 must now carry a dg. This is the effect, not just the marker.
  4. UNCOVERED REACTIONS ARE UNCHANGED, row for row, against r8's committed seam. This is
     the invariant the whole mechanism was designed around -- no MetaNetX props key is
     ever overwritten -- checked at the far end of the cluster rather than in a unit test.
     A diff here is worse than a missing substitution: it means uncovered chemistry moved.

Exit 0 only if all four pass on both members. Anything else and the run must be redone.
"""
import sys
from pathlib import Path
sys.path.insert(0, "src")
import numpy as np
import pandas as pd
from ecspr.bake.direction import substitute as S
from ecspr.bake.direction.refdata import (load_mnxr_stoich, load_mnxm_names,
                                          load_mnxm_props, load_mnxm_formulas)

MNX = Path("data/fabfos/originals/metanetx/4.5")
R8 = Path("data/fabfos/processed/metabolism_bake/seams")
TABLES = Path("src/ecspr/bake/direction")
SILENT = {"no_props", "no_smiles", "wildcard", "unresolved", "no_stoich", "unparseable"}
# kJ/mol. See check 4: eight orders below anything that can move a tier.
TOL = 1e-9

new_eq, new_dg = Path(sys.argv[1]), Path(sys.argv[2])

props = load_mnxm_props(MNX / "chem_prop.tsv")
names = load_mnxm_names(MNX / "chem_prop.tsv")
formulas = load_mnxm_formulas(MNX / "chem_prop.tsv")
allst = load_mnxr_stoich(MNX / "reac_prop.tsv")

# THE COVERED SET IS PER MEMBER. A row refused for one member and kept for the other is
# the ordinary `member_drift` path, so a reaction covered for dGbyG may be untouched for
# eQuilibrator. Loading once and reusing the set would test check 3 against reactions this
# member was never given, and -- worse -- exempt from check 4 rows that no table touches
# for this member, which is the one invariant nothing else asserts.
covered_by = {}
for member in ("eq", "dgbyg"):
    subs = S.load(TABLES, props, names, formulas=formulas, member=member)
    covered_by[member] = {m for m, v in allst.items() if subs.covers(v[0])}
    print(f"substitution tables cover {len(covered_by[member]):,} reactions for {member}")
print()

bad = []
for member, newp, oldp in (("eq", new_eq, R8 / "direction_member_eq.parquet"),
                           ("dgbyg", new_dg, R8 / "direction_member_dgbyg.parquet")):
    print(f"===== {member} =====")
    covered = covered_by[member]
    if not newp.exists():
        bad.append(f"{member}: {newp} does not exist"); print("  MISSING\n"); continue
    new = pd.read_parquet(newp)
    old = pd.read_parquet(oldp)
    print(f"  rows: r9 {len(new):,}   r8 {len(old):,}")

    # 1 -- the empty-table failure mode
    if len(new) == 0:
        bad.append(f"{member}: EMPTY member table -- the member was absent in the image")
        print("  [1] FAIL empty\n"); continue
    if len(new) != len(old):
        bad.append(f"{member}: universe changed {len(old):,} -> {len(new):,}")
    print(f"  [1] {'ok' if len(new) == len(old) else 'FAIL'} full universe")

    # 2 -- the marker
    nsub = int((new["sigma_sub"].fillna(0) > 0).sum()) if "sigma_sub" in new else 0
    print(f"  [2] {'ok' if nsub else 'FAIL'} sigma_sub>0 rows: {nsub:,}")
    if not nsub:
        bad.append(f"{member}: no sigma_sub>0 row -- tables did not reach the member")

    # 3 -- the effect
    o = old.set_index("mnxr"); n = new.set_index("mnxr")
    cov = sorted(covered & set(n.index) & set(o.index))
    was_silent = [m for m in cov if str(o.loc[m, "reason"]) in SILENT]
    now_speaks = [m for m in was_silent if pd.notna(n.loc[m, "dg"])]
    print(f"  [3] {'ok' if now_speaks else 'FAIL'} covered+r8-silent {len(was_silent):,}"
          f" -> now answer {len(now_speaks):,}")
    if was_silent and not now_speaks:
        bad.append(f"{member}: {len(was_silent):,} covered reactions were silent in r8 "
                   f"and NONE speaks in r9")

    # 4 -- the invariant, at the far end of the cluster.
    #
    # THE VERDICT COLUMNS ARE COMPARED EXACTLY AND THE MEASURED ONES ARE NOT. `flag` and
    # `reason` are decisions, so any change is a changed decision. `dg` and `sigma` are
    # float64 arriving from a different SHARDING than the deployed bake's -- the eq lane
    # ran as one pass in r8 and 16-wide here -- and a member's linear algebra does not
    # promise bit-identical accumulation across a different batching. Exact `!=` on those
    # asserts reproducible summation order, which nothing offers and which was only ever
    # holding because the two sides had shared provenance.
    #
    # TOL IS SET WHERE CHEMISTRY CANNOT HIDE UNDER IT. `canon.DIR_DECADE` is 5.71 kJ/mol
    # and DIR_SIGMA_FLOOR is a comparable scale; 1e-9 kJ/mol is eight orders below the
    # smallest difference that could move a tier or a ratio. The largest tolerated drift
    # is PRINTED rather than swallowed, so a real movement that happens to sit under the
    # bound still shows up as a number that grew.
    unc = sorted((set(n.index) & set(o.index)) - covered)
    verdicts, measured = ["flag", "reason"], ["dg", "sigma"]
    a, b = n.loc[unc], o.loc[unc]

    va, vb = a[verdicts], b[verdicts]
    changed = (va.ne(vb) & ~(va.isna() & vb.isna())).any(axis=1)

    worst = 0.0
    for c in measured:
        x, y = a[c].astype(float), b[c].astype(float)
        both = x.notna() & y.notna()
        drift = (x[both] - y[both]).abs()
        if len(drift):
            worst = max(worst, float(drift.max()))
        # NaN on exactly one side is an appearance or a disappearance, not a drift.
        changed |= (x.isna() != y.isna())
        changed |= both & ~np.isclose(x, y, rtol=1e-12, atol=TOL, equal_nan=True)

    ndiff = int(changed.sum())
    print(f"  [4] {'ok' if ndiff == 0 else 'FAIL'} uncovered rows unchanged:"
          f" {len(unc):,} compared, {ndiff:,} differ"
          f"  (largest tolerated drift {worst:.2e} kJ/mol, bound {TOL:g})")
    if ndiff:
        bad.append(f"{member}: {ndiff:,} UNCOVERED reactions changed -- chemistry moved "
                   f"where no table touches it")
        cols = verdicts + measured
        print(a[changed][cols].join(b[changed][cols], lsuffix="_r9",
                                    rsuffix="_r8").head(15).to_string())
    print(f"  reasons r9: {dict(new['reason'].value_counts().head(8))}\n")

print("=" * 70)
if bad:
    print("FAILED -- the run must be redone, not carried forward:")
    for x in bad:
        print("  -", x)
    sys.exit(1)
print("all four checks pass on both members: the tables reached the job")
