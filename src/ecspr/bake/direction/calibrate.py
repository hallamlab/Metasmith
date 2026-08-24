# T3: category -> dG' calibration on the eQuilibrator MEASURED arm.
#
# For every MNXR that carries an orientation-aligned curated category (T2) and is
# eQ-routable, compute eQ dGr'0 in MNXR orientation, then bin by category and read
# off a robust location and spread. Only the measured (reactant-contribution) arm
# feeds the bins: the group-contribution arm returns identically zero for
# group-conserving chemistry (isomerases/transferases -- the algebra cancels),
# which is exactly the chemistry that dominates the REVERSIBLE bin, so a
# GC-populated calibration manufactures a fictitiously tight zero-centred bin.
#
# That restriction is necessary and not sufficient, because the cancellation can
# take the GC LABEL with it: an isomerase whose two sides share a decomposition
# comes back as a fully-measured zero with sigma at the floor. Anchors with no
# uncertainty are therefore dropped as well -- see `calibrate`.
#
# The bins are a property of the category, so calibration uses ALL MetaCyc-cap-eQ,
# not just the base-graph overlap (more data, and it rescues the small
# right-to-left bins). Per-bin n is recorded; a bin with few measured members is
# noise and must widen, not sharpen.
#
# Emits a calibration table (one row per category) and a per-reaction table (the
# measured points, for LOO at combine time and for the antisymmetry check).
#
# Runs under the `equilibrator` env.
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from . import canon
from .refdata import load_mnxr_stoich, load_mnxm_props
from .thermo_eq import EquilibratorMember

MAD_K = 1.4826  # MAD -> robust sigma for a normal

# Below this, eQuilibrator's reported uncertainty is a floor rather than a measurement,
# and the estimate it accompanies carries no information. See `calibrate` for why the
# arm restriction does not already exclude these. One value, in canon, because the
# combiner rejects the same rows on the same grounds -- if the two ever disagree, sigma_0
# gets fitted over a population the combiner does not use.
SIGMA_FLOOR_KJ = canon.DIR_SIGMA_FLOOR


def robust(x: np.ndarray):
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    return med, MAD_K * mad


def points_from_member(curated_per_mnxr, reac_prop, eq_member,
                       balance_gate=canon.DIR_BALANCE_GATE):
    # The same table as `compute_points`, READ rather than recomputed.
    #
    # WHY THIS EXISTS. `compute_points` instantiates `EquilibratorMember()` and re-scores
    # every curated reaction -- recomputing, minutes later in the SAME transform, exactly
    # what the member step already wrote to `_member_eq.parquet`. Loading
    # component-contribution's 1.3 GB cache and re-running its linear algebra to reproduce
    # numbers already on disk is about half this lane's wall clock, and it adds no
    # artifact and no check: if the two disagreed, nothing here would notice.
    #
    # The cheap half is kept: `load_mnxr_stoich` is a reac_prop parse, and the transport
    # and unbalanced exclusions have to come from somewhere -- a standard dGr'0 omits the
    # membrane term, and the dGr'0 of an unbalanced equation is meaningless.
    cur = pd.read_parquet(curated_per_mnxr)
    cur = cur[cur["aligned"].notna()][["mnxr", "aligned"]]
    mem = pd.read_parquet(eq_member).set_index("mnxr")
    stoich = load_mnxr_stoich(reac_prop)

    rows = []
    for mnxr, cat in zip(cur["mnxr"], cur["aligned"]):
        s = stoich.get(mnxr)
        if s is None:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="no_stoich", is_transport=None))
            continue
        _st, is_bal, is_tr = s
        if is_tr:
            # A transport reaction carries a membrane term that a standard dGr'0 does not
            # model at all. Unlike the balance test below, restaging cannot fix that, so
            # this gate stays above the member.
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="transport", is_transport=True))
            continue
        if mnxr not in mem.index:
            # The member ran over the universe; a curated reaction outside it is a
            # coverage gap, and saying so is the point of recording a reason at all.
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="not_in_member", is_transport=False))
            continue
        r = mem.loc[mnxr]
        # THE BALANCE TEST BELONGS HERE, BELOW THE MEMBER, NOT ABOVE IT. `is_balanced`
        # comes off raw `reac_prop`, but the member does not score the raw equation -- the
        # substitution lane restages polymer and carrier chemistry first, and 546 curated
        # reactions that reac_prop calls unbalanced come back from the member balanced and
        # answered. Refusing them on the raw verdict discards a real measurement and, worse,
        # changes the POPULATION the category prior is fitted on. An equation the member
        # could not close still fails: it fails by the member declining it, which is a
        # verdict about the equation that was actually scored.
        answered = (balance_gate == "after_member"
                    and r["reason"] == "ok" and pd.notna(r["dg"]))
        if not is_bal and not answered:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="unbalanced", is_transport=False))
            continue
        # The member writes the arm indicator as `flag`; calibration knows it as
        # `uses_gc`. One rename, in one place, rather than two schemas.
        rows.append(dict(mnxr=mnxr, category=cat, dg=r["dg"], sigma=r["sigma"],
                         uses_gc=r["flag"], reason=r["reason"], is_transport=False))
    return pd.DataFrame(rows)


def compute_points(curated_per_mnxr, reac_prop, chem_prop, limit=None,
                   substitutions=None):
    cur = pd.read_parquet(curated_per_mnxr)
    cur = cur[cur["aligned"].notna()][["mnxr", "aligned"]]
    stoich = load_mnxr_stoich(reac_prop)
    props = load_mnxm_props(chem_prop)
    # The calibration fits the curated prior on what the member ACTUALLY answered, so it
    # has to be handed the same equations the member was. Restaged here and not only in
    # `drive`: a prior fitted on the unsubstituted arm and applied to the substituted one
    # is the same class of mistake as an unstamped cache.
    from .refdata import load_mnxm_formulas, load_mnxm_names
    from . import substitute
    # `eq` AND NOT `any`: this path re-scores with EquilibratorMember, so it must be handed
    # the set eQuilibrator was admitted for. Handing it the union would restage reactions
    # under a couple this member is refused on and fit the prior to answers the run never
    # produced.
    subs = substitute.load(
        substitutions, props,
        load_mnxm_names(chem_prop) if substitutions else {},
        formulas=load_mnxm_formulas(chem_prop) if substitutions else None,
        member="eq")
    props = subs.props(props)
    eq = EquilibratorMember()

    rows, n = [], 0
    for mnxr, cat in zip(cur["mnxr"], cur["aligned"]):
        s = stoich.get(mnxr)
        if s is None:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="no_stoich", is_transport=None))
            continue
        st, is_bal, is_tr = s
        if is_tr:                              # membrane term standard dGr'0 omits
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="transport", is_transport=True))
            continue
        if not is_bal:                         # dGr'0 of an unbalanced eqn is meaningless
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="unbalanced", is_transport=False))
            continue
        dg, sig, gc, reason = eq.dgr(subs.rewrite(st), props)
        rows.append(dict(mnxr=mnxr, category=cat, dg=dg, sigma=sig,
                         uses_gc=gc, reason=reason, is_transport=False))
        n += 1
        if n % 2000 == 0:
            print(f"[calib] {n} eQ reactions attempted...", flush=True)
        if limit and n >= limit:
            break
    return pd.DataFrame(rows)


def calibrate(points: pd.DataFrame) -> pd.DataFrame:
    # One row per category from the MEASURED arm. tau deconvolves the eQ
    # measurement noise out of the observed spread (v = tau^2 + mean(sigma^2)).
    #
    # THE ARM RESTRICTION IS NECESSARY AND WAS NOT SUFFICIENT. Excluding `uses_gc` keeps
    # out the reactions whose group-contribution estimate is a structural zero -- but it
    # only catches the ones eQuilibrator still LABELS as group-contribution. For an
    # isomerase written `1 A = 1 B`, the two sides decompose into the same groups, so the
    # GC term cancels along with its uncertainty and what comes back is dG exactly 0 with
    # sigma at the floor, flagged as fully measured. The cancellation that makes it
    # meaningless is the same cancellation that makes it look clean.
    #
    # Measured on the first full run: 116 of 471 measured-arm anchors, concentrated in the
    # two STRICT bins -- 41% of LEFT-TO-RIGHT and 27% of RIGHT-TO-LEFT. They dragged both
    # medians to zero, and since these medians ARE the prior, 2,331 reactions carrying
    # MetaCyc's strictest directional call came out fully reversible while the weaker
    # PHYSIOL bins gave correct signs. Backwards, and in the flattering direction.
    #
    # THE DISCRIMINATOR IS SIGMA, NOT dG. 22 of the 116 have |dG| up to 6.3 kJ/mol, so a
    # near-zero-value test misses them; and one genuine measurement reads 0 +/- 2.9, which
    # a value test would wrongly discard. An estimate with no uncertainty is the thing
    # that carries no information, whatever value it happens to take.
    ok = points[(points["reason"] == "ok") & points["dg"].notna()]
    measured = ok[ok["uses_gc"] == False]     # noqa: E712  (arm restriction)
    n_arm = len(measured)
    measured = measured[measured["sigma"] > SIGMA_FLOOR_KJ]
    if n_arm != len(measured):
        # Named, not silent: this drops anchors, and a calibration that quietly shrinks
        # its own evidence base is indistinguishable from one that never had it.
        print(f"[calib] dropped {n_arm - len(measured)} of {n_arm} measured-arm anchors "
              f"with sigma at the {SIGMA_FLOOR_KJ} floor (no-information estimates)",
              flush=True)
    out = []
    for cat, g in measured.groupby("category"):
        dg = g["dg"].to_numpy(float)
        sig = g["sigma"].to_numpy(float)
        med, spread = robust(dg)
        var_obs = float(np.var(dg)) if len(dg) > 1 else 0.0
        tau2 = max(0.0, var_obs - float(np.mean(sig ** 2)))
        out.append(dict(category=cat, n=len(g), median=med, mad_spread=spread,
                        mean=float(np.mean(dg)), std=float(np.std(dg)),
                        tau=float(np.sqrt(tau2)), mean_sigma=float(np.mean(sig))))
    return pd.DataFrame(out).sort_values("n", ascending=False)


def sanity(cal: pd.DataFrame, points: pd.DataFrame):
    d = {r.category: r for r in cal.itertuples()}
    print("\n[calib] per-category (MEASURED arm):")
    print(cal.to_string(index=False))
    if "REVERSIBLE" in d:
        print(f"\n[check] REVERSIBLE median = {d['REVERSIBLE'].median:.2f} kJ/mol "
              f"(expect ~0)  n={d['REVERSIBLE'].n}")
    for a, b in (("PHYSIOL-LEFT-TO-RIGHT", "PHYSIOL-RIGHT-TO-LEFT"),
                 ("LEFT-TO-RIGHT", "RIGHT-TO-LEFT")):
        if a in d and b in d:
            print(f"[check] antisymmetry {a} median={d[a].median:.2f} vs "
                  f"-({b}) = {-d[b].median:.2f}  (expect ~equal)")
    ok = points[points["reason"] == "ok"]
    if len(ok):
        gc = int((ok["uses_gc"] == True).sum())    # noqa: E712
        meas = int((ok["uses_gc"] == False).sum())  # noqa: E712
        print(f"\n[arm-split] eQ ok on {len(ok)} reactions: measured={meas} "
              f"({meas/len(ok):.1%}), group-contribution={gc} ({gc/len(ok):.1%})")
    print(f"[tractable] eQ reason breakdown:\n{points['reason'].value_counts().to_string()}")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated", required=True, help="curated per-MNXR parquet (T2)")
    ap.add_argument("--reac-prop", required=True)
    ap.add_argument("--chem-prop", default=None,
                    help="only needed on the RE-SCORING path; ignored with --eq-member")
    ap.add_argument("--eq-member", default=None,
                    help="the eQuilibrator member's own table. PREFERRED: the member step "
                         "already scored every one of these reactions minutes ago in the "
                         "same transform, so re-instantiating component-contribution to "
                         "reproduce them is about half this lane's wall clock for no new "
                         "artifact and no new check")
    ap.add_argument("--balance-gate", default=canon.DIR_BALANCE_GATE,
                    choices=list(canon.DIR_BALANCE_GATES),
                    help="whether the raw reac_prop balance test runs before or after the "
                         "member is consulted")
    ap.add_argument("--out-calibration", required=True)
    ap.add_argument("--out-points", required=True)
    ap.add_argument("--limit", type=int, default=None, help="cap eQ reactions (testing)")
    ap.add_argument("--substitutions", default=None,
                   help="a substitution table directory, for the --chem-prop recompute "
                        "path. The --eq-member path needs none: it READS the member "
                        "table, which already carries whatever the run was given.")
    a = ap.parse_args(argv)
    if a.eq_member:
        points = points_from_member(Path(a.curated), Path(a.reac_prop), Path(a.eq_member),
                                    balance_gate=a.balance_gate)
    elif a.chem_prop:
        points = compute_points(Path(a.curated), Path(a.reac_prop),
                                Path(a.chem_prop), a.limit, a.substitutions)
    else:
        raise SystemExit("[calib] need --eq-member, or --chem-prop to re-score")
    cal = calibrate(points)
    points.to_parquet(a.out_points, index=False)
    cal.to_parquet(a.out_calibration, index=False)
    sanity(cal, points)


if __name__ == "__main__":
    main()
