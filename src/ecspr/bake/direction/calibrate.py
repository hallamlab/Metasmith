from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .refdata import load_mnxr_stoich, load_mnxm_props
from .thermo_eq import EquilibratorMember

MAD_K = 1.4826

SIGMA_FLOOR_KJ = 1e-4


def robust(x: np.ndarray):
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    return med, MAD_K * mad


def points_from_member(curated_per_mnxr, reac_prop, eq_member):
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
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="transport", is_transport=True))
            continue
        if not is_bal:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="unbalanced", is_transport=False))
            continue
        if mnxr not in mem.index:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="not_in_member", is_transport=False))
            continue
        r = mem.loc[mnxr]
        rows.append(dict(mnxr=mnxr, category=cat, dg=r["dg"], sigma=r["sigma"],
                         uses_gc=r["flag"], reason=r["reason"], is_transport=False))
    return pd.DataFrame(rows)


def compute_points(curated_per_mnxr, reac_prop, chem_prop, limit=None):
    cur = pd.read_parquet(curated_per_mnxr)
    cur = cur[cur["aligned"].notna()][["mnxr", "aligned"]]
    stoich = load_mnxr_stoich(reac_prop)
    props = load_mnxm_props(chem_prop)
    eq = EquilibratorMember()

    rows, n = [], 0
    for mnxr, cat in zip(cur["mnxr"], cur["aligned"]):
        s = stoich.get(mnxr)
        if s is None:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="no_stoich", is_transport=None))
            continue
        st, is_bal, is_tr = s
        if is_tr:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="transport", is_transport=True))
            continue
        if not is_bal:
            rows.append(dict(mnxr=mnxr, category=cat, dg=None, sigma=None,
                             uses_gc=None, reason="unbalanced", is_transport=False))
            continue
        dg, sig, gc, reason = eq.dgr(st, props)
        rows.append(dict(mnxr=mnxr, category=cat, dg=dg, sigma=sig,
                         uses_gc=gc, reason=reason, is_transport=False))
        n += 1
        if n % 2000 == 0:
            print(f"[calib] {n} eQ reactions attempted...", flush=True)
        if limit and n >= limit:
            break
    return pd.DataFrame(rows)


def calibrate(points: pd.DataFrame) -> pd.DataFrame:
    ok = points[(points["reason"] == "ok") & points["dg"].notna()]
    measured = ok[ok["uses_gc"] == False]     # noqa: E712  (arm restriction)
    n_arm = len(measured)
    measured = measured[measured["sigma"] > SIGMA_FLOOR_KJ]
    if n_arm != len(measured):
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
    ap.add_argument("--out-calibration", required=True)
    ap.add_argument("--out-points", required=True)
    ap.add_argument("--limit", type=int, default=None, help="cap eQ reactions (testing)")
    a = ap.parse_args(argv)
    if a.eq_member:
        points = points_from_member(Path(a.curated), Path(a.reac_prop), Path(a.eq_member))
    elif a.chem_prop:
        points = compute_points(Path(a.curated), Path(a.reac_prop),
                                Path(a.chem_prop), a.limit)
    else:
        raise SystemExit("[calib] need --eq-member, or --chem-prop to re-score")
    cal = calibrate(points)
    points.to_parquet(a.out_points, index=False)
    cal.to_parquet(a.out_calibration, index=False)
    sanity(cal, points)


if __name__ == "__main__":
    main()
