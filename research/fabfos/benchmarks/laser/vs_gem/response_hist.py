#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402
import make_figures as MF  # noqa: E402  (style block + palette)
import score as S  # noqa: E402

ECSPR_UNIT = "{host}__gem__C__dir"
FBA_UNIT = "{host}__fba_a0.1__C__dir"
HOSTS = ("e_coli_k12", "e_coli_dh10b")

ABSTAIN = "not_in_base"


def own_targets(idx: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [(r.design_id, m) for r in idx.itertuples(index=False)
         for m in str(r.target_mnxms).split(";") if m],
        columns=["design_id", "mnxm"]).drop_duplicates()


def matched(pred: pd.DataFrame, own: pd.DataFrame) -> pd.DataFrame:
    cols = ["design_id", "mnxm", "host", "value", "status", "noise_floor"]
    out = []
    for host in HOSTS:
        eu, fu = ECSPR_UNIT.format(host=host), FBA_UNIT.format(host=host)
        real = pred[~pred.is_counterfactual]
        e = real[real.unit == eu][cols].merge(own, on=["design_id", "mnxm"])
        f = real[real.unit == fu][cols].merge(own, on=["design_id", "mnxm"])
        if e.empty or f.empty:
            continue
        j = e.rename(columns={"value": "d_ecspr", "status": "s_ecspr",
                              "noise_floor": "floor_ecspr"}).merge(
            f.rename(columns={"value": "d_fba", "status": "s_fba"})
             .drop(columns=["noise_floor", "host"]),
            on=["design_id", "mnxm"])
        out.append(j)
    return pd.concat(out, ignore_index=True)


def fba_noop_floor(pred: pd.DataFrame, scal: pd.DataFrame) -> dict:
    units = [FBA_UNIT.format(host=h) for h in HOSTS]
    s = scal[scal.unit.isin(units) & (scal.status == "ok")]
    noop = s[(s.inserted_add.fillna(0) == 0) & (s.knockout.fillna(0) == 0)]
    v = pred[pred.unit.isin(units) & pred.design_id.isin(set(noop.design_id))].value
    v = v[np.isfinite(v)].abs()
    if v.empty:
        return dict(kind="none", band=np.nan, n_designs=0, n_cells=0)
    return dict(
        kind="empirical: designs whose edits left the model unchanged",
        band=float(np.percentile(v, 99)), n_designs=int(noop.design_id.nunique()),
        n_cells=int(v.size), frac_exact_zero=float((v == 0).mean()),
        p50=float(np.median(v)), p99=float(np.percentile(v, 99)),
        worst=float(v.max()))


def ecspr_floor(cells: pd.DataFrame) -> dict:
    f = cells.floor_ecspr.dropna()
    return dict(kind="measured: baseline re-solved with weights jittered by 1e-10",
                band=float(10 * f.median()) if len(f) else np.nan,
                n_designs=int(cells.design_id.nunique()), n_cells=len(cells),
                p50=float(f.median()) if len(f) else np.nan,
                worst=float(f.max()) if len(f) else np.nan)


def split(v: np.ndarray, band: np.ndarray | float) -> np.ndarray:
    return np.isfinite(v) & (np.abs(v) > np.asarray(band))


def table(cells: pd.DataFrame, ans: dict, floors: dict, n_abstain: int) -> pd.DataFrame:
    rows = []
    for name, col, unit_s in (("ECSPr", "d_ecspr", "current (arbitrary units)"),
                              ("FBA", "d_fba", "mmol / gDW / h")):
        v = cells[col].to_numpy(float)
        a = ans[name]
        nz = np.abs(v[np.isfinite(v) & (v != 0)])
        fl = floors[name]
        rows.append(dict(
            method=name, readout=("delta draw at target" if name == "ECSPr"
                                  else "delta max flux at target"),
            units=unit_s, n_designs=cells.design_id.nunique(), n_cells=len(cells),
            n_abstained=n_abstain if name == "ECSPr" else 0,
            floor_kind=fl["kind"], band=fl["band"],
            band_n_cells=fl.get("n_cells"), band_n_designs=fl.get("n_designs"),
            band_worst=fl.get("worst"),
            n_answered=int(a.sum()), frac_answered=float(a.mean()),
            n_unanswered=int((~a).sum()),
            n_up=int((v > 0).sum()), n_down=int((v < 0).sum()),
            n_exact_zero=int((v == 0).sum()),
            median_abs_nonzero=float(np.median(nz)) if nz.size else np.nan,
            max_abs=float(np.nanmax(np.abs(v))) if len(v) else np.nan))
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T15_response_resolution.tsv", sep="\t", index=False)
    return out


def symlog_bins(v: np.ndarray, lin: float, n: int = 13) -> np.ndarray:
    top = max(np.nanmax(np.abs(v)) * 1.2, lin * 10)
    pos = np.geomspace(lin, top, n + 1)
    return np.concatenate([-pos[::-1], pos])


def decade_ticks(v: np.ndarray, lin: float, want: int = 5) -> list:
    lo = int(np.ceil(np.log10(lin * 10)))
    hi = int(np.floor(np.log10(max(np.nanmax(np.abs(v)) * 1.2, lin * 100))))
    exps = list(range(lo, hi + 1))
    step = max(1, int(np.ceil(len(exps) / want)))
    exps = exps[::-1][::step][::-1]
    return [-10.0 ** e for e in reversed(exps)] + [0.0] + [10.0 ** e for e in exps]


def figure(cells: pd.DataFrame, ans: dict, floors: dict, n_abstain: int,
           dest: Path) -> Path:
    fig, axes = plt.subplots(2, 1, figsize=(6.6, 6.2))
    for ax, (name, col, xlabel) in zip(axes, (
            ("ECSPr", "d_ecspr", "delta current drawn at the target"),
            ("FBA", "d_fba", "delta max flux at the target   (mmol/gDW/h)"))):
        v = cells[col].to_numpy(float)
        band = floors[name]["band"]
        a = ans[name]
        bins = symlog_bins(v, band)
        ax.hist(v[a], bins=bins, color=MF.ACCENT, alpha=0.9, edgecolor="white",
                linewidth=0.4, label=f"resolved: {int(a.sum())}")
        ax.hist(v[~a], bins=bins, color=MF.MUTED, alpha=0.85, edgecolor="white",
                linewidth=0.4, label=f"inside the noise band: {int((~a).sum())}")
        ax.axvspan(-band, band, color=MF.SIGNAL, alpha=0.10, zorder=0)
        ax.axvline(0, color=MF.MUTED, lw=0.7)
        ax.set_xscale("symlog", linthresh=band)
        ax.set_xticks(decade_ticks(v, band))
        ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
        ax.set_xlabel(xlabel)
        ax.set_ylabel("cells (design × target)")
        ax.set_title(
            f"{name} — answers on {a.mean():.0%} of designs   "
            f"({int(a.sum())}/{len(v)});  {int((v > 0).sum())} up, "
            f"{int((v < 0).sum())} down\n"
            f"shaded: noise band ±{band:.1g}, {floors[name]['kind']}",
            fontsize=8.5, loc="left")
        ax.legend(fontsize=7, loc="upper left")
    fig.suptitle(
        "Does the method say anything? — same designs, same targets\n"
        f"{cells.design_id.nunique()} LASER designs at the target their own paper "
        f"measured; ECSPr abstains on {n_abstain} more (target not in its graph)",
        fontsize=9.5, y=1.005)
    fig.tight_layout()
    dest.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "svg"):
        fig.savefig(dest / f"fig_response_histograms.{ext}", bbox_inches="tight")
    plt.close(fig)
    return dest / "fig_response_histograms.png"


def main():
    pred = S.load_predictions()
    scal = S.load_scalars()
    idx = S.design_index()
    own = own_targets(idx)
    print(f"design index: {len(idx)} designs, {len(own)} own-target cells")

    cells = matched(pred, own)
    n_abstain = int((cells.s_ecspr == ABSTAIN).sum())
    print(f"matched by both arms: {len(cells)} cells / "
          f"{cells.design_id.nunique()} designs")
    cells = cells[cells.s_ecspr != ABSTAIN].copy()
    print(f"ECSPr abstentions dropped: {n_abstain} -> {len(cells)} scored cells")

    floors = {"ECSPr": ecspr_floor(cells), "FBA": fba_noop_floor(pred, scal)}
    for k, f in floors.items():
        print(f"  {k:6s} band {f['band']:.3g}   ({f['kind']})")
    print(f"  FBA band from {floors['FBA']['n_designs']} unchanged-model designs, "
          f"{floors['FBA']['n_cells']} cells, "
          f"{floors['FBA']['frac_exact_zero']:.0%} exactly zero, "
          f"worst {floors['FBA']['worst']:.3g}")

    ans = {"ECSPr": split(cells.d_ecspr.to_numpy(float),
                          10 * cells.floor_ecspr.to_numpy(float)),
           "FBA": split(cells.d_fba.to_numpy(float), floors["FBA"]["band"])}

    t = table(cells, ans, floors, n_abstain)
    print("\n== T15 response resolution ==")
    print(t[["method", "n_cells", "band", "n_answered", "frac_answered",
             "n_up", "n_down", "median_abs_nonzero"]].to_string(index=False))
    p = figure(cells, ans, floors, n_abstain, C.CACHE)
    print(f"\nwrote {p}\nwrote {C.OUT / 'T15_response_resolution.tsv'}")


if __name__ == "__main__":
    main()
