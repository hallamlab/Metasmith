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
import score as S  # noqa: E402

INK = "#1b2021"
ACCENT = "#1c5f5a"
SIGNAL = "#a94b19"
MUTED = "#7d8a8c"
GRID = "#dfe4e3"

plt.rcParams.update({
    "figure.dpi": 130, "savefig.dpi": 200, "font.size": 8.5,
    "axes.edgecolor": INK, "axes.labelcolor": INK, "text.color": INK,
    "xtick.color": INK, "ytick.color": INK, "axes.grid": True,
    "grid.color": GRID, "grid.linewidth": 0.5, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False,
    "figure.facecolor": "white", "savefig.facecolor": "white",
    "legend.frameon": False,
    "axes.prop_cycle": plt.cycler(color=[
        "#1c5f5a", "#a94b19", "#4a7fa5", "#7a5c99", "#b08b2e",
        "#3f7d5c", "#8c4a5e", "#5b6e70"]),
})


def save(fig, name):
    for ext in ("png", "svg"):
        fig.savefig(C.OUT / f"fig_{name}.{ext}", bbox_inches="tight")
    plt.close(fig)
    print(f"  fig_{name}.png")


def label(u: str) -> str:
    parts = u.split("__")
    host = {"e_coli_k12": "iML1515", "e_coli_dh10b": "iECDH10B"}.get(parts[0], parts[0])
    return f"{parts[1]} · {host} · {parts[3] if len(parts) > 3 else ''}"


def fig_pdist(nulls, head):
    units = list(head.sort_values("frac_sig", ascending=False).unit)
    n = len(units)
    cols = min(3, n)
    rows = int(np.ceil(n / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(3.1 * cols, 2.5 * rows),
                             squeeze=False)
    for ax, u in zip(axes.ravel(), units):
        g = nulls[nulls.unit == u]
        p = g.p_resid.dropna().to_numpy()
        ax.hist(p, bins=np.linspace(0, 1, 21), color=ACCENT, alpha=0.85,
                edgecolor="white", linewidth=0.4)
        ax.axhline(len(p) / 20, color=SIGNAL, lw=1.2, ls="--")
        h = head[head.unit == u].iloc[0]
        ax.set_title(f"{label(u)}\nq<0.05: {h.frac_sig:.0%}  "
                     f"[{h.ci_lo:.0%}, {h.ci_hi:.0%}]  n={int(h.n_conditions)}",
                     fontsize=7.5)
        ax.set_xlabel("empirical p vs counterfactual designs")
        ax.set_ylabel("conditions")
    for ax in axes.ravel()[n:]:
        ax.set_visible(False)
    fig.suptitle("Each design against the designs nobody built\n"
                 "dashed line = uniform null (no signal)", fontsize=9.5, y=1.005)
    fig.tight_layout()
    save(fig, "1_p_distribution")


def fig_forest(nulls, unit):
    g = nulls[nulls.unit == unit].copy()
    if g.empty:
        return
    g = g.sort_values("observed").reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(5.8, min(9.0, max(2.5, 0.075 * len(g)))))
    y = np.arange(len(g))
    ax.hlines(y, g.null_median, g.null_p95, color=MUTED, lw=1.4, alpha=0.6)
    ax.scatter(g.observed, y, s=7, color=ACCENT, zorder=3, label="observed")
    ax.scatter(g.null_median, y, s=4, color=MUTED, zorder=2,
               label="counterfactual median")
    sig = g.p_resid < 0.05
    ax.scatter(g.observed[sig], y[sig], s=22, facecolors="none",
               edgecolors=SIGNAL, lw=0.8, zorder=4, label="p < 0.05")
    ax.set_xscale("symlog", linthresh=1e-12)
    ax.set_yticks([])
    ax.set_xlabel("delta at the paper's own target")
    ax.set_ylabel(f"{len(g)} conditions, sorted by effect")
    ax.set_title(f"Which designs the method gets right — {label(unit)}", fontsize=9)
    ax.legend(loc="lower right", fontsize=7)
    fig.tight_layout()
    save(fig, f"2_forest_{unit}")


def fig_auroc(od):
    if od.empty:
        return
    units = sorted(od.unit.unique())
    fig, ax = plt.subplots(figsize=(1.1 * len(units) + 2.4, 3.2))
    data = [od[od.unit == u].auroc.dropna().to_numpy() for u in units]
    bp = ax.boxplot(data, patch_artist=True, widths=0.55,
                    medianprops=dict(color=SIGNAL, lw=1.4))
    for b in bp["boxes"]:
        b.set(facecolor=ACCENT, alpha=0.35, edgecolor=ACCENT)
    for i, (u, d) in enumerate(zip(units, data), start=1):
        ax.scatter(np.full(d.size, i) + RNGJ(d.size), d, s=7, color=ACCENT,
                   alpha=0.6, zorder=3)
    ax.axhline(0.5, color=MUTED, ls="--", lw=1)
    ax.set_xticks(range(1, len(units) + 1))
    ax.set_xticklabels([label(u) for u in units], rotation=30, ha="right",
                       fontsize=7)
    ax.set_ylabel("AUROC per target column")
    ax.set_title("Off-diagonal: this target's real designs against the other\n"
                 "papers' real designs (negatives are published designs)",
                 fontsize=9)
    fig.tight_layout()
    save(fig, "3_off_diagonal_auroc")


_rj = np.random.default_rng(5)


def RNGJ(n):
    return _rj.uniform(-0.11, 0.11, n)


def fig_biomass():
    files = sorted(C.OUT.glob("biomass_scatter_*.tsv"))
    if not files:
        return
    fig, axes = plt.subplots(1, len(files), figsize=(3.4 * len(files), 3.1),
                             squeeze=False)
    for ax, f in zip(axes[0], files):
        d = pd.read_csv(f, sep="\t")
        ax.scatter(d.d_mu, d.d_prec, s=15, color=ACCENT, alpha=0.7,
                   edgecolors="white", linewidths=0.3)
        rho, n = S.spearman(d.d_prec, d.d_mu)
        ax.axhline(0, color=MUTED, lw=0.7)
        ax.axvline(0, color=MUTED, lw=0.7)
        ax.set_xscale("symlog", linthresh=1e-3)
        ax.set_yscale("symlog", linthresh=1e-9)
        ax.set_xlabel("FBA  delta growth  (1/h)")
        ax.set_ylabel("ECSPr  delta precursor share")
        ax.set_title(f"{f.stem[len('biomass_scatter_'):]}\n"
                     f"Spearman {rho:.2f}  (n={n})", fontsize=8)
    fig.suptitle("Biomass, label-free: the one target flux balance was built for",
                 fontsize=9.5, y=1.02)
    fig.tight_layout()
    save(fig, "4_biomass")


def fig_method_vs_method(pred):
    mv = pd.read_csv(C.OUT / "T6_method_vs_method.tsv", sep="\t")
    if mv.empty:
        return
    best = mv.sort_values("n_cells", ascending=False).iloc[0]
    f = pred[pred.unit == best.fba_unit][["design_id", "mnxm", "value"]].rename(
        columns={"value": "flux"})
    e = pred[pred.unit == best.ecspr_unit][["design_id", "mnxm", "value"]].rename(
        columns={"value": "draw"})
    j = e.merge(f, on=["design_id", "mnxm"]).dropna()
    fig, axes = plt.subplots(1, 3, figsize=(10.2, 3.2))
    ax = axes[0]
    ax.scatter(j.flux, j.draw, s=4, alpha=0.3, color=ACCENT)
    ax.set_xscale("symlog", linthresh=1e-9)
    ax.set_yscale("symlog", linthresh=1e-14)
    ax.xaxis.set_major_locator(matplotlib.ticker.SymmetricalLogLocator(
        base=100.0, linthresh=1e-9))
    ax.yaxis.set_major_locator(matplotlib.ticker.SymmetricalLogLocator(
        base=1e4, linthresh=1e-14))
    ax.set_xlabel("FBA delta flux")
    ax.set_ylabel("ECSPr delta draw")
    ax.set_title(f"pooled cells (n={int(best.n_cells)})\n"
                 f"Spearman {best.pooled_spearman:.2f}", fontsize=8)
    for ax, key, ttl in ((axes[1], "mnxm", "column-wise: do they rank DESIGNS alike?"),
                         (axes[2], "design_id",
                          "row-wise: do they rank METABOLITES alike?")):
        rhos = [S.spearman(g.draw, g.flux)[0] for _, g in j.groupby(key)
                if len(g) >= (20 if key == "mnxm" else 10)]
        rhos = [r for r in rhos if np.isfinite(r)]
        ax.hist(rhos, bins=21, color=ACCENT, alpha=0.85, edgecolor="white",
                linewidth=0.4)
        ax.axvline(0, color=MUTED, ls="--", lw=1)
        if rhos:
            ax.axvline(np.median(rhos), color=SIGNAL, lw=1.4)
        ax.set_xlabel("Spearman rho")
        ax.set_ylabel("count")
        ax.set_title(f"{ttl}\nmedian "
                     f"{np.median(rhos):.2f} (n={len(rhos)})" if rhos else ttl,
                     fontsize=8)
    fig.suptitle(f"Conductance against flux — {label(best.ecspr_unit)} vs "
                 f"{label(best.fba_unit)}", fontsize=9.5, y=1.03)
    fig.tight_layout()
    save(fig, "5_conductance_vs_flux")


def fig_noise(scal):
    d = scal[scal.status == "ok"].copy()
    if "noise_floor" not in d or d.noise_floor.isna().all():
        return
    fig, ax = plt.subplots(figsize=(5.4, 3.4))
    for u, g in d.groupby("unit"):
        if g.noise_floor.isna().all():
            continue
        sig = (g.i_eff_pert - g.i_eff_base).abs()
        n_zero = int((sig == 0).sum())
        ax.scatter(g.noise_floor, sig.clip(lower=1e-20), s=9, alpha=0.55,
                   label=f"{label(u)}" + (f"  ({n_zero} exact 0)" if n_zero else ""))
    lim = np.array([1e-18, 1e-2])
    ax.plot(lim, 10 * lim, color=SIGNAL, lw=1.2, ls="--", label="10x floor gate")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("solver noise floor (jittered baseline)")
    ax.set_ylabel("|delta I_eff| of the design   (exact zeros drawn at 1e-20)")
    ax.set_title("Signal against the noise floor: eleven orders of magnitude",
                 fontsize=9)
    ax.legend(fontsize=6.5, loc="upper left")
    fig.tight_layout()
    save(fig, "6_signal_vs_noise")


def fig_fold(nulls):
    g = nulls[(nulls.measurement_type == "fold") &
              nulls.status.isin(("ok", "created", "destroyed"))].copy()
    g["mag"] = pd.to_numeric(g.magnitude, errors="coerce")
    g = g.dropna(subset=["mag"])
    units = [u for u in g.unit.unique() if len(g[g.unit == u]) >= 8]
    if not units:
        return
    fig, axes = plt.subplots(1, len(units), figsize=(3.2 * len(units), 3.0),
                             squeeze=False)
    for ax, u in zip(axes[0], units):
        s = g[g.unit == u]
        ax.scatter(np.log2(s.mag.clip(lower=1e-6)), s.observed, s=14,
                   color=ACCENT, alpha=0.75)
        rho, n = S.spearman(s.observed, np.log2(s.mag.clip(lower=1e-6)))
        ax.set_yscale("symlog", linthresh=1e-13)
        ax.set_xlabel("log2 measured fold change")
        ax.set_ylabel("predicted delta at target")
        ax.set_title(f"{label(u)}\nSpearman {rho:.2f} (n={n})", fontsize=8)
    fig.suptitle("Does the size of the prediction track the size of the effect?",
                 fontsize=9.5, y=1.03)
    fig.tight_layout()
    save(fig, "7_fold_change")


def main():
    pred = S.load_predictions()
    pred = pd.concat([pred, S.trivial_arm([])], ignore_index=True)
    nulls = pd.read_csv(C.OUT / "counterfactual_nulls.tsv", sep="\t")
    head = pd.read_csv(C.OUT / "T2_headline.tsv", sep="\t")
    od = pd.read_csv(C.OUT / "T4_off_diagonal_auroc.tsv", sep="\t")
    scal = pd.read_parquet(C.OUT / "scalars_all.parquet")

    print("figures:")
    fig_pdist(nulls, head)
    ec = head[head.arm.astype(str).str.startswith(("gem", "denovo"))]
    for u in ec.sort_values("n_conditions", ascending=False).unit.head(2):
        fig_forest(nulls, u)
    fig_auroc(od)
    fig_biomass()
    fig_method_vs_method(pred)
    fig_noise(scal)
    fig_fold(nulls)


if __name__ == "__main__":
    main()
