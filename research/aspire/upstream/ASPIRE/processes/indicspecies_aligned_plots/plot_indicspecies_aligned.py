#!/usr/bin/env python3
"""
plot_indicspecies_aligned.py

Plot and summarize indicspecies analysis outputs aligned with current workflow:
- run_indicspecies.R outputs (status stratified by type_group + type_group paired)
- power_indicspecies.R outputs (with subgroup columns)
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"
mpl.rcParams["savefig.dpi"] = 600
plt.rcParams.update({"font.size": 11})
plt.rcParams["font.family"] = "Source Sans Pro"
sns.set_theme()
sns.set_style("white")


LEGACY_TYPE_INDEX = (
    "1=BAL,2=Bronchial Brush,3=Oral Rinse,"
    "4=BAL+Bronchial Brush,5=BAL+Oral Rinse,6=Bronchial Brush+Oral Rinse,"
    "7=Oral Rinse+BAL+Bronchial Brush"
)
LEGACY_STATUS_INDEX = "1=Cancer,2=Non-Cancer,3=Cancer+Non-Cancer"
LEGACY_TYPE_PALETTE = (
    "Oral Rinse=#6A3D9A,BAL=#0072B2,Bronchial Brush=#009E73,"
    "BAL+Oral Rinse=#E78AC3,BAL+Bronchial Brush=#5CC8C8,"
    "Bronchial Brush+Oral Rinse=#B8E186,Oral Rinse+BAL+Bronchial Brush=#CBB6E9"
)
LEGACY_TYPE_ORDER = "Oral Rinse,BAL+Oral Rinse,BAL,BAL+Bronchial Brush,Bronchial Brush,Bronchial Brush+Oral Rinse,BAL+Bronchial Brush+Oral Rinse,not_indicator"
LEGACY_STATUS_PALETTE = "Cancer=#A50026,Non-Cancer=#FFFFFF,Cancer+Non-Cancer=#000000"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_summary_name(path: Path) -> dict:
    name = path.name
    m_type = re.match(r"^type_group_indicator_species(_DULEG)?_summary\.tsv$", name)
    if m_type:
        return {
            "grouping_col": "type_group",
            "subgroup_col": pd.NA,
            "subgroup_level": pd.NA,
            "status_subset": pd.NA,
            "duleg": bool(m_type.group(1)),
        }

    m_status = re.match(
        r"^status_(.+?)(_no_contralateral)?_indicator_species(_DULEG)?_summary\.tsv$",
        name,
    )
    if m_status:
        site = m_status.group(1).replace("_", " ")
        no_contralateral = bool(m_status.group(2))
        return {
            "grouping_col": "status",
            "subgroup_col": "type_group",
            "subgroup_level": site,
            "status_subset": "no_contralateral" if no_contralateral else "all",
            "duleg": bool(m_status.group(3)),
        }
    return {}


def make_analysis_label(df: pd.DataFrame) -> pd.Series:
    status_subset = df.get("status_subset", pd.Series(pd.NA, index=df.index))
    status_subset = status_subset.fillna("all").astype(str)
    status_suffix = np.where(status_subset.eq("no_contralateral"), " (no contralateral)", "")
    label = np.where(
        df["grouping_col"].eq("status"),
        "status | " + df["subgroup_level"].fillna("NA").astype(str) + status_suffix,
        df["grouping_col"].astype(str),
    )
    return pd.Series(label, index=df.index)


def decode_index_label(df: pd.DataFrame) -> pd.Series:
    s_cols = [c for c in df.columns if c.startswith("s.")]
    if not s_cols:
        return df["index"].astype(str)
    labels = []
    for _, row in df.iterrows():
        active = [c[2:] for c in s_cols if pd.to_numeric(row.get(c), errors="coerce") == 1]
        if active:
            labels.append(" + ".join(active))
        else:
            labels.append(str(row.get("index", "NA")))
    return pd.Series(labels, index=df.index)


def load_indicspecies(indir: Path, alpha: float, min_stat: float) -> pd.DataFrame:
    files = sorted(indir.glob("*_indicator_species*_summary.tsv"))
    rows = []
    for f in files:
        meta = parse_summary_name(f)
        if not meta:
            continue
        df = pd.read_csv(f, sep="\t")
        if "ASV" not in df.columns:
            continue
        if "q.value" not in df.columns and "p.value" in df.columns:
            df["q.value"] = pd.to_numeric(df["p.value"], errors="coerce")
        df["q.value"] = pd.to_numeric(df.get("q.value"), errors="coerce")
        df["stat"] = pd.to_numeric(df.get("stat"), errors="coerce")
        for k, v in meta.items():
            df[k] = v
        rows.append(df)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, axis=0, ignore_index=True)
    out["analysis_label"] = make_analysis_label(out)
    out["duleg_label"] = np.where(out["duleg"], "duleg=TRUE", "duleg=FALSE")
    out["neglog10_q"] = -np.log10(out["q.value"])
    out.loc[~np.isfinite(out["neglog10_q"]), "neglog10_q"] = np.nan
    out["index_label"] = decode_index_label(out)
    out["significant"] = (out["q.value"] < alpha) & (out["stat"] >= min_stat)
    return out


def save_indicspecies_summaries(df: pd.DataFrame, outdir: Path, top_n: int) -> None:
    ensure_dir(outdir)
    df.to_csv(outdir / "indicspecies_all_summary.tsv", sep="\t", index=False)

    sig = df[df["significant"]].copy()
    sig.to_csv(outdir / "indicspecies_significant_filtered.tsv", sep="\t", index=False)

    counts = (
        df.groupby(
            ["grouping_col", "subgroup_col", "subgroup_level", "status_subset", "analysis_label", "duleg"],
            dropna=False,
        )
        .agg(
            n_total=("ASV", "size"),
            n_significant=("significant", "sum"),
            min_q=("q.value", "min"),
            max_stat=("stat", "max"),
        )
        .reset_index()
        .sort_values(["grouping_col", "subgroup_level", "status_subset", "duleg"])
    )
    counts.to_csv(outdir / "indicspecies_counts_by_analysis.tsv", sep="\t", index=False)

    top = (
        sig.sort_values(["analysis_label", "duleg", "q.value", "stat"], ascending=[True, True, True, False])
        .groupby(["analysis_label", "duleg"], as_index=False, group_keys=False)
        .head(top_n)
    )
    top.to_csv(outdir / f"indicspecies_top_{top_n}_significant.tsv", sep="\t", index=False)

    # Additional summary: type_group indicators split by assigned indicator label
    type_sig = sig[sig["grouping_col"] == "type_group"].copy()
    if not type_sig.empty:
        type_counts = (
            type_sig.groupby(["duleg", "index_label"], as_index=False)
            .agg(n_significant=("ASV", "size"))
            .sort_values(["duleg", "n_significant"], ascending=[True, False])
        )
        type_counts.to_csv(outdir / "indicspecies_type_indicator_counts_by_label.tsv", sep="\t", index=False)


def plot_indicspecies(df: pd.DataFrame, outdir: Path) -> None:
    ensure_dir(outdir)
    plot_dir = outdir / "plots"
    ensure_dir(plot_dir)

    counts = (
        df.groupby(["analysis_label", "duleg_label"], as_index=False)
        .agg(n_significant=("significant", "sum"))
        .sort_values(["analysis_label", "duleg_label"])
    )
    if not counts.empty:
        plt.figure(figsize=(10, 5))
        ax = sns.barplot(
            data=counts,
            x="analysis_label",
            y="n_significant",
            hue="duleg_label",
            palette={"duleg=FALSE": "#4C78A8", "duleg=TRUE": "#F58518"},
        )
        ax.set_xlabel("Analysis")
        ax.set_ylabel("Significant ASVs (q < alpha)")
        ax.tick_params(axis="x", rotation=30)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), title="Duleg")
        plt.tight_layout()
        for ext in ("svg", "pdf"):
            plt.savefig(plot_dir / f"indicspecies_significant_counts.{ext}", bbox_inches="tight")
        plt.close()

    plot_df = df.dropna(subset=["stat", "neglog10_q"]).copy()
    if not plot_df.empty:
        g = sns.relplot(
            data=plot_df,
            x="stat",
            y="neglog10_q",
            col="analysis_label",
            row="duleg_label",
            hue="significant",
            kind="scatter",
            alpha=0.75,
            s=25,
            facet_kws={"sharex": False, "sharey": False},
            height=3.2,
            aspect=1.15,
            palette={False: "lightgray", True: "#D55E00"},
        )
        g.set_axis_labels("Indicator statistic", "-log10(q.value)")
        for ax in g.axes.flatten():
            if ax is None:
                continue
            ax.grid(alpha=0.25)
        for ext in ("svg", "pdf"):
            g.figure.savefig(plot_dir / f"indicspecies_stat_vs_q.{ext}", bbox_inches="tight")
        plt.close(g.figure)

    # Bar plot: type_group indicator counts split by indicator label
    type_df = df[(df["grouping_col"] == "type_group") & (df["significant"])].copy()
    if not type_df.empty:
        singles = {"BAL", "Bronchial Brush", "Oral Rinse"}
        type_df["label_class"] = np.where(type_df["index_label"].isin(singles), "single-site", "combination")
        type_df["count_one"] = 1
        order = (
            type_df.groupby("index_label")["ASV"]
            .count()
            .sort_values(ascending=False)
            .index
            .tolist()
        )

        plt.figure(figsize=(11, 5))
        ax = sns.barplot(
            data=type_df,
            x="index_label",
            y="count_one",
            estimator=np.sum,
            errorbar=None,
            hue="duleg_label",
            order=order,
            palette={"duleg=FALSE": "#4C78A8", "duleg=TRUE": "#F58518"},
        )
        ax.set_xlabel("Assigned indicator label (type_group)")
        ax.set_ylabel("Significant ASV count")
        ax.tick_params(axis="x", rotation=35)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), title="Duleg")
        plt.tight_layout()
        for ext in ("svg", "pdf"):
            plt.savefig(plot_dir / f"indicspecies_type_indicator_counts_by_label.{ext}", bbox_inches="tight")
        plt.close()

        plt.figure(figsize=(8, 5))
        ax2 = sns.barplot(
            data=type_df,
            x="label_class",
            y="count_one",
            estimator=np.sum,
            errorbar=None,
            hue="duleg_label",
            palette={"duleg=FALSE": "#4C78A8", "duleg=TRUE": "#F58518"},
        )
        ax2.set_xlabel("Indicator label class")
        ax2.set_ylabel("Significant ASV count")
        ax2.grid(axis="y", alpha=0.3)
        ax2.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), title="Duleg")
        plt.tight_layout()
        for ext in ("svg", "pdf"):
            plt.savefig(plot_dir / f"indicspecies_type_indicator_counts_single_vs_combo.{ext}", bbox_inches="tight")
        plt.close()


def load_power(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, sep="\t")
    for c in ("subgroup_col", "subgroup_level"):
        if c not in df.columns:
            df[c] = pd.NA
    if "duleg" in df.columns:
        df["duleg"] = df["duleg"].astype(str).str.upper().isin(["TRUE", "T", "1"])
    df["analysis_label"] = make_analysis_label(df)
    df["duleg_label"] = np.where(df["duleg"], "duleg=TRUE", "duleg=FALSE")
    df["scenario"] = df["scenario"].astype(str)
    if "n_size" in df.columns:
        df["n_size"] = pd.to_numeric(df["n_size"], errors="coerce")
    elif "n_cancer" in df.columns:
        df["n_size"] = pd.to_numeric(df["n_cancer"], errors="coerce")
    elif "n_patients" in df.columns:
        df["n_size"] = pd.to_numeric(df["n_patients"], errors="coerce")
    else:
        df["n_size"] = np.nan
    for c in ("power", "sensitivity", "fdr", "fdr_conditional"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def save_power_summaries(df: pd.DataFrame, outdir: Path, target_power: float) -> None:
    ensure_dir(outdir)
    df.to_csv(outdir / "power_all_summary.tsv", sep="\t", index=False)
    nonnull = df[~df["scenario"].isin(["Null", "Observed"])].copy()
    nonnull.to_csv(outdir / "power_nonnull_only.tsv", sep="\t", index=False)
    simple = df[df["scenario"].isin(["Null", "Observed"])].copy()
    simple.to_csv(outdir / "power_null_observed_only.tsv", sep="\t", index=False)

    def n_for_target(sub: pd.DataFrame) -> float:
        ok = sub[sub["power"] >= target_power]
        if ok.empty:
            return np.nan
        return float(ok["n_size"].min())

    def current_power_at_n(sub: pd.DataFrame, n: int = 8) -> float:
        """Get power at current sample size (default n=8)."""
        match = sub[sub["n_size"] == n]
        if match.empty:
            return np.nan
        return float(match["power"].iloc[0])

    def max_n_tested(sub: pd.DataFrame) -> float:
        """Get maximum sample size tested."""
        if sub["n_size"].isna().all():
            return np.nan
        return float(sub["n_size"].max())

    summary = (
        df.groupby(["analysis_label", "duleg", "scenario"], as_index=False)
        .apply(
            lambda x: pd.Series(
                {
                    "current_n": 8,
                    "current_power": current_power_at_n(x, 8),
                    "n_for_target_power": n_for_target(x),
                    "max_n_tested": max_n_tested(x),
                    "max_power": x["power"].max(),
                    "n_at_max_power": x.loc[x["power"].idxmax(), "n_size"] if not x["power"].isna().all() else np.nan,
                }
            )
        )
        .reset_index(drop=True)
    )
    summary.to_csv(outdir / "power_target_summary.tsv", sep="\t", index=False)

    # Create simple summary table matching PERMANOVA/Shannon format
    simple_summary_rows = []

    # Filter for Observed scenario only, status grouping (cancer vs control)
    df_observed = df[(df["scenario"] == "Observed") & (df["grouping_col"] == "status")].copy()

    if not df_observed.empty:
        # Process both duleg=FALSE and duleg=TRUE
        for duleg_val in [False, True]:
            df_duleg = df_observed[df_observed["duleg"] == duleg_val].copy()

            for analysis in df_duleg["analysis_label"].unique():
                df_subset = df_duleg[df_duleg["analysis_label"] == analysis].sort_values("n_size")

                # Extract Sample_Type from analysis_label (e.g., "status | BAL" -> "BAL")
                sample_type = analysis.split(" | ")[1] if " | " in analysis else "All"

                # Current power at n=8
                current = df_subset[df_subset["n_size"] == 8]
                current_power = float(current["power"].iloc[0]) if not current.empty else np.nan

                # n for 80% power
                ok = df_subset[df_subset["power"] >= target_power]
                n_for_80 = int(ok["n_size"].min()) if not ok.empty else None

                # Max n tested
                max_n = int(df_subset["n_size"].max())

                simple_summary_rows.append({
                    'Sample_Type': sample_type,
                    'Analysis': 'ISA',
                    'duleg': 'TRUE' if duleg_val else 'FALSE',
                    'Current_n': 8,
                    'Current_Power': f'{current_power:.3f}' if not np.isnan(current_power) else 'N/A',
                    'n_for_80%_Power': str(n_for_80) if n_for_80 else f'>{max_n}',
                    'Status': 'Adequate' if current_power >= target_power else 'Underpowered'
                })

    if simple_summary_rows:
        simple_df = pd.DataFrame(simple_summary_rows)
        simple_df.to_csv(outdir / "power_summary_simple.tsv", sep="\t", index=False)

        print("\n" + "="*60)
        print("Power Summary: ISA Cancer vs Control (Observed Effect)")
        print("="*60)
        print(simple_df.to_string(index=False))
        print(f"\n✓ Saved: power_summary_simple.tsv")


def plot_power(df: pd.DataFrame, outdir: Path, target_power: float) -> None:
    ensure_dir(outdir)
    plot_dir = outdir / "plots"
    ensure_dir(plot_dir)

    if df.empty:
        return

    def _plot_for_subset(sub: pd.DataFrame, suffix: str) -> None:
        if sub.empty:
            return

        g = sns.relplot(
            data=sub,
            x="n_size",
            y="power",
            hue="scenario",
            style="duleg_label",
            col="analysis_label",
            col_wrap=2,
            kind="line",
            marker="o",
            facet_kws={"sharey": True},
            height=3.2,
            aspect=1.3,
        )
        g.set_axis_labels("Sample size parameter", "Power")
        for ax in g.axes.flatten():
            ax.axhline(target_power, ls="--", lw=1, color="gray", alpha=0.6)
            ax.axhline(0.05, ls=":", lw=1, color="red", alpha=0.4)
            ax.grid(alpha=0.25)
            ax.set_ylim(-0.02, 1.02)
        for ext in ("svg", "pdf"):
            g.figure.savefig(plot_dir / f"indicspecies_power_curves_{suffix}.{ext}", bbox_inches="tight")
        plt.close(g.figure)

        # Simplified version: Null + Observed only (matches other power workflow style)
        simple = sub[sub["scenario"].isin(["Null", "Observed"])].copy()
        if not simple.empty:
            g_simple = sns.relplot(
                data=simple,
                x="n_size",
                y="power",
                hue="scenario",
                style="duleg_label",
                col="analysis_label",
                col_wrap=2,
                kind="line",
                marker="o",
                facet_kws={"sharey": True},
                height=3.2,
                aspect=1.3,
                palette={"Null": "#999999", "Observed": "#000000"},
            )
            g_simple.set_axis_labels("Sample size parameter", "Power")
            for ax in g_simple.axes.flatten():
                ax.axhline(target_power, ls="--", lw=1, color="gray", alpha=0.6)
                ax.axhline(0.05, ls=":", lw=1, color="red", alpha=0.4)
                ax.grid(alpha=0.25)
                ax.set_ylim(-0.02, 1.02)
            for ext in ("svg", "pdf"):
                g_simple.figure.savefig(plot_dir / f"indicspecies_power_curves_simple_{suffix}.{ext}", bbox_inches="tight")
            plt.close(g_simple.figure)

        nonnull = sub[~sub["scenario"].isin(["Null", "Observed"])].copy()
        if nonnull.empty:
            return

        g2 = sns.relplot(
            data=nonnull,
            x="n_size",
            y="sensitivity",
            hue="scenario",
            style="duleg_label",
            col="analysis_label",
            col_wrap=2,
            kind="line",
            marker="o",
            facet_kws={"sharey": True},
            height=3.2,
            aspect=1.3,
        )
        g2.set_axis_labels("Sample size parameter", "Sensitivity")
        for ax in g2.axes.flatten():
            ax.grid(alpha=0.25)
            ax.set_ylim(-0.02, 1.02)
        for ext in ("svg", "pdf"):
            g2.figure.savefig(plot_dir / f"indicspecies_sensitivity_curves_{suffix}.{ext}", bbox_inches="tight")
        plt.close(g2.figure)

        g3 = sns.relplot(
            data=nonnull,
            x="n_size",
            y="fdr",
            hue="scenario",
            style="duleg_label",
            col="analysis_label",
            col_wrap=2,
            kind="line",
            marker="o",
            facet_kws={"sharey": True},
            height=3.2,
            aspect=1.3,
        )
        g3.set_axis_labels("Sample size parameter", "Empirical FDR (diluted)")
        for ax in g3.axes.flatten():
            ax.axhline(0.05, ls=":", lw=1, color="red", alpha=0.4)
            ax.grid(alpha=0.25)
            ax.set_ylim(-0.02, 1.02)
        for ext in ("svg", "pdf"):
            g3.figure.savefig(plot_dir / f"indicspecies_fdr_curves_{suffix}.{ext}", bbox_inches="tight")
        plt.close(g3.figure)

        # Conditional FDR (only among simulations with discoveries)
        if "fdr_conditional" in nonnull.columns:
            g4 = sns.relplot(
                data=nonnull,
                x="n_size",
                y="fdr_conditional",
                hue="scenario",
                style="duleg_label",
                col="analysis_label",
                col_wrap=2,
                kind="line",
                marker="o",
                facet_kws={"sharey": True},
                height=3.2,
                aspect=1.3,
            )
            g4.set_axis_labels("Sample size parameter", "Conditional FDR (among discoveries)")
            for ax in g4.axes.flatten():
                ax.axhline(0.05, ls=":", lw=1, color="red", alpha=0.4)
                ax.grid(alpha=0.25)
                ax.set_ylim(-0.02, 1.02)
            for ext in ("svg", "pdf"):
                g4.figure.savefig(plot_dir / f"indicspecies_fdr_conditional_curves_{suffix}.{ext}", bbox_inches="tight")
            plt.close(g4.figure)

    if "grouping_col" in df.columns:
        for grp in ("status", "type_group"):
            sub = df[df["grouping_col"] == grp].copy()
            _plot_for_subset(sub, grp)
    else:
        _plot_for_subset(df, "all")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot and summarize aligned indicspecies + ISA power outputs.")
    p.add_argument("--indicspecies-dir", type=Path, default=Path("output/indicspecies"),
                   help="Directory with *_indicator_species*_summary.tsv files.")
    p.add_argument("--power-results", type=Path, default=Path("output/power_indicspecies/indicspecies_power_results.tsv"),
                   help="Path to indicspecies_power_results.tsv (optional).")
    p.add_argument("--outdir", type=Path, default=Path("output/indicspecies_plots"),
                   help="Output directory for plots and tables.")
    p.add_argument("--alpha", type=float, default=0.05,
                   help="q-value threshold for significance filtering.")
    p.add_argument("--min-stat", type=float, default=0.0,
                   help="Minimum indicator stat threshold for significance filtering.")
    p.add_argument("--top-n", type=int, default=25,
                   help="Top N significant indicators per analysis to save.")
    p.add_argument("--power-target", type=float, default=0.80,
                   help="Target power threshold for summary table.")
    p.add_argument("--also-legacy", action="store_true",
                   help="Also generate legacy-style ISA figures via plot_indicspecies.py.")
    p.add_argument("--legacy-script", type=Path, default=Path(__file__).resolve().parent.parent / "indicspecies_plots" / "plot_indicspecies.py",
                   help="Path to legacy plot_indicspecies.py script.")
    return p.parse_args()


def run_legacy_plots(indicspecies_dir: Path, outdir: Path, legacy_script: Path, alpha: float) -> None:
    if not legacy_script.exists():
        print(f"[WARN] Legacy script not found: {legacy_script}")
        return

    type_files = {
        "duleg_false": indicspecies_dir / "type_group_indicator_species_results.tsv",
        "duleg_true": indicspecies_dir / "type_group_indicator_species_DULEG_results.tsv",
    }

    status_files = sorted(indicspecies_dir.glob("status_*_indicator_species_results.tsv"))
    status_files_duleg = sorted(indicspecies_dir.glob("status_*_indicator_species_DULEG_results.tsv"))

    tasks = []
    for sf in status_files:
        site = sf.name.replace("status_", "").replace("_indicator_species_results.tsv", "")
        tasks.append(("duleg_false", site, sf))
    for sf in status_files_duleg:
        site = sf.name.replace("status_", "").replace("_indicator_species_DULEG_results.tsv", "")
        tasks.append(("duleg_true", site, sf))

    if not tasks:
        print(f"[WARN] No status ISA results found for legacy plotting in: {indicspecies_dir}")
        return

    legacy_root = outdir / "legacy_plots"
    ensure_dir(legacy_root)

    for mode, site, status_file in tasks:
        type_file = type_files.get(mode)
        if type_file is None or not type_file.exists():
            print(f"[WARN] Missing type_group results for {mode}: {type_file}")
            continue

        legacy_out = legacy_root / f"{site}_{mode}"
        ensure_dir(legacy_out)

        cmd = [
            sys.executable,
            str(legacy_script),
            "--type-results", str(type_file),
            "--status-results", str(status_file),
            "--outdir", str(legacy_out),
            "--p-thresh", str(alpha),
            "--stat-thresh", "0.0",
            "--type-index", LEGACY_TYPE_INDEX,
            "--status-index", LEGACY_STATUS_INDEX,
            "--group1-order", LEGACY_TYPE_ORDER,
            "--type-palette", LEGACY_TYPE_PALETTE,
            "--status-palette", LEGACY_STATUS_PALETTE,
        ]
        try:
            subprocess.run(cmd, check=True)
            print(f"[OK] Legacy plots: site={site}, mode={mode}, out={legacy_out}")
        except subprocess.CalledProcessError as e:
            print(f"[WARN] Legacy plotting failed for site={site}, mode={mode}: {e}")


def main() -> None:
    args = parse_args()
    ensure_dir(args.outdir)

    isa_df = load_indicspecies(args.indicspecies_dir, args.alpha, args.min_stat)
    if isa_df.empty:
        print(f"[WARN] No indicspecies summary files found in: {args.indicspecies_dir}")
    else:
        save_indicspecies_summaries(isa_df, args.outdir, args.top_n)
        plot_indicspecies(isa_df, args.outdir)
        print(f"[OK] ISA summaries/plots written to: {args.outdir}")

    power_df = load_power(args.power_results)
    if power_df.empty:
        print(f"[WARN] Power results not found or empty: {args.power_results}")
    else:
        save_power_summaries(power_df, args.outdir, args.power_target)
        plot_power(power_df, args.outdir, args.power_target)
        print(f"[OK] Power summaries/plots written to: {args.outdir}")

    if args.also_legacy:
        run_legacy_plots(
            indicspecies_dir=args.indicspecies_dir,
            outdir=args.outdir,
            legacy_script=args.legacy_script,
            alpha=args.alpha,
        )


if __name__ == "__main__":
    main()
