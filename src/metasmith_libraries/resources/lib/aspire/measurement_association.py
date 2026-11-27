#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
from pathlib import Path
from typing import Iterable

os.environ.setdefault("MPLCONFIGDIR", "/tmp/aspire_matplotlib")

import matplotlib as mpl
mpl.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import spearmanr
from statsmodels.stats.multitest import multipletests


sns.set_theme(style="white")
mpl.rcParams["savefig.dpi"] = 300
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42


DEFAULT_EXCLUDE = {
    "sampleid", "sample_id", "sample", "samplecode", "sample_code", "longid",
    "date", "color", "month_color", "month_marker", "index", "year", "month",
    "day", "cruise", "plateid", "asv_id", "feature id", "taxon",
}


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in re.split(r"[,|]", value) if x.strip()]


def read_table(path: str | Path, sep: str | None = None) -> pd.DataFrame:
    p = Path(path)
    if sep is None:
        sep = "\t" if p.suffix.lower() in {".tsv", ".tab", ".txt"} else ","
    return pd.read_csv(p, sep=sep, low_memory=False)


def normalize_join_token(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = re.sub(r"\.0$", "", text)
    return text.lower()


def build_key(df: pd.DataFrame, cols: list[str], out_col: str) -> pd.DataFrame:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing join columns {missing} in table with columns: {list(df.columns)}")
    out = df.copy()
    out[out_col] = out[cols].apply(lambda row: "|".join(normalize_join_token(v) for v in row), axis=1)
    return out


def merge_measurements(
    metadata: pd.DataFrame,
    table: pd.DataFrame | None,
    sample_col: str,
    measurement_sample_col: str,
    metadata_join_cols: list[str],
    measurement_join_cols: list[str],
) -> pd.DataFrame:
    if table is None:
        out = metadata.copy()
        out["__measurement_key__"] = out[sample_col].map(normalize_join_token)
        return out
    if metadata_join_cols or measurement_join_cols:
        if len(metadata_join_cols) != len(measurement_join_cols):
            raise ValueError("metadata_join_cols and measurement_join_cols must have the same length")
        left = build_key(metadata, metadata_join_cols, "__measurement_key__")
        right = build_key(table, measurement_join_cols, "__measurement_key__")
    else:
        if measurement_sample_col not in table.columns:
            raise ValueError(f"Measurement sample column not found: {measurement_sample_col}")
        left = metadata.copy()
        left["__measurement_key__"] = left[sample_col].map(normalize_join_token)
        right = table.copy()
        right["__measurement_key__"] = right[measurement_sample_col].map(normalize_join_token)
    right = right.drop_duplicates(subset=["__measurement_key__"], keep="first")
    return left.merge(right, on="__measurement_key__", how="left", suffixes=("", "_measurement"))


def load_counts(path: str | Path, asv_id_col: str) -> pd.DataFrame:
    counts = read_table(path, sep="\t")
    if asv_id_col in counts.columns:
        counts[asv_id_col] = counts[asv_id_col].astype(str).str.strip().str.split(";", n=1).str[0]
        counts = counts.drop_duplicates(subset=[asv_id_col]).set_index(asv_id_col)
    else:
        counts = counts.set_index(counts.columns[0])
        counts.index = counts.index.astype(str).str.strip().str.split(";", n=1).str[0]
        counts = counts[~counts.index.duplicated(keep="first")]
    return counts.apply(pd.to_numeric, errors="coerce").fillna(0.0)


def choose_measurement_columns(df: pd.DataFrame, requested: list[str], exclude: list[str]) -> list[str]:
    if requested:
        missing = [c for c in requested if c not in df.columns]
        if missing:
            print(f"[w] Requested measurement columns missing and skipped: {missing}")
        return [c for c in requested if c in df.columns]
    excluded = {x.lower() for x in exclude} | DEFAULT_EXCLUDE
    cols = []
    for col in df.columns:
        if col.lower() in excluded or col.startswith("__"):
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().sum() >= 3 and numeric.nunique(dropna=True) > 1:
            cols.append(col)
    return cols


def benjamini_hochberg(pvals: Iterable[float]) -> np.ndarray:
    vals = np.asarray(list(pvals), dtype=float)
    mask = np.isfinite(vals)
    out = np.full(vals.shape, np.nan, dtype=float)
    if mask.any():
        out[mask] = multipletests(vals[mask], method="fdr_bh")[1]
    return out


def correlation_tables(
    asv_samples: pd.DataFrame,
    measurements: pd.DataFrame,
    direction: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for asv in asv_samples.columns:
        x = pd.to_numeric(asv_samples[asv], errors="coerce")
        for measurement in measurements.columns:
            y = pd.to_numeric(measurements[measurement], errors="coerce")
            ok = x.notna() & y.notna()
            if ok.sum() < 3 or x[ok].nunique() < 2 or y[ok].nunique() < 2:
                rho, pval = np.nan, np.nan
            else:
                rho, pval = spearmanr(x[ok], y[ok])
            rows.append({"ASV_ID": asv, "measurement": measurement, "rho": rho, "p_value": pval, "n": int(ok.sum())})
    long_df = pd.DataFrame(rows)
    long_df["q_value"] = benjamini_hochberg(long_df["p_value"].to_numpy())
    if direction == "positive":
        long_df = long_df[long_df["rho"] > 0].copy()
    elif direction == "negative":
        long_df = long_df[long_df["rho"] < 0].copy()
    matrix = long_df.pivot(index="ASV_ID", columns="measurement", values="rho")
    return matrix, long_df.sort_values(["q_value", "rho"], ascending=[True, False])


def save_fig(fig: plt.Figure, out_base: Path, formats: list[str]) -> None:
    for fmt in formats:
        path = out_base.with_suffix("." + fmt)
        fig.savefig(path, bbox_inches="tight")


def save_clustermap(matrix: pd.DataFrame, out_base: Path, formats: list[str], title: str) -> None:
    matrix = matrix.dropna(how="all", axis=0).dropna(how="all", axis=1)
    if matrix.shape[0] < 2 or matrix.shape[1] < 2:
        (out_base.with_suffix(".skipped.txt")).write_text("Not enough rows/columns for clustermap\n")
        return
    plot_df = matrix.fillna(0.0)
    height = min(28, max(6, plot_df.shape[0] * 0.18 + 3))
    width = min(24, max(7, plot_df.shape[1] * 0.35 + 4))
    g = sns.clustermap(
        plot_df,
        cmap="vlag",
        center=0,
        metric="correlation",
        figsize=(width, height),
        linewidths=0,
        cbar_kws={"label": "Spearman rho"},
    )
    g.fig.suptitle(title, y=1.01)
    for fmt in formats:
        g.fig.savefig(out_base.with_suffix("." + fmt), bbox_inches="tight")
    plt.close(g.fig)


def scale_arrows(points: pd.DataFrame, arrows: pd.DataFrame, x_col: str, y_col: str) -> float:
    if points.empty or arrows.empty:
        return 1.0
    cloud = np.nanpercentile(np.sqrt(points[x_col] ** 2 + points[y_col] ** 2), 90)
    arrow = np.nanmax(np.sqrt(arrows[x_col] ** 2 + arrows[y_col] ** 2))
    if not np.isfinite(cloud) or not np.isfinite(arrow) or arrow == 0:
        return 1.0
    return 0.75 * cloud / arrow


def coerce_score_axes(df: pd.DataFrame, id_cols: set[str]) -> tuple[pd.DataFrame, str | None, str | None]:
    numeric_cols = [c for c in df.columns if c not in id_cols and pd.to_numeric(df[c], errors="coerce").notna().any()]
    if len(numeric_cols) < 2:
        return df, None, None
    x_col, y_col = numeric_cols[:2]
    out = df.copy()
    out[x_col] = pd.to_numeric(out[x_col], errors="coerce")
    out[y_col] = pd.to_numeric(out[y_col], errors="coerce")
    return out.dropna(subset=[x_col, y_col]), x_col, y_col


def parse_palette(spec: str | None) -> dict[str, str]:
    out = {}
    for item in parse_csv(spec):
        if "=" in item:
            key, val = item.split("=", 1)
            out[key.strip()] = val.strip()
    return out


def plot_ordination(
    method: str,
    tables_dir: Path,
    plots_dir: Path,
    metadata: pd.DataFrame,
    sample_col: str,
    group_col: str,
    palette: dict[str, str],
    top_vectors: int,
    formats: list[str],
) -> None:
    site_path = tables_dir / f"{method}_site_scores.tsv"
    meas_path = tables_dir / f"{method}_measurement_scores.tsv"
    asv_path = tables_dir / f"{method}_asv_scores.tsv"
    if not site_path.exists() or not meas_path.exists():
        return
    site = read_table(site_path, sep="\t")
    meas = read_table(meas_path, sep="\t")
    asv_scores = read_table(asv_path, sep="\t") if asv_path.exists() else pd.DataFrame()
    site, x_col, y_col = coerce_score_axes(site, {"sample_id"})
    if x_col is None or y_col is None:
        return
    if x_col not in meas.columns or y_col not in meas.columns:
        # Vegan can name biplot axes differently for some models; fall back by position.
        meas_numeric = [c for c in meas.columns if c != "measurement" and pd.to_numeric(meas[c], errors="coerce").notna().any()]
        if len(meas_numeric) < 2:
            return
        mx_col, my_col = meas_numeric[:2]
    else:
        mx_col, my_col = x_col, y_col
    if not asv_scores.empty:
        asv_scores, asv_x_col, asv_y_col = coerce_score_axes(asv_scores, {"ASV_ID"})
        if asv_x_col != x_col or asv_y_col != y_col:
            asv_scores = pd.DataFrame()
    meta = metadata.drop_duplicates(subset=[sample_col]).copy()
    site = site.merge(meta, left_on="sample_id", right_on=sample_col, how="left")
    meas[mx_col] = pd.to_numeric(meas[mx_col], errors="coerce")
    meas[my_col] = pd.to_numeric(meas[my_col], errors="coerce")
    meas["strength"] = np.sqrt(meas[mx_col].fillna(0) ** 2 + meas[my_col].fillna(0) ** 2)
    meas = meas.dropna(subset=[mx_col, my_col]).sort_values("strength", ascending=False).head(top_vectors)
    arrow_scale = scale_arrows(site, meas.rename(columns={mx_col: x_col, my_col: y_col}), x_col, y_col)

    fig, ax = plt.subplots(figsize=(10, 8))
    if not asv_scores.empty:
        ax.scatter(
            asv_scores[x_col],
            asv_scores[y_col],
            s=12,
            alpha=0.26,
            color="#5f6c72",
            marker=".",
            label="ASVs",
            zorder=1,
        )
    if group_col and group_col in site.columns:
        labels = [str(x) for x in site[group_col].fillna("NA")]
        missing = sorted({x for x in labels if x not in palette})
        if missing:
            auto = sns.color_palette("tab20", n_colors=max(3, len(missing))).as_hex()
            palette.update({lab: auto[i % len(auto)] for i, lab in enumerate(missing)})
        for label, sub in site.groupby(labels, sort=False):
            ax.scatter(sub[x_col], sub[y_col], s=34, alpha=0.78, color=palette.get(label, "0.55"), label=label, edgecolor="white", linewidth=0.35, zorder=2)
    else:
        ax.scatter(site[x_col], site[y_col], s=34, alpha=0.72, color="0.55", edgecolor="white", linewidth=0.35, label="Samples", zorder=2)

    for _, row in meas.iterrows():
        dx = float(row[mx_col]) * arrow_scale
        dy = float(row[my_col]) * arrow_scale
        ax.plot([0, dx], [0, dy], color="white", linewidth=3.2, zorder=3)
        ax.arrow(0, 0, dx, dy, color="#2f4858", linewidth=1.8, head_width=0.035, length_includes_head=True, zorder=4)
        ha = "left" if dx >= 0 else "right"
        va = "bottom" if dy >= 0 else "top"
        ax.text(dx * 1.04, dy * 1.04, str(row["measurement"]), ha=ha, va=va, fontsize=9, color="#2f4858")

    ax.axhline(0, color="0.82", linewidth=0.8, zorder=0)
    ax.axvline(0, color="0.82", linewidth=0.8, zorder=0)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    ax.set_title(f"{method.upper()} biplot")
    if group_col and group_col in site.columns:
        ax.legend(title=group_col, loc="upper left", bbox_to_anchor=(1.02, 1.0), frameon=False)
    save_fig(fig, plots_dir / f"{method}_biplot", formats)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(description="Associate ASV abundances with external sample measurements.")
    ap.add_argument("--asv-meta", required=True)
    ap.add_argument("--metadata", required=True)
    ap.add_argument("--asv-counts", required=True)
    ap.add_argument("--measurement-table")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--r-script", required=True)
    ap.add_argument("--sample-col", default="sampleID")
    ap.add_argument("--asv-id-col", default="ASV_ID")
    ap.add_argument("--measurement-sample-col", default="sampleID")
    ap.add_argument("--metadata-join-cols", default="")
    ap.add_argument("--measurement-join-cols", default="")
    ap.add_argument("--measurement-cols", default="")
    ap.add_argument("--exclude-cols", default="")
    ap.add_argument("--group-col", default="")
    ap.add_argument("--group-palette", default="")
    ap.add_argument("--max-asvs", type=int, default=300)
    ap.add_argument("--min-total", type=float, default=0.0)
    ap.add_argument("--min-prevalence", type=float, default=0.0)
    ap.add_argument("--top-correlations", type=int, default=100)
    ap.add_argument("--correlation-direction", choices=["positive", "negative", "both"], default="both")
    ap.add_argument("--ordination-methods", default="cca,rda,dbrda")
    ap.add_argument("--permutations", type=int, default=999)
    ap.add_argument("--top-vectors", type=int, default=12)
    ap.add_argument("--formats", default="pdf,png,svg")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    tables_dir = outdir / "tables"
    plots_dir = outdir / "plots"
    ord_dir = outdir / "ordination"
    tables_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)
    ord_dir.mkdir(parents=True, exist_ok=True)

    metadata = read_table(args.metadata, sep="\t")
    if args.sample_col not in metadata.columns:
        raise ValueError(f"Metadata sample column not found: {args.sample_col}")
    measurement_table = read_table(args.measurement_table) if args.measurement_table else None
    merged = merge_measurements(
        metadata,
        measurement_table,
        args.sample_col,
        args.measurement_sample_col,
        parse_csv(args.metadata_join_cols),
        parse_csv(args.measurement_join_cols),
    )
    measurement_cols = choose_measurement_columns(merged, parse_csv(args.measurement_cols), parse_csv(args.exclude_cols))
    if not measurement_cols:
        raise ValueError("No measurement columns selected")
    for col in measurement_cols:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged = merged.drop_duplicates(subset=[args.sample_col], keep="first")

    counts = load_counts(args.asv_counts, args.asv_id_col)
    common_samples = [s for s in counts.columns.astype(str) if s in set(merged[args.sample_col].astype(str))]
    if len(common_samples) < 3:
        raise ValueError(f"Need at least 3 overlapping samples; found {len(common_samples)}")
    counts = counts.loc[:, common_samples]
    totals = counts.sum(axis=1)
    prevalence = (counts > 0).mean(axis=1)
    counts = counts.loc[(totals >= args.min_total) & (prevalence >= args.min_prevalence)]
    if counts.empty:
        raise ValueError("No ASVs remain after abundance/prevalence filtering")
    if args.max_asvs > 0 and counts.shape[0] > args.max_asvs:
        rank = counts.var(axis=1).sort_values(ascending=False)
        counts = counts.loc[rank.head(args.max_asvs).index]

    meta_indexed = merged.set_index(args.sample_col).loc[common_samples].copy()
    measurement_matrix = meta_indexed[measurement_cols].copy()
    measurement_matrix = measurement_matrix.loc[:, measurement_matrix.notna().sum(axis=0) >= 3]
    measurement_matrix = measurement_matrix.loc[:, measurement_matrix.nunique(dropna=True) > 1]
    if measurement_matrix.empty:
        raise ValueError("No measurement columns remain after overlap filtering")
    for col in measurement_matrix.columns:
        measurement_matrix[col] = measurement_matrix[col].fillna(measurement_matrix[col].median())

    asv_samples = counts.transpose()
    asv_samples.index.name = args.sample_col
    measurement_matrix.index.name = args.sample_col
    asv_samples.to_csv(tables_dir / "asv_matrix.samples_by_asv.tsv", sep="\t")
    measurement_matrix.to_csv(tables_dir / "measurement_matrix.samples_by_measurement.tsv", sep="\t")
    meta_indexed.reset_index().to_csv(tables_dir / "measurement_metadata.tsv", sep="\t", index=False)
    pd.DataFrame({"measurement": measurement_matrix.columns}).to_csv(tables_dir / "measurement_columns.tsv", sep="\t", index=False)

    corr_matrix, corr_long = correlation_tables(asv_samples, measurement_matrix, args.correlation_direction)
    corr_matrix.to_csv(tables_dir / "asv_measurement_spearman.tsv", sep="\t")
    corr_long.to_csv(tables_dir / "asv_measurement_spearman_long.tsv", sep="\t", index=False)
    if not corr_long.empty:
        keep_asvs = corr_long.assign(abs_rho=lambda d: d["rho"].abs()).sort_values(["q_value", "abs_rho"], ascending=[True, False])["ASV_ID"].drop_duplicates().head(args.top_correlations)
        keep_measurements = corr_long[corr_long["ASV_ID"].isin(set(keep_asvs))]["measurement"].drop_duplicates()
        save_clustermap(
            corr_matrix.loc[corr_matrix.index.intersection(keep_asvs), corr_matrix.columns.intersection(keep_measurements)],
            plots_dir / "asv_measurement_spearman_clustermap",
            parse_csv(args.formats),
            "ASV-measurement Spearman correlations",
        )

    run_config = vars(args).copy()
    run_config["measurement_cols_selected"] = list(measurement_matrix.columns)
    run_config["n_samples"] = len(common_samples)
    run_config["n_asvs"] = int(counts.shape[0])
    (outdir / "run_config.json").write_text(json.dumps(run_config, indent=2))

    methods = [m for m in parse_csv(args.ordination_methods) if m in {"cca", "rda", "dbrda"}]
    if methods:
        cmd = [
            "Rscript",
            args.r_script,
            "--asv-matrix", str(tables_dir / "asv_matrix.samples_by_asv.tsv"),
            "--measurement-matrix", str(tables_dir / "measurement_matrix.samples_by_measurement.tsv"),
            "--metadata", str(tables_dir / "measurement_metadata.tsv"),
            "--outdir", str(ord_dir),
            "--methods", ",".join(methods),
            "--permutations", str(args.permutations),
        ]
        subprocess.run(cmd, check=True)
        ord_tables = ord_dir / "tables"
        palette = parse_palette(args.group_palette)
        for method in methods:
            plot_ordination(
                method,
                ord_tables,
                plots_dir,
                meta_indexed.reset_index(),
                args.sample_col,
                args.group_col,
                palette.copy(),
                args.top_vectors,
                parse_csv(args.formats),
            )


if __name__ == "__main__":
    main()
