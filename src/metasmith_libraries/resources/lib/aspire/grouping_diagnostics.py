#!/usr/bin/env python3
"""Compare categorical metadata groupings against ASV community structure."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.spatial.distance import pdist, squareform
from sklearn.metrics import balanced_accuracy_score, silhouette_score


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in re.split(r"[,|]", value) if item.strip()]


def parse_json_map(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_palette(value: str | dict | None) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items() if str(k)}
    if not value:
        return {}
    palette: dict[str, str] = {}
    for part in str(value).split(","):
        if "=" not in part:
            continue
        key, color = part.split("=", 1)
        key = key.strip()
        color = color.strip()
        if key and color:
            palette[key] = color
    return palette


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return cleaned.strip("_") or "group"


def read_table(path: Path) -> pd.DataFrame:
    sep = "\t" if path.suffix.lower() in {".tsv", ".txt"} else ","
    return pd.read_csv(path, sep=sep)


def read_asv_counts(path: Path, sample_col: str, metadata_samples: set[str]) -> pd.DataFrame:
    table = pd.read_csv(path, sep="\t", index_col=0)
    table.index = table.index.astype(str)
    table.columns = table.columns.astype(str)
    if sample_col in table.columns:
        table = table.set_index(sample_col)

    row_hits = len(set(table.index).intersection(metadata_samples))
    col_hits = len(set(table.columns).intersection(metadata_samples))
    if col_hits >= row_hits:
        table = table.T
    table.index = table.index.astype(str)
    table = table.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    table = table.loc[:, table.sum(axis=0) > 0]
    return table


def transform_counts(counts: pd.DataFrame, transform: str) -> pd.DataFrame:
    transform = transform.lower()
    if transform in {"relative", "rel"}:
        totals = counts.sum(axis=1).replace(0, np.nan)
        return counts.div(totals, axis=0).fillna(0.0)
    if transform in {"sqrt_relative", "sqrt-rel", "hellinger"}:
        totals = counts.sum(axis=1).replace(0, np.nan)
        return np.sqrt(counts.div(totals, axis=0).fillna(0.0))
    if transform == "log1p":
        return np.log1p(counts)
    return counts.copy()


def distance_matrix(values: pd.DataFrame, metric: str) -> np.ndarray:
    metric = metric.lower()
    if metric == "bray":
        metric = "braycurtis"
    if metric == "jaccard":
        values = values.gt(0).astype(int)
    distances = pdist(values.to_numpy(dtype=float), metric=metric)
    matrix = squareform(distances)
    matrix[~np.isfinite(matrix)] = 0.0
    return matrix


def pcoa(distance: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    n = distance.shape[0]
    if n == 0:
        return np.empty((0, 2)), np.array([0.0, 0.0])
    centered = -0.5 * (distance ** 2)
    eye = np.eye(n)
    ones = np.ones((n, n)) / n
    centered = (eye - ones) @ centered @ (eye - ones)
    eigenvalues, eigenvectors = np.linalg.eigh(centered)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = eigenvectors[:, order]
    positive = np.maximum(eigenvalues[:2], 0.0)
    coords = eigenvectors[:, :2] * np.sqrt(positive)
    if coords.shape[1] < 2:
        coords = np.pad(coords, ((0, 0), (0, 2 - coords.shape[1])))
    total = eigenvalues[eigenvalues > 0].sum()
    variance = positive / total if total > 0 else np.array([0.0, 0.0])
    return coords[:, :2], variance[:2]


def permanova(distance: np.ndarray, labels: pd.Series, permutations: int, seed: int) -> dict[str, float | str]:
    labels = labels.astype(str).to_numpy()
    groups = pd.unique(labels)
    n = len(labels)
    g = len(groups)
    if n < 3 or g < 2 or g >= n:
        return {"pseudo_f": np.nan, "r2": np.nan, "p_value": np.nan, "permutations": 0, "status": "skipped_insufficient_groups"}

    d2 = distance ** 2
    sst = d2.sum() / n
    ssw = 0.0
    for group in groups:
        idx = np.where(labels == group)[0]
        if len(idx) < 1:
            continue
        sub = d2[np.ix_(idx, idx)]
        ssw += sub.sum() / len(idx)
    ssb = sst - ssw
    dfb = g - 1
    dfw = n - g
    if dfw <= 0 or ssw <= 0:
        pseudo_f = np.inf if ssb > 0 else np.nan
    else:
        pseudo_f = (ssb / dfb) / (ssw / dfw)
    r2 = ssb / sst if sst > 0 else np.nan

    if not np.isfinite(pseudo_f) or permutations <= 0:
        return {"pseudo_f": pseudo_f, "r2": r2, "p_value": np.nan, "permutations": 0, "status": "ok"}

    rng = np.random.default_rng(seed)
    hits = 0
    for _ in range(permutations):
        shuffled = rng.permutation(labels)
        perm = permanova(distance, pd.Series(shuffled), 0, seed)
        if np.isfinite(perm["pseudo_f"]) and perm["pseudo_f"] >= pseudo_f:
            hits += 1
    p_value = (hits + 1) / (permutations + 1)
    return {"pseudo_f": pseudo_f, "r2": r2, "p_value": p_value, "permutations": permutations, "status": "ok"}


def subset_distance(distance: np.ndarray, indices: np.ndarray) -> np.ndarray:
    return distance[np.ix_(indices, indices)]


def within_between(distance: np.ndarray, labels: pd.Series) -> dict[str, float]:
    labels_arr = labels.astype(str).to_numpy()
    within: list[float] = []
    between: list[float] = []
    for i in range(len(labels_arr)):
        for j in range(i + 1, len(labels_arr)):
            if labels_arr[i] == labels_arr[j]:
                within.append(distance[i, j])
            else:
                between.append(distance[i, j])
    within_median = float(np.median(within)) if within else np.nan
    between_median = float(np.median(between)) if between else np.nan
    ratio = within_median / between_median if between_median and np.isfinite(between_median) else np.nan
    return {
        "within_median": within_median,
        "between_median": between_median,
        "within_between_ratio": ratio,
        "within_pairs": len(within),
        "between_pairs": len(between),
    }


def soft_label_missing(
    distance: np.ndarray,
    metadata: pd.DataFrame,
    group_col: str,
    k: int,
    excluded_labels: set[str] | None = None,
    min_class_samples: int = 1,
) -> pd.DataFrame:
    if group_col not in metadata.columns:
        return pd.DataFrame()
    labels = metadata[group_col].where(metadata[group_col].notna(), "").astype(str)
    missing_mask = labels.isin(["", "nan", "None", "NA", "NaN"])
    excluded = {str(label).strip() for label in (excluded_labels or set())}
    class_counts = labels[~missing_mask & ~labels.isin(excluded)].value_counts()
    eligible_labels = set(class_counts[class_counts >= max(1, min_class_samples)].index)
    labeled_mask = ~missing_mask & labels.isin(eligible_labels)
    if missing_mask.sum() == 0 or labeled_mask.sum() == 0:
        return pd.DataFrame()

    labeled_idx = np.where(labeled_mask.to_numpy())[0]
    rows: list[dict] = []
    for pos in np.where(missing_mask.to_numpy())[0]:
        d = distance[pos, labeled_idx]
        finite = np.isfinite(d)
        if not finite.any():
            continue
        candidates = labeled_idx[finite]
        d = d[finite]
        order = np.argsort(d)[: max(1, min(k, len(d)))]
        neighbor_idx = candidates[order]
        neighbor_dist = d[order]
        weights = 1.0 / (neighbor_dist + 1e-9)
        votes: dict[str, float] = {}
        counts: dict[str, int] = {}
        for idx, weight in zip(neighbor_idx, weights):
            label = labels.iloc[idx]
            votes[label] = votes.get(label, 0.0) + float(weight)
            counts[label] = counts.get(label, 0) + 1
        total = sum(votes.values())
        if total <= 0:
            continue
        ranked = sorted(votes.items(), key=lambda item: item[1], reverse=True)
        assigned, score = ranked[0]
        runner_up = ranked[1][0] if len(ranked) > 1 else ""
        rows.append(
            {
                "sample": metadata.index[pos],
                "group_col": group_col,
                "assigned_label": assigned,
                "confidence": score / total,
                "runner_up_label": runner_up,
                "nearest_distance": float(neighbor_dist[0]),
                "neighbor_count": int(len(neighbor_idx)),
                "assigned_neighbor_count": int(counts.get(assigned, 0)),
                "neighbor_agreement": float(counts.get(assigned, 0) / len(neighbor_idx)),
                "method": f"inverse_distance_{k}nn",
            }
        )
    return pd.DataFrame(rows)


def validate_soft_labels(
    distance: np.ndarray,
    metadata: pd.DataFrame,
    group_col: str,
    k: int,
    excluded_labels: set[str] | None = None,
    min_class_samples: int = 1,
    distance_quantile: float = 0.95,
) -> tuple[pd.DataFrame, dict]:
    if group_col not in metadata.columns:
        return pd.DataFrame(), {"group_col": group_col, "status": "missing_column"}

    excluded = {str(label).strip() for label in (excluded_labels or set())}
    labels = metadata[group_col].where(metadata[group_col].notna(), "").astype(str)
    labels = labels.replace({"nan": "", "None": "", "NA": "", "NaN": ""})
    class_counts = labels[~labels.isin(excluded | {""})].value_counts()
    eligible_labels = set(class_counts[class_counts >= max(2, min_class_samples)].index)
    eligible_idx = np.where(labels.isin(eligible_labels).to_numpy())[0]
    rows: list[dict] = []

    for pos in eligible_idx:
        candidates = eligible_idx[eligible_idx != pos]
        if len(candidates) == 0:
            continue
        d = distance[pos, candidates]
        finite = np.isfinite(d)
        candidates = candidates[finite]
        d = d[finite]
        if len(candidates) == 0:
            continue
        order = np.argsort(d)[: max(1, min(k, len(d)))]
        neighbor_idx = candidates[order]
        neighbor_dist = d[order]
        weights = 1.0 / (neighbor_dist + 1e-9)
        votes: dict[str, float] = {}
        counts: dict[str, int] = {}
        for idx, weight in zip(neighbor_idx, weights):
            label = labels.iloc[idx]
            votes[label] = votes.get(label, 0.0) + float(weight)
            counts[label] = counts.get(label, 0) + 1
        ranked = sorted(votes.items(), key=lambda item: item[1], reverse=True)
        predicted = ranked[0][0]
        total = sum(votes.values())
        rows.append(
            {
                "sample": metadata.index[pos],
                "group_col": group_col,
                "observed_label": labels.iloc[pos],
                "predicted_label": predicted,
                "correct": bool(predicted == labels.iloc[pos]),
                "confidence": float(ranked[0][1] / total),
                "nearest_distance": float(neighbor_dist[0]),
                "neighbor_count": int(len(neighbor_idx)),
                "assigned_neighbor_count": int(counts.get(predicted, 0)),
                "neighbor_agreement": float(counts.get(predicted, 0) / len(neighbor_idx)),
            }
        )

    validation = pd.DataFrame(rows)
    if validation.empty:
        return validation, {
            "group_col": group_col,
            "status": "skipped_insufficient_training_data",
            "n_validation_samples": 0,
            "n_eligible_classes": len(eligible_labels),
        }

    threshold = float(validation["nearest_distance"].quantile(distance_quantile))
    return validation, {
        "group_col": group_col,
        "status": "ok",
        "n_validation_samples": len(validation),
        "n_eligible_classes": len(eligible_labels),
        "accuracy": float(validation["correct"].mean()),
        "balanced_accuracy": float(
            balanced_accuracy_score(validation["observed_label"], validation["predicted_label"])
        ),
        "nearest_distance_threshold": threshold,
        "distance_quantile": distance_quantile,
    }


def run_grouping_power(
    distance: np.ndarray,
    metadata: pd.DataFrame,
    group_cols: list[str],
    sample_sizes: list[int],
    simulations: int,
    permutations: int,
    alpha: float,
    min_groups: int,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows: list[dict] = []
    for group_col in group_cols:
        if group_col not in metadata.columns:
            continue
        labels = metadata[group_col].where(metadata[group_col].notna(), "").astype(str)
        labels = labels.replace({"nan": "", "None": "", "NA": "", "NaN": ""})
        valid_idx = np.where(labels.ne("").to_numpy())[0]
        if len(valid_idx) < 3:
            continue
        label_values = labels.iloc[valid_idx]
        groups = sorted(label_values.unique())
        group_to_idx = {group: valid_idx[np.where(label_values.to_numpy() == group)[0]] for group in groups}
        for n_per_group in sample_sizes:
            eligible = {group: idx for group, idx in group_to_idx.items() if len(idx) >= n_per_group}
            if len(eligible) < min_groups:
                rows.append(
                    {
                        "group_col": group_col,
                        "n_per_group": n_per_group,
                        "eligible_groups": len(eligible),
                        "simulations": 0,
                        "power": np.nan,
                        "median_r2": np.nan,
                        "status": "skipped_insufficient_groups",
                    }
                )
                continue
            p_values: list[float] = []
            r2_values: list[float] = []
            group_names = list(eligible)
            for sim in range(simulations):
                selected: list[int] = []
                sampled_labels: list[str] = []
                for group in group_names:
                    choice = rng.choice(eligible[group], size=n_per_group, replace=False)
                    selected.extend(choice.tolist())
                    sampled_labels.extend([group] * n_per_group)
                selected_arr = np.array(selected, dtype=int)
                dist_sub = subset_distance(distance, selected_arr)
                stats = permanova(dist_sub, pd.Series(sampled_labels), permutations, seed + sim + n_per_group)
                if np.isfinite(stats["p_value"]):
                    p_values.append(float(stats["p_value"]))
                if np.isfinite(stats["r2"]):
                    r2_values.append(float(stats["r2"]))
            rows.append(
                {
                    "group_col": group_col,
                    "n_per_group": n_per_group,
                    "eligible_groups": len(eligible),
                    "simulations": len(p_values),
                    "power": float(np.mean(np.array(p_values) < alpha)) if p_values else np.nan,
                    "median_r2": float(np.median(r2_values)) if r2_values else np.nan,
                    "alpha": alpha,
                    "permutations": permutations,
                    "status": "ok" if p_values else "skipped_no_valid_simulations",
                }
            )
    return pd.DataFrame(rows)


def grouping_order(labels: pd.Series, configured: list[str]) -> list[str]:
    values = [str(v) for v in labels.dropna().unique()]
    ordered = [str(v) for v in configured if str(v) in values]
    ordered.extend(sorted(v for v in values if v not in set(ordered)))
    return ordered


def color_for_groups(order: list[str], palette: dict[str, str]) -> list[str]:
    fallback = sns.color_palette("tab20", max(len(order), 1)).as_hex()
    return [palette.get(str(group), fallback[i % len(fallback)]) for i, group in enumerate(order)]


def save_fig(fig: plt.Figure, outbase: Path, formats: list[str]) -> None:
    outbase.parent.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        fig.savefig(outbase.with_suffix(f".{fmt}"), dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_metric_summary(metrics: pd.DataFrame, outdir: Path, formats: list[str]) -> None:
    ok = metrics[metrics["status"].eq("ok")].copy()
    if ok.empty:
        return
    melted = ok.melt(
        id_vars=["group_col", "metric"],
        value_vars=["r2", "silhouette", "within_between_ratio"],
        var_name="score",
        value_name="value",
    ).dropna()
    if melted.empty:
        return
    fig, axes = plt.subplots(1, 3, figsize=(13, max(3.5, 0.4 * ok["group_col"].nunique())), constrained_layout=True)
    score_titles = {
        "r2": "PERMANOVA R2",
        "silhouette": "Silhouette",
        "within_between_ratio": "Within / Between Distance",
    }
    for ax, score in zip(axes, score_titles):
        sub = melted[melted["score"].eq(score)]
        sns.barplot(data=sub, y="group_col", x="value", hue="metric", ax=ax)
        ax.set_title(score_titles[score])
        ax.set_ylabel("")
        ax.set_xlabel("")
        if score == "within_between_ratio":
            ax.axvline(1.0, color="0.35", linestyle="--", linewidth=1)
        if ax.legend_:
            ax.legend(title="Distance", loc="upper left", bbox_to_anchor=(1.02, 1.0), frameon=False)
    save_fig(fig, outdir / "plots" / "grouping_metric_summary", formats)


def plot_power(power: pd.DataFrame, outdir: Path, formats: list[str]) -> None:
    ok = power[power["status"].eq("ok")].copy() if not power.empty else pd.DataFrame()
    if ok.empty:
        return
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), constrained_layout=True)
    sns.lineplot(data=ok, x="n_per_group", y="power", hue="group_col", marker="o", ax=axes[0])
    axes[0].set_ylim(-0.02, 1.02)
    axes[0].set_xlabel("Samples per group")
    axes[0].set_ylabel("Estimated power")
    axes[0].axhline(0.8, color="0.4", linestyle="--", linewidth=1)
    axes[0].legend(title="Grouping", loc="upper left", bbox_to_anchor=(1.02, 1.0), frameon=False)

    sns.lineplot(data=ok, x="n_per_group", y="median_r2", hue="group_col", marker="o", ax=axes[1])
    axes[1].set_xlabel("Samples per group")
    axes[1].set_ylabel("Median PERMANOVA R2")
    axes[1].legend(title="Grouping", loc="upper left", bbox_to_anchor=(1.02, 1.0), frameon=False)
    save_fig(fig, outdir / "plots" / "grouping_power", formats)


def plot_crosswalk(meta: pd.DataFrame, baseline: str, primary: str, orders: dict[str, list[str]], outdir: Path, formats: list[str]) -> None:
    if not baseline or not primary or baseline not in meta.columns or primary not in meta.columns:
        return
    left = meta[baseline].where(meta[baseline].notna(), "Missing").astype(str).replace({"nan": "Missing", "": "Missing"})
    right = meta[primary].where(meta[primary].notna(), "Missing").astype(str).replace({"nan": "Missing", "": "Missing"})
    cross = pd.crosstab(left, right)
    row_order = [v for v in orders.get(baseline, []) if str(v) in cross.index]
    col_order = [v for v in orders.get(primary, []) if str(v) in cross.columns]
    row_order = [str(v) for v in row_order] + [v for v in cross.index if v not in set(map(str, row_order))]
    col_order = [str(v) for v in col_order] + [v for v in cross.columns if v not in set(map(str, col_order))]
    cross = cross.reindex(index=row_order, columns=col_order, fill_value=0)
    table_path = outdir / "tables" / f"grouping_crosswalk_{safe_name(baseline)}_vs_{safe_name(primary)}.tsv"
    table_path.parent.mkdir(parents=True, exist_ok=True)
    cross.to_csv(table_path, sep="\t")

    width = max(6, 0.55 * len(cross.columns) + 3)
    height = max(4, 0.35 * len(cross.index) + 2)
    fig, ax = plt.subplots(figsize=(width, height))
    sns.heatmap(cross, cmap="viridis", annot=True, fmt="d", linewidths=0.5, linecolor="white", ax=ax)
    ax.set_xlabel(primary)
    ax.set_ylabel(baseline)
    ax.set_title(f"{baseline} vs {primary}")
    save_fig(fig, outdir / "plots" / f"grouping_crosswalk_{safe_name(baseline)}_vs_{safe_name(primary)}", formats)


def plot_ordination(
    coords: pd.DataFrame,
    variance: np.ndarray,
    group_cols: list[str],
    palettes: dict[str, dict[str, str]],
    orders: dict[str, list[str]],
    metric: str,
    outdir: Path,
    formats: list[str],
) -> None:
    if coords.empty:
        return
    cols = min(2, len(group_cols))
    rows = math.ceil(len(group_cols) / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(6.5 * cols, 5.2 * rows), squeeze=False, constrained_layout=True)
    for ax, group_col in zip(axes.flat, group_cols):
        order = grouping_order(coords[group_col], orders.get(group_col, []))
        palette = color_for_groups(order, palettes.get(group_col, {}))
        for group, color in zip(order, palette):
            sub = coords[coords[group_col].astype(str).eq(str(group))]
            ax.scatter(sub["PCo1"], sub["PCo2"], s=34, alpha=0.82, label=str(group), color=color, edgecolor="white", linewidth=0.4)
        ax.set_title(group_col)
        ax.set_xlabel(f"PCo1 ({variance[0] * 100:.1f}%)")
        ax.set_ylabel(f"PCo2 ({variance[1] * 100:.1f}%)")
        ax.axhline(0, color="0.85", linewidth=0.8)
        ax.axvline(0, color="0.85", linewidth=0.8)
        ax.legend(
            title=group_col,
            frameon=False,
            fontsize=8,
            title_fontsize=9,
            loc="upper left",
            bbox_to_anchor=(1.02, 1.0),
        )
    for ax in axes.flat[len(group_cols):]:
        ax.axis("off")
    save_fig(fig, outdir / "plots" / f"grouping_ordination_{safe_name(metric)}", formats)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--asv-counts", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--sample-col", default="sampleid")
    parser.add_argument("--group-cols", required=True)
    parser.add_argument("--baseline-group", default="")
    parser.add_argument("--primary-group", default="")
    parser.add_argument("--group-palettes-json", default="{}")
    parser.add_argument("--group-orders-json", default="{}")
    parser.add_argument("--metrics", default="bray")
    parser.add_argument("--transform", default="relative")
    parser.add_argument("--permutations", type=int, default=999)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--formats", default="pdf,png,svg")
    parser.add_argument("--soft-label-missing", action="store_true")
    parser.add_argument("--soft-label-k", type=int, default=7)
    parser.add_argument("--soft-label-group-cols", default="")
    parser.add_argument("--soft-label-exclude-labels", default="outlier")
    parser.add_argument("--soft-label-min-class-samples", type=int, default=3)
    parser.add_argument("--soft-label-distance-quantile", type=float, default=0.95)
    parser.add_argument("--power-enabled", action="store_true")
    parser.add_argument("--power-sample-sizes", default="3,5,10,15,20")
    parser.add_argument("--power-simulations", type=int, default=100)
    parser.add_argument("--power-permutations", type=int, default=99)
    parser.add_argument("--power-alpha", type=float, default=0.05)
    parser.add_argument("--power-min-groups", type=int, default=2)
    args = parser.parse_args()

    outdir = Path(args.outdir)
    (outdir / "tables").mkdir(parents=True, exist_ok=True)
    (outdir / "plots").mkdir(parents=True, exist_ok=True)

    metadata = read_table(Path(args.metadata))
    if args.sample_col not in metadata.columns:
        raise SystemExit(f"Metadata sample column not found: {args.sample_col}")
    metadata[args.sample_col] = metadata[args.sample_col].astype(str)
    metadata = metadata.drop_duplicates(subset=[args.sample_col]).set_index(args.sample_col, drop=False)

    counts = read_asv_counts(Path(args.asv_counts), args.sample_col, set(metadata.index))
    common = [sample for sample in metadata.index if sample in counts.index]
    if not common:
        raise SystemExit("No shared samples between metadata and ASV count table.")
    metadata = metadata.loc[common].copy()
    counts = counts.loc[common].copy()
    values = transform_counts(counts, args.transform)

    group_cols = [col for col in parse_csv(args.group_cols) if col]
    palette_map_raw = parse_json_map(args.group_palettes_json)
    order_map_raw = parse_json_map(args.group_orders_json)
    palettes = {str(col): parse_palette(palette) for col, palette in palette_map_raw.items()}
    orders = {
        str(col): [str(item) for item in value]
        for col, value in order_map_raw.items()
        if isinstance(value, list)
    }
    formats = parse_csv(args.formats) or ["pdf", "png"]
    metrics = parse_csv(args.metrics) or ["bray"]

    summary_rows: list[dict] = []
    distance_cache: dict[str, np.ndarray] = {}
    for metric in metrics:
        distance = distance_matrix(values, metric)
        distance_cache[metric] = distance
        coords, variance = pcoa(distance)
        ordination = pd.DataFrame(coords, index=metadata.index, columns=["PCo1", "PCo2"])
        valid_for_plot: list[str] = []

        for group_col in group_cols:
            if group_col not in metadata.columns:
                summary_rows.append({"group_col": group_col, "metric": metric, "status": "missing_column"})
                continue
            labels = metadata[group_col].astype(str).replace({"nan": np.nan, "": np.nan})
            valid = labels.notna()
            labels = labels[valid]
            dist_sub = distance[np.ix_(np.where(valid)[0], np.where(valid)[0])]
            n_groups = labels.nunique()
            row = {
                "group_col": group_col,
                "metric": metric,
                "n_samples": int(valid.sum()),
                "n_groups": int(n_groups),
            }
            if valid.sum() < 3 or n_groups < 2 or n_groups >= valid.sum():
                row.update({"status": "skipped_insufficient_groups"})
                summary_rows.append(row)
                continue
            stats = permanova(dist_sub, labels, args.permutations, args.random_state)
            wb = within_between(dist_sub, labels)
            try:
                sil = silhouette_score(dist_sub, labels.astype(str), metric="precomputed")
            except ValueError:
                sil = np.nan
            row.update(stats)
            row.update(wb)
            row["silhouette"] = sil
            summary_rows.append(row)
            ordination[group_col] = (
                metadata[group_col]
                .where(metadata[group_col].notna(), "Missing")
                .astype(str)
                .replace({"nan": "Missing", "": "Missing"})
            )
            valid_for_plot.append(group_col)

        if valid_for_plot:
            plot_ordination(ordination, variance, valid_for_plot, palettes, orders, metric, outdir, formats)

        if metric == metrics[0] and args.soft_label_missing:
            soft_group_cols = parse_csv(args.soft_label_group_cols) or group_cols
            excluded_soft_labels = set(parse_csv(args.soft_label_exclude_labels))
            soft_rows = [
                soft_label_missing(
                    distance,
                    metadata,
                    group_col,
                    args.soft_label_k,
                    excluded_soft_labels,
                    args.soft_label_min_class_samples,
                )
                for group_col in soft_group_cols
            ]
            soft_nonempty = [df for df in soft_rows if not df.empty]
            soft = pd.concat(soft_nonempty, ignore_index=True) if soft_nonempty else pd.DataFrame()
            assignment_columns = [
                "sample", "group_col", "assigned_label", "confidence", "runner_up_label",
                "nearest_distance", "neighbor_count", "assigned_neighbor_count",
                "neighbor_agreement", "method",
            ]
            soft.reindex(columns=assignment_columns).to_csv(
                outdir / "tables" / "grouping_soft_label_assignments.tsv", sep="\t", index=False
            )

            validation_rows: list[pd.DataFrame] = []
            validation_summaries: list[dict] = []
            for group_col in soft_group_cols:
                validation, validation_summary = validate_soft_labels(
                    distance,
                    metadata,
                    group_col,
                    args.soft_label_k,
                    excluded_soft_labels,
                    args.soft_label_min_class_samples,
                    args.soft_label_distance_quantile,
                )
                if not validation.empty:
                    validation_rows.append(validation)
                validation_summaries.append(validation_summary)
            validation_columns = [
                "sample", "group_col", "observed_label", "predicted_label", "correct",
                "confidence", "nearest_distance", "neighbor_count", "assigned_neighbor_count",
                "neighbor_agreement",
            ]
            validation_all = pd.concat(validation_rows, ignore_index=True) if validation_rows else pd.DataFrame()
            validation_all.reindex(columns=validation_columns).to_csv(
                outdir / "tables" / "grouping_soft_label_validation.tsv", sep="\t", index=False
            )
            pd.DataFrame(validation_summaries).to_csv(
                outdir / "tables" / "grouping_soft_label_validation_summary.tsv", sep="\t", index=False
            )

        if metric == metrics[0] and args.power_enabled:
            power_sizes = [int(item) for item in parse_csv(args.power_sample_sizes) if item.isdigit()]
            if not power_sizes:
                power_sizes = [3, 5, 10, 15, 20]
            power = run_grouping_power(
                distance=distance,
                metadata=metadata,
                group_cols=group_cols,
                sample_sizes=power_sizes,
                simulations=args.power_simulations,
                permutations=args.power_permutations,
                alpha=args.power_alpha,
                min_groups=args.power_min_groups,
                seed=args.random_state,
            )
            if not power.empty:
                power.to_csv(outdir / "tables" / "grouping_power.tsv", sep="\t", index=False)
                plot_power(power, outdir, formats)

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(outdir / "tables" / "grouping_diagnostics_summary.tsv", sep="\t", index=False)
    if not summary.empty:
        metric_cols = [
            "group_col", "metric", "n_samples", "n_groups", "pseudo_f", "r2", "p_value",
            "permutations", "status",
        ]
        summary[[col for col in metric_cols if col in summary.columns]].to_csv(
            outdir / "tables" / "grouping_permanova.tsv", sep="\t", index=False
        )
        distance_cols = [
            "group_col", "metric", "within_median", "between_median",
            "within_between_ratio", "within_pairs", "between_pairs", "status",
        ]
        summary[[col for col in distance_cols if col in summary.columns]].to_csv(
            outdir / "tables" / "grouping_pairwise_distance_summary.tsv", sep="\t", index=False
        )
        silhouette_cols = ["group_col", "metric", "silhouette", "n_samples", "n_groups", "status"]
        summary[[col for col in silhouette_cols if col in summary.columns]].to_csv(
            outdir / "tables" / "grouping_silhouette.tsv", sep="\t", index=False
        )
        plot_metric_summary(summary, outdir, formats)

    plot_crosswalk(metadata, args.baseline_group, args.primary_group, orders, outdir, formats)
    print(f"[i] Grouping diagnostics wrote results for {len(common)} samples to {outdir}")


if __name__ == "__main__":
    main()
