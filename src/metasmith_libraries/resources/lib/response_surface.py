"""
Response surface analysis for media optimization.

Combined from growth_analysis_helpers.py and dse.growth_panel.py.
Accepts CLI args: python response_surface.py <input.csv> <output_dir>
"""

import sys
import os
import re
import argparse
from pathlib import Path
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit, minimize
from sklearn.preprocessing import PolynomialFeatures
from sklearn import linear_model
from sklearn.cluster import KMeans
from scipy.spatial.distance import cosine

from local.figures.template import BaseFigure, ApplyTemplate, go
from local.figures.colors import Color, COLORS, Palettes


# ── Configuration ────────────────────────────────────────────────────────────

DEFAULT_DEGREE = 3
DEFAULT_CRASH_THRESHOLD = 0.05
DEGREE = DEFAULT_DEGREE
OUTPUT_DIR = "./output"


# ── Data structures ──────────────────────────────────────────────────────────

@dataclass
class Run:
    media: np.ndarray  # 1D
    growth: np.ndarray  # 3D: timepoints x (time, growth) x replicates


# ── Logistic growth model ────────────────────────────────────────────────────

def logistic_growth(x, b, L, k, x0):
    """
    Logistic growth model.

    Args:
        x: time (array)
        b: y offset
        L: maximum value (carrying capacity)
        k: growth rate, where doubling time = ln(2)/k
        x0: inflection point
    """
    z = k * (x - x0)
    growth = L * np.exp(-np.logaddexp(0, -z))
    return growth + b


def compute_logistic_auc(params: tuple, t_min: float, t_max: float, n_points: int = 100) -> float:
    """Compute AUC from fitted logistic curve parameters."""
    b, L, k, x0 = params
    t = np.linspace(t_min, t_max, n_points)
    od = logistic_growth(t, b, L, k, x0)
    auc = np.trapz(od, t)
    time_span = t_max - t_min
    return auc / time_span if time_span > 0 else 0.0


def fit_logistic_curve(x, y):
    """Fit logistic curve to growth data."""
    bounds = [
        (-0.5, 0, 0, 0),
        (1, 15, 2, np.inf)
    ]

    L_guess = max(y) * 1.5
    x0_guess = x[np.argmax(y - np.median(y))]
    k_guess = (y[-1] - y[0]) / (x[-1] - x[0]) / L_guess * 4
    initial_guess = [
        float(np.clip(v, bounds[0][i], bounds[1][i]))
        for i, v in enumerate([0, L_guess, k_guess, x0_guess])
    ]
    params, covariance = curve_fit(
        logistic_growth, x, y, p0=initial_guess, maxfev=10000, bounds=bounds
    )
    return params


def fit_replicate(t: np.ndarray, od: np.ndarray) -> tuple:
    """Fit logistic curve to a single replicate."""
    valid = ~np.isnan(t) & ~np.isnan(od)
    t_valid = t[valid]
    od_valid = od[valid]

    if len(t_valid) < 4:
        return None, 0.0, False

    try:
        params = fit_logistic_curve(t_valid, od_valid)
        auc = compute_logistic_auc(params, t_valid.min(), t_valid.max())
        return params, auc, True
    except Exception:
        return None, 0.0, False


# ── Outlier detection ────────────────────────────────────────────────────────

def detect_crash(ods: np.ndarray, crash_threshold: float = DEFAULT_CRASH_THRESHOLD, n_clusters: int = 2):
    """
    Detect crashed vs healthy growth curves using K-means clustering.

    Returns:
        (clusters, cosine_distance)
        - clusters: array where 0=healthy, 1=crashed
        - cosine_distance: distance between final cluster centers
    """
    X = ods.T
    n_replicates = X.shape[0]
    n_timepoints = X.shape[1] if X.ndim > 1 else 1

    if n_replicates < n_clusters:
        return np.zeros(n_replicates, dtype=int), 0.0

    X_sqrt = X

    # Repeat to better resolve singlets
    X_repeated = np.vstack([X_sqrt, X_sqrt, X_sqrt])

    valid_mask = ~np.any(np.isnan(X_repeated), axis=1)
    if valid_mask.sum() < n_clusters:
        return np.zeros(n_replicates, dtype=int), 0.0

    X_valid = X_repeated[valid_mask]

    time_weights = np.linspace(0.5, 1.5, n_timepoints)
    time_weights = time_weights / time_weights.sum()

    best_clusters = None
    best_cosine_dist = -1

    for seed in range(100):
        kmeans = KMeans(n_clusters=n_clusters, random_state=seed, n_init=1)
        kmeans.fit(X_valid)

        all_labels = np.zeros(X_repeated.shape[0], dtype=int)
        all_labels[valid_mask] = kmeans.labels_
        raw_clusters = all_labels[:n_replicates]
        centers = kmeans.cluster_centers_.copy()

        cluster_members = {i: (raw_clusters == i) for i in range(n_clusters)}
        active_clusters = list(range(n_clusters))

        while len(active_clusters) > 2:
            pairwise_dist = {}
            for i, ci in enumerate(active_clusters):
                for j, cj in enumerate(active_clusters[i + 1:], i + 1):
                    pairwise_dist[(ci, cj)] = cosine(centers[ci], centers[cj])

            min_pair = min(pairwise_dist, key=pairwise_dist.get)
            combine_i, combine_j = min_pair

            cluster_members[combine_i] = cluster_members[combine_i] | cluster_members[combine_j]
            centers[combine_i] = (centers[combine_i] + centers[combine_j]) / 2

            active_clusters.remove(combine_j)
            del cluster_members[combine_j]

        c0, c1 = active_clusters
        mask_c0 = cluster_members[c0]
        mask_c1 = cluster_members[c1]

        score_c0 = np.sum(centers[c0] * time_weights)
        score_c1 = np.sum(centers[c1] * time_weights)

        clusters = np.zeros(n_replicates, dtype=int)
        if score_c0 >= score_c1:
            clusters[mask_c1] = 1
            healthy_center, crashed_center = centers[c0], centers[c1]
        else:
            clusters[mask_c0] = 1
            healthy_center, crashed_center = centers[c1], centers[c0]

        cos_dist = cosine(healthy_center, crashed_center)

        if cos_dist > best_cosine_dist:
            best_cosine_dist = cos_dist
            best_clusters = clusters.copy()

    if best_cosine_dist < crash_threshold:
        return np.zeros(n_replicates, dtype=int), best_cosine_dist

    return best_clusters, best_cosine_dist


def detect_outlier_replicates(t: np.ndarray, y: np.ndarray, crash_threshold: float = DEFAULT_CRASH_THRESHOLD, n_clusters: int = 2):
    """
    Detect outlier replicates using K-means clustering on growth curves,
    and compute fitted logistic AUCs.

    Returns:
        (outliers, auc_values, fitted_params_list, cosine_distance)
    """
    n_replicates = t.shape[1]

    clusters, cos_dist = detect_crash(y, crash_threshold, n_clusters)
    outliers = clusters == 1

    auc_values = []
    fitted_params_list = []

    for rep_idx in range(n_replicates):
        t_rep = t[:, rep_idx]
        y_rep = y[:, rep_idx]

        params, auc, success = fit_replicate(t_rep, y_rep)
        fitted_params_list.append(params)
        auc_values.append(auc if success else np.nan)

    auc_array = np.array(auc_values)

    for rep_idx in range(n_replicates):
        if fitted_params_list[rep_idx] is None:
            outliers[rep_idx] = True

    return outliers, auc_array, fitted_params_list, cos_dist


# ── Polynomial model ─────────────────────────────────────────────────────────

class PolyModel:
    """Polynomial regression model."""

    def __init__(self, degree=DEFAULT_DEGREE):
        self.degree = degree
        self.poly = PolynomialFeatures(degree=degree)
        self.regression = linear_model.LinearRegression()
        self.model = None
        self.score = None

    def fit(self, X, Y):
        poly_variables = self.poly.fit_transform(X)
        self.model = self.regression.fit(poly_variables, Y)
        self.score = self.model.score(poly_variables, Y)
        return self

    def transform(self, X):
        poly_variables = self.poly.fit_transform(X)
        return self.model.predict(poly_variables)


# ── Formatting helpers ───────────────────────────────────────────────────────

def format_molecule(v):
    """Format molecule names with subscripts for HTML."""
    return "".join(f"<sub>{s}</sub>" if s in "0123456789" else s for s in v)


def get_power(coef):
    """Extract power from coefficient name."""
    x = re.findall(r"<sup>\d+</sup>", coef)
    x = [v[len("<sup>"):-len("</sup>")] for v in x]
    x = [int(v) for v in x]
    if len(x) == 0:
        return 1
    return x[0]


def calc_degree(name):
    """Calculate degree of polynomial term."""
    toks = name.split(" * ")
    d = sum(get_power(t) for t in toks)
    return d


# ── Data loading ─────────────────────────────────────────────────────────────

def load_data(data_path: str):
    """Load and process experimental data from CSV."""
    df = pd.read_csv(data_path)

    if "number" in df.columns and "HoursSinceStart" in df.columns:
        return load_data_dse(df)
    else:
        raise ValueError("Unsupported data format: expected 'number' and 'HoursSinceStart' columns")


def load_data_dse(df: pd.DataFrame):
    """Load DSE format data (R2-2_ReDecoded_Data or R4-1 axis_aligned style)."""
    all_cols = list(df.columns)
    od_value_idx = all_cols.index("OD_Value")

    # Try DSE format first: OD_R1, OD_R2, etc.
    replicate_cols = [c for c in all_cols if c.startswith("OD_R") and c[4:].isdigit()]

    if replicate_cols:
        # DSE format (R2-2): OD_R1, OD_R2, ...
        replicate_cols = sorted(replicate_cols, key=lambda x: int(x[4:]))
        first_rep_idx = all_cols.index(replicate_cols[0])
        media_cols = all_cols[od_value_idx + 1:first_rep_idx]
    else:
        # Axis_aligned format (R4-1): OD_<number> columns (excluding OD_Value)
        replicate_cols_unsorted = [c for c in all_cols if re.match(r'^OD_\d+$', c)]

        if not replicate_cols_unsorted:
            raise ValueError("Could not find replicate columns (OD_R* or OD_<number>)")

        first_rep_col = replicate_cols_unsorted[0]
        first_rep_idx = all_cols.index(first_rep_col)

        replicate_cols = sorted(replicate_cols_unsorted, key=lambda x: int(x[3:]))

        if "initial_OD" in all_cols:
            initial_od_idx = all_cols.index("initial_OD")
            media_cols = all_cols[initial_od_idx + 1:first_rep_idx]
        else:
            media_cols = all_cols[od_value_idx + 1:first_rep_idx]

    valid_numbers = df["number"].dropna().unique()
    groups = sorted([str(int(g)) for g in valid_numbers])

    RUNS: dict[str, Run] = {}

    for sample_num in valid_numbers:
        sample_df = df[df["number"] == sample_num].sort_values("HoursSinceStart")

        times = sample_df["HoursSinceStart"].astype(float).values

        n_timepoints = len(times)
        n_replicates = len(replicate_cols)

        growth = np.zeros((n_timepoints, 2, n_replicates))
        for t_idx, (_, row) in enumerate(sample_df.iterrows()):
            for r_idx, rep_col in enumerate(replicate_cols):
                growth[t_idx, 0, r_idx] = float(row["HoursSinceStart"])
                od_val = row[rep_col]
                if pd.isna(od_val) or od_val == "NA":
                    growth[t_idx, 1, r_idx] = np.nan
                else:
                    growth[t_idx, 1, r_idx] = float(od_val)

        media = sample_df[media_cols].iloc[0].values.astype(float)

        k = str(int(sample_num))
        RUNS[k] = Run(
            media=media,
            growth=growth,
        )

    df.attrs['media_cols'] = media_cols

    return df, groups, RUNS


# ── Model fitting ────────────────────────────────────────────────────────────

def fit_growth_models(RUNS):
    """Fit logistic models and compute AUC for all runs."""
    models = {}
    outlier_info = {}

    for k, v in RUNS.items():
        growth = v.growth
        t = growth[:, 0]
        y = growth[:, 1]

        outliers, auc_per_rep, params_per_rep, cos_dist = detect_outlier_replicates(t, y)
        outlier_info[k] = (outliers, auc_per_rep, params_per_rep, cos_dist)

        healthy_mask = ~outliers
        healthy_aucs = auc_per_rep[healthy_mask & ~np.isnan(auc_per_rep)]

        if len(healthy_aucs) > 0:
            auc = np.mean(healthy_aucs)
            t_healthy = t[:, healthy_mask].flatten()
            y_healthy = y[:, healthy_mask].flatten()
            try:
                valid = ~np.isnan(t_healthy) & ~np.isnan(y_healthy)
                fitted_params = fit_logistic_curve(t_healthy[valid], y_healthy[valid])
            except Exception:
                best_rep = np.argmax(auc_per_rep)
                fitted_params = params_per_rep[best_rep]
        else:
            auc = np.nanmean(auc_per_rep) if np.any(~np.isnan(auc_per_rep)) else 0.0
            t_flat = t.flatten()
            y_flat = y.flatten()
            valid = ~np.isnan(t_flat) & ~np.isnan(y_flat)
            try:
                fitted_params = fit_logistic_curve(t_flat[valid], y_flat[valid])
            except Exception:
                fitted_params = None

        if fitted_params is not None:
            metrics = {
                'auc': auc,
                'b': fitted_params[0],
                'L': fitted_params[1],
                'k': fitted_params[2],
                'x0': fitted_params[3],
            }
        else:
            metrics = {'auc': auc}

        models[k] = (fitted_params, auc, metrics)

    return models, outlier_info


# ── Crashed cultures output ──────────────────────────────────────────────────

def save_crashed_cultures(outlier_info, output_dir):
    """Save crashed cultures table to CSV.

    Columns: sample_id, replicate_index, is_outlier, replicate_auc, cosine_distance
    """
    rows = []
    for sample_id, (outliers, auc_per_rep, params_per_rep, cos_dist) in outlier_info.items():
        for rep_idx in range(len(outliers)):
            rows.append({
                'sample_id': sample_id,
                'replicate_index': rep_idx,
                'is_outlier': bool(outliers[rep_idx]),
                'replicate_auc': auc_per_rep[rep_idx],
                'cosine_distance': cos_dist,
            })

    df = pd.DataFrame(rows)
    df.to_csv(f"{output_dir}/crashed_cultures.csv", index=False)
    return df


# ── Feature preparation ──────────────────────────────────────────────────────

def prepare_features(models, RUNS, outlier_info=None):
    """Prepare feature matrix and target values (AUC)."""
    media = []
    values = []
    labels = []
    for k in models:
        fitted_params, auc, metrics = models[k]

        if outlier_info and k in outlier_info:
            outliers = outlier_info[k][0]
            if np.all(outliers):
                continue

        meta = RUNS[k]
        labels.append(k)
        media.append(meta.media)
        values.append(auc)

    X = np.vstack(media)
    Y = np.array(values)
    return X, Y, labels


def get_media_labels(df):
    """Get formatted media labels."""
    if 'media_cols' in df.attrs:
        media_labels = [format_molecule(v.strip()) for v in df.attrs['media_cols']]
    else:
        media_labels = [re.sub(r"\(?g/L\)?", "", v) for v in df.columns[4:]]
        media_labels = [format_molecule(v.strip()) for v in media_labels]
    return media_labels


def get_coefficient_names(model, media_labels):
    """Get coefficient names for polynomial model."""
    coef_names = []
    for x in model.poly.powers_:
        _sel = np.array([(i, v) for i, v in enumerate(x) if v > 0])

        def p(power):
            if power == 1:
                return ""
            return f"<sup>{power}</sup>"

        name = " * ".join([f"{media_labels[i]}{p(v)}" for i, v in _sel])
        if len(_sel) == 0:
            name = "bias"
        coef_names.append(name)
    return np.array(coef_names)


# ── Plotting ─────────────────────────────────────────────────────────────────

def plot_growth_panel(RUNS, models, outlier_info, output_dir):
    """Create growth panel figure for all runs with outlier detection."""
    n_runs = len(RUNS)
    COLS = int(np.ceil(np.sqrt(n_runs)))
    ROWS = int(np.ceil(n_runs / COLS))

    color_healthy = Color.Hex("212121")
    color_outlier = Palettes.PLOTLY[1]
    color_fit = Palettes.PLOTLY[0]

    titles = []
    for k in RUNS.keys():
        if k in models:
            fitted_params, auc, metrics = models[k]
            if k in outlier_info:
                outliers = outlier_info[k][0]
                has_outlier = np.any(outliers)
                if has_outlier:
                    _outlier = f'<span style="color: {color_outlier.color_value}">*</span>'
                    titles.append(f"{k} [AUC={auc:.2f}] {_outlier}")
                else:
                    titles.append(f"{k} [AUC={auc:.2f}]")
            else:
                titles.append(f"{k} [AUC={auc:.2f}]")
        else:
            titles.append(f"{k}")

    while len(titles) < COLS * ROWS:
        titles.append("")

    fig = BaseFigure(
        shape=(COLS, ROWS),
        x_title="Day",
        y_title="OD",
        subplot_titles=titles,
        horizontal_spacing=0.01, vertical_spacing=0.07,
    )

    max_od = 0
    max_time = 0
    for k, v in RUNS.items():
        growth = v.growth
        t = growth[:, 0]
        y = growth[:, 1]
        valid_y = y[~np.isnan(y)]
        valid_t = t[~np.isnan(t)]
        if len(valid_y) > 0:
            max_od = max(max_od, np.nanmax(valid_y))
        if len(valid_t) > 0:
            max_time = max(max_time, np.nanmax(valid_t))

    for i, (k, v) in enumerate(RUNS.items()):
        if k not in models:
            continue
        row, col = divmod(i, COLS)
        row += 1
        col += 1

        growth = RUNS[k].growth
        t = growth[:, 0]
        y = growth[:, 1]
        fitted_params, auc, metrics = models[k]

        if k in outlier_info:
            outliers = outlier_info[k][0]
        else:
            outliers = np.zeros(y.shape[1], dtype=bool)

        if fitted_params is not None:
            t_fit = np.linspace(np.nanmin(t), np.nanmax(t), 100)
            y_fit = logistic_growth(t_fit, *fitted_params)

            fig.add_trace(
                go.Scatter(
                    x=np.concatenate([t_fit / 24, t_fit[::-1] / 24]),
                    y=np.concatenate([y_fit, np.zeros_like(y_fit)]),
                    fill='toself',
                    fillcolor=color_fit.Fade(0.15).color_value,
                    line=dict(width=0, color=COLORS.TRANSPARENT),
                    marker=dict(size=0, color=COLORS.TRANSPARENT),
                    showlegend=False,
                    hoverinfo='skip',
                ),
                col=col, row=row,
            )

            fig.add_trace(
                go.Scatter(
                    x=t_fit / 24,
                    y=y_fit,
                    mode='lines',
                    line=dict(width=2, color=color_fit.color_value),
                    showlegend=False,
                ),
                col=col, row=row,
            )

        for is_outlier, _color in [(False, color_healthy), (True, color_outlier)]:
            mask = outliers == is_outlier
            if not np.any(mask):
                continue

            xx, yy = [], []
            for rep_idx in np.where(mask)[0]:
                xs = t[:, rep_idx]
                ys = y[:, rep_idx]
                xx += (xs / 24).tolist() + [None]
                yy += ys.tolist() + [None]

            if not is_outlier:
                _name = "healthy"
                _marker = dict(
                    size=5,
                    color=COLORS.TRANSPARENT,
                    line=dict(width=0.5, color=_color.color_value),
                    opacity=0.7,
                )
            else:
                _name = "outlier"
                _marker = dict(
                    size=5,
                    color=_color.color_value,
                    opacity=0.7,
                )

            fig.add_trace(
                go.Scatter(
                    x=xx, y=yy,
                    mode='markers+lines',
                    name=_name,
                    showlegend=False,
                    marker=_marker,
                    line=dict(width=0.5, color=_color.Fade(0.5).color_value),
                ),
                col=col, row=row,
            )

    axis_show = dict(ticks="outside", showticklabels=True)
    axis_hide = dict(ticks=None, showticklabels=False)

    od_range = [-0.1, max_od * 1.1] if max_od > 0 else [-0.1, 1]
    time_range = [-0.2, max_time / 24 + 0.1]

    fig = ApplyTemplate(
        fig,
        default_xaxis=axis_hide,
        default_yaxis=axis_hide,
        axis={
            f"1 {y + 1} y": axis_show | dict(range=od_range)
            for y in range(ROWS)
        } | {
            f"{x + 1} {ROWS} x": axis_show | dict(range=time_range)
            for x in range(COLS)
        },
        layout=dict(
            width=min(1600, COLS * 200),
            height=min(1600, ROWS * 200),
            margin=dict(l=75, r=15, t=45, b=65),
        ),
    )
    fig.write_image(f"{output_dir}/growth_characteristics.svg")


def plot_factor_importance(model, coef_names, output_dir, K=10):
    """Plot first-degree (linear) factor importance."""
    first_degree_mask = np.array([calc_degree(name) == 1 for name in coef_names])
    first_degree_indices = np.where(first_degree_mask)[0]

    if len(first_degree_indices) == 0:
        return

    first_degree_coefs = model.model.coef_[first_degree_indices]
    first_degree_names = coef_names[first_degree_indices]

    coef_order = np.abs(first_degree_coefs).argsort()[::-1]
    _labels = first_degree_names[coef_order][:K][::-1]
    _values = first_degree_coefs[coef_order][:K][::-1]

    fig = BaseFigure()

    fig.add_trace(go.Bar(
        y=[l for l, v in zip(range(len(_labels)), _values) if v <= 0],
        x=[-v for l, v in zip(range(len(_labels)), _values) if v <= 0],
        marker_color=Palettes.PLOTLY[1].color_value,
        orientation="h",
        name="Negative",
    ))
    fig.add_trace(go.Bar(
        y=[l for l, v in zip(range(len(_labels)), _values) if v > 0],
        x=[v for l, v in zip(range(len(_labels)), _values) if v > 0],
        marker_color=Palettes.PLOTLY[0].color_value,
        orientation="h",
        name="Positive",
    ))

    fig = ApplyTemplate(
        fig,
        axis={
            "1 1 y": dict(title="coefficient", tickvals=[i for i in range(len(_labels))], ticktext=_labels),
            "1 1 x": dict(title="coefficient value"),
        },
        layout=dict(width=600, height=300),
    )
    fig.write_image(f"{output_dir}/top_10_factor_importance.svg")


def save_coefficients(coef_names, model, output_dir):
    """Save coefficient table to CSV."""
    coef_order = np.abs(model.model.coef_).argsort()[::-1]
    _labels = coef_names[coef_order][::-1]
    _values = model.model.coef_[coef_order][::-1]

    rows = []
    for l, v in zip(_labels, _values):
        d = calc_degree(l)
        if l == "":
            l = "CONSTANT_OFFSET"
        rows.append((l, d, v, abs(v)))

    df_coef = pd.DataFrame(rows)
    df_coef.columns = ["coefficient", "degree", "value", "abs_value"]
    df_coef = df_coef.sort_values("abs_value", ascending=False)
    df_coef.to_csv(f"{output_dir}/response_surface_coefficients.csv", index=False)
    return df_coef


# ── Optimization ─────────────────────────────────────────────────────────────

def optimize_media(X, Y, media_labels):
    """Run optimization to find optimal media composition."""
    _rows = []
    v2i_maps = []
    max_indices = []
    for col_idx, x in enumerate(X.T):
        x = x[:-1] if len(x) > 1 else x
        xvals = np.unique(x)
        xvals.sort()
        n_unique = len(xvals)
        max_indices.append(n_unique - 1 if n_unique > 1 else 1)
        v2i = {v: i for i, v in enumerate(xvals)}
        v2i_maps.append(v2i)
        xi = np.array([v2i.get(v, 0) for v in x])
        _rows.append(xi)
    x_by_index = np.array(_rows).T

    X_train = X[:-1] if len(X) > 1 else X
    Y_train = Y[:-1] if len(Y) > 1 else Y

    _scale_from_index = PolyModel(degree=1).fit(x_by_index, X_train)
    _scale_to_index = PolyModel(degree=1).fit(X_train, x_by_index)

    _model = PolyModel().fit(x_by_index, Y_train)

    def f(x):
        return -_model.transform(x.reshape(1, -1))[0]

    method = "L-BFGS-B"

    results = []
    bounds = [(0, max_idx) for max_idx in max_indices]
    for i, x in enumerate(x_by_index):
        res = minimize(f, x0=x, bounds=bounds, method=method)
        _y = -res.fun
        if not res.success:
            continue
        results.append((res, i, _y))

    results = sorted(results, key=lambda x: x[-1], reverse=True)
    return results, x_by_index, _scale_to_index


def plot_model_suggestions(results, Y, X, media_labels, _scale_to_index, output_dir):
    """Plot model suggestions vs best tested."""
    best_i = Y.argmax()

    best = results[0][-1]
    topK = 0
    for r, i, s in results:
        if best - s > 1e-3:
            break
        topK += 1

    scaled_media_vals = np.array([r.x for r, i, s in results[:topK]])
    _media_labels = np.array([[media_labels[i] for i, _ in enumerate(r.x)] for r, i, s in results[:topK]])
    best_true = _scale_to_index.transform([X[best_i]])[0]

    fig = BaseFigure(shape=(1, 1), vertical_spacing=0.2)
    x = [v for v in _media_labels.flatten()]
    y = [v - 1 for v in scaled_media_vals.flatten()]
    bx = [v for v in media_labels]
    by = [v - 1 for v in best_true]

    for i, (_x, _y, _bx, _by) in enumerate([(x, y, bx, by)]):
        fig.add_trace(
            go.Scatter(
                name="Model Recommends",
                x=_x, y=_y,
                mode="markers",
                marker=dict(color=Color.Hex("212121").color_value, size=7),
                showlegend=i == 0,
            ),
            row=i + 1, col=1,
        )

        fig.add_trace(
            go.Scatter(
                name="Best Tested",
                mode="markers",
                x=_bx,
                y=_by,
                marker=dict(
                    color=COLORS.TRANSPARENT,
                    line=dict(color=Color.Hex("212121").color_value, width=1),
                    size=12,
                ),
                showlegend=i == 0,
            ),
            row=i + 1, col=1,
        )

    fig = ApplyTemplate(
        fig,
        default_yaxis=dict(
            tickvals=[-1, 0, 1],
            ticktext=["Minimize", "Don't Change", "Maximize"],
            zerolinecolor=COLORS.BLACK,
            zerolinewidth=1
        ),
        default_xaxis=dict(linecolor=COLORS.TRANSPARENT),
        axis={"1 1 x": dict(linecolor=COLORS.TRANSPARENT)},
        layout=dict(width=800, height=200, font_size=16, legend_font_size=16),
    )
    fig.write_image(f"{output_dir}/model_suggestions.svg")

    return scaled_media_vals, _media_labels


def save_model_suggestions(scaled_media_vals, _media_labels, X, df_coef, output_dir):
    """Save model suggestions to CSV."""
    c2v = {str(r.coefficient): r.value for _, r in df_coef[df_coef.degree == 1].iterrows()}
    xbest = scaled_media_vals.mean(axis=0)
    xbest = xbest / 2
    x2 = X.max(axis=0)
    x1 = X.min(axis=0)
    x_range = x2 - x1
    x_range[x_range == 0] = 1
    xbest_scaled = (xbest - x1) * x_range
    labels = _media_labels[0]

    rows = []
    for x, lbl in zip(xbest_scaled, labels):
        impact = c2v.get(lbl, 0.0)
        rows.append((lbl, x, impact))

    df = pd.DataFrame(rows, columns=["media", "optimum_concentration", "impact"])
    df.to_csv(f"{output_dir}/model_suggestions.csv", index=False)
    return df, xbest


COLOR_BASELINE = Color.Hex("#006ddb")
COLOR_OPTIMUM = Color.Hex("#32cd32")


def plot_response_surface_1d(X, Y, model, od2pct, media_labels, xbest, output_dir):
    """Plot 1D cross sections of response surface."""
    xmax = X.max(axis=0)
    xmin = X.min(axis=0)
    xmid = xmin + (xmax - xmin) / 2

    n_media = len(media_labels)
    facet_cols = int(np.ceil(np.sqrt(n_media)))
    max_rows = int(np.ceil(n_media / facet_cols))

    titles = media_labels.copy()
    while len(titles) < facet_cols * max_rows:
        titles.append("")

    fig = BaseFigure(
        shape=(facet_cols, max_rows), subplot_titles=titles,
        vertical_spacing=0.15, shared_xaxes=False,
        x_title="concentration",
        y_title="% change from baseline",
    )

    x_ranges = []
    X_train = X[:-1] if len(X) > 1 else X
    Y_train = Y[:-1] if len(Y) > 1 else Y

    for i, c in enumerate(media_labels):
        row, col = divmod(i, facet_cols)
        W = 3
        x, y = X_train[:, i], od2pct.transform(Y_train.reshape(-1, 1))
        xmin_i, xmax_i, resolution = x.min(), x.max(), 30
        if xmin_i == xmax_i:
            xmax_i = xmin_i + 1
        x_ind = np.linspace(xmin_i, xmax_i, resolution)
        x_ranges.append((col, row, [xmin_i, xmid[i], xmax_i]))

        # Others fixed to mid
        mx = np.ones(shape=(len(x_ind), X.shape[1]))
        mx = mx * xmid
        mx[:, i] = x_ind
        my = od2pct.transform(model.transform(mx).reshape(-1, 1))
        color_ind = COLOR_BASELINE.Fade(0.7).color_value
        fig.add_trace(go.Scatter(
            x=x_ind, y=my,
            mode="lines",
            line=dict(color=color_ind, width=W),
            showlegend=False,
        ), row=row + 1, col=col + 1)

        # Using best
        mx = np.ones(shape=(len(x_ind), X.shape[1]))
        mx = mx * xbest
        mx[:, i] = x_ind
        my = od2pct.transform(model.transform(mx).reshape(-1, 1))
        color_ind = COLOR_OPTIMUM.Fade(0.7).color_value
        fig.add_trace(go.Scatter(
            x=x_ind, y=my,
            mode="lines",
            line=dict(color=color_ind, width=W),
            showlegend=False,
        ), row=row + 1, col=col + 1)

        # Using true data points
        mx_true = x.reshape(-1, 1)
        _model = PolyModel().fit(mx_true, y)
        y_pred = _model.transform(x_ind.reshape(-1, 1))
        color_true = Color.Hex("212121").Fade(1).color_value
        fig.add_trace(go.Scatter(
            x=x_ind, y=y_pred,
            mode="lines",
            line=dict(color=color_true, width=W),
            showlegend=False,
        ), row=row + 1, col=col + 1)

        # Points
        fig.add_trace(go.Box(
            x=x, y=y,
            boxpoints="all",
            jitter=0.5,
            marker=dict(color=Color.Hex("212121").Fade(1).color_value, size=3),
            line=dict(color=Color.Hex("888888").Fade(1).color_value, width=1),
            showlegend=False,
        ), row=row + 1, col=col + 1)

    axis_inner = dict(showticklabels=False, linecolor=COLORS.TRANSPARENT, ticks=None)
    axis_outer = dict(showticklabels=True, linecolor=COLORS.BLACK, ticks="outside")
    _pad = 15

    fig = ApplyTemplate(
        fig,
        default_xaxis=axis_outer,
        default_yaxis=axis_inner,
        axis={
            f"1 {i} y": axis_outer | dict(range=[-110, 50]) for i in range(1, max_rows + 1)
        } | {
            f"{col + 1} {row + 1} x": axis_outer | dict(tickvals=_range) for col, row, _range in x_ranges
        },
        layout=dict(
            width=min(1200, facet_cols * 200),
            height=min(1000, max_rows * 200),
            margin=dict(l=85, r=_pad, t=50, b=75)
        )
    )

    fig.write_image(f"{output_dir}/response_surface.svg")


def plot_response_surface_2d(X, Y, model, od2pct, media_labels, xbest, output_dir):
    """Plot 2D heatmap of response surface."""
    xmax = X.max(axis=0)
    xmin = X.min(axis=0)
    xmid = xmin + (xmax - xmin) / 2

    n_media = len(media_labels)
    facet_cols = int(np.ceil(np.sqrt(n_media)))
    max_rows = int(np.ceil(n_media / facet_cols))

    color_baseline = COLOR_BASELINE
    color_optimum = COLOR_OPTIMUM

    titles = media_labels.copy()
    while len(titles) < facet_cols * max_rows:
        titles.append("")

    fig = BaseFigure(
        shape=(facet_cols, max_rows), subplot_titles=titles,
        horizontal_spacing=0.05,
        vertical_spacing=0.15,
        shared_xaxes=False,
        shared_yaxes=False,
        x_title="concentration",
        y_title=f'<b><span style="color: {color_baseline.color_value}">baseline</span> ↔ <span style="color: {color_optimum.color_value}">optimum</span></b>',
    )

    x_ranges = []

    X_train = X[:-1] if len(X) > 1 else X
    Y_train = Y[:-1] if len(Y) > 1 else Y

    all_heatmaps = []
    all_x_inds = []
    resolution = 31

    for i, c in enumerate(media_labels):
        row, col = divmod(i, facet_cols)
        x, y = X_train[:, i], od2pct.transform(Y_train.reshape(-1, 1))
        xmin_i, xmax_i = x.min(), x.max()
        if xmin_i == xmax_i:
            xmax_i = xmin_i + 1
        x_ind = np.linspace(xmin_i, xmax_i, resolution)
        x_ranges.append((col, row, [xmin_i, xmid[i], xmax_i]))
        all_x_inds.append(x_ind)

        mx = np.ones(shape=(X.shape[1], resolution, resolution))
        for j, (a, b) in enumerate(zip(xmid, xbest)):
            mid2best = np.linspace(a, b, resolution, endpoint=True)
            mx[j] = mid2best
        mx = mx.transpose(0, 2, 1)
        mx[i] = x_ind

        mx = mx.transpose(1, 2, 0).reshape(resolution ** 2, -1)
        my = od2pct.transform(model.transform(mx).reshape(-1, 1))
        my = my.reshape(resolution, resolution)
        all_heatmaps.append(my)

    all_values = np.concatenate([h.flatten() for h in all_heatmaps])
    zmin_auto = np.min(all_values)
    zmax_auto = np.max(all_values)

    if zmax_auto - zmin_auto < 1:
        zmid = (zmax_auto + zmin_auto) / 2
        zmin_auto = zmid - 0.5
        zmax_auto = zmid + 0.5

    colorscale = "blackbody"

    for i, label in enumerate(media_labels):
        row, col = divmod(i, facet_cols)
        x_ind = all_x_inds[i]
        xmin_i, xmax_i = x_ind[0], x_ind[-1]

        fig.add_trace(
            go.Heatmap(
                z=all_heatmaps[i],
                x=x_ind,
                colorscale=colorscale,
                showscale=i == 0,
                zmin=zmin_auto,
                zmax=zmax_auto,
            ),
            row=row + 1, col=col + 1
        )

        step = (xmax_i - xmin_i) / resolution / 2 if xmax_i > xmin_i else 0.5
        for y_line, line_color in [
            [resolution + 0.5, color_optimum.color_value],
            [-0.5, color_baseline.color_value],
        ]:
            fig.add_trace(
                go.Scatter(
                    x=[xmin_i - step, xmax_i + step],
                    y=[y_line - 0.5, y_line - 0.5],
                    mode="lines",
                    line=dict(color=line_color, width=3),
                    showlegend=False,
                ),
                row=row + 1, col=col + 1
            )

    axis_inner = dict(showticklabels=False, linecolor=COLORS.TRANSPARENT, ticks=None)
    axis_outer = dict(showticklabels=True, linecolor=COLORS.BLACK, ticks="outside")
    _pad = 15

    fig = ApplyTemplate(
        fig,
        default_xaxis=axis_inner,
        default_yaxis=axis_inner,
        axis={
            f"1 {i} y": dict(showticklabels=False, ticks=None, linecolor=COLORS.TRANSPARENT)
            for i in range(1, max_rows + 1)
        } | {
            f"{col + 1} {row + 1} x": axis_outer | dict(tickvals=_range)
            for col, row, _range in x_ranges
        },
        layout=dict(
            width=min(1200, facet_cols * 200),
            height=min(1000, max_rows * 200),
            margin=dict(l=85, r=_pad, t=50, b=75)
        )
    )
    fig.write_image(f"{output_dir}/response_surface2.svg")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Response Surface Analysis for Media Optimization")
    parser.add_argument("data_path", help="Path to CSV data file")
    parser.add_argument("output_dir", help="Output directory for results")
    args = parser.parse_args()

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    print(f"Loading data... (output to {output_dir})")
    df, groups, RUNS = load_data(args.data_path)
    print(f"Loaded {len(RUNS)} runs")

    # Fit logistic models and detect outliers
    print("Fitting logistic models and detecting outliers...")
    models, outlier_info = fit_growth_models(RUNS)

    n_with_outliers = sum(1 for k, (outliers, _, _, _) in outlier_info.items() if np.any(outliers))
    print(f"Found {n_with_outliers}/{len(outlier_info)} samples with outlier replicates")

    # Save crashed cultures table
    print("Saving crashed cultures table...")
    save_crashed_cultures(outlier_info, output_dir)

    # Print AUC summary
    aucs = [auc for _, auc, _ in models.values()]
    print(f"AUC range: {min(aucs):.3f} - {max(aucs):.3f}, mean: {np.mean(aucs):.3f}")

    # Create growth panel
    print("Plotting growth panel...")
    plot_growth_panel(RUNS, models, outlier_info, output_dir)

    # Prepare features
    X, Y, labels = prepare_features(models, RUNS, outlier_info)
    print(f"Feature matrix shape: {X.shape}, Target shape: {Y.shape}")

    # Fit polynomial model
    print("Fitting polynomial model on AUC...")
    model = PolyModel().fit(X, Y)
    print(f"R² score: {model.score:.4f}")

    # Find base media index
    BASE_MEDIA_INDEX = None
    for i in range(X.shape[0]):
        row = X[i]
        if np.all(np.isnan(row)):
            X[i] = np.zeros_like(row)
            BASE_MEDIA_INDEX = i
            break
        elif np.all(row == 1):
            BASE_MEDIA_INDEX = i
            break

    if BASE_MEDIA_INDEX is None:
        print("Warning: No base media (all zeros/NaNs) found, using last entry")
        BASE_MEDIA_INDEX = -1
    print(f"Base media: [{BASE_MEDIA_INDEX}={Y[BASE_MEDIA_INDEX]}]")

    # Create OD to percentage change model
    od2pct = PolyModel(degree=1).fit(
        Y.reshape(-1, 1),
        ((Y - Y[BASE_MEDIA_INDEX]) / Y[BASE_MEDIA_INDEX]) * 100
    )

    # Get media labels and coefficient names
    media_labels = get_media_labels(df)
    print(f"Media components: {media_labels}")
    coef_names = get_coefficient_names(model, media_labels)

    # Plot factor importance
    print("Plotting factor importance...")
    plot_factor_importance(model, coef_names, output_dir, K=10)

    # Save coefficients
    df_coef = save_coefficients(coef_names, model, output_dir)
    print(f"Coefficients saved to {output_dir}/response_surface_coefficients.csv")

    # Run optimization
    print("Running optimization...")
    results, x_by_index, _scale_to_index = optimize_media(X, Y, media_labels)

    # Plot model suggestions
    scaled_media_vals, _media_labels = plot_model_suggestions(
        results, Y, X, media_labels, _scale_to_index, output_dir
    )

    # Save model suggestions
    df_suggestions, xbest = save_model_suggestions(
        scaled_media_vals, _media_labels, X, df_coef, output_dir
    )
    print(f"Model suggestions saved to {output_dir}/model_suggestions.csv")
    print(df_suggestions)

    # Plot response surfaces
    print("Plotting response surfaces...")
    plot_response_surface_1d(X, Y, model, od2pct, media_labels, xbest, output_dir)
    plot_response_surface_2d(X, Y, model, od2pct, media_labels, xbest, output_dir)

    print("Done!")


if __name__ == "__main__":
    main()
