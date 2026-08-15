#!/usr/bin/env python3

from __future__ import annotations

import argparse
import glob
import math
import re
from pathlib import Path

import matplotlib as mpl
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.patches import Patch
from scipy.stats import mannwhitneyu, spearmanr

mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"
mpl.rcParams["savefig.dpi"] = 600
plt.rcParams.update({"font.size": 12})
plt.rcParams["font.family"] = "Source Sans Pro"
sns.set_theme()
sns.set_style("white")

FIGURE_FORMATS = (".pdf", ".png", ".svg")

GROUP_TYPE_PALETTE = {
    "Oral Rinse": "#6A3D9A",
    "BAL+Oral Rinse": "#E78AC3",
    "BAL": "#0072B2",
    "BAL+Bronchial Brush": "#5CC8C8",
    "BAL+Lung Brush": "#5CC8C8",
    "Bronchial Brush": "#009E73",
    "Lung Brush": "#009E73",
    "Bronchial Brush+Oral Rinse": "#B8E186",
    "Lung Brush+Oral Rinse": "#B8E186",
    "BAL+Bronchial Brush+Oral Rinse": "#CBB6E9",
    "BAL+Lung Brush+Oral Rinse": "#CBB6E9",
    "not_indicator": "#D3D3D3",
}

CANONICAL_GROUP_TYPE_ORDER = [
    "Oral Rinse",
    "BAL+Oral Rinse",
    "BAL",
    "BAL+Bronchial Brush",
    "Bronchial Brush",
    "Bronchial Brush+Oral Rinse",
    "BAL+Bronchial Brush+Oral Rinse",
    "not_indicator",
]

CASE_STATUS_PALETTE = {
    "Control": "#BDBDBD",
    "Cancer": "#A50026",
}

VOC_SUBCLASS2_PALETTE = {
    "ctrl-brush": "#FFFFFF",
    "ca-contra": "#F4A3B5",
    "ca-lung": "#D7191C",
    "missing": "#D9D9D9",
}

BRUSH_SIDE_PALETTE = {
    "Left Brush": "#FFFFFF",
    "Right Brush": "#000000",
    "missing": "#D9D9D9",
}

LEGACY_VOC_SUBSET = [
    "VOC_1", "Undecane_144", "1-Propanol_23", "VOC_595",
    "Dimethyl sulfone_358", "1-Octanol_377", "2-Butanone_31",
    "Nonane, 3-methyl-_365", "Oxetane, 2-ethyl-3-methyl-_451",
    "Dodecane_152", "Butanal_30", "VOC_149", "1-Octanol_140",
    "Acetoin_774", "Acetone_14", "2(3H)-Furanone, dihydro-5-methyl-_441",
    "Carbamic acid, monoammonium salt_2", "3-Heptanone_347",
    "VOC_900", "Benzene_50", "Heptane, 2,4-dimethyl-_86",
    "1,3,5-Trifluorobenzene_37", "Heptane, 2,2,4,6,6-pentamethyl-_439",
    "Decane_123", "Decane, 1,1'-oxybis-_243", "Octane_81",
    "Undecane, 2-methyl-_241", "Decane, 2,6,7-trimethyl-_372",
    "Nonane, 2-methyl-_363", "1-Butanol_53", "Levomenthol_384",
    "Hexane, 2,5-dimethyl-_319", "VOC_3", "Octanoic acid_242",
    "Butanal, 3-methyl-_417", "Dodecane, 2,7,1VOC-trimethyl-_165",
    "Heptane, 2,2,4,6,6-pentamethyl-_118", "Decane, 4-methyl-_376",
    "Nonadecane_207", "1-Octene_224", "VOC_599", "Acetic acid, methyl ester_19",
    "2-Butenedioic acid (Z)-, monododecyl ester_248", "Heptane, 3-ethyl-2-methyl-_434",
    "Decane_378", "Isopropyl myristate_284", "Dodecane, 2,6,11-trimethyl-_250",
    "Decane, 2-methyl-_137", "2-Propanol_15", "Methanesulfonic anhydride_22",
    "Hexane, 2-methyl-_310", "1,2-Ethanediol, monoacetate_228",
    "Heptane_56", "Octane, 4-methyl-_93", "VOC_679", "(2-Aziridinylethyl)amine_4",
    "1,2-Benzenedicarboxylic acid, bis(2-methylpropyl) ester_968",
    "Benzene, 1-ethyl-3-methyl-_606", "Butanoic acid, 4-hydroxy-_355",
    "Methyl propionate_41", "Ethanol_12", "1-Heptene_61", "2-Pentanone_52",
    "Acetic acid_27", "Acetic acid, butyl ester_84",
]

GREY_CORR_CMAP = mcolors.LinearSegmentedColormap.from_list(
    "voc_corr_greys",
    ["#1A1A1A", "#FAFAFA", "#1A1A1A"],
)
POSITIVE_CORR_CMAP = mcolors.LinearSegmentedColormap.from_list(
    "voc_corr_positive_greys",
    ["#FAFAFA", "#1A1A1A"],
)
NEGATIVE_CORR_CMAP = mcolors.LinearSegmentedColormap.from_list(
    "voc_corr_negative_greys",
    ["#1A1A1A", "#FAFAFA"],
)
VOC_ZSCORE_CMAP = mcolors.LinearSegmentedColormap.from_list(
    "voc_zscore_blue_orange",
    ["#2B6CB0", "#FAFAFA", "#E66100"],
)

GROUP_PART_ORDER = {
    "BAL": 0,
    "Bronchial Brush": 1,
    "Lung Brush": 1,
    "Oral Rinse": 2,
}


def parse_csv_list(text: str) -> list[str]:
    return [item.strip() for item in str(text).split(",") if item.strip()]


def parse_mapping(text: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for token in parse_csv_list(text):
        if "=" not in token:
            raise ValueError(f"Invalid palette entry '{token}'; expected label=#RRGGBB")
        label, color = (part.strip() for part in token.split("=", 1))
        if label and color:
            mapping[label] = color
    return mapping


def normalize_sample_id(value: object, mode: str) -> str:
    text = str(value).strip()
    if not text:
        return text
    if mode in {"none", "exact"}:
        return text
    parts = text.split("_")
    if mode == "legacy_patient_pair":
        if text.startswith("P") and len(parts) >= 2:
            return "_".join(parts[:2])
        return parts[0]
    if mode == "prefix1":
        return parts[0]
    raise ValueError(f"Unsupported sample_id_mode: {mode}")


def canonicalize_sample_type(value: object) -> str:
    text = str(value).strip()
    lowered = text.lower()
    if lowered in {"oral", "oral rinse", "oral_rinse"}:
        return "Oral Rinse"
    if lowered in {"bal", "bronchoalveolar lavage"}:
        return "BAL"
    if lowered in {"bronchial brush", "brochial brush"}:
        return "Bronchial Brush"
    if lowered in {"lung brush"}:
        return "Lung Brush"
    if lowered == "brush":
        return "Bronchial Brush"
    return text


def canonicalize_group_label(label: object) -> str:
    parts = [canonicalize_sample_type(part) for part in str(label).split("+") if str(part).strip()]
    unique_parts: list[str] = []
    for part in parts:
        if part not in unique_parts:
            unique_parts.append(part)
    unique_parts.sort(key=lambda item: (GROUP_PART_ORDER.get(item, 99), item))
    return "+".join(unique_parts) if unique_parts else str(label)


def ordered_group_type_labels(labels: list[str], *, include_all_known: bool = False) -> list[str]:
    norm: list[str] = []
    for label in labels:
        canon = canonicalize_group_label(label)
        if canon and canon not in norm:
            norm.append(canon)
    ordered = list(CANONICAL_GROUP_TYPE_ORDER) if include_all_known else [lbl for lbl in CANONICAL_GROUP_TYPE_ORDER if lbl in norm]
    extras = [lbl for lbl in norm if lbl not in ordered]
    return ordered + extras


def normalize_case_status(value: object) -> str:
    text = str(value).strip().lower()
    if not text:
        return "Control"
    if "non-cancer" in text or "non cancer" in text:
        return "Control"
    if text in {"control", "healthy", "benign", "noncancer", "non_cancer"}:
        return "Control"
    if "cancer" in text or text in {"case", "tumor", "tumour"}:
        return "Cancer"
    return "Control"


def split_taxa_string(taxa_str: str, delimiter: str = ";") -> dict[str, str | None]:
    levels = ["Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"]
    if taxa_str == "Unassigned":
        parts = ["Unassigned"]
    else:
        parts = [part.strip().split("__", 1)[1] if "__" in part else part.strip() for part in taxa_str.split(delimiter)]
    return {lvl: (parts[i] if i < len(parts) else None) for i, lvl in enumerate(levels)}


def bh_adjust(pvals: pd.Series) -> pd.Series:
    values = pd.to_numeric(pvals, errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(values)
    out = np.full(values.shape, np.nan, dtype=float)
    if not valid.any():
        return pd.Series(out, index=pvals.index)
    p = values[valid]
    order = np.argsort(p)
    ranked = p[order]
    n = len(ranked)
    adj = ranked * n / (np.arange(n) + 1)
    adj = np.minimum.accumulate(adj[::-1])[::-1]
    adj = np.clip(adj, 0, 1)
    restored = np.empty_like(adj)
    restored[order] = adj
    out[np.where(valid)[0]] = restored
    return pd.Series(out, index=pvals.index)


def normalize_fraction_threshold(x: float) -> float:
    if not np.isfinite(x):
        raise ValueError("Threshold must be finite.")
    if x < 0:
        raise ValueError("Threshold must be >= 0.")
    if 1 < x <= 100:
        return x / 100.0
    if x > 100:
        raise ValueError("Threshold must be a fraction (0-1) or percent (0-100).")
    return float(x)


def choose_voc_columns(voc_df: pd.DataFrame, requested: list[str], sample_col: str, use_legacy_subset: bool) -> list[str]:
    if requested:
        missing = [col for col in requested if col not in voc_df.columns]
        if missing:
            raise ValueError(f"Requested VOC columns not found: {', '.join(missing)}")
        return requested
    if use_legacy_subset:
        kept = [col for col in LEGACY_VOC_SUBSET if col in voc_df.columns]
        if kept:
            return kept
    numeric_cols: list[str] = []
    for col in voc_df.columns:
        if col == sample_col:
            continue
        numeric = pd.to_numeric(voc_df[col], errors="coerce")
        if numeric.notna().any():
            numeric_cols.append(col)
    if not numeric_cols:
        raise ValueError("No numeric VOC columns were available after selection.")
    return numeric_cols


def load_isa_annotations(pattern: str, q_threshold: float = 0.05) -> pd.DataFrame:
    columns = ["ASV", "isa_source", "isa_groups", "source_category"]
    rows: list[pd.DataFrame] = []
    normalized_pattern = str(pattern).replace(r"\_", "_")
    for match in sorted(glob.glob(normalized_pattern)):
        path = Path(match)
        name = path.name
        if not name.endswith("_summary.tsv") or "DULEG" in name:
            continue
        try:
            df = pd.read_csv(path, sep="\t")
        except Exception:
            continue
        if "ASV" not in df.columns:
            continue
        isa_source = re.sub(r"_indicator_species.*$", "", path.name)
        sig_cols = [col for col in df.columns if col.startswith("s.")]
        if not sig_cols:
            continue
        ann = df[["ASV", "index"] + sig_cols].copy() if "index" in df.columns else df[["ASV"] + sig_cols].copy()
        q_col = next((col for col in ["q.value", "q_value", "qvalue", "q"] if col in df.columns), None)
        sig_col = next((col for col in ["significant", "is_significant", "sig"] if col in df.columns), None)
        stat_col = next((col for col in ["stat", "Stat", "STAT"] if col in df.columns), None)
        keep = pd.Series(True, index=df.index)
        if sig_col:
            keep &= df[sig_col].astype(str).str.strip().str.lower().isin({"true", "t", "1", "yes", "y"})
        elif q_col:
            keep &= pd.to_numeric(df[q_col], errors="coerce").le(q_threshold)
        if stat_col:
            keep &= pd.to_numeric(df[stat_col], errors="coerce").gt(0.0)
        ann = ann.loc[keep].copy()
        if ann.empty:
            continue

        def index_label(row: pd.Series) -> str:
            labels = [
                canonicalize_group_label(col[2:])
                for col in sig_cols
                if pd.notna(row.get(col)) and float(row.get(col)) == 1.0
            ]
            return canonicalize_group_label("+".join(labels)) if labels else ""

        ann["isa_groups"] = ann.apply(index_label, axis=1)
        ann["isa_source"] = isa_source
        ann = ann.loc[ann["isa_groups"].astype(bool), ["ASV", "isa_source", "isa_groups"]]
        rows.append(ann)
    if not rows:
        return pd.DataFrame(columns=columns)
    out = pd.concat(rows, ignore_index=True).drop_duplicates()
    out["source_category"] = out["isa_source"].map(isa_source_category)
    return out.reindex(columns=columns)


def isa_source_category(source: object) -> str:
    text = str(source).strip().lower()
    if text == "type_group" or "type_group" in text or "sample_type" in text:
        return "group"
    if text == "case" or "case" in text:
        return "case"
    return "other"


def isa_group_contains_bronchial(label: object) -> bool:
    for grouped_label in parse_csv_list(str(label)):
        parts = [canonicalize_sample_type(part) for part in str(grouped_label).split("+")]
        if "Bronchial Brush" in parts:
            return True
    return False


def clean_taxon_value(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text == "Unassigned":
        return None
    return text


def build_taxonomy_table(asv_meta: pd.DataFrame) -> pd.DataFrame:
    tax_cols = [col for col in ["ASV_ID", "Taxon", "Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"] if col in asv_meta.columns]
    if "ASV_ID" not in tax_cols:
        raise ValueError("Expected 'ASV_ID' column in ASV meta table.")
    tax_df = asv_meta[tax_cols].drop_duplicates(subset=["ASV_ID"]).copy()
    if "Taxon" in tax_df.columns:
        parsed = tax_df["Taxon"].fillna("Unassigned").map(split_taxa_string).apply(pd.Series)
        for col in parsed.columns:
            if col not in tax_df.columns:
                tax_df[col] = parsed[col]
    for level in ["Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"]:
        if level not in tax_df.columns:
            tax_df[level] = "Unassigned"
    tax_df["ASV_ID"] = tax_df["ASV_ID"].astype(str).str.replace(r";size=.*$", "", regex=True)
    tax_df = tax_df.drop_duplicates(subset=["ASV_ID"], keep="first").set_index("ASV_ID")
    return tax_df


def asv_display_label(asv_id: str, taxonomy_df: pd.DataFrame) -> str:
    if asv_id not in taxonomy_df.index:
        return asv_id
    genus = clean_taxon_value(taxonomy_df.at[asv_id, "Genus"]) if "Genus" in taxonomy_df.columns else None
    species = clean_taxon_value(taxonomy_df.at[asv_id, "Species"]) if "Species" in taxonomy_df.columns else None
    if genus and species:
        tax_text = species if species.lower().startswith(genus.lower()) else f"{genus} {species}"
        return f"{tax_text} ({asv_id})"
    if genus:
        return f"{genus} ({asv_id})"
    if species:
        return f"{species} ({asv_id})"
    return asv_id


def load_brush_metadata(
    asv_meta_path: str,
    metadata_sample_col: str,
    patient_col: str,
    case_col: str,
    type_col: str,
    sample_types_csv: str,
    sample_id_mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    asv_meta = pd.read_csv(asv_meta_path, sep="\t")
    required = ["ASV_ID", metadata_sample_col, patient_col, case_col, type_col]
    missing = [col for col in required if col not in asv_meta.columns]
    if missing:
        raise ValueError(f"ASV meta columns not found: {', '.join(missing)}")
    asv_meta = asv_meta.copy()
    asv_meta["ASV_ID"] = asv_meta["ASV_ID"].astype(str).str.replace(r";size=.*$", "", regex=True)
    asv_meta[patient_col] = asv_meta[patient_col].astype(str)
    asv_meta["normalized_sample_id"] = asv_meta[metadata_sample_col].map(lambda x: normalize_sample_id(x, sample_id_mode))
    asv_meta["sample_type_normalized"] = asv_meta[type_col].map(canonicalize_sample_type)
    keep_types = {canonicalize_sample_type(item) for item in parse_csv_list(sample_types_csv)}
    asv_meta = asv_meta.loc[asv_meta["sample_type_normalized"].isin(keep_types)].copy()
    asv_meta["case_status"] = asv_meta[case_col].map(normalize_case_status)
    sample_meta = asv_meta[
        ["normalized_sample_id", metadata_sample_col, patient_col, "case_status", "sample_type_normalized"]
    ].drop_duplicates(subset=["normalized_sample_id"], keep="first")
    sample_meta = sample_meta.rename(columns={
        metadata_sample_col: "sample_id",
        patient_col: "patient_id",
    })
    sample_meta["patient_id"] = sample_meta["patient_id"].astype(str)
    sample_meta = sample_meta.sort_values(["case_status", "patient_id", "sample_id"]).reset_index(drop=True)
    return sample_meta, asv_meta

def load_asv_counts(asv_counts_path: str, sample_id_mode: str) -> pd.DataFrame:
    counts = pd.read_csv(asv_counts_path, sep="\t", index_col=0)
    counts = counts.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    counts.index = counts.index.astype(str).str.replace(r";size=.*$", "", regex=True)
    counts.columns = [normalize_sample_id(col, sample_id_mode) for col in counts.columns.astype(str)]
    counts = counts.groupby(level=0).sum()
    counts = counts.T.groupby(level=0).sum().T
    return counts


def load_voc_table(voc_path: str, sample_col: str, sample_id_mode: str, requested_cols: list[str], use_legacy_subset: bool) -> pd.DataFrame:
    voc_df = pd.read_csv(voc_path, sep="\t")
    if sample_col not in voc_df.columns:
        raise ValueError(f"VOC sample column '{sample_col}' not found.")
    voc_df[sample_col] = voc_df[sample_col].map(lambda x: normalize_sample_id(x, sample_id_mode))
    voc_df = voc_df.loc[~voc_df[sample_col].duplicated(keep="first")].set_index(sample_col)
    selected = choose_voc_columns(voc_df.reset_index(), requested_cols, sample_col, use_legacy_subset)
    voc_df = voc_df[selected].apply(pd.to_numeric, errors="coerce")
    return voc_df


def load_voc_metadata(voc_path: str, sample_col: str, sample_id_mode: str) -> pd.DataFrame:
    voc_df = pd.read_csv(voc_path, sep="\t")
    if sample_col not in voc_df.columns:
        raise ValueError(f"VOC sample column '{sample_col}' not found.")
    needed = [sample_col] + [col for col in ["subclass2", "Type"] if col in voc_df.columns]
    voc_meta = voc_df[needed].copy()
    for column in ["subclass2", "Type"]:
        if column not in voc_meta.columns:
            voc_meta[column] = "missing"
    voc_meta[sample_col] = voc_meta[sample_col].map(lambda x: normalize_sample_id(x, sample_id_mode))
    voc_meta = voc_meta.loc[~voc_meta[sample_col].duplicated(keep="first")].set_index(sample_col)
    voc_meta["subclass2"] = voc_meta["subclass2"].fillna("missing").astype(str)
    voc_meta["Type"] = voc_meta["Type"].fillna("missing").astype(str)
    return voc_meta


def apply_spieceasi_asv_filter(
    asv_counts_t: pd.DataFrame,
    min_rel_abund: float,
    min_prevalence: float,
    remove_zero_var: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    mat = asv_counts_t.copy()
    keep = pd.Series(True, index=mat.columns)
    if min_rel_abund > 0:
        row_sums = mat.sum(axis=1).replace(0, np.nan)
        rel = mat.div(row_sums, axis=0).fillna(0.0)
        keep &= rel.max(axis=0) >= min_rel_abund
    if min_prevalence > 0:
        keep &= ((mat > 0).sum(axis=0) / max(1, mat.shape[0])) >= min_prevalence
    mat = mat.loc[:, keep]
    if remove_zero_var and not mat.empty:
        mat = mat.loc[:, mat.var(axis=0) > 0]
    summary = pd.DataFrame([{
        "n_samples_input": int(asv_counts_t.shape[0]),
        "n_asvs_input": int(asv_counts_t.shape[1]),
        "n_asvs_kept": int(mat.shape[1]),
        "min_rel_abund": float(min_rel_abund),
        "min_prevalence": float(min_prevalence),
        "remove_zero_var": bool(remove_zero_var),
    }])
    return mat, summary


def correlation_direction_mask(values: pd.DataFrame | pd.Series, direction: str) -> pd.DataFrame | pd.Series:
    if direction == "positive":
        return values > 0
    if direction == "negative":
        return values < 0
    return pd.DataFrame(True, index=values.index, columns=values.columns) if isinstance(values, pd.DataFrame) else pd.Series(True, index=values.index)


def correlation_results(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_label: str,
    right_label: str,
    direction: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    out = pd.DataFrame(index=left.columns, columns=right.columns, dtype=float)
    for left_col in left.columns:
        left_series = left[left_col]
        if left_series.nunique(dropna=True) <= 1:
            continue
        for right_col in right.columns:
            right_series = right[right_col]
            if right_series.nunique(dropna=True) <= 1:
                continue
            paired = pd.DataFrame({"left": left_series, "right": right_series}).dropna()
            if len(paired) < 3:
                continue
            rho, pval = spearmanr(paired["left"], paired["right"], nan_policy="omit")
            out.loc[left_col, right_col] = rho
            rows.append({
                left_label: left_col,
                right_label: right_col,
                "rho": rho,
                "p_value": pval,
                "n": int(len(paired)),
            })
    matrix = out.where(correlation_direction_mask(out, direction))
    matrix = matrix.dropna(how="all").dropna(axis=1, how="all").fillna(0.0)
    long_df = pd.DataFrame(rows)
    if not long_df.empty:
        long_df["q_value"] = bh_adjust(long_df["p_value"])
        long_df["significant"] = long_df["q_value"] <= 0.05
        long_df = long_df.loc[correlation_direction_mask(long_df["rho"], direction)].copy()
        long_df = long_df.sort_values(["q_value", "p_value", "rho"], ascending=[True, True, False]).reset_index(drop=True)
    return matrix, long_df


def build_asv_group_colors(
    asv_ids: list[str],
    isa_annotations: pd.DataFrame,
    taxonomy_df: pd.DataFrame,
) -> tuple[pd.Series, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    if isa_annotations.empty:
        series = pd.Series(
            [GROUP_TYPE_PALETTE["not_indicator"] for _ in asv_ids],
            index=[asv_display_label(asv, taxonomy_df) for asv in asv_ids],
            name="ISA group",
        )
        return series, pd.DataFrame(columns=["ASV_ID", "asv_label", "isa_group_primary", "isa_group_color"])

    for asv_id in asv_ids:
        sub = isa_annotations.loc[isa_annotations["ASV"].astype(str) == str(asv_id)]
        labels: list[str] = []
        for value in sub["isa_groups"].tolist():
            labels.extend(parse_csv_list(str(value)))
        labels = [canonicalize_group_label(label) for label in labels if str(label).strip()]
        ordered_labels = ordered_group_type_labels(labels)
        primary = ordered_labels[0] if ordered_labels else "not_indicator"
        rows.append({
            "ASV_ID": str(asv_id),
            "asv_label": asv_display_label(str(asv_id), taxonomy_df),
            "isa_group_primary": primary,
            "isa_group_color": GROUP_TYPE_PALETTE.get(primary, GROUP_TYPE_PALETTE["not_indicator"]),
        })
    color_key = pd.DataFrame(rows)
    series = pd.Series(color_key["isa_group_color"].tolist(), index=color_key["asv_label"].tolist(), name="ISA group")
    return series, color_key


def build_case_status_colors(patient_ids: list[str], patient_cases: pd.Series) -> pd.Series:
    case_map = {str(idx): str(val) for idx, val in patient_cases.items()}
    return pd.Series(
        [CASE_STATUS_PALETTE.get(case_map.get(str(patient_id), ""), "#7A7A7A") for patient_id in patient_ids],
        index=patient_ids,
        name="Case status",
    )


def build_sample_voc_matrix(
    voc_df: pd.DataFrame,
    sample_meta: pd.DataFrame,
    voc_meta: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if "normalized_sample_id" in sample_meta.columns:
        sample_id_col = "normalized_sample_id"
    elif "index" in sample_meta.columns:
        sample_id_col = "index"
    elif "sample_id" in sample_meta.columns:
        sample_id_col = "sample_id"
    else:
        raise ValueError(f"Expected one of normalized_sample_id/index/sample_id in sample metadata, found: {list(sample_meta.columns)}")
    meta = sample_meta.copy()
    meta[sample_id_col] = meta[sample_id_col].astype(str)
    meta["patient_id"] = meta["patient_id"].astype(str)
    meta["case_status"] = meta["case_status"].map(normalize_case_status)
    common = pd.Index(meta[sample_id_col]).intersection(voc_df.index).intersection(voc_meta.index)
    plot_meta = meta.set_index(sample_id_col).loc[common].copy()
    plot_meta["subclass2"] = voc_meta.loc[common, "subclass2"].fillna("missing").astype(str).values
    plot_meta["brush_side"] = voc_meta.loc[common, "Type"].fillna("missing").astype(str).values
    plot_meta["sample_label"] = [
        f"{sample_id} ({patient_id})"
        for sample_id, patient_id in zip(plot_meta.index.astype(str), plot_meta["patient_id"].astype(str), strict=False)
    ]
    matrix = voc_df.loc[common].copy()
    matrix.index = plot_meta["sample_label"].astype(str).tolist()
    plot_meta.index = matrix.index
    color_df = pd.DataFrame({
        "Case": [CASE_STATUS_PALETTE.get(value, "#7A7A7A") for value in plot_meta["case_status"]],
        "subclass2": [VOC_SUBCLASS2_PALETTE.get(value, VOC_SUBCLASS2_PALETTE["missing"]) for value in plot_meta["subclass2"]],
        "Type": [BRUSH_SIDE_PALETTE.get(value, BRUSH_SIDE_PALETTE["missing"]) for value in plot_meta["brush_side"]],
    }, index=matrix.index)
    return matrix, plot_meta.reset_index(drop=True), color_df


def observed_legend_items(values: pd.Series, palette: dict[str, str]) -> list[tuple[str, str]]:
    observed = set(values.dropna().astype(str))
    return [(label, color) for label, color in palette.items() if label in observed]


def sample_voc_legend_blocks(plot_meta: pd.DataFrame) -> list[tuple[str, list[tuple[str, str]]]]:
    return [
        ("Case", observed_legend_items(plot_meta["case_status"], CASE_STATUS_PALETTE)),
        ("subclass2", observed_legend_items(plot_meta["subclass2"], VOC_SUBCLASS2_PALETTE)),
        ("Type", observed_legend_items(plot_meta["brush_side"], BRUSH_SIDE_PALETTE)),
    ]


def legend_items_from_color_key(color_key: pd.DataFrame) -> list[tuple[str, str]]:
    labels = [] if color_key.empty else color_key["isa_group_primary"].astype(str).dropna().drop_duplicates().tolist()
    ordered = ordered_group_type_labels(labels, include_all_known=False)
    return [(label, GROUP_TYPE_PALETTE.get(label, GROUP_TYPE_PALETTE["not_indicator"])) for label in ordered]


def add_side_legend(
    fig: plt.Figure,
    anchor_x: float,
    anchor_y: float,
    title: str,
    items: list[tuple[str, str]],
) -> None:
    if not items:
        return
    handles = [Patch(facecolor=color, edgecolor="#404040", linewidth=0.4, label=label) for label, color in items]
    fig.legend(
        handles=handles,
        title=title,
        loc="upper left",
        bbox_to_anchor=(anchor_x, anchor_y),
        borderaxespad=0.0,
        frameon=False,
        fontsize=10,
        title_fontsize=11,
    )


def matrix_figsize(n_rows: int, n_cols: int) -> tuple[float, float]:
    width = max(7.5, min(26.0, 4.0 + 0.55 * max(n_cols, 1)))
    height = max(8.0, min(120.0, 4.0 + 0.22 * max(n_rows, 1)))
    return width, height


def zscore_columns(df: pd.DataFrame) -> pd.DataFrame:
    centered = df.astype(float) - df.astype(float).mean(axis=0)
    scaled = centered.div(df.astype(float).std(axis=0).replace(0, np.nan), axis=1)
    return scaled.fillna(0.0)


def clustermap_scale(df: pd.DataFrame, correlation_direction: str) -> tuple[mcolors.Colormap, mpl.colors.Normalize, str | None, float | None, float | None]:
    if correlation_direction == "positive":
        return POSITIVE_CORR_CMAP, mpl.colors.Normalize(vmin=0, vmax=1), None, 0, 1
    if correlation_direction == "negative":
        return NEGATIVE_CORR_CMAP, mpl.colors.Normalize(vmin=-1, vmax=0), None, -1, 0
    if correlation_direction == "data":
        values = df.astype(float).to_numpy()
        finite = values[np.isfinite(values)]
        max_abs = float(np.max(np.abs(finite))) if finite.size else 1.0
        if max_abs == 0:
            max_abs = 1.0
        return VOC_ZSCORE_CMAP, mpl.colors.Normalize(vmin=-max_abs, vmax=max_abs), 0, -max_abs, max_abs
    return GREY_CORR_CMAP, mpl.colors.Normalize(vmin=-1, vmax=1), 0, -1, 1


def save_clustermap(
    df: pd.DataFrame,
    output_stem: Path,
    row_colors: pd.Series | pd.DataFrame | None = None,
    col_colors: pd.Series | pd.DataFrame | None = None,
    row_color_legend: list[tuple[str, str]] | None = None,
    row_color_legends: list[tuple[str, list[tuple[str, str]]]] | None = None,
    correlation_direction: str = "both",
    cbar_label: str = "Spearman rho",
) -> None:
    if df.empty:
        return
    base_fig_width, fig_height = matrix_figsize(df.shape[0], df.shape[1])
    legend_shift_in = 1.0
    fig_width = base_fig_width + legend_shift_in
    font_size_pt = float(plt.rcParams.get("font.size", 12))
    char_width_in = max(0.06, (font_size_pt / 72.0) * 0.60)
    max_row_chars = max((len(str(label)) for label in df.index), default=0)
    label_width_in = max_row_chars * char_width_in
    gap_in = max_row_chars * 0.20 * char_width_in
    cbar_width_in = 0.30
    outer_pad_in = 0.15
    label_width_frac = label_width_in / max(fig_width, 1.0)
    gap_frac = gap_in / max(fig_width, 1.0)
    cbar_width_frac = cbar_width_in / max(fig_width, 1.0)
    outer_pad_frac = outer_pad_in / max(fig_width, 1.0)
    legend_shift_frac = legend_shift_in / max(fig_width, 1.0)
    heatmap_right = max(0.34, 1.0 - (label_width_frac + gap_frac + legend_shift_frac + cbar_width_frac + outer_pad_frac))
    cbar_left = min(0.98 - cbar_width_frac, heatmap_right + label_width_frac + gap_frac + legend_shift_frac)

    plot_row_colors = row_colors.to_frame() if isinstance(row_colors, pd.Series) else row_colors
    plot_col_colors = col_colors.to_frame() if isinstance(col_colors, pd.Series) else col_colors
    row_cluster = df.shape[0] > 1
    col_cluster = df.shape[1] > 1
    cmap, norm, center, vmin, vmax = clustermap_scale(df, correlation_direction)
    n_row_annotations = 0 if plot_row_colors is None else plot_row_colors.shape[1]
    n_col_annotations = 0 if plot_col_colors is None else plot_col_colors.shape[1]
    annotation_ratio = min(0.12, max(0.045, 0.035 * max(n_row_annotations, n_col_annotations, 1)))
    grid = sns.clustermap(
        df.astype(float),
        cmap=cmap,
        center=center,
        vmin=vmin,
        vmax=vmax,
        metric="correlation",
        method="average",
        figsize=(fig_width, fig_height),
        dendrogram_ratio=(0.12, 0.12),
        colors_ratio=(annotation_ratio, annotation_ratio),
        cbar_pos=None,
        row_cluster=row_cluster,
        col_cluster=col_cluster,
        row_colors=plot_row_colors,
        col_colors=plot_col_colors,
        xticklabels=True,
        yticklabels=True,
    )
    grid.fig.subplots_adjust(left=0.05, right=heatmap_right, bottom=0.10, top=0.97)
    grid.ax_heatmap.set_aspect("auto")
    grid.ax_heatmap.tick_params(axis="x", which="both", bottom=True, top=False, length=3)
    grid.ax_heatmap.tick_params(axis="y", which="both", left=True, right=False, length=3)
    grid.ax_heatmap.set_xticklabels(grid.ax_heatmap.get_xticklabels(), rotation=90, ha="center", va="top")
    grid.ax_heatmap.set_yticklabels(grid.ax_heatmap.get_yticklabels(), rotation=0, va="center")

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar_ax = grid.fig.add_axes([cbar_left, 0.62, cbar_width_frac, 0.25])
    grid.fig.colorbar(sm, cax=cbar_ax, orientation="vertical")
    cbar_ax.set_ylabel(cbar_label, rotation=90, va="center", labelpad=18)
    legend_blocks = row_color_legends or []
    if row_color_legend:
        legend_title = str(row_colors.name) if isinstance(row_colors, pd.Series) and row_colors.name else "Annotation"
        legend_blocks = [(legend_title, row_color_legend)] + legend_blocks
    for idx, (legend_title, legend_items) in enumerate(legend_blocks):
        add_side_legend(grid.fig, cbar_left, 0.54 - idx * 0.16, legend_title, legend_items)

    for suffix in FIGURE_FORMATS:
        grid.fig.savefig(output_stem.with_suffix(suffix), dpi=600, bbox_inches="tight", pad_inches=0.35)
    plt.close(grid.fig)


def relabel_asv_matrix(matrix: pd.DataFrame, taxonomy_df: pd.DataFrame) -> pd.DataFrame:
    renamed = matrix.copy()
    renamed.index = [asv_display_label(str(idx), taxonomy_df) for idx in renamed.index]
    return renamed


def add_asv_labels(long_df: pd.DataFrame, taxonomy_df: pd.DataFrame) -> pd.DataFrame:
    if long_df.empty:
        return long_df
    out = long_df.copy()
    out["ASV_ID"] = out["asv"].astype(str)
    out["asv_label"] = out["ASV_ID"].map(lambda x: asv_display_label(x, taxonomy_df))
    return out


def build_patient_voc_matrix(voc_df: pd.DataFrame, sample_meta: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    if "normalized_sample_id" in sample_meta.columns:
        sample_id_col = "normalized_sample_id"
    elif "index" in sample_meta.columns:
        sample_id_col = "index"
    elif "sample_id" in sample_meta.columns:
        sample_id_col = "sample_id"
    else:
        raise ValueError(f"Expected one of normalized_sample_id/index/sample_id in sample_meta columns, found: {list(sample_meta.columns)}")
    meta = sample_meta.copy()
    meta["patient_id"] = meta["patient_id"].astype(str)
    meta["case_status"] = meta["case_status"].map(normalize_case_status)
    meta = meta.set_index(sample_id_col)
    common = voc_df.index.intersection(meta.index)
    patient_df = voc_df.loc[common].copy()
    patient_df["patient_id"] = meta.loc[common, "patient_id"].values
    patient_df["case_status"] = meta.loc[common, "case_status"].values
    patient_df["patient_id"] = patient_df["patient_id"].astype(str)
    patient_df["case_status"] = patient_df["case_status"].map(normalize_case_status)
    patient_case = (
        patient_df.groupby("patient_id")["case_status"]
        .agg(lambda values: "Cancer" if (values.astype(str) == "Cancer").any() else "Control")
        .rename("case_status")
        .reset_index()
    )
    patient_case = patient_case.merge(
        patient_df.groupby("patient_id").size().rename("n_brush_samples").reset_index(),
        on="patient_id",
        how="left",
    )
    patient_case["case_rank"] = patient_case["case_status"].map({"Control": 0, "Cancer": 1}).fillna(2)
    patient_matrix = patient_df.groupby("patient_id").mean(numeric_only=True)
    patient_order = patient_case.sort_values(["case_rank", "patient_id"])["patient_id"].tolist()
    patient_matrix = patient_matrix.loc[[pid for pid in patient_order if pid in patient_matrix.index]]
    patient_case_series = patient_case.set_index("patient_id").loc[patient_matrix.index, "case_status"]
    patient_matrix.index = patient_matrix.index.astype(str)
    patient_case_series.index = patient_case_series.index.astype(str)
    return patient_matrix, patient_case_series, patient_case.drop(columns=["case_rank"])


def patient_case_voc_tests(patient_matrix: pd.DataFrame, patient_case: pd.Series) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for voc in patient_matrix.columns:
        cancer = patient_matrix.loc[patient_case == "Cancer", voc].dropna().to_numpy(dtype=float)
        control = patient_matrix.loc[patient_case == "Control", voc].dropna().to_numpy(dtype=float)
        if len(cancer) < 3 or len(control) < 3:
            continue
        stat, pval = mannwhitneyu(cancer, control, alternative="two-sided")
        rows.append({
            "voc": voc,
            "n_cancer_patients": int(len(cancer)),
            "n_control_patients": int(len(control)),
            "mean_cancer": float(np.mean(cancer)),
            "mean_control": float(np.mean(control)),
            "median_cancer": float(np.median(cancer)),
            "median_control": float(np.median(control)),
            "effect_median_diff": float(np.median(cancer) - np.median(control)),
            "u_statistic": float(stat),
            "p_value": float(pval),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["q_value"] = bh_adjust(out["p_value"])
    out["significant"] = out["q_value"] <= 0.05
    return out.sort_values(["q_value", "p_value", "effect_median_diff"], ascending=[True, True, False]).reset_index(drop=True)


def q_to_stars(q_value: float) -> str:
    if q_value <= 0.001:
        return "***"
    if q_value <= 0.01:
        return "**"
    if q_value <= 0.05:
        return "*"
    return "ns"


def save_case_voc_barplots(patient_matrix: pd.DataFrame, patient_case: pd.Series, test_results: pd.DataFrame, output_stem: Path) -> None:
    if patient_matrix.empty:
        return
    plot_df = zscore_columns(patient_matrix).copy()
    plot_df["patient_id"] = plot_df.index.astype(str)
    plot_df["case_status"] = patient_case.loc[plot_df.index].astype(str).values
    plot_df = plot_df.melt(id_vars=["patient_id", "case_status"], var_name="voc", value_name="abundance_zscore").dropna()
    if plot_df.empty:
        return

    voc_order = test_results["voc"].tolist() if not test_results.empty else sorted(plot_df["voc"].unique().tolist())
    plot_df["voc"] = pd.Categorical(plot_df["voc"], categories=voc_order, ordered=True)
    n_cols = 4
    n_panels = max(1, len(voc_order))
    g = sns.catplot(
        data=plot_df,
        x="case_status",
        y="abundance_zscore",
        hue="case_status",
        col="voc",
        col_wrap=n_cols,
        kind="bar",
        order=["Control", "Cancer"],
        hue_order=["Control", "Cancer"],
        palette=CASE_STATUS_PALETTE,
        sharey=True,
        legend=False,
        height=3.2,
        aspect=1.0,
        errorbar="sd",
    )
    axes = list(g.axes.flat)
    for ax, voc in zip(axes, voc_order):
        sub = plot_df.loc[plot_df["voc"] == voc]
        sns.stripplot(
            data=sub,
            x="case_status",
            y="abundance_zscore",
            order=["Control", "Cancer"],
            color="#222222",
            alpha=0.45,
            size=3,
            ax=ax,
        )
        row = test_results.loc[test_results["voc"] == voc].head(1) if not test_results.empty else pd.DataFrame()
        if not row.empty:
            q_value = float(row["q_value"].iloc[0])
            stars = q_to_stars(q_value)
            ax.set_title(f"{voc}\nq={q_value:.3g}")
            if stars != "ns":
                y_min = float(sub["abundance_zscore"].min())
                y_max = float(sub["abundance_zscore"].max())
                y_span = max(0.1, y_max - y_min)
                bracket_y = y_max + 0.10 * y_span
                ax.plot([0, 0, 1, 1], [bracket_y, bracket_y + 0.04 * y_span, bracket_y + 0.04 * y_span, bracket_y], color="#222222", lw=1.1)
                ax.text(0.5, bracket_y + 0.05 * y_span, stars, ha="center", va="bottom", fontsize=11, fontweight="bold")
                ax.set_ylim(y_min, bracket_y + 0.16 * y_span)
        ax.axhline(0, color="#8A8A8A", lw=0.8, ls="--", zorder=0)
        ax.tick_params(axis="x", rotation=90)
    g.set_axis_labels("", "VOC abundance z-score")
    for ax in axes[len(voc_order):]:
        ax.set_visible(False)
    handles = [Patch(facecolor=CASE_STATUS_PALETTE[label], edgecolor="#404040", linewidth=0.4, label=label) for label in ["Control", "Cancer"]]
    g.fig.legend(
        handles=handles,
        title="Case status",
        loc="upper left",
        bbox_to_anchor=(1.02, 0.98),
        borderaxespad=0.0,
        frameon=False,
        fontsize=10,
        title_fontsize=11,
    )
    g.fig.suptitle("Brush Patient-Level VOC Abundance Z-Score by Case Status", y=1.01)
    g.fig.tight_layout(rect=(0, 0, 0.88, 1))
    for suffix in FIGURE_FORMATS:
        g.fig.savefig(output_stem.with_suffix(suffix), dpi=600, bbox_inches="tight", pad_inches=0.55)
    plt.close(g.fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asv-meta", required=True)
    parser.add_argument("--asv-counts", required=True)
    parser.add_argument("--voc", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--metadata-sample-col", default="Sample")
    parser.add_argument("--type-col", default="Type_Group")
    parser.add_argument("--patient-col", default="Participant_ID")
    parser.add_argument("--case-col", default="Case")
    parser.add_argument("--sample-types", default="Bronchial Brush,Lung Brush")
    parser.add_argument("--voc-sample-col", default="sample")
    parser.add_argument(
        "--sample-id-mode",
        choices=["exact", "none", "legacy_patient_pair", "prefix1"],
        default="legacy_patient_pair",
    )
    parser.add_argument("--voc-col", action="append", default=[])
    parser.add_argument("--use-legacy-voc-subset", action="store_true")
    parser.add_argument("--spieceasi-min-rel-abund", type=float, default=0.0)
    parser.add_argument("--spieceasi-min-prevalence", type=float, default=0.0)
    parser.add_argument("--spieceasi-remove-zero-var", type=str, default="true")
    parser.add_argument("--correlation-direction", choices=["positive", "negative", "both"], default="positive")
    parser.add_argument("--indicspecies-glob", default="*_indicator_species*.tsv")
    parser.add_argument("--isa-q-threshold", type=float, default=0.05)
    parser.add_argument("--case-palette", default="", help="Comma-separated label=#hex mapping.")
    parser.add_argument("--isa-palette", default="", help="Comma-separated ISA-group=#hex mapping.")
    args = parser.parse_args()

    CASE_STATUS_PALETTE.update(parse_mapping(args.case_palette))
    GROUP_TYPE_PALETTE.update(parse_mapping(args.isa_palette))

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    sample_meta, asv_meta = load_brush_metadata(
        asv_meta_path=args.asv_meta,
        metadata_sample_col=args.metadata_sample_col,
        patient_col=args.patient_col,
        case_col=args.case_col,
        type_col=args.type_col,
        sample_types_csv=args.sample_types,
        sample_id_mode=args.sample_id_mode,
    )
    sample_meta.to_csv(outdir / "brush_metadata.tsv", sep="\t", index=False)
    taxonomy_df = build_taxonomy_table(asv_meta)

    asv_counts = load_asv_counts(args.asv_counts, args.sample_id_mode)
    asv_counts_t_unfiltered = asv_counts.T
    spieceasi_remove_zero_var = str(args.spieceasi_remove_zero_var).strip().lower() in {"true", "1", "yes", "y"}
    asv_counts_t, spieceasi_summary = apply_spieceasi_asv_filter(
        asv_counts_t_unfiltered,
        min_rel_abund=normalize_fraction_threshold(float(args.spieceasi_min_rel_abund)),
        min_prevalence=normalize_fraction_threshold(float(args.spieceasi_min_prevalence)),
        remove_zero_var=spieceasi_remove_zero_var,
    )
    spieceasi_summary.to_csv(outdir / "spieceasi_asv_filter.tsv", sep="\t", index=False)

    voc_df = load_voc_table(
        voc_path=args.voc,
        sample_col=args.voc_sample_col,
        sample_id_mode=args.sample_id_mode,
        requested_cols=args.voc_col,
        use_legacy_subset=args.use_legacy_voc_subset,
    )
    voc_meta = load_voc_metadata(args.voc, args.voc_sample_col, args.sample_id_mode)

    common_samples = (
        pd.Index(sample_meta["normalized_sample_id"])
        .intersection(asv_counts_t_unfiltered.index)
        .intersection(voc_df.index)
    )
    if len(common_samples) < 3:
        raise ValueError(f"Only {len(common_samples)} overlapping brush samples between ASV, ASV_meta, and VOC tables.")

    sample_meta = sample_meta.set_index("normalized_sample_id").loc[common_samples].reset_index()
    asv_counts_t = asv_counts_t.loc[common_samples]
    asv_counts_t_unfiltered = asv_counts_t_unfiltered.loc[common_samples]
    voc_df = voc_df.loc[common_samples]

    isa_annotations = load_isa_annotations(args.indicspecies_glob, q_threshold=args.isa_q_threshold)
    isa_group_annotations = isa_annotations.loc[isa_annotations["source_category"] == "group"].copy()
    if not isa_group_annotations.empty:
        isa_group_annotations.to_csv(outdir / "isa_annotations_group.tsv", sep="\t", index=False)
        bronchial_mask = isa_group_annotations["isa_groups"].map(isa_group_contains_bronchial)
        bronchial_asv_set = set(isa_group_annotations.loc[bronchial_mask, "ASV"].astype(str))
        brush_isa_annotations = isa_group_annotations.loc[
            isa_group_annotations["ASV"].astype(str).isin(bronchial_asv_set)
            & bronchial_mask
        ].copy()
        brush_isa_annotations.to_csv(outdir / "isa_annotations_bronchial_brush.tsv", sep="\t", index=False)
    else:
        brush_isa_annotations = isa_group_annotations

    all_asv_corr, all_asv_long = correlation_results(
        asv_counts_t, voc_df, "asv", "voc", direction=args.correlation_direction
    )
    all_asv_corr_display = relabel_asv_matrix(all_asv_corr, taxonomy_df)
    all_row_colors, all_color_key = build_asv_group_colors(list(all_asv_corr.index.astype(str)), isa_group_annotations, taxonomy_df)
    all_color_key.to_csv(outdir / "asv_isa_group_colors.tsv", sep="\t", index=False)
    all_asv_corr_display.to_csv(outdir / "asv_voc_spearman.tsv", sep="\t")
    if not all_asv_long.empty:
        add_asv_labels(all_asv_long, taxonomy_df).to_csv(outdir / "asv_voc_spearman_long.tsv", sep="\t", index=False)
    save_clustermap(
        all_asv_corr_display,
        outdir / "asv_voc_clustermap",
        row_colors=all_row_colors,
        row_color_legend=legend_items_from_color_key(all_color_key),
        correlation_direction=args.correlation_direction,
        cbar_label=f"Spearman rho ({args.correlation_direction} only)" if args.correlation_direction != "both" else "Spearman rho",
    )

    brush_asv_ids = [
        asv
        for asv in sorted(set(brush_isa_annotations["ASV"].astype(str)))
        if asv in asv_counts_t_unfiltered.columns
    ]
    if brush_asv_ids:
        brush_asv_counts_t = asv_counts_t_unfiltered[brush_asv_ids]
        brush_asv_corr, brush_asv_long = correlation_results(
            brush_asv_counts_t, voc_df, "asv", "voc", direction=args.correlation_direction
        )
        brush_asv_corr_display = relabel_asv_matrix(brush_asv_corr, taxonomy_df)
        brush_row_colors, brush_color_key = build_asv_group_colors(list(brush_asv_corr.index.astype(str)), brush_isa_annotations, taxonomy_df)
        brush_color_key.to_csv(outdir / "isa_bronchial_brush_asv_group_colors.tsv", sep="\t", index=False)
        brush_asv_corr_display.to_csv(outdir / "isa_bronchial_brush_asv_voc_spearman.tsv", sep="\t")
        if not brush_asv_long.empty:
            add_asv_labels(brush_asv_long, taxonomy_df).to_csv(outdir / "isa_bronchial_brush_asv_voc_spearman_long.tsv", sep="\t", index=False)
        save_clustermap(
            brush_asv_corr_display,
            outdir / "isa_bronchial_brush_asv_voc_clustermap",
            row_colors=brush_row_colors,
            row_color_legend=legend_items_from_color_key(brush_color_key),
            correlation_direction=args.correlation_direction,
            cbar_label=f"Spearman rho ({args.correlation_direction} only)" if args.correlation_direction != "both" else "Spearman rho",
        )

    sample_voc_matrix, sample_voc_meta, sample_row_colors = build_sample_voc_matrix(voc_df, sample_meta, voc_meta)
    sample_voc_matrix.to_csv(outdir / "sample_voc_matrix_brush.tsv", sep="\t")
    sample_voc_meta.to_csv(outdir / "sample_voc_annotations_brush.tsv", sep="\t", index=False)
    save_clustermap(
        zscore_columns(sample_voc_matrix),
        outdir / "sample_voc_brush_clustermap",
        row_colors=sample_row_colors,
        row_color_legends=sample_voc_legend_blocks(sample_voc_meta),
        correlation_direction="data",
        cbar_label="VOC abundance z-score",
    )

    patient_matrix, patient_case, patient_case_table = build_patient_voc_matrix(voc_df, sample_meta)
    patient_matrix.to_csv(outdir / "patient_voc_matrix_brush.tsv", sep="\t")
    zscore_columns(patient_matrix).to_csv(outdir / "patient_voc_matrix_brush_zscore.tsv", sep="\t")
    patient_case_table.to_csv(outdir / "patient_voc_case_status_brush.tsv", sep="\t", index=False)
    patient_tests = patient_case_voc_tests(patient_matrix, patient_case)
    if not patient_tests.empty:
        patient_tests.to_csv(outdir / "patient_voc_case_tests_brush.tsv", sep="\t", index=False)
    save_case_voc_barplots(patient_matrix, patient_case, patient_tests, outdir / "patient_case_voc_barplots_brush")


if __name__ == "__main__":
    main()
