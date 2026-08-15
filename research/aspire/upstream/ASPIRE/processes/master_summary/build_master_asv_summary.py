#!/usr/bin/env python3
"""
Build unified ASV master tables from ASV_meta + selected downstream outputs.

Outputs:
1) Long-form master table (ASV_meta rows + ASV-level downstream columns).
2) ASV count-table style master (ASV counts wide + ASV-level metadata/downstream columns).
3) Source manifest + column-mapping and collision audit tables.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd


DELIM_BY_SUFFIX = {
    ".tsv": "\t",
    ".tab": "\t",
    ".txt": "\t",
    ".csv": ",",
}

# Default downstream whitelist for current ASPIRE outputs.
DEFAULT_WHITELIST = [
    "type_group_ISA_enriched.tsv",
    "case_ISA_enriched.tsv",
    "type_group_case_ISA_results.tsv",
    "Type_status_ISA_results.tsv",
    "Type_status_Venn_results.tsv",
    "type_group_indicator_species_summary.tsv",
    "Case_indicator_species_summary.tsv",
    "type_group_indicator_species_results.tsv",
    "Case_indicator_species_results.tsv",
    "spieceasi_node_features.csv",
    "spieceasi_modules_sub.tsv",
    "spieceasi_modules_all.tsv",
    "module_asv_anchor_table.tsv",
    "network_component_membership_POS_ALL.tsv",
    "network_component_membership_POS_SUB.tsv",
    "clustermap_ASV_ID_plot.tsv",
    "asv2mag_pairing.tsv",
    "asv2mag_summary.tsv",
    "asv2mag_genome_summary.tsv",
    "barrnap_16s_reference_catalog.tsv",
]

# Enforced key mapping for known files
PREFERRED_KEY_BY_BASENAME = {
    "type_group_ISA_enriched.tsv": "ASV_ID",
    "case_ISA_enriched.tsv": "ASV_ID",
    "type_group_case_ISA_results.tsv": "ASV_ID",
    "status_ISA_enriched.tsv": "ASV_ID",
    "Type_status_ISA_results.tsv": "ASV_ID",
    "Type_status_Venn_results.tsv": "ASV_ID",
    "node_features.status.tsv": "Taxon",
    "node_features.type.tsv": "Taxon",
    "type_group_indicator_species_summary.tsv": "ASV_ID",
    "Case_indicator_species_summary.tsv": "ASV_ID",
    "type_group_indicator_species_results.tsv": "ASV_ID",
    "Case_indicator_species_results.tsv": "ASV_ID",
    "spieceasi_node_features.csv": "Taxon",
    "spieceasi_modules_sub.tsv": "Taxon",
    "spieceasi_modules_all.tsv": "Taxon",
    "module_asv_anchor_table.tsv": "Taxon",
    "clustermap_ASV_ID_plot.tsv": "ASV_ID",
    "network_component_membership_POS_ALL.tsv": "ASV_ID",
    "network_component_membership_POS_SUB.tsv": "ASV_ID",
    "asv2mag_pairing.tsv": "ASV_ID",
    "asv2mag_genome_summary.tsv": "genome_id",
    "barrnap_16s_reference_catalog.tsv": "genome_id",
}

GLOBAL_NETWORK_SUMMARY_BASENAMES = {
    "network_topology_summary.tsv",
}


def info(msg: str) -> None:
    print(f"[i] {msg}")


def warn(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr)


def normalize_asv_id(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    if ";" in text:
        text = text.split(";", 1)[0]
    return text


def sanitize_token(text: str) -> str:
    out = re.sub(r"[^A-Za-z0-9]+", "_", str(text).strip())
    out = re.sub(r"_+", "_", out).strip("_")
    return out or "field"


def source_prefix(kind: str, path: Path) -> str:
    parts = [sanitize_token(kind)]
    path_parts = list(path.parts)
    try:
        anchor_idx = max(i for i, part in enumerate(path_parts) if part == kind)
        rel_parts = path_parts[anchor_idx + 1 :]
    except ValueError:
        rel_parts = [path.name]

    for part in rel_parts[:-1]:
        token = sanitize_token(part)
        if token:
            parts.append(token)
    parts.append(sanitize_token(path.stem))
    return "__".join(parts)


def build_unique_rename_map(value_cols: list[object], prefix: str) -> dict[object, str]:
    rename_map: dict[object, str] = {}
    seen: Counter[str] = Counter()
    for col in value_cols:
        base = f"{prefix}__{sanitize_token(col)}"
        seen[base] += 1
        rename_map[col] = base if seen[base] == 1 else f"{base}__dup{seen[base]}"
    return rename_map


def read_table(path: Path) -> pd.DataFrame:
    sep = DELIM_BY_SUFFIX.get(path.suffix.lower(), ",")
    return pd.read_csv(path, sep=sep, header=0, low_memory=False)


def detect_asv_column(df: pd.DataFrame) -> str | None:
    exact = {
        "asv_id",
        "asv",
        "feature id",
        "feature_id",
        "feature",
        "otu",
        "taxon",
    }
    for col in df.columns:
        key = str(col).strip().lower()
        if key in exact:
            return col
    for col in df.columns:
        key = str(col).strip().lower()
        if "asv" in key and ("id" in key or key.startswith("asv")):
            return col
    return None


def collapse_text(series: pd.Series, limit: int = 25) -> str | float:
    vals = sorted({str(v).strip() for v in series.dropna() if str(v).strip()})
    if not vals:
        return np.nan
    if len(vals) == 1:
        return vals[0]
    if len(vals) > limit:
        return "|".join(vals[:limit]) + "|..."
    return "|".join(vals)


def collapse_numeric(col: str, series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return np.nan
    uniq = pd.unique(s)
    if len(uniq) == 1:
        return float(uniq[0])
    cl = col.lower()
    if "count" in cl or cl.endswith("_sum") or cl.endswith("_degree") or cl.endswith("_n"):
        return float(s.sum())
    return float(s.mean())


def collapse_by_asv(df: pd.DataFrame) -> pd.DataFrame:
    if "ASV_ID" not in df.columns:
        raise ValueError("collapse_by_asv requires ASV_ID column.")
    records = []
    for asv_id, grp in df.groupby("ASV_ID", sort=False):
        row = {"ASV_ID": asv_id}
        for col in grp.columns:
            if col == "ASV_ID":
                continue
            if pd.api.types.is_numeric_dtype(grp[col]):
                row[col] = collapse_numeric(col, grp[col])
            else:
                row[col] = collapse_text(grp[col])
        records.append(row)
    return pd.DataFrame.from_records(records)


def parse_whitelist(whitelist_csv: str) -> set[str]:
    names = [x.strip() for x in whitelist_csv.split(",") if x.strip()]
    return set(names)


def choose_existing_path(candidates: list[Path], label: str) -> Path:
    for p in candidates:
        if p.exists():
            return p
    pretty = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"Could not auto-detect {label}. Tried: {pretty}")


def collect_whitelist_paths(
    whitelist: set[str],
    clustermaps_dir: Path | None,
    indicspecies_dir: Path | None,
    spieceasi_dir: Path | None,
    asv_mag_dir: Path | None,
) -> list[tuple[str, Path]]:
    roots = [
        ("clustermaps", clustermaps_dir),
        ("indicspecies", indicspecies_dir),
        ("spieceasi", spieceasi_dir),
        ("asv_mag_link", asv_mag_dir),
    ]

    found: list[tuple[str, Path]] = []
    found_names: set[str] = set()
    allowed = {".tsv", ".tab", ".txt", ".csv"}

    for kind, root in roots:
        if root is None or not root.exists():
            continue
        for p in sorted(root.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in allowed:
                continue
            if p.name in whitelist:
                found.append((kind, p))
                found_names.add(p.name)

    missing = sorted(whitelist - found_names)
    for m in missing:
        warn(f"Whitelisted table not found in provided dirs: {m}")

    # Keep deterministic by basename then path
    found.sort(key=lambda kp: (kp[1].name, str(kp[1])))
    return found


def edge_table_to_features(df: pd.DataFrame, prefix: str) -> tuple[pd.DataFrame | None, dict]:
    meta = {
        "edge_mode": False,
        "key_column": "",
        "original_columns": list(df.columns),
        "column_map": [],
        "key_unique_n": 0,
        "key_duplicate_rows": 0,
    }

    lc = {str(c).strip().lower(): c for c in df.columns}
    c1 = lc.get("taxon1")
    c2 = lc.get("taxon2")
    if c1 is None or c2 is None:
        return None, meta

    weight_col = lc.get("weight")
    left = pd.DataFrame({"ASV_ID": df[c1].map(normalize_asv_id)})
    right = pd.DataFrame({"ASV_ID": df[c2].map(normalize_asv_id)})
    if weight_col is not None:
        left["weight"] = pd.to_numeric(df[weight_col], errors="coerce")
        right["weight"] = pd.to_numeric(df[weight_col], errors="coerce")
    else:
        left["weight"] = 1.0
        right["weight"] = 1.0

    long_df = pd.concat([left, right], ignore_index=True)
    long_df = long_df[long_df["ASV_ID"] != ""].copy()
    if long_df.empty:
        return None, meta

    out = (
        long_df.groupby("ASV_ID", as_index=False)
        .agg(
            **{
                f"{prefix}__edge_degree": ("ASV_ID", "size"),
                f"{prefix}__edge_weight_sum": ("weight", "sum"),
                f"{prefix}__edge_weight_mean": ("weight", "mean"),
                f"{prefix}__edge_weight_abs_sum": ("weight", lambda s: np.abs(s).sum()),
            }
        )
    )

    out_cols = [c for c in out.columns if c != "ASV_ID"]
    meta.update(
        {
            "edge_mode": True,
            "key_column": f"{c1}|{c2}",
            "key_unique_n": int(long_df["ASV_ID"].nunique(dropna=True)),
            "key_duplicate_rows": int(long_df["ASV_ID"].duplicated().sum()),
            "column_map": [("<edge_features>", c) for c in out_cols],
        }
    )
    return out, meta


def table_to_asv_features(
    path: Path,
    prefix: str,
    max_direct_cols: int,
    preferred_key: str | None,
) -> tuple[pd.DataFrame | None, dict]:
    table_meta = {
        "status": "unknown",
        "file": str(path),
        "key_column": "",
        "preferred_key": preferred_key or "",
        "original_columns": [],
        "column_map": [],
        "rows_in": 0,
        "asv_rows": 0,
        "feature_cols": 0,
        "key_unique_n": 0,
        "key_duplicate_rows": 0,
    }

    try:
        df = read_table(path)
    except Exception as exc:
        table_meta["status"] = f"read_error: {exc}"
        return None, table_meta

    table_meta["rows_in"] = int(len(df))
    table_meta["original_columns"] = list(df.columns)
    if df.empty:
        table_meta["status"] = "empty_table"
        return None, table_meta

    edge_features, edge_meta = edge_table_to_features(df, prefix)
    if edge_features is not None:
        table_meta.update(edge_meta)
        table_meta["status"] = "edge_list_features"
        table_meta["asv_rows"] = int(len(edge_features))
        table_meta["feature_cols"] = int(edge_features.shape[1] - 1)
        return edge_features, table_meta

    asv_col = None
    if preferred_key and preferred_key in df.columns:
        asv_col = preferred_key
    else:
        asv_col = detect_asv_column(df)

    if asv_col is None:
        table_meta["status"] = "no_asv_key"
        return None, table_meta

    work = df.rename(columns={asv_col: "ASV_ID"}).copy()
    work["ASV_ID"] = work["ASV_ID"].map(normalize_asv_id)
    work = work[work["ASV_ID"] != ""].copy()

    table_meta["key_column"] = str(asv_col)
    table_meta["key_unique_n"] = int(work["ASV_ID"].nunique(dropna=True))
    table_meta["key_duplicate_rows"] = int(work["ASV_ID"].duplicated().sum())

    if work.empty:
        table_meta["status"] = "empty_after_asv_normalization"
        return None, table_meta

    value_cols = [c for c in work.columns if c != "ASV_ID"]
    if not value_cols:
        out = work[["ASV_ID"]].drop_duplicates()
        table_meta["status"] = "asv_only"
        table_meta["asv_rows"] = int(len(out))
        table_meta["feature_cols"] = 0
        return out, table_meta

    if len(value_cols) > max_direct_cols:
        num = work[value_cols].apply(pd.to_numeric, errors="coerce")
        if num.notna().any().any():
            out = pd.DataFrame(
                {
                    "ASV_ID": work["ASV_ID"],
                    f"{prefix}__row_nonnull": num.notna().sum(axis=1),
                    f"{prefix}__row_nonzero": num.fillna(0).ne(0).sum(axis=1),
                    f"{prefix}__row_sum": num.sum(axis=1, skipna=True),
                    f"{prefix}__row_mean": num.mean(axis=1, skipna=True),
                    f"{prefix}__row_max": num.max(axis=1, skipna=True),
                    f"{prefix}__row_min": num.min(axis=1, skipna=True),
                }
            )
            out = collapse_by_asv(out)
            out_cols = [c for c in out.columns if c != "ASV_ID"]
            table_meta["column_map"] = [("<row_stats>", c) for c in out_cols]
            table_meta["status"] = f"row_stats_from_{len(value_cols)}_cols"
            table_meta["asv_rows"] = int(len(out))
            table_meta["feature_cols"] = int(len(out_cols))
            return out, table_meta

    rename_map = build_unique_rename_map(value_cols, prefix)
    keep = work[["ASV_ID"] + value_cols].rename(columns=rename_map)
    out = collapse_by_asv(keep)

    table_meta["column_map"] = [(src, dst) for src, dst in rename_map.items()]
    table_meta["status"] = f"direct_cols_{len(value_cols)}"
    table_meta["asv_rows"] = int(len(out))
    table_meta["feature_cols"] = int(len(rename_map))
    return out, table_meta


def table_to_global_features(
    path: Path,
    prefix: str,
) -> tuple[pd.DataFrame | None, dict]:
    table_meta = {
        "status": "unknown",
        "file": str(path),
        "key_column": "<global>",
        "preferred_key": "",
        "original_columns": [],
        "column_map": [],
        "rows_in": 0,
        "asv_rows": 1,
        "feature_cols": 0,
        "key_unique_n": 1,
        "key_duplicate_rows": 0,
    }

    try:
        df = read_table(path)
    except Exception as exc:
        table_meta["status"] = f"read_error: {exc}"
        return None, table_meta

    table_meta["rows_in"] = int(len(df))
    table_meta["original_columns"] = list(df.columns)
    if df.empty:
        table_meta["status"] = "empty_table"
        return None, table_meta
    if "network_id" not in df.columns:
        table_meta["status"] = "no_network_id"
        return None, table_meta

    records = {"__GLOBAL__": "__GLOBAL__"}
    colmap = []
    for _, row in df.iterrows():
        net = sanitize_token(str(row["network_id"]))
        for col in df.columns:
            if col == "network_id":
                continue
            out_col = f"{prefix}__{net}__{sanitize_token(col)}"
            records[out_col] = row[col]
            colmap.append((f"{row['network_id']}::{col}", out_col))

    out = pd.DataFrame([records])
    out = out.rename(columns={"__GLOBAL__": "GLOBAL_KEY"})
    table_meta["column_map"] = colmap
    table_meta["status"] = f"global_network_summary_{len(colmap)}"
    table_meta["feature_cols"] = int(len(colmap))
    return out, table_meta


def build_asv_feature_table(
    selected_paths: list[tuple[str, Path]],
    max_direct_cols: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    feature_frames: list[pd.DataFrame] = []
    global_feature_frames: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, object]] = []
    column_map_rows: list[dict[str, object]] = []
    original_col_rows: list[dict[str, object]] = []

    for kind, table_path in selected_paths:
        basename = table_path.name
        preferred_key = PREFERRED_KEY_BY_BASENAME.get(basename)
        prefix = source_prefix(kind, table_path)
        if basename in GLOBAL_NETWORK_SUMMARY_BASENAMES:
            feats, meta = table_to_global_features(table_path, prefix=prefix)
            included = feats is not None and not feats.empty
            if included:
                global_feature_frames.append(feats)
        else:
            feats, meta = table_to_asv_features(
                table_path,
                prefix=prefix,
                max_direct_cols=max_direct_cols,
                preferred_key=preferred_key,
            )
            included = feats is not None and not feats.empty
            if included:
                feature_frames.append(feats)


        manifest_rows.append(
            {
                "source_group": kind,
                "file": str(table_path),
                "basename": basename,
                "preferred_key": preferred_key or "",
                "key_column_used": meta.get("key_column", ""),
                "status": meta.get("status", ""),
                "included": bool(included),
                "rows_in": int(meta.get("rows_in", 0)),
                "key_unique_n": int(meta.get("key_unique_n", 0)),
                "key_duplicate_rows": int(meta.get("key_duplicate_rows", 0)),
                "asv_rows": int(meta.get("asv_rows", 0)),
                "feature_cols": int(meta.get("feature_cols", 0)),
            }
        )

        for c in meta.get("original_columns", []):
            original_col_rows.append({"file": str(table_path), "basename": basename, "original_column": str(c)})

        for src, dst in meta.get("column_map", []):
            column_map_rows.append(
                {
                    "source_group": kind,
                    "file": str(table_path),
                    "basename": basename,
                    "key_column_used": meta.get("key_column", ""),
                    "source_column": str(src),
                    "output_column": str(dst),
                }
            )

    manifest = pd.DataFrame.from_records(manifest_rows)
    column_map = pd.DataFrame.from_records(column_map_rows)

    # Original-name collision audit (before prefixing)
    original_cols = pd.DataFrame.from_records(original_col_rows)
    if not original_cols.empty:
        grp = original_cols.groupby("original_column", as_index=False).agg(
            n_files=("file", "nunique"),
            files=("file", lambda s: "|".join(sorted(set(s)))),
        )
        collisions_original = grp[grp["n_files"] > 1].sort_values(["n_files", "original_column"], ascending=[False, True])
    else:
        collisions_original = pd.DataFrame(columns=["original_column", "n_files", "files"])

    # No unresolved collisions allowed after prefix mapping
    if not column_map.empty:
        out_col_counts = Counter(column_map["output_column"].tolist())
        dup_out = sorted([c for c, n in out_col_counts.items() if n > 1])
        if dup_out:
            preview = ", ".join(dup_out[:10])
            raise ValueError(f"Duplicate output columns detected after mapping: {preview}")

    if not feature_frames:
        merged = pd.DataFrame(columns=["ASV_ID"])
    else:
        merged = feature_frames[0]
        for frame in feature_frames[1:]:
            merged = merged.merge(frame, on="ASV_ID", how="outer")
        merged = merged.drop_duplicates(subset=["ASV_ID"]).reset_index(drop=True)

    if not global_feature_frames:
        global_features = pd.DataFrame(columns=["GLOBAL_KEY"])
    else:
        global_features = global_feature_frames[0]
        for frame in global_feature_frames[1:]:
            global_features = global_features.merge(frame, on="GLOBAL_KEY", how="outer")
        global_features = global_features.drop_duplicates(subset=["GLOBAL_KEY"]).reset_index(drop=True)

    return merged, global_features, manifest, column_map, collisions_original


def rollup_asv_meta(asv_meta: pd.DataFrame) -> pd.DataFrame:
    work = asv_meta.copy()
    if "ASV_ID" not in work.columns:
        raise ValueError("ASV meta table must contain ASV_ID.")
    work["ASV_ID"] = work["ASV_ID"].map(normalize_asv_id)
    work = work[work["ASV_ID"] != ""].copy()

    out_rows: list[dict[str, object]] = []
    non_key_cols = [c for c in work.columns if c != "ASV_ID"]

    for asv_id, grp in work.groupby("ASV_ID", sort=False):
        row: dict[str, object] = {
            "ASV_ID": asv_id,
            "meta__row_n": int(len(grp)),
        }
        if "sample" in grp.columns:
            row["meta__sample_n"] = int(grp["sample"].nunique(dropna=True))
        for col in non_key_cols:
            pref_col = f"meta__{sanitize_token(col)}"
            if pd.api.types.is_numeric_dtype(grp[col]):
                row[pref_col] = collapse_numeric(col, grp[col])
            else:
                row[pref_col] = collapse_text(grp[col])
        out_rows.append(row)

    return pd.DataFrame.from_records(out_rows)


def read_asv_counts(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", header=0, low_memory=False)
    if df.empty:
        raise ValueError(f"ASV count table is empty: {path}")
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "ASV_ID"})
    df["ASV_ID"] = df["ASV_ID"].map(normalize_asv_id)
    df = df[df["ASV_ID"] != ""].copy()
    return df


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Create long + count-table master ASV summary tables from selected downstream outputs."
    )
    p.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Pipeline output root (used for auto-detection of inputs/dirs).",
    )
    p.add_argument("--asv-meta", type=Path, required=False, default=None, help="ASV_meta TSV (e.g., ASV_meta_micro.tsv).")
    p.add_argument("--asv-counts", type=Path, required=False, default=None, help="ASV count table TSV (e.g., ASV_final.micro.tsv).")

    p.add_argument("--clustermaps-dir", type=Path, default=None, help="clustermaps output directory (optional).")
    p.add_argument("--indicspecies-dir", type=Path, default=None, help="indicspecies output directory.")
    p.add_argument("--spieceasi-dir", type=Path, default=None, help="spieceasi output directory.")
    p.add_argument("--asv-mag-dir", type=Path, default=None, help="ASV-to-genome/MAG linking output directory.")

    p.add_argument(
        "--whitelist",
        default=",".join(DEFAULT_WHITELIST),
        help="Comma-separated downstream basenames to include. Default matches requested whitelist.",
    )
    p.add_argument("--outdir", type=Path, default=None, help="Output directory (default: <data-dir>/summary/tables).")
    p.add_argument(
        "--max-direct-cols",
        type=int,
        default=300,
        help="If a downstream table has more than this many non-ASV columns, store row stats instead of every column.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.data_dir is not None:
        args.data_dir = args.data_dir.resolve()

    # Auto-detect core inputs from current output structure when not explicitly provided.
    if args.asv_meta is None:
        if args.data_dir is None:
            raise ValueError("--asv-meta is required when --data-dir is not provided.")
        args.asv_meta = choose_existing_path(
            [
                args.data_dir / "metadata" / "ASV_meta_micro.corrected.tsv",
                args.data_dir / "metadata" / "ASV_meta_micro.tsv",
                args.data_dir / "metadata" / "ASV_meta.tsv",
            ],
            "ASV meta table",
        )
    if args.asv_counts is None:
        if args.data_dir is None:
            raise ValueError("--asv-counts is required when --data-dir is not provided.")
        args.asv_counts = choose_existing_path(
            [
                args.data_dir / "batch_correction" / "asv_corrected_abundance.features_rows.tsv",
                args.data_dir / "ASVs" / "ASV_final.micro.tsv",
                args.data_dir / "ASVs" / "ASV_target.micro.tsv",
            ],
            "ASV count table",
        )

    if args.clustermaps_dir is None and args.data_dir is not None:
        args.clustermaps_dir = args.data_dir / "clustermaps"
    if args.indicspecies_dir is None and args.data_dir is not None:
        args.indicspecies_dir = args.data_dir / "indicspecies"
    if args.spieceasi_dir is None and args.data_dir is not None:
        args.spieceasi_dir = args.data_dir / "spieceasi"
    if args.asv_mag_dir is None and args.data_dir is not None:
        args.asv_mag_dir = args.data_dir / "asv_mag_link"

    if args.outdir is None:
        if args.data_dir is not None:
            args.outdir = args.data_dir / "summary" / "tables"
        else:
            args.outdir = Path("summary/tables")

    args.asv_meta = args.asv_meta.resolve()
    args.asv_counts = args.asv_counts.resolve()
    args.outdir = args.outdir.resolve()
    args.outdir.mkdir(parents=True, exist_ok=True)

    whitelist = parse_whitelist(args.whitelist)
    if not whitelist:
        raise ValueError("Whitelist is empty. Provide at least one downstream table basename.")

    info(f"Reading ASV meta: {args.asv_meta}")
    asv_meta = pd.read_csv(args.asv_meta, sep="\t", header=0, low_memory=False)
    if "ASV_ID" not in asv_meta.columns:
        raise ValueError(f"ASV meta missing ASV_ID column: {args.asv_meta}")
    asv_meta["ASV_ID"] = asv_meta["ASV_ID"].map(normalize_asv_id)
    asv_meta = asv_meta[asv_meta["ASV_ID"] != ""].copy()

    selected = collect_whitelist_paths(
        whitelist=whitelist,
        clustermaps_dir=args.clustermaps_dir,
        indicspecies_dir=args.indicspecies_dir,
        spieceasi_dir=args.spieceasi_dir,
        asv_mag_dir=args.asv_mag_dir,
    )
    info(f"Whitelisted downstream files found: {len(selected)}")

    ds_features, global_features, manifest, colmap, col_collisions = build_asv_feature_table(
        selected_paths=selected,
        max_direct_cols=args.max_direct_cols,
    )
    info(
        f"Collected downstream features: "
        f"{max(0, ds_features.shape[1] - 1)} ASV-level columns, "
        f"{max(0, global_features.shape[1] - 1)} global columns."
    )

    # 1) Long-form master
    long_master = asv_meta.merge(ds_features, on="ASV_ID", how="left")
    if not global_features.empty:
        long_master["GLOBAL_KEY"] = "__GLOBAL__"
        long_master = long_master.merge(global_features, on="GLOBAL_KEY", how="left").drop(columns=["GLOBAL_KEY"])
    long_path = args.outdir / "ASV_master_long.tsv"
    long_master.to_csv(long_path, sep="\t", index=False)
    info(f"Wrote long master: {long_path} ({long_master.shape[0]} rows, {long_master.shape[1]} cols)")

    # 2) Count-table master (ASV x sample + metadata/downstream at end)
    info(f"Reading ASV count table: {args.asv_counts}")
    asv_counts = read_asv_counts(args.asv_counts)
    meta_rollup = rollup_asv_meta(asv_meta)

    count_master = asv_counts.merge(meta_rollup, on="ASV_ID", how="left")
    count_master = count_master.merge(ds_features, on="ASV_ID", how="left")
    if not global_features.empty:
        count_master["GLOBAL_KEY"] = "__GLOBAL__"
        count_master = count_master.merge(global_features, on="GLOBAL_KEY", how="left").drop(columns=["GLOBAL_KEY"])
    count_path = args.outdir / "ASV_master_count_wide.tsv"
    count_master.to_csv(count_path, sep="\t", index=False)
    info(f"Wrote count-style master: {count_path} ({count_master.shape[0]} rows, {count_master.shape[1]} cols)")

    # Audits
    manifest_path = args.outdir / "ASV_master_source_manifest.tsv"
    manifest.to_csv(manifest_path, sep="\t", index=False)
    info(f"Wrote manifest: {manifest_path} ({manifest.shape[0]} sources)")

    colmap_path = args.outdir / "ASV_master_column_mapping.tsv"
    colmap.to_csv(colmap_path, sep="\t", index=False)
    info(f"Wrote column mapping: {colmap_path} ({colmap.shape[0]} mapped columns)")

    collisions_path = args.outdir / "ASV_master_column_collisions_original.tsv"
    col_collisions.to_csv(collisions_path, sep="\t", index=False)
    info(f"Wrote pre-prefix collision audit: {collisions_path} ({col_collisions.shape[0]} shared source column names)")


if __name__ == "__main__":
    main()
