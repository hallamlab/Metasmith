#!/usr/bin/env python3
"""Export ASV association networks annotated with sequence-derived MAG links."""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns


RANKS = ["domain", "phylum", "class", "order", "family", "genus", "species"]
RANK_PREFIXES = {
    "d": "domain",
    "k": "domain",
    "p": "phylum",
    "c": "class",
    "o": "order",
    "f": "family",
    "g": "genus",
    "s": "species",
}


def info(msg: str) -> None:
    print(f"[i] {msg}")


def die(msg: str) -> None:
    raise SystemExit(msg)


def read_table(path: Path | None) -> pd.DataFrame:
    if path is None or not path.is_file() or path.stat().st_size == 0:
        return pd.DataFrame()
    sep = "\t" if path.suffix.lower() in {".tsv", ".tab", ".txt"} else ","
    return pd.read_csv(path, sep=sep, low_memory=False)


def write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep="\t", index=False)


def clean_taxon(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "unclassified", "uncultured", "unknown"}:
        return ""
    text = re.sub(r"^[dkpcofgs]__?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^[a-z]__", "", text, flags=re.IGNORECASE)
    text = text.replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if text.lower() in {"", "na", "n/a", "uncultured", "unclassified", "unknown"}:
        return ""
    return text.lower()


def strip_sequence_suffix(value: str) -> str:
    text = str(value).strip()
    changed = True
    while changed:
        changed = False
        for suffix in (".gz", ".gff3", ".gff", ".fasta", ".fna", ".fa", ".fas", ".ffn", ".faa"):
            if text.endswith(suffix):
                text = text[: -len(suffix)]
                changed = True
                break
    text = re.sub(r"\.full$", "", text, flags=re.IGNORECASE)
    return text


def standardize_mag_id(value: object, mode: str) -> str:
    if value is None or pd.isna(value):
        return ""
    text = strip_sequence_suffix(str(value).strip())
    if "::" in text:
        text = text.split("::", 1)[1]
    if mode == "suffix_after_double_underscore" and "__" in text:
        return text.rsplit("__", 1)[-1]
    return text


def pairing_mag_match_id(row: pd.Series, mode: str) -> str:
    for col in ("mag_native_genome_id_raw", "Genome_Id", "native_genome_id", "genome_id"):
        if col in row.index:
            value = standardize_mag_id(row.get(col), mode)
            if value:
                return value
    return ""


def parse_taxonomy_string(value: object) -> dict[str, str]:
    if value is None or pd.isna(value):
        return {}
    parts = [part.strip() for part in re.split(r";|\|", str(value)) if part.strip()]
    out: dict[str, str] = {}
    positional = 0
    for part in parts:
        match = re.match(r"^([dkpcofgs])__?(.*)$", part, flags=re.IGNORECASE)
        if match:
            rank = RANK_PREFIXES.get(match.group(1).lower())
            value_clean = clean_taxon(match.group(2))
        else:
            rank = RANKS[positional] if positional < len(RANKS) else ""
            value_clean = clean_taxon(part)
        if rank and value_clean:
            out[rank] = value_clean
        positional += 1
    return out


def load_asv_taxonomy(path: Path | None) -> pd.DataFrame:
    df = read_table(path)
    if df.empty:
        return pd.DataFrame(columns=["ASV_ID", *[f"asv_{rank}" for rank in RANKS], "asv_taxonomy"])
    id_col = next((c for c in df.columns if str(c).strip().lower() in {"feature id", "feature_id", "asv_id", "asv", "id"}), df.columns[0])
    tax_col = next((c for c in df.columns if str(c).strip().lower() in {"taxon", "taxonomy", "consensus"}), None)
    rows = []
    for _, row in df.iterrows():
        asv_id = str(row.get(id_col, "")).strip()
        tax = parse_taxonomy_string(row.get(tax_col, "")) if tax_col else {}
        record = {"ASV_ID": asv_id, "asv_taxonomy": row.get(tax_col, "") if tax_col else ""}
        for rank in RANKS:
            record[f"asv_{rank}"] = tax.get(rank, "")
        rows.append(record)
    return pd.DataFrame(rows).drop_duplicates("ASV_ID")


def mag_taxonomy_from_row(row: pd.Series) -> dict[str, str]:
    out: dict[str, str] = {}
    for rank in RANKS:
        candidates = [
            f"mag_{rank}",
            rank,
            rank.capitalize(),
            f"gtdb_{rank}",
            f"mag_gtdb_{rank}",
        ]
        for col in candidates:
            if col in row.index:
                value = clean_taxon(row.get(col))
                if value:
                    out[rank] = value
                    break
    joined_cols = [c for c in row.index if str(c).lower() in {"taxonomy", "mag_taxonomy", "gtdb_taxonomy", "classification"}]
    for col in joined_cols:
        parsed = parse_taxonomy_string(row.get(col))
        for rank, value in parsed.items():
            out.setdefault(rank, value)
    return out


def normalize_taxonomy_source(value: str) -> str:
    text = (value or "auto").strip().lower()
    if text in {"", "auto", "unknown", "none"}:
        return "unknown"
    if text in {"ncbi", "silva", "rdp", "pr2"}:
        return "ncbi_like"
    if text in {"gtdb", "gtdbtk", "gtdb-tk"}:
        return "gtdb"
    return text


def validate_pair(row: pd.Series, asv_source: str = "unknown", mag_source: str = "unknown") -> dict[str, object]:
    comparable: list[str] = []
    conflicts: list[str] = []
    agreements: list[str] = []
    lowest = ""
    crossdb = asv_source != "unknown" and mag_source != "unknown" and asv_source != mag_source
    for rank in RANKS:
        asv_value = clean_taxon(row.get(f"asv_{rank}", ""))
        mag_value = clean_taxon(row.get(f"mag_tax_{rank}", ""))
        if not asv_value or not mag_value:
            continue
        comparable.append(rank)
        lowest = rank
        if asv_value == mag_value:
            agreements.append(rank)
        else:
            conflicts.append(rank)
    if conflicts and crossdb:
        status = "taxonomy_unvalidated_crossdb"
        agreement = pd.NA
    elif conflicts:
        status = "taxonomy_rejected"
        agreement = False
    elif comparable:
        status = "taxonomy_accepted"
        agreement = True
    else:
        status = "taxonomy_unvalidated"
        agreement = pd.NA
    return {
        "taxonomy_validation_status": status,
        "taxonomy_agreement": agreement,
        "taxonomy_cross_database": crossdb,
        "asv_taxonomy_source": asv_source,
        "mag_taxonomy_source": mag_source,
        "lowest_comparable_rank": lowest,
        "comparable_ranks": ",".join(comparable),
        "conflicting_ranks": ",".join(conflicts),
        "agreeing_ranks": ",".join(agreements),
    }


def normalize_pairing(
    pairing: pd.DataFrame,
    asv_tax: pd.DataFrame,
    asv_taxonomy_source: str,
    mag_taxonomy_source: str,
    mag_id_mode: str,
) -> pd.DataFrame:
    if pairing.empty:
        return pd.DataFrame()
    out = pairing.copy()
    if "ASV_ID" not in out.columns:
        id_col = next((c for c in out.columns if str(c).lower() in {"asv", "asv_id", "taxon"}), None)
        if id_col:
            out = out.rename(columns={id_col: "ASV_ID"})
    if "genome_id" not in out.columns:
        genome_col = next((c for c in out.columns if str(c).lower() in {"mag", "mag_id", "genome", "genome_id"}), None)
        if genome_col:
            out = out.rename(columns={genome_col: "genome_id"})
    if "ASV_ID" not in out.columns or "genome_id" not in out.columns:
        die("ASV-MAG pairing table must contain ASV_ID and genome_id columns.")
    out["mag_match_id"] = out.apply(lambda row: pairing_mag_match_id(row, mag_id_mode), axis=1)

    for rank in RANKS:
        values = []
        for _, row in out.iterrows():
            values.append(mag_taxonomy_from_row(row).get(rank, ""))
        out[f"mag_tax_{rank}"] = values

    out = out.merge(asv_tax, on="ASV_ID", how="left")
    asv_source = normalize_taxonomy_source(asv_taxonomy_source)
    mag_source = normalize_taxonomy_source(mag_taxonomy_source)
    validations = out.apply(lambda row: validate_pair(row, asv_source, mag_source), axis=1, result_type="expand")
    out = pd.concat([out, validations], axis=1)

    status = out.get("pairing_status", pd.Series("", index=out.index)).astype(str)
    out["mapping_class"] = np.select(
        [
            status.eq("unpaired") | out["genome_id"].isna() | out["genome_id"].astype(str).str.strip().eq(""),
            status.str.contains("ambiguous", case=False, na=False),
            out["taxonomy_validation_status"].eq("taxonomy_rejected"),
            status.str.contains("unique", case=False, na=False),
        ],
        ["no_match", "ambiguous_match", "taxonomy_rejected", "unique_match"],
        default=status.replace({"": "unknown"}),
    )
    out["accepted_paper_pair"] = out["mapping_class"].eq("unique_match") & out["taxonomy_validation_status"].isin(
        ["taxonomy_accepted", "taxonomy_unvalidated", "taxonomy_unvalidated_crossdb"]
    )
    return out.sort_values(["ASV_ID", "genome_id"], na_position="last")


def graph_edges_to_table(graph: nx.Graph) -> pd.DataFrame:
    rows = []
    for source, target, attrs in sorted(graph.edges(data=True), key=lambda item: (str(item[0]), str(item[1]))):
        weight = attrs.get("weight", attrs.get("Weight", attrs.get("partial_correlation", "")))
        try:
            weight_float = float(weight)
        except (TypeError, ValueError):
            weight_float = np.nan
        rows.append(
            {
                "source": source,
                "target": target,
                "edge_type": attrs.get("edge_type", "asv_association"),
                "sign": "positive" if not np.isfinite(weight_float) or weight_float >= 0 else "negative",
                "weight": weight_float if np.isfinite(weight_float) else weight,
                "association_interpretation": "SPIEC-EASI statistical association; not a direct biological interaction",
            }
        )
    return pd.DataFrame(rows)


def read_node_features(path: Path | None) -> pd.DataFrame:
    df = read_table(path)
    if df.empty:
        return pd.DataFrame(columns=["ASV_ID"])
    if "Taxon" in df.columns:
        df = df.rename(columns={"Taxon": "ASV_ID"})
    elif "ASV_ID" not in df.columns:
        df = df.rename(columns={df.columns[0]: "ASV_ID"})
    return df.drop_duplicates("ASV_ID")


def summarize_functional_annotations(paths: list[Path], module_min_fraction: float = 0.5, mag_id_mode: str = "exact") -> pd.DataFrame:
    summaries: dict[str, Counter] = defaultdict(Counter)
    module_rows: list[dict[str, object]] = []
    if not paths:
        return pd.DataFrame(columns=["genome_id"])
    for path in paths:
        df = read_table(path)
        if df.empty:
            continue
        lower_cols = {str(c).lower(): c for c in df.columns}
        genome_col = next((c for c in df.columns if str(c).lower() in {"genome_id", "genome", "mag", "mag_id", "bin_id", "bin id"}), None)
        if genome_col is None:
            continue
        if {"feature_id", "feature_name", "fraction_covered"}.issubset(lower_cols):
            feature_id_col = lower_cols["feature_id"]
            feature_name_col = lower_cols["feature_name"]
            fraction_col = lower_cols["fraction_covered"]
            observed_col = lower_cols.get("observed_ko_count")
            total_col = lower_cols.get("total_feature_kos")
            for _, row in df.iterrows():
                genome = str(row.get(genome_col, "")).strip()
                mag_match_id = standardize_mag_id(genome, mag_id_mode)
                module_id = str(row.get(feature_id_col, "")).strip()
                if not genome or not mag_match_id or not module_id:
                    continue
                fraction = pd.to_numeric(pd.Series([row.get(fraction_col)]), errors="coerce").iloc[0]
                present = bool(pd.notna(fraction) and fraction >= module_min_fraction)
                module_rows.append(
                    {
                        "genome_id": genome,
                        "mag_match_id": mag_match_id,
                        "module_id": module_id,
                        "module_name": row.get(feature_name_col, ""),
                        "observed_ko_count": row.get(observed_col, pd.NA) if observed_col else pd.NA,
                        "total_feature_kos": row.get(total_col, pd.NA) if total_col else pd.NA,
                        "fraction_covered": fraction,
                        "present": present,
                    }
                )
                if present:
                    summaries[genome][f"{module_id}:{row.get(feature_name_col, '')}"] += 1
            continue
        ann_cols = [c for c in df.columns if str(c).lower() in {"ko", "kegg", "ec", "pfam", "cog", "pathway", "module"}]
        if not ann_cols:
            ann_cols = [c for c in df.columns if c != genome_col][:3]
        for _, row in df.iterrows():
            genome = str(row.get(genome_col, "")).strip()
            mag_match_id = standardize_mag_id(genome, mag_id_mode)
            if not genome or not mag_match_id:
                continue
            for col in ann_cols:
                value = row.get(col)
                if pd.isna(value) or not str(value).strip():
                    continue
                summaries[mag_match_id][f"{col}:{value}"] += 1
    rows = []
    for mag_match_id, counts in summaries.items():
        top = [item for item, _count in counts.most_common(25)]
        module_records = [row for row in module_rows if row["mag_match_id"] == mag_match_id]
        present_modules = [row for row in module_records if row.get("present")]
        rows.append(
            {
                "mag_match_id": mag_match_id,
                "genome_id": next((row["genome_id"] for row in module_records), mag_match_id),
                "functional_feature_count": sum(counts.values()),
                "functional_summary": "|".join(top),
                "module_count": len(module_records),
                "module_present_count": len(present_modules),
                "module_present_ids": "|".join(str(row["module_id"]) for row in present_modules),
            }
        )
    out = pd.DataFrame(rows)
    if module_rows:
        module_df = pd.DataFrame(module_rows)
        out.attrs["module_table"] = module_df
    return out


def load_mag_abundance(
    path: Path,
    fmt: str,
    genome_col: str,
    sample_col: str,
    value_col: str,
    mag_id_mode: str,
) -> pd.DataFrame:
    mag = read_table(path)
    if mag.empty:
        return pd.DataFrame()
    fmt = fmt.lower()
    if fmt == "auto":
        lower = {c.lower(): c for c in mag.columns}
        fmt = "long" if genome_col.lower() in lower and sample_col.lower() in lower and value_col.lower() in lower else "wide"
    if fmt == "long":
        lower = {c.lower(): c for c in mag.columns}
        gcol = lower.get(genome_col.lower(), genome_col)
        scol = lower.get(sample_col.lower(), sample_col)
        vcol = lower.get(value_col.lower(), value_col)
        missing = [c for c in [gcol, scol, vcol] if c not in mag.columns]
        if missing:
            die(f"MAG abundance long table missing columns: {', '.join(missing)}")
        mag = mag[[gcol, scol, vcol]].copy()
        mag[vcol] = pd.to_numeric(mag[vcol], errors="coerce").fillna(0.0)
        mag["_mag_match_id"] = mag[gcol].map(lambda value: standardize_mag_id(value, mag_id_mode))
        mag = mag.loc[mag["_mag_match_id"].astype(bool)].copy()
        return mag.pivot_table(index="_mag_match_id", columns=scol, values=vcol, aggfunc="sum", fill_value=0.0)
    mag = mag.set_index(mag.columns[0])
    mag.index = mag.index.map(lambda value: standardize_mag_id(value, mag_id_mode))
    mag = mag.loc[mag.index.astype(bool)]
    return mag.apply(pd.to_numeric, errors="coerce").fillna(0.0)


def relative_by_sample(table: pd.DataFrame) -> pd.DataFrame:
    numeric = table.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    denom = numeric.sum(axis=0).replace(0, np.nan)
    return numeric.div(denom, axis=1).fillna(0.0)


def abundance_agreement(
    pairing: pd.DataFrame,
    mag_abundance_path: Path | None,
    asv_counts_path: Path | None,
    mag_abundance_format: str,
    mag_genome_col: str,
    mag_sample_col: str,
    mag_value_col: str,
    mag_id_mode: str,
    min_shared_samples: int,
    transform: str,
) -> pd.DataFrame:
    if mag_abundance_path is None or not mag_abundance_path.is_file() or asv_counts_path is None or not asv_counts_path.is_file():
        return pd.DataFrame()
    mag = load_mag_abundance(mag_abundance_path, mag_abundance_format, mag_genome_col, mag_sample_col, mag_value_col, mag_id_mode)
    asv = pd.read_csv(asv_counts_path, sep="\t", index_col=0)
    if mag.empty or asv.empty:
        return pd.DataFrame()
    asv.index = asv.index.astype(str)
    asv.columns = asv.columns.astype(str)
    mag.index = mag.index.astype(str)
    mag.columns = mag.columns.astype(str)
    # ASPIRE count tables are usually features x samples; transpose only if needed.
    pair_samples = set(asv.columns).intersection(mag.columns)
    if len(pair_samples) < 2 and set(asv.index).intersection(mag.columns):
        asv = asv.T
        pair_samples = set(asv.columns).intersection(mag.columns)
    asv = relative_by_sample(asv)
    mag = relative_by_sample(mag)
    if transform == "log1p":
        asv = np.log1p(asv)
        mag = np.log1p(mag)
    samples = sorted(pair_samples)
    rows = []
    accepted = pairing.loc[pairing["accepted_paper_pair"]].dropna(subset=["genome_id"])
    for _, row in accepted.iterrows():
        asv_id = str(row["ASV_ID"])
        genome_id = str(row["genome_id"])
        mag_match_id = str(row.get("mag_match_id", genome_id))
        if asv_id not in asv.index or mag_match_id not in mag.index or len(samples) < min_shared_samples:
            continue
        x = pd.to_numeric(asv.loc[asv_id, samples], errors="coerce")
        y = pd.to_numeric(mag.loc[mag_match_id, samples], errors="coerce")
        valid = x.notna() & y.notna()
        if valid.sum() < min_shared_samples:
            continue
        rows.append(
            {
                "ASV_ID": asv_id,
                "genome_id": genome_id,
                "mag_match_id": mag_match_id,
                "shared_samples": int(valid.sum()),
                "abundance_metric": "relative_abundance",
                "transform": transform,
                "spearman_rho": float(x[valid].corr(y[valid], method="spearman")),
            }
        )
    return pd.DataFrame(rows)


def cytoscape_json(nodes: pd.DataFrame, edges: pd.DataFrame) -> dict:
    elements = {"nodes": [], "edges": []}
    for _, row in nodes.iterrows():
        data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        data["id"] = str(data.get("id", data.get("node_id", data.get("ASV_ID", data.get("genome_id", "")))))
        elements["nodes"].append({"data": data})
    for idx, row in edges.iterrows():
        data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        data["id"] = str(data.get("id", f"e{idx}"))
        elements["edges"].append({"data": data})
    return {"elements": elements}


def add_table_attrs(graph: nx.Graph, node_table: pd.DataFrame, id_col: str) -> None:
    for _, row in node_table.iterrows():
        node_id = str(row.get(id_col, ""))
        if node_id not in graph:
            continue
        for key, value in row.items():
            if key == id_col or pd.isna(value):
                continue
            graph.nodes[node_id][str(key)] = str(value) if isinstance(value, (list, dict)) else value


def make_paper_outputs(
    graph: nx.Graph,
    node_features: pd.DataFrame,
    validation: pd.DataFrame,
    functional: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, nx.Graph]:
    accepted = validation.loc[validation["accepted_paper_pair"]].copy()
    if not functional.empty:
        functional_for_merge = functional.drop(columns=["genome_id"], errors="ignore")
        accepted = accepted.merge(functional_for_merge, on="mag_match_id", how="left")
    mag_cols = [
        c
        for c in accepted.columns
        if c.startswith("mag_")
        or c
        in {
            "genome_id",
            "link_pident",
            "link_qcov",
            "link_bitscore",
            "taxonomy_validation_status",
            "taxonomy_agreement",
            "lowest_comparable_rank",
            "functional_feature_count",
            "functional_summary",
        }
    ]
    accepted_one = accepted.sort_values(["ASV_ID", "genome_id"]).drop_duplicates("ASV_ID", keep="first")
    nodes = pd.DataFrame({"ASV_ID": sorted(str(n) for n in graph.nodes())})
    nodes = nodes.merge(node_features, on="ASV_ID", how="left")
    if mag_cols:
        nodes = nodes.merge(accepted_one[["ASV_ID", *mag_cols]], on="ASV_ID", how="left")
    nodes["node_type"] = "ASV"
    nodes["has_accepted_mag"] = nodes["genome_id"].notna() if "genome_id" in nodes.columns else False
    edges = graph_edges_to_table(graph)
    paper_graph = graph.copy()
    for node in paper_graph.nodes:
        paper_graph.nodes[node]["node_type"] = "ASV"
    add_table_attrs(paper_graph, nodes, "ASV_ID")
    for u, v, attrs in paper_graph.edges(data=True):
        attrs["edge_type"] = "asv_association"
        weight = attrs.get("weight", attrs.get("Weight", 0))
        try:
            attrs["sign"] = "positive" if float(weight) >= 0 else "negative"
        except (TypeError, ValueError):
            attrs["sign"] = "unknown"
        attrs["association_interpretation"] = "SPIEC-EASI statistical association; not a direct biological interaction"
    return nodes, edges, paper_graph


def make_heterogeneous_outputs(
    graph: nx.Graph,
    paper_nodes: pd.DataFrame,
    paper_edges: pd.DataFrame,
    validation: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, nx.Graph]:
    asv_nodes = pd.DataFrame({"id": [str(n) for n in sorted(graph.nodes())], "node_type": "ASV"})
    asv_nodes = asv_nodes.merge(paper_nodes.rename(columns={"ASV_ID": "id"}), on=["id", "node_type"], how="left")
    accepted = validation.loc[validation["accepted_paper_pair"]].dropna(subset=["genome_id"]).copy()
    mag_records = []
    for genome_id, grp in accepted.groupby("genome_id", dropna=True):
        row = {"id": str(genome_id), "node_type": "MAG", "genome_id": str(genome_id), "linked_asv_count": grp["ASV_ID"].nunique()}
        for rank in RANKS:
            values = sorted({clean_taxon(v) for v in grp.get(f"mag_tax_{rank}", pd.Series(dtype=object)).dropna() if clean_taxon(v)})
            if values:
                row[f"mag_tax_{rank}"] = values[0]
        for col in [c for c in grp.columns if c.startswith("mag_") and c not in row]:
            vals = [v for v in grp[col].dropna().astype(str).unique() if v and v.lower() != "nan"]
            if vals:
                row[col] = vals[0]
        mag_records.append(row)
    mag_nodes = pd.DataFrame(mag_records)
    nodes = pd.concat([asv_nodes, mag_nodes], ignore_index=True, sort=False)

    seq_edges = accepted.copy()
    seq_edges = seq_edges.assign(
        source=seq_edges["ASV_ID"].astype(str),
        target=seq_edges["genome_id"].astype(str),
        edge_type="sequence_match",
        evidence_type="near_exact_16S_alignment",
        identity=seq_edges.get("link_pident", pd.NA),
        coverage=seq_edges.get("link_qcov", pd.NA),
    )
    seq_edge_cols = [
        "source",
        "target",
        "edge_type",
        "evidence_type",
        "identity",
        "coverage",
        "taxonomy_agreement",
        "taxonomy_validation_status",
        "lowest_comparable_rank",
    ]
    edges = pd.concat([paper_edges, seq_edges[[c for c in seq_edge_cols if c in seq_edges.columns]]], ignore_index=True, sort=False)
    hetero = nx.Graph()
    for _, row in nodes.iterrows():
        node_id = str(row["id"])
        attrs = {k: v for k, v in row.to_dict().items() if k != "id" and not pd.isna(v)}
        hetero.add_node(node_id, **attrs)
    for _, row in edges.iterrows():
        source = str(row["source"])
        target = str(row["target"])
        attrs = {k: v for k, v in row.to_dict().items() if k not in {"source", "target"} and not pd.isna(v)}
        hetero.add_edge(source, target, **attrs)
    return nodes, edges, hetero


def plot_summary(summary: pd.DataFrame, outdir: Path) -> None:
    plots_dir = outdir / "qc"
    plots_dir.mkdir(parents=True, exist_ok=True)
    if "mapping_class" in summary.columns:
        fig, ax = plt.subplots(figsize=(7, 4))
        sns.barplot(data=summary, x="mapping_class", y="n_asvs", color="#4C78A8", ax=ax)
        ax.set_xlabel("Mapping class")
        ax.set_ylabel("ASVs")
        ax.set_title("ASV-MAG network mapping classes")
        ax.tick_params(axis="x", rotation=25)
        fig.tight_layout()
        fig.savefig(plots_dir / "asv_mag_network_mapping_classes.png", dpi=300)
        fig.savefig(plots_dir / "asv_mag_network_mapping_classes.svg")
        plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph", required=True)
    parser.add_argument("--node-features", required=True)
    parser.add_argument("--asv-mag-pairing", required=True)
    parser.add_argument("--taxonomy", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--prefix", default="asv_mag_network")
    parser.add_argument("--genome-summary")
    parser.add_argument("--reference-catalog")
    parser.add_argument("--asv-counts")
    parser.add_argument("--mag-abundance")
    parser.add_argument("--mag-id-mode", default="exact", choices=["exact", "suffix_after_double_underscore"])
    parser.add_argument("--mag-abundance-format", default="auto", choices=["auto", "long", "wide"])
    parser.add_argument("--mag-abundance-genome-col", default="genome_id")
    parser.add_argument("--mag-abundance-sample-col", default="sample_id")
    parser.add_argument("--mag-abundance-value-col", default="read_count")
    parser.add_argument("--min-shared-samples", type=int, default=5)
    parser.add_argument("--abundance-transform", default="log1p", choices=["none", "log1p"])
    parser.add_argument("--functional-annotation", action="append", default=[])
    parser.add_argument("--functional-module-min-fraction", type=float, default=0.5)
    parser.add_argument("--asv-taxonomy-source", default="ncbi")
    parser.add_argument("--mag-taxonomy-source", default="gtdb")
    parser.add_argument("--min-pident", type=float, default=99.5)
    parser.add_argument("--min-qcov", type=float, default=100.0)
    args = parser.parse_args()

    outdir = Path(args.outdir)
    for subdir in ["validation", "filtering", "spieceasi", "rrna", "mapping", "network", "abundance", "functional", "qc"]:
        (outdir / subdir).mkdir(parents=True, exist_ok=True)

    graph_path = Path(args.graph)
    graph = nx.read_graphml(graph_path)
    graph = nx.relabel_nodes(graph, {node: str(data.get("name", node)) for node, data in graph.nodes(data=True)})
    for u, v, attrs in graph.edges(data=True):
        attrs["edge_type"] = "asv_association"

    node_features = read_node_features(Path(args.node_features))
    asv_tax = load_asv_taxonomy(Path(args.taxonomy))
    pairing = normalize_pairing(
        read_table(Path(args.asv_mag_pairing)),
        asv_tax,
        args.asv_taxonomy_source,
        args.mag_taxonomy_source,
        args.mag_id_mode,
    )
    if pairing.empty:
        die("ASV-MAG pairing table is empty; cannot build ASV_MAG_NETWORK outputs.")
    pairing["passes_paper_thresholds"] = (
        pd.to_numeric(pairing.get("link_pident"), errors="coerce").ge(args.min_pident)
        & pd.to_numeric(pairing.get("link_qcov"), errors="coerce").ge(args.min_qcov)
    )
    pairing["accepted_paper_pair"] = pairing["accepted_paper_pair"] & pairing["passes_paper_thresholds"]

    functional = summarize_functional_annotations(
        [Path(p) for p in args.functional_annotation],
        args.functional_module_min_fraction,
        args.mag_id_mode,
    )
    if not functional.empty:
        write_table(functional, outdir / "functional" / f"{args.prefix}_functional_summary.tsv")
        module_table = functional.attrs.get("module_table")
        if isinstance(module_table, pd.DataFrame) and not module_table.empty:
            accepted_genomes = set(pairing.loc[pairing["accepted_paper_pair"], "mag_match_id"].dropna().astype(str))
            if accepted_genomes:
                module_table = module_table.loc[module_table["mag_match_id"].astype(str).isin(accepted_genomes)].copy()
            write_table(module_table, outdir / "functional" / f"{args.prefix}_functional_modules.tsv")

    agreement = abundance_agreement(
        pairing,
        Path(args.mag_abundance) if args.mag_abundance else None,
        Path(args.asv_counts) if args.asv_counts else None,
        args.mag_abundance_format,
        args.mag_abundance_genome_col,
        args.mag_abundance_sample_col,
        args.mag_abundance_value_col,
        args.mag_id_mode,
        args.min_shared_samples,
        args.abundance_transform,
    )
    if not agreement.empty:
        write_table(agreement, outdir / "abundance" / f"{args.prefix}_asv_mag_abundance_agreement.tsv")
        pairing = pairing.merge(agreement, on=["ASV_ID", "genome_id", "mag_match_id"], how="left")

    write_table(pairing, outdir / "validation" / f"{args.prefix}_taxonomy_validation.tsv")
    write_table(pairing.loc[pairing["mapping_class"].eq("ambiguous_match")], outdir / "mapping" / f"{args.prefix}_ambiguous_mappings.tsv")
    write_table(pairing.loc[pairing["accepted_paper_pair"]], outdir / "mapping" / f"{args.prefix}_accepted_mappings.tsv")

    if args.reference_catalog:
        ref = read_table(Path(args.reference_catalog))
        if not ref.empty:
            write_table(ref, outdir / "rrna" / f"{args.prefix}_mag_16s_reference_catalog.tsv")
            genome_ids = set(pairing["genome_id"].dropna().astype(str))
            lacking = ref.loc[~ref["genome_id"].astype(str).isin(genome_ids)].copy() if "genome_id" in ref.columns else pd.DataFrame()
            write_table(lacking, outdir / "rrna" / f"{args.prefix}_mags_without_accepted_asv.tsv")

    paper_nodes, paper_edges, paper_graph = make_paper_outputs(graph, node_features, pairing, functional)
    hetero_nodes, hetero_edges, hetero_graph = make_heterogeneous_outputs(graph, paper_nodes, paper_edges, pairing)

    write_table(paper_nodes, outdir / "network" / f"{args.prefix}_paper_nodes.tsv")
    write_table(paper_edges, outdir / "network" / f"{args.prefix}_paper_edges.tsv")
    nx.write_graphml(paper_graph, outdir / "network" / f"{args.prefix}_paper.graphml")
    (outdir / "network" / f"{args.prefix}_paper.cyjs").write_text(json.dumps(cytoscape_json(paper_nodes.rename(columns={"ASV_ID": "id"}), paper_edges), indent=2))

    write_table(hetero_nodes, outdir / "network" / f"{args.prefix}_heterogeneous_nodes.tsv")
    write_table(hetero_edges, outdir / "network" / f"{args.prefix}_heterogeneous_edges.tsv")
    nx.write_graphml(hetero_graph, outdir / "network" / f"{args.prefix}_heterogeneous.graphml")
    (outdir / "network" / f"{args.prefix}_heterogeneous.cyjs").write_text(json.dumps(cytoscape_json(hetero_nodes, hetero_edges), indent=2))

    summary = pairing.groupby("mapping_class", dropna=False)["ASV_ID"].nunique().reset_index(name="n_asvs")
    summary["fraction_asvs"] = summary["n_asvs"] / max(1, pairing["ASV_ID"].nunique())
    write_table(summary, outdir / "qc" / f"{args.prefix}_summary.tsv")
    plot_summary(summary, outdir)

    params = {
        "graph": str(graph_path),
        "node_features": args.node_features,
        "asv_mag_pairing": args.asv_mag_pairing,
        "taxonomy": args.taxonomy,
        "min_pident": args.min_pident,
        "min_qcov": args.min_qcov,
        "asv_taxonomy_source": args.asv_taxonomy_source,
        "mag_taxonomy_source": args.mag_taxonomy_source,
        "mag_abundance": args.mag_abundance,
        "mag_id_mode": args.mag_id_mode,
        "mag_abundance_format": args.mag_abundance_format,
        "mag_abundance_value_col": args.mag_abundance_value_col,
        "min_shared_samples": args.min_shared_samples,
        "abundance_transform": args.abundance_transform,
        "functional_annotation": args.functional_annotation,
        "functional_module_min_fraction": args.functional_module_min_fraction,
        "note": "SPIEC-EASI edges are ASV-only statistical associations. MAG edges are sequence-derived annotation/validation edges.",
    }
    (outdir / "qc" / f"{args.prefix}_parameters.json").write_text(json.dumps(params, indent=2))
    info(f"ASV_MAG_NETWORK wrote outputs to {outdir}")


if __name__ == "__main__":
    main()
