#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def read_table(path: str, sep: str | None = None) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    if sep is None:
        sep = "\t" if p.suffix.lower() in {".tsv", ".tab", ".txt"} else ","
    return pd.read_csv(p, sep=sep, low_memory=False)


def normalize_asv_id(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.split(";", n=1).str[0]


def collapse_text(values: pd.Series, limit: int = 10) -> str:
    uniq = []
    for value in values.dropna():
        text = str(value).strip()
        if not text or text.lower() == "nan":
            continue
        if text not in uniq:
            uniq.append(text)
    if not uniq:
        return ""
    if len(uniq) > limit:
        return "|".join(uniq[:limit]) + "|..."
    return "|".join(uniq)


def extract_rank(taxon: str, prefix: str) -> str:
    text = str(taxon).strip()
    if not text or text.lower() == "nan":
        return ""
    prefix = f"{prefix}__"
    for part in text.split(";"):
        token = part.strip()
        if token.startswith(prefix):
            return token[len(prefix):].strip()
    return ""


def rank_within_module(df: pd.DataFrame, metric: str) -> pd.Series:
    return (
        df.groupby("module_label")[metric]
        .rank(method="dense", ascending=False, na_option="bottom")
        .astype("Int64")
    )


def top_asvs(group: pd.DataFrame, metric: str, top_n: int = 3) -> str:
    sub = group.sort_values([metric, "Taxon"], ascending=[False, True]).head(top_n)
    return collapse_text(sub["Taxon"], limit=top_n)


def top_taxonomy(group: pd.DataFrame, metric: str, tax_col: str, top_n: int = 3) -> str:
    if tax_col not in group.columns:
        return ""
    sub = group.sort_values([metric, "Taxon"], ascending=[False, True]).head(top_n)
    return collapse_text(sub[tax_col], limit=top_n)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Summarize SPIEC-EASI modules with MAG links and ASV anchors.")
    ap.add_argument("--modules", required=True, help="spieceasi_modules_all.tsv")
    ap.add_argument("--node-features", required=True, help="spieceasi_node_features.csv")
    ap.add_argument("--asv-mag-pairing", required=True, help="asv2mag_pairing.tsv")
    ap.add_argument("--asv-counts", required=True, help="ASV count table used for module scoring")
    ap.add_argument("--metadata", default=None, help="Optional sample metadata for sorting and annotation")
    ap.add_argument("--sample-col", default="sampleID", help="Sample column in metadata")
    ap.add_argument("--sample-code-col", default="sample_code", help="Optional sample code column in metadata")
    ap.add_argument("--best-stats", default=None, help="Optional network_modules_best_stats_all.tsv")
    ap.add_argument("--taxonomy", default=None, help="Optional taxonomy table with Feature ID / Taxon columns")
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument("--top-n", type=int, default=3, help="Top N anchors to summarize per module")
    return ap.parse_args()


def load_asv_counts(path: str) -> pd.DataFrame:
    counts = read_table(path, sep="\t")
    if counts.empty:
        return pd.DataFrame()
    if "ASV_ID" in counts.columns:
        counts["ASV_ID"] = normalize_asv_id(counts["ASV_ID"])
        counts = counts.drop_duplicates(subset=["ASV_ID"]).set_index("ASV_ID")
    else:
        counts.index = normalize_asv_id(pd.Series(counts.index, index=counts.index))
        counts = counts[~counts.index.duplicated(keep="first")]
    return counts.apply(pd.to_numeric, errors="coerce").fillna(0.0)


def compute_module_sample_matrix(counts: pd.DataFrame, modules: pd.DataFrame) -> pd.DataFrame:
    if counts.empty or modules.empty:
        return pd.DataFrame()
    rel = counts.div(counts.sum(axis=0).replace(0, np.nan), axis=1).fillna(0.0)
    rows = []
    for module_label, grp in modules.groupby("module_label", sort=True):
        taxa = grp["Taxon"].astype(str).tolist()
        sub = rel.loc[rel.index.intersection(taxa)]
        if sub.empty:
            continue
        rows.append(pd.Series(sub.sum(axis=0), name=module_label))
    if not rows:
        return pd.DataFrame()
    mat = pd.DataFrame(rows)
    mat.index.name = "module_label"
    return mat.sort_index()


def build_sample_module_tables(module_sample_matrix: pd.DataFrame, metadata: pd.DataFrame, sample_col: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if module_sample_matrix.empty:
        empty_long = pd.DataFrame(columns=[sample_col, "module_label", "module_score", "module_rank_within_sample", "is_top_module"])
        empty_top = pd.DataFrame(columns=[
            sample_col, "n_modules_present",
            "top_module", "top_module_score",
            "second_module", "second_module_score",
            "third_module", "third_module_score",
        ])
        return empty_long, empty_top, pd.DataFrame()

    long_df = (
        module_sample_matrix.transpose()
        .stack()
        .rename("module_score")
        .reset_index()
        .rename(columns={"level_0": sample_col, "module_label": "module_label"})
    )
    long_df["module_rank_within_sample"] = (
        long_df.groupby(sample_col)["module_score"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    long_df["is_top_module"] = long_df["module_rank_within_sample"].eq(1)
    long_df = long_df.sort_values([sample_col, "module_rank_within_sample", "module_label"])

    top_rows = []
    for sample_id, grp in long_df.groupby(sample_col, sort=True):
        grp = grp.sort_values(["module_score", "module_label"], ascending=[False, True]).reset_index(drop=True)
        row = {
            sample_col: sample_id,
            "n_modules_present": int((grp["module_score"] > 0).sum()),
            "top_module": grp.loc[0, "module_label"] if len(grp) > 0 else "",
            "top_module_score": float(grp.loc[0, "module_score"]) if len(grp) > 0 else 0.0,
            "second_module": grp.loc[1, "module_label"] if len(grp) > 1 else "",
            "second_module_score": float(grp.loc[1, "module_score"]) if len(grp) > 1 else 0.0,
            "third_module": grp.loc[2, "module_label"] if len(grp) > 2 else "",
            "third_module_score": float(grp.loc[2, "module_score"]) if len(grp) > 2 else 0.0,
        }
        top_rows.append(row)
    top_df = pd.DataFrame(top_rows)

    sample_order_df = top_df.copy()
    if not metadata.empty and sample_col in metadata.columns:
        meta = metadata.copy()
        meta[sample_col] = meta[sample_col].astype(str)
        sample_order_df[sample_col] = sample_order_df[sample_col].astype(str)
        sample_order_df = sample_order_df.merge(meta, on=sample_col, how="left")
        sort_cols = [c for c in ["top_module", "Depth", "Cruise", "sample_code", sample_col] if c in sample_order_df.columns]
    else:
        sort_cols = ["top_module", sample_col]
    sample_order_df = sample_order_df.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    return long_df, top_df, sample_order_df


def plot_module_sample_heatmap(module_sample_matrix: pd.DataFrame, sample_order_df: pd.DataFrame, sample_col: str, sample_code_col: str, out_prefix: Path) -> None:
    if module_sample_matrix.empty or sample_order_df.empty:
        return

    sample_order = [str(x) for x in sample_order_df[sample_col].tolist() if str(x) in module_sample_matrix.columns]
    if not sample_order:
        return
    plot_mat = module_sample_matrix.loc[:, sample_order]
    plot_mat = plot_mat.loc[plot_mat.sum(axis=1).sort_values(ascending=False).index]

    width = max(14.0, len(sample_order) * 0.12)
    height = max(6.0, plot_mat.shape[0] * 0.35)
    fig, ax = plt.subplots(figsize=(width, height))
    im = ax.imshow(plot_mat.to_numpy(), aspect="auto", interpolation="nearest", cmap="Greys", vmin=0)

    if len(sample_order) <= 80:
        label_source = sample_order_df.set_index(sample_col)
        xticklabels = []
        for sample_id in sample_order:
            if sample_code_col in label_source.columns and pd.notna(label_source.at[sample_id, sample_code_col]):
                xticklabels.append(str(label_source.at[sample_id, sample_code_col]))
            else:
                xticklabels.append(sample_id)
        ax.set_xticks(np.arange(len(sample_order)))
        ax.set_xticklabels(xticklabels, rotation=90, fontsize=7)
    else:
        ax.set_xticks([])

    ax.set_yticks(np.arange(plot_mat.shape[0]))
    ax.set_yticklabels(plot_mat.index.tolist(), fontsize=8)
    ax.set_xlabel("Samples")
    ax.set_ylabel("Modules")
    ax.set_title("Sample-by-Module Relative Abundance")

    top_modules = sample_order_df["top_module"].fillna("").astype(str).tolist()
    transitions = []
    for idx in range(1, len(top_modules)):
        if top_modules[idx] != top_modules[idx - 1]:
            transitions.append(idx - 0.5)
    for xpos in transitions:
        ax.axvline(x=xpos, color="white", linewidth=0.8, alpha=0.9)

    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Module score")
    fig.tight_layout()
    for ext in ("png", "pdf", "svg"):
        fig.savefig(f"{out_prefix}.{ext}", bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    modules = read_table(args.modules, sep="\t")
    node_features = read_table(args.node_features)
    pairing = read_table(args.asv_mag_pairing, sep="\t")
    best_stats = read_table(args.best_stats, sep="\t") if args.best_stats else pd.DataFrame()
    taxonomy = read_table(args.taxonomy, sep="\t") if args.taxonomy else pd.DataFrame()
    counts = load_asv_counts(args.asv_counts)
    metadata = read_table(args.metadata, sep="\t") if args.metadata else pd.DataFrame()

    if modules.empty:
        raise SystemExit("[ERROR] modules table is empty or missing")
    if node_features.empty:
        raise SystemExit("[ERROR] node features table is empty or missing")

    modules["Taxon"] = normalize_asv_id(modules["Taxon"])
    modules["module_label"] = modules["module_label"].astype(str).str.strip()
    modules = modules.dropna(subset=["Taxon", "module_label"]).drop_duplicates(subset=["Taxon"])

    node_features["Taxon"] = normalize_asv_id(node_features["Taxon"])
    keep_nf = ["Taxon", "Degree", "Betweenness", "Closeness", "EigenCentral"]
    for col in keep_nf[1:]:
        if col not in node_features.columns:
            node_features[col] = np.nan
        node_features[col] = pd.to_numeric(node_features[col], errors="coerce")
    node_features = node_features[keep_nf].drop_duplicates(subset=["Taxon"])

    module_asv = modules.merge(node_features, on="Taxon", how="left")

    if not taxonomy.empty and {"Feature ID", "Taxon"}.issubset(taxonomy.columns):
        tax = taxonomy[["Feature ID", "Taxon"]].copy().rename(
            columns={"Feature ID": "feature_id_raw", "Taxon": "taxonomy_string"}
        )
        tax["Taxon"] = normalize_asv_id(tax["feature_id_raw"])
        tax = tax.drop_duplicates(subset=["Taxon"])
        tax["taxonomy_string"] = tax["taxonomy_string"].fillna("").astype(str)
        for rank, col in [
            ("p", "tax_phylum"),
            ("c", "tax_class"),
            ("o", "tax_order"),
            ("f", "tax_family"),
            ("g", "tax_genus"),
            ("s", "tax_species"),
        ]:
            tax[col] = tax["taxonomy_string"].map(lambda value, r=rank: extract_rank(value, r))
        tax = tax.drop(columns=["feature_id_raw"])
        module_asv = module_asv.merge(tax, on="Taxon", how="left")
    else:
        module_asv["taxonomy_string"] = ""
        for col in ["tax_phylum", "tax_class", "tax_order", "tax_family", "tax_genus", "tax_species"]:
            module_asv[col] = ""

    if not pairing.empty and "ASV_ID" in pairing.columns:
        pairing["ASV_ID"] = normalize_asv_id(pairing["ASV_ID"])
        if "pairing_status" not in pairing.columns:
            pairing["pairing_status"] = np.where(pairing.get("genome_id").notna(), "paired", "unpaired")
        pairing = pairing.loc[pairing["pairing_status"].astype(str).str.lower() != "unpaired"].copy()
        asv_mag = (
            pairing.groupby("ASV_ID", dropna=False)
            .agg(
                has_mag_pair=("genome_id", lambda s: bool(s.notna().any()) if "genome_id" in pairing.columns else True),
                n_mag_links=("genome_id", "count") if "genome_id" in pairing.columns else ("ASV_ID", "count"),
                n_unique_mags=("genome_id", pd.Series.nunique) if "genome_id" in pairing.columns else ("ASV_ID", pd.Series.nunique),
                mag_genome_ids=("genome_id", collapse_text) if "genome_id" in pairing.columns else ("ASV_ID", collapse_text),
                mag_species=("mag_species", collapse_text) if "mag_species" in pairing.columns else ("ASV_ID", lambda s: ""),
                mag_genera=("mag_genus", collapse_text) if "mag_genus" in pairing.columns else ("ASV_ID", lambda s: ""),
                mag_phyla=("mag_phylum", collapse_text) if "mag_phylum" in pairing.columns else ("ASV_ID", lambda s: ""),
                pairing_statuses=("pairing_status", collapse_text),
            )
            .reset_index()
            .rename(columns={"ASV_ID": "Taxon"})
        )
    else:
        asv_mag = pd.DataFrame(columns=[
            "Taxon", "has_mag_pair", "n_mag_links", "n_unique_mags", "mag_genome_ids",
            "mag_species", "mag_genera", "mag_phyla", "pairing_statuses",
        ])

    module_asv = module_asv.merge(asv_mag, on="Taxon", how="left")
    module_asv["has_mag_pair"] = module_asv["has_mag_pair"].map(lambda value: bool(value) if pd.notna(value) else False)
    for col in ["n_mag_links", "n_unique_mags"]:
        module_asv[col] = pd.to_numeric(module_asv[col], errors="coerce").fillna(0).astype(int)
    for col in ["mag_genome_ids", "mag_species", "mag_genera", "mag_phyla", "pairing_statuses"]:
        if col not in module_asv.columns:
            module_asv[col] = ""
        module_asv[col] = module_asv[col].fillna("").astype(str)
    for col in ["taxonomy_string", "tax_phylum", "tax_class", "tax_order", "tax_family", "tax_genus", "tax_species"]:
        if col not in module_asv.columns:
            module_asv[col] = ""
        module_asv[col] = module_asv[col].fillna("").astype(str)

    module_asv["anchor_rank_degree"] = rank_within_module(module_asv, "Degree")
    module_asv["anchor_rank_eigencentral"] = rank_within_module(module_asv, "EigenCentral")
    module_asv["anchor_rank_betweenness"] = rank_within_module(module_asv, "Betweenness")
    module_asv["is_anchor_degree"] = module_asv["anchor_rank_degree"].le(args.top_n).fillna(False)
    module_asv["is_anchor_eigencentral"] = module_asv["anchor_rank_eigencentral"].le(args.top_n).fillna(False)
    module_asv["is_anchor_betweenness"] = module_asv["anchor_rank_betweenness"].le(args.top_n).fillna(False)
    module_asv["is_anchor_any"] = (
        module_asv["is_anchor_degree"] |
        module_asv["is_anchor_eigencentral"] |
        module_asv["is_anchor_betweenness"]
    )

    if not best_stats.empty and "module_label" in best_stats.columns:
        stats_keep = ["module_label"]
        for col in ["n_nodes", "mean_node_stability", "is_best"]:
            if col in best_stats.columns:
                stats_keep.append(col)
        module_asv = module_asv.merge(best_stats[stats_keep].drop_duplicates(subset=["module_label"]), on="module_label", how="left")
    else:
        module_asv["n_nodes"] = module_asv.groupby("module_label")["Taxon"].transform("nunique")
        module_asv["mean_node_stability"] = pd.to_numeric(module_asv["node_stability"], errors="coerce")
        module_asv["mean_node_stability"] = module_asv.groupby("module_label")["mean_node_stability"].transform("mean")
        module_asv["is_best"] = pd.NA

    module_asv = module_asv.sort_values(["module_label", "anchor_rank_degree", "anchor_rank_eigencentral", "Taxon"])

    summary_rows = []
    for module_label, group in module_asv.groupby("module_label", sort=True):
        mag_group = group[group["has_mag_pair"]].copy()
        summary_rows.append({
            "module_label": module_label,
            "module_id": collapse_text(group["module_id"]),
            "n_asvs": int(group["Taxon"].nunique()),
            "n_mag_linked_asvs": int(mag_group["Taxon"].nunique()),
            "frac_mag_linked_asvs": float(mag_group["Taxon"].nunique() / group["Taxon"].nunique()) if group["Taxon"].nunique() else 0.0,
            "n_unique_mags": int(mag_group["mag_genome_ids"].str.split("|", regex=False).explode().replace("", pd.NA).dropna().nunique()) if not mag_group.empty else 0,
            "mean_node_stability": float(pd.to_numeric(group["mean_node_stability"], errors="coerce").dropna().iloc[0]) if group["mean_node_stability"].notna().any() else np.nan,
            "is_best_module": group["is_best"].dropna().iloc[0] if group["is_best"].notna().any() else pd.NA,
            "anchor_asvs_degree": top_asvs(group, "Degree", args.top_n),
            "anchor_asvs_eigencentral": top_asvs(group, "EigenCentral", args.top_n),
            "anchor_asvs_betweenness": top_asvs(group, "Betweenness", args.top_n),
            "anchor_asv_taxonomy_degree": top_taxonomy(group, "Degree", "taxonomy_string", args.top_n),
            "anchor_asv_taxonomy_eigencentral": top_taxonomy(group, "EigenCentral", "taxonomy_string", args.top_n),
            "anchor_asv_taxonomy_betweenness": top_taxonomy(group, "Betweenness", "taxonomy_string", args.top_n),
            "anchor_mag_linked_asvs_degree": top_asvs(mag_group, "Degree", args.top_n) if not mag_group.empty else "",
            "anchor_mag_linked_asvs_eigencentral": top_asvs(mag_group, "EigenCentral", args.top_n) if not mag_group.empty else "",
            "module_taxonomy_strings": collapse_text(group["taxonomy_string"], limit=50),
            "module_phyla": collapse_text(group["tax_phylum"], limit=50),
            "module_genera": collapse_text(group["tax_genus"], limit=50),
            "module_species": collapse_text(group["tax_species"], limit=50),
            "mag_genome_ids": collapse_text(mag_group["mag_genome_ids"], limit=50) if not mag_group.empty else "",
            "mag_species": collapse_text(mag_group["mag_species"], limit=50) if not mag_group.empty else "",
            "mag_genera": collapse_text(mag_group["mag_genera"], limit=50) if not mag_group.empty else "",
            "mag_phyla": collapse_text(mag_group["mag_phyla"], limit=50) if not mag_group.empty else "",
        })

    module_summary = pd.DataFrame(summary_rows).sort_values(["n_mag_linked_asvs", "n_asvs", "module_label"], ascending=[False, False, True])

    module_sample_matrix = compute_module_sample_matrix(counts, modules)
    sample_module_scores, sample_top_modules, sample_order = build_sample_module_tables(
        module_sample_matrix,
        metadata,
        args.sample_col,
    )
    if not sample_order.empty:
        sample_top_modules = sample_order.copy()

    module_asv.to_csv(outdir / "module_asv_anchor_table.tsv", sep="\t", index=False)
    module_summary.to_csv(outdir / "module_mag_anchor_summary.tsv", sep="\t", index=False)
    sample_module_scores.to_csv(outdir / "sample_module_scores.tsv", sep="\t", index=False)
    sample_top_modules.to_csv(outdir / "sample_top_modules.tsv", sep="\t", index=False)

    matrix_out = module_sample_matrix.transpose()
    matrix_out.index.name = args.sample_col
    matrix_out.to_csv(outdir / "sample_module_score_matrix.tsv", sep="\t")
    if not module_sample_matrix.empty:
        plot_module_sample_heatmap(
            module_sample_matrix,
            sample_order,
            args.sample_col,
            args.sample_code_col,
            outdir / "sample_module_score_heatmap",
        )


if __name__ == "__main__":
    main()
