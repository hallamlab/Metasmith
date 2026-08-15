#!/usr/bin/env python3
"""Validate an ASPIRE mock run against its dataset and publication contract."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import sys
from pathlib import Path

import pandas as pd


class Audit:
    def __init__(self):
        self.failures: list[str] = []

    def check(self, condition: bool, label: str, detail: str) -> None:
        status = "PASS" if condition else "FAIL"
        print(f"[{status}] {label}: {detail}")
        if not condition:
            self.failures.append(f"{label}: {detail}")


def read_table(path: Path, **kwargs) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_csv(path, sep="\t", **kwargs)


def sample_ids_from_manifest(path: Path) -> set[str]:
    frame = pd.read_csv(path, sep="\t", comment="#")
    if "sample_id" in frame.columns:
        return set(frame["sample_id"].dropna().astype(str))
    frame = pd.read_csv(path, sep="\t", comment="#", header=None)
    return set(frame.iloc[:, 0].dropna().astype(str))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def reverse_complement(sequence: str) -> str:
    return sequence.translate(str.maketrans("ACGTN", "TGCAN"))[::-1]


def read_fasta(path: Path) -> dict[str, str]:
    opener = gzip.open if path.suffix == ".gz" else open
    records: dict[str, str] = {}
    identifier = None
    sequence: list[str] = []
    with opener(path, "rt") as handle:
        for line in handle:
            line = line.strip()
            if line.startswith(">"):
                if identifier is not None:
                    records[identifier] = "".join(sequence).upper()
                identifier = line[1:].split(";", 1)[0].split()[0]
                sequence = []
            elif line:
                sequence.append(line)
    if identifier is not None:
        records[identifier] = "".join(sequence).upper()
    return records


def map_inferred_to_truth(dataset: Path, results: Path) -> dict[str, str]:
    registry = read_table(dataset / "ground_truth_feature_registry.tsv")
    truth_sequences = {
        str(row.ASV_ID): str(row.v4_sequence).upper()
        for row in registry[["ASV_ID", "v4_sequence"]].itertuples(index=False)
    }
    inferred = read_fasta(results / "intermediates/ASVs/ASVs.fasta.gz")
    mapping: dict[str, str] = {}
    for inferred_id, sequence in inferred.items():
        oriented = (sequence, reverse_complement(sequence))
        matches = [
            truth_id for truth_id, truth_sequence in truth_sequences.items()
            if any(candidate in truth_sequence or truth_sequence in candidate for candidate in oriented)
        ]
        if len(matches) == 1:
            mapping[inferred_id] = matches[0]
    return mapping


def true_values(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--results", required=True, type=Path)
    args = parser.parse_args()
    dataset = args.dataset.expanduser().resolve()
    results = args.results.expanduser().resolve()
    if not dataset.is_dir():
        raise SystemExit(f"Mock dataset directory not found: {dataset}")
    if not results.is_dir():
        raise SystemExit(f"Completed ASPIRE output directory not found: {results}")
    modules = results / "modules"
    audit = Audit()

    required_files = (
        "summary/tables/run_manifest.tsv",
        "summary/tables/module_output_manifest.tsv",
        "summary/tables/nextflow_artifact_manifest.tsv",
        "intermediates/ASVs/ASVs.fasta.gz",
        "modules/non_target_filtering/tables/ASV_target.tsv",
        "modules/non_target_filtering/tables/mitomap/nontarget.master.tsv",
        "modules/taxonomy/tables/ASV_SILVA_tax.full-length.vsearch.tsv",
        "modules/voc_correlation/tables/asv_voc_spearman_long.tsv",
        "modules/network_analysis/tables/spieceasi_edge_list.csv",
        "modules/network_analysis/tables/spieceasi_modules_all.tsv",
        "modules/non_target_filtering/plots/mitomap/nontarget_non_target_cumulative.svg",
        "modules/outlier_detection/plots/outliers_Case_summary.svg",
        "modules/umap_clustering/plots/umap_clustering_type_group.svg",
        "summary/plots/module_output_summary.svg",
        "summary/report/ASPIRE_run_report.html",
        "logs/controller.log",
        "logs/task_execution.tsv",
        "logs/nextflow_report.html",
        "logs/nextflow_timeline.html",
        "logs/nextflow_trace.tsv",
        "logs/nextflow_dag.html",
        "logs/launch_command.txt",
        "logs/nextflow_version.txt",
    )
    missing_files = [relative for relative in required_files if not (results / relative).is_file()]
    if missing_files:
        raise SystemExit("Completed output is missing required files:\n  " + "\n  ".join(missing_files))

    required_modules = {
        "batch_correction", "bubbleplotter", "clustermaps", "collectors_curve",
        "diversity", "general_stats", "indicator_analysis", "lung_status_analysis",
        "metadata_plots", "network_analysis", "non_target_filtering",
        "outlier_detection", "power_analysis", "sankey", "taxonomy",
        "umap_clustering", "upset", "voc_correlation",
    }
    missing_modules = sorted(name for name in required_modules if not (modules / name).is_dir())
    audit.check(not missing_modules, "publication_layout", f"missing modules={missing_modules or 'none'}")

    supplied = sample_ids_from_manifest(dataset / "fastq_manifest.tsv")
    metadata = read_table(dataset / "sample_metadata.tsv")
    published = sample_ids_from_manifest(results / "summary/tables/run_manifest.tsv")
    metadata_ids = set(metadata["sample_id"].astype(str))
    audit.check(
        supplied == metadata_ids == published,
        "sample_accounting",
        f"manifest={len(supplied)}, metadata={len(metadata_ids)}, published={len(published)}",
    )

    truth = read_table(dataset / "ground_truth_reference_filters.tsv")
    nontarget = read_table(modules / "non_target_filtering/tables/mitomap/nontarget.master.tsv")
    final = read_table(modules / "non_target_filtering/tables/ASV_target.tsv")
    final_ids = set(final.iloc[:, 0].astype(str))
    identified: set[str] = set()
    for column in ("BF_ID", "MI_Accession"):
        if column in nontarget:
            identified.update(nontarget[column].dropna().astype(str))
    truth_ids = set(truth["ASV_ID"].astype(str))
    identified_rows = nontarget[nontarget.apply(
        lambda row: any(str(row.get(column, "")) in truth_ids for column in ("BF_ID", "MI_Accession")), axis=1
    )]
    removed = set(identified_rows["Sequence_ID"].astype(str)).isdisjoint(final_ids)
    audit.check(
        truth_ids <= identified and removed,
        "non_target_truth",
        f"identified={len(truth_ids & identified)}/{len(truth_ids)}, excluded_from_micro={removed}",
    )

    taxonomy = read_table(modules / "taxonomy/tables/ASV_SILVA_tax.full-length.vsearch.tsv")
    tax_col = "Taxon" if "Taxon" in taxonomy else taxonomy.columns[1]
    n_tax = int(taxonomy[tax_col].fillna("").astype(str).str.strip().ne("").sum())
    audit.check(n_tax > 0, "taxonomy", f"assigned ASVs={n_tax}")

    id_map = map_inferred_to_truth(dataset, results)
    audit.check(bool(id_map), "truth_id_mapping", f"mapped inferred ASVs={len(id_map)}")

    indicator_dir = modules / "indicator_analysis/tables"
    indicator_files = [indicator_dir / "Type_Group_indicator_species_summary.tsv", indicator_dir / "Case_indicator_species_summary.tsv"]
    significant = 0
    significant_ids: set[str] = set()
    for path in indicator_files:
        if path.is_file():
            frame = read_table(path)
            if "significant" in frame:
                frame = frame[true_values(frame["significant"])]
                significant += len(frame)
                significant_ids.update(frame["ASV"].astype(str))
    group_truth = read_table(dataset / "ground_truth_group_effects.tsv")
    significant_truth = {id_map[item] for item in significant_ids if item in id_map}
    recovered_indicators = significant_truth & set(group_truth["ASV_ID"].astype(str))
    audit.check(
        bool(recovered_indicators),
        "indicator_analysis",
        f"significant={significant}, implanted_recovered={len(recovered_indicators)}",
    )

    voc = read_table(modules / "voc_correlation/tables/asv_voc_spearman_long.tsv")
    positive = voc[voc["rho"] >= 0.30].copy() if "rho" in voc else voc.iloc[0:0].copy()
    positive["truth_asv"] = positive["ASV_ID"].astype(str).map(id_map)
    chemistry_truth = read_table(dataset / "ground_truth_asv_chem.tsv")
    chemistry_truth = chemistry_truth[chemistry_truth["direction"].astype(str).str.lower() == "positive"]
    truth_pairs = set(zip(chemistry_truth["ASV_ID"].astype(str), chemistry_truth["compound"].astype(str)))
    recovered_voc = {
        (str(row.truth_asv), str(row.voc)) for row in positive.itertuples(index=False)
        if (str(row.truth_asv), str(row.voc)) in truth_pairs
    }
    audit.check(
        bool(recovered_voc),
        "voc_correlation",
        f"rho>=0.30 pairs={len(positive)}, implanted_positive_recovered={len(recovered_voc)}",
    )

    edge_path = modules / "network_analysis/tables/spieceasi_edge_list.csv"
    edges = pd.read_csv(edge_path) if edge_path.is_file() else pd.DataFrame()
    module_path = modules / "network_analysis/tables/spieceasi_modules_all.tsv"
    assignments = read_table(module_path) if module_path.is_file() else pd.DataFrame()
    max_module = int(assignments.groupby("module_label").size().max()) if not assignments.empty else 0
    network_truth = read_table(dataset / "ground_truth_network_modules.tsv")
    truth_module = dict(zip(network_truth["ASV_ID"].astype(str), network_truth["module"].astype(str)))
    within_truth = 0
    for row in edges.itertuples(index=False):
        left = id_map.get(str(row.Taxon1))
        right = id_map.get(str(row.Taxon2))
        if left in truth_module and right in truth_module and truth_module[left] == truth_module[right]:
            within_truth += 1
    audit.check(
        len(edges) >= 10 and max_module >= 2 and within_truth >= 1,
        "network_analysis",
        f"edges={len(edges)}, largest_module={max_module}, implanted_within_module_edges={within_truth}",
    )

    plot_modules = (
        "clustermaps", "diversity", "indicator_analysis", "network_analysis",
        "non_target_filtering", "outlier_detection", "sankey", "taxonomy", "voc_correlation",
    )
    missing_svgs = [name for name in plot_modules if not any((modules / name / "plots").rglob("*.svg"))]
    audit.check(not missing_svgs, "plot_contract", f"modules without SVG={missing_svgs or 'none'}")

    manifest_path = results / "summary/tables/module_output_manifest.tsv"
    manifest = read_table(manifest_path)
    bad_manifest = []
    for row in manifest.itertuples(index=False):
        path = results / str(row.relative_path)
        if not path.is_file() or sha256(path) != str(row.sha256):
            bad_manifest.append(str(row.relative_path))
    audit.check(not bad_manifest, "manifest_integrity", f"invalid entries={len(bad_manifest)}")

    print(f"\nValidated dataset with {len(supplied)} samples against {results}")
    if audit.failures:
        print("Validation failed:", file=sys.stderr)
        for failure in audit.failures:
            print(f"  - {failure}", file=sys.stderr)
        raise SystemExit(1)
    print("All mock-run checks passed.")


if __name__ == "__main__":
    main()
