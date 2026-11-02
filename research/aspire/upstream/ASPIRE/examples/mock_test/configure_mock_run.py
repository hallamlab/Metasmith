#!/usr/bin/env python3
"""Create a portable ASPIRE configuration for a supplied mock dataset."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

import pandas as pd
import yaml


REQUIRED_DATASET_FILES = (
    "fastq_manifest.tsv",
    "sample_metadata.tsv",
    "chemistry.tsv",
    "references/mitochondria.fasta",
    "references/contaminants.fasta",
    "ground_truth_reference_filters.tsv",
    "ground_truth_feature_registry.tsv",
    "ground_truth_group_effects.tsv",
    "ground_truth_asv_chem.tsv",
    "ground_truth_network_modules.tsv",
)
REQUIRED_METADATA_COLUMNS = {"sample_id", "Participant_ID", "Case", "Type_Group", "lung_status", "batch"}
FASTQ_SUFFIXES = (".fastq.gz", ".fq.gz", ".fastq", ".fq")
THREAD_KEYS = {"threads", "ncores", "num_core", "num_cores", "n_core", "cpus", "conqur_num_core"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_checksums(dataset: Path) -> None:
    checksum_file = dataset / "checksums.tsv"
    if not checksum_file.is_file():
        return
    rows = checksum_file.read_text().splitlines()
    failures = []
    for index, line in enumerate(rows):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if index == 0 and fields[0].strip().lower() in {"relative_path", "path"}:
            continue
        if len(fields) < 2:
            failures.append(f"invalid row: {line}")
            continue
        path = dataset / fields[0]
        expected = fields[1].strip().lower()
        if not path.is_file() or sha256(path) != expected:
            failures.append(fields[0])
    if failures:
        raise SystemExit("Dataset checksum validation failed:\n  " + "\n  ".join(failures))


def read_manifest(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep="\t", comment="#")
    if "sample_id" not in frame.columns:
        frame = pd.read_csv(path, sep="\t", comment="#", header=None)
        frame.columns = ["sample_id", "fastq_r1", "fastq_r2"][: len(frame.columns)]
    if "fastq_r1" not in frame.columns:
        raise SystemExit(f"Manifest must contain sample_id and fastq_r1 columns: {path}")
    if "fastq_r2" not in frame.columns:
        frame["fastq_r2"] = ""
    frame = frame[["sample_id", "fastq_r1", "fastq_r2"]].fillna("")
    frame["sample_id"] = frame["sample_id"].astype(str)
    if frame["sample_id"].eq("").any() or frame["sample_id"].duplicated().any():
        raise SystemExit(f"Manifest contains empty or duplicate sample IDs: {path}")
    return frame


def resolve_fastq(raw_path: str, manifest_path: Path, dataset: Path, sample_id: str, read: str) -> Path:
    candidates = []
    if raw_path:
        supplied = Path(raw_path).expanduser()
        candidates.extend([supplied, manifest_path.parent / supplied, dataset / supplied])
    fastq_dir = dataset / "fastq"
    candidates.extend(
        path for path in fastq_dir.glob(f"{sample_id}*{read}*")
        if path.name.endswith(FASTQ_SUFFIXES)
    )
    existing = []
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate.is_file() and candidate not in existing:
            existing.append(candidate)
    if not existing:
        raise SystemExit(f"Could not resolve {read} FASTQ for sample '{sample_id}'")
    return existing[0]


def write_portable_manifest(dataset: Path, destination: Path) -> tuple[Path, set[str]]:
    source = dataset / "fastq_manifest.tsv"
    frame = read_manifest(source)
    rows = ["sample_id\tfastq_r1\tfastq_r2"]
    for row in frame.itertuples(index=False):
        r1 = resolve_fastq(str(row.fastq_r1), source, dataset, row.sample_id, "R1")
        r2 = resolve_fastq(str(row.fastq_r2), source, dataset, row.sample_id, "R2")
        rows.append(f"{row.sample_id}\t{r1}\t{r2}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(rows) + "\n")
    return destination.resolve(), set(frame["sample_id"])


def validate_tabular_inputs(dataset: Path, manifest_ids: set[str]) -> None:
    metadata = pd.read_csv(dataset / "sample_metadata.tsv", sep="\t")
    missing_columns = sorted(REQUIRED_METADATA_COLUMNS - set(metadata.columns))
    if missing_columns:
        raise SystemExit(f"Metadata is missing required columns: {', '.join(missing_columns)}")
    metadata_ids = set(metadata["sample_id"].dropna().astype(str))
    if len(metadata_ids) != len(metadata) or metadata_ids != manifest_ids:
        raise SystemExit(
            f"Manifest/metadata sample mismatch: manifest={len(manifest_ids)}, metadata={len(metadata_ids)}"
        )
    chemistry = pd.read_csv(dataset / "chemistry.tsv", sep="\t")
    if "sample_id" not in chemistry.columns:
        raise SystemExit("chemistry.tsv must contain a sample_id column")
    chemistry_ids = set(chemistry["sample_id"].dropna().astype(str))
    if not chemistry_ids or not chemistry_ids <= metadata_ids:
        raise SystemExit(
            f"Chemistry sample IDs must be a non-empty subset of metadata IDs: chemistry={len(chemistry_ids)}"
        )
    numeric = chemistry.drop(columns=["sample_id"]).select_dtypes(include="number")
    if numeric.shape[1] == 0:
        raise SystemExit("chemistry.tsv must contain at least one numeric VOC column")


def absolute_project_paths(value, project_dir: Path):
    if isinstance(value, dict):
        return {key: absolute_project_paths(item, project_dir) for key, item in value.items()}
    if isinstance(value, list):
        return [absolute_project_paths(item, project_dir) for item in value]
    if isinstance(value, str) and value.startswith("processes/"):
        return str((project_dir / value).resolve())
    return value


def default_thread_count() -> int:
    available = os.cpu_count() or 1
    return max(1, int(available * 0.8))


def apply_thread_defaults(value, threads: int):
    if isinstance(value, dict):
        updated = {}
        for key, item in value.items():
            if str(key) in THREAD_KEYS and isinstance(item, int):
                updated[key] = threads
            else:
                updated[key] = apply_thread_defaults(item, threads)
        return updated
    if isinstance(value, list):
        return [apply_thread_defaults(item, threads) for item in value]
    return value


def build_config(
    template: Path,
    dataset: Path,
    output: Path,
    runtime: Path,
    project_dir: Path,
    threads: int | None = None,
):
    threads = default_thread_count() if threads is None else threads
    if threads < 1:
        raise ValueError("threads must be at least 1")
    config = yaml.safe_load(template.read_text())
    config = absolute_project_paths(config, project_dir)
    config = apply_thread_defaults(config, threads)

    metadata = str((dataset / "sample_metadata.tsv").resolve())
    config["paths"].update(
        input_dir=str((dataset / "fastq").resolve()),
        manifest=str((dataset / "fastq_manifest.tsv").resolve()),
        output_dir=str(output.resolve()),
        runtime_dir=str(runtime.resolve()),
        keep_runtime_dir=True,
    )
    config["table_filter"]["script"] = str(
        (project_dir / "processes/filter_table/filter_ASV_table.py").resolve()
    )
    config["mito"]["mito_fasta"] = str((dataset / "references/mitochondria.fasta").resolve())
    config["mito"]["contaminant_fasta"] = str(
        (dataset / "references/contaminants.fasta").resolve()
    )
    for section in ("filter_counts", "sankey", "metadata_plots", "spieceasi"):
        config[section]["metadata"] = metadata
    config["voc_correlation"]["voc_table"] = str((dataset / "chemistry.tsv").resolve())
    palette = str((project_dir / "examples/metadata_palette.template.tsv").resolve())
    config["metadata_plots"]["palette_file"] = palette
    config["sankey"]["palette_file"] = palette
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, type=Path, help="Supplied ASPIRE mock dataset directory")
    parser.add_argument("--output", required=True, type=Path, help="Final ASPIRE output directory")
    parser.add_argument("--runtime", type=Path, help="Runtime directory; defaults to <output>/.aspire")
    parser.add_argument("--config-out", required=True, type=Path, help="Generated YAML path")
    parser.add_argument(
        "--threads",
        type=int,
        default=default_thread_count(),
        help="Thread/core count for generated mock config; defaults to 80%% of detected CPUs",
    )
    args = parser.parse_args()
    if args.threads < 1:
        raise SystemExit("--threads must be at least 1")

    dataset = args.dataset.expanduser().resolve()
    output = args.output.expanduser().resolve()
    runtime = args.runtime.expanduser().resolve() if args.runtime else output / ".aspire"
    project_dir = Path(__file__).resolve().parents[2]
    template = project_dir / "examples/mock.local.yml"

    missing = [str(dataset / relative) for relative in REQUIRED_DATASET_FILES if not (dataset / relative).is_file()]
    fastq_dir = dataset / "fastq"
    if not fastq_dir.is_dir() or not any(fastq_dir.glob("*.f*q*")):
        missing.append(f"{fastq_dir} (directory containing FASTQs)")
    if missing:
        raise SystemExit("Dataset is incomplete; missing:\n  " + "\n  ".join(missing))
    validate_checksums(dataset)
    if output == runtime:
        raise SystemExit("--output and --runtime must be different directories")

    portable_manifest = args.config_out.with_suffix(".manifest.tsv").resolve()
    portable_manifest, manifest_ids = write_portable_manifest(dataset, portable_manifest)
    validate_tabular_inputs(dataset, manifest_ids)
    config = build_config(template, dataset, output, runtime, project_dir, args.threads)
    config["paths"]["manifest"] = str(portable_manifest)
    args.config_out.parent.mkdir(parents=True, exist_ok=True)
    args.config_out.write_text(yaml.safe_dump(config, sort_keys=False))
    print(f"Wrote mock-run configuration: {args.config_out.resolve()}")
    print(f"Wrote mock-run manifest: {portable_manifest} ({len(manifest_ids)} samples)")
    print(f"Configured mock-run threads: {args.threads}")
    print(f"Run: ./run_asv_pipeline.sh {args.config_out.resolve()} --no-resume")


if __name__ == "__main__":
    main()
