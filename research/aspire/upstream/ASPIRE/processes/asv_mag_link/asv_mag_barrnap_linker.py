#!/usr/bin/env python3
"""
Link ASVs to genome/MAG barrnap outputs by aligning ASV sequences to 16S rRNA
references derived from barrnap result files.

Supported input modes:
1. GFF/GFF3 barrnap outputs + corresponding genome FASTA files
2. FASTA barrnap outputs that already contain extracted rRNA sequences

The script writes:
- references/barrnap_16s_reference_catalog.tsv
- references/barrnap_16s_sequences.fasta
- blast/asv_vs_barrnap_16s.tsv
- tables/asv2mag_pairing.tsv
- tables/asv2mag_summary.tsv
- tables/asv2mag_genome_summary.tsv
- plots/asv2mag_pairing_status.{png,svg}
- plots/asv2mag_top_genomes.{png,svg}
- plots/asv2mag_identity_vs_coverage.{png,svg}
"""

from __future__ import annotations

import argparse
import gzip
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

os.environ.setdefault("MPLCONFIGDIR", "/tmp/aspire_matplotlib")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord


FASTA_SUFFIXES = (
    ".fa",
    ".fna",
    ".fasta",
    ".fas",
    ".ffn",
    ".faa",
    ".fa.gz",
    ".fna.gz",
    ".fasta.gz",
    ".fas.gz",
    ".ffn.gz",
)
GFF_SUFFIXES = (".gff", ".gff3", ".gff.gz", ".gff3.gz")
RRNA_PATTERNS = (
    "16s",
    "16s_rrna",
    "16s ribosomal rna",
    "ssu",
    "small subunit",
    "small-subunit",
)


@dataclass
class ReferenceRecord:
    ref_id: str
    genome_id: str
    native_genome_id: str
    source_label: str
    genome_qc_dir: str
    source_type: str
    source_file: str
    seq_id: str
    start: int | None
    end: int | None
    strand: str | None
    length: int
    description: str
    sequence: str


@dataclass
class GenomeQcSource:
    source_label: str
    genome_qc_dir: Path | None
    barrnap_dir: Path
    genome_fasta_dir: Path | None
    allowed_genomes: set[str] | None
    exact_barrnap_map: dict[str, str]
    exact_genome_fasta_map: dict[str, Path]
    mag_metadata: pd.DataFrame
    multi_source: bool
    id_token_index: int | None


def info(msg: str) -> None:
    print(f"[i] {msg}")


def warn(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr)


def save_png_and_svg(path: Path) -> None:
    """Save the current matplotlib figure as PNG plus matching SVG."""
    plt.savefig(path, dpi=300)
    plt.savefig(path.with_suffix(".svg"))


def die(msg: str) -> None:
    raise SystemExit(msg)


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return path.open("r")


def open_fasta(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return path.open("r")


def normalize_existing_path_string(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    path = Path(text)
    try:
        if path.exists():
            return str(path.resolve())
    except OSError:
        pass
    return str(path)


def canonical_stem(path: Path) -> str:
    name = path.name
    for suffix in (".gz", ".gff3", ".gff", ".fasta", ".fna", ".fa", ".fas", ".ffn", ".faa"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    name = re.sub(r"\.(rRNA|rrna)$", "", name)
    name = re.sub(r"_(rRNA|rrna)$", "", name)
    name = re.sub(r"\.barrnap$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\.\d{3,}$", "", name)
    return name


def barrnap_stem(path: Path) -> str:
    name = path.name
    for suffix in (".gz", ".gff3", ".gff", ".fasta", ".fna", ".fa", ".fas", ".ffn", ".faa"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    name = re.sub(r"\.(rRNA|rrna)$", "", name)
    name = re.sub(r"_(rRNA|rrna)$", "", name)
    name = re.sub(r"\.barrnap$", "", name, flags=re.IGNORECASE)
    return name


def sanitize_token(text: str) -> str:
    out = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text).strip())
    out = re.sub(r"_+", "_", out).strip("_")
    return out or "unknown"


def normalize_join_id(value: object) -> str:
    if pd.isna(value):
        return ""
    text = sanitize_token(str(value))
    return text.lower()


def derive_native_genome_id_from_stem(stem: str, token_index: int | None = None, warn_label: str | None = None) -> str:
    if token_index is not None:
        parts = stem.split(".")
        if 0 <= token_index < len(parts):
            stem = parts[token_index]
        elif warn_label is not None:
            warn(f"Configured id_token_index={token_index} is out of range for {warn_label}; using canonical stem instead.")
    stem = re.sub(r"\.full$", "", stem, flags=re.IGNORECASE)
    return normalize_join_id(stem)


def derive_native_genome_id(path: Path, token_index: int | None = None) -> str:
    return derive_native_genome_id_from_stem(canonical_stem(path), token_index, warn_label=path.name)


def exact_match_key_from_stem(stem: str) -> str:
    return normalize_join_id(stem)


def exact_match_key(path: Path) -> str:
    return exact_match_key_from_stem(barrnap_stem(path))


def resolve_aligned_option(values: list[int] | None, n_sources: int, label: str) -> list[int | None]:
    if not values:
        return [None] * n_sources
    if len(values) == 1:
        value = values[0]
        return [None if value is None or value < 0 else value] * n_sources
    if len(values) != n_sources:
        die(f"{label} must be provided once or once per source (expected {n_sources}, got {len(values)}).")
    return [None if value is None or value < 0 else value for value in values]


def make_source_labels(paths: list[Path]) -> dict[Path, str]:
    cleaned_parts: dict[Path, list[str]] = {
        path: [sanitize_token(p) for p in path.parts if sanitize_token(p)]
        for path in paths
    }
    widths: dict[Path, int] = {path: 1 for path in paths}

    while True:
        label_groups: dict[str, list[Path]] = {}
        for path in paths:
            parts = cleaned_parts[path]
            width = min(widths[path], len(parts)) if parts else 0
            label = "_".join(parts[-width:]) if width else f"source_{paths.index(path)+1}"
            label_groups.setdefault(label, []).append(path)

        collisions = [group for group in label_groups.values() if len(group) > 1]
        if not collisions:
            return {
                path: ("_".join(cleaned_parts[path][-min(widths[path], len(cleaned_parts[path])):]) if cleaned_parts[path] else f"source_{idx+1}")
                for idx, path in enumerate(paths)
            }

        advanced = False
        for group in collisions:
            for path in group:
                parts = cleaned_parts[path]
                if widths[path] < len(parts):
                    widths[path] += 1
                    advanced = True
        if advanced:
            continue

        # Paths are still colliding even after consuming all path parts.
        result: dict[Path, str] = {}
        for idx, path in enumerate(paths, start=1):
            parts = cleaned_parts[path]
            label = "_".join(parts) if parts else f"source_{idx}"
            result[path] = f"{label}_{idx}"
        return result


def compose_genome_id(native_genome_id: str, source_label: str, multi_source: bool) -> str:
    native = sanitize_token(native_genome_id)
    if multi_source:
        return f"{source_label}::{native}"
    return native


def looks_like_rrna(text: str) -> bool:
    t = str(text).strip().lower()
    return any(p in t for p in RRNA_PATTERNS)


def parse_attributes(attr_text: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for field in attr_text.split(";"):
        field = field.strip()
        if not field:
            continue
        if "=" in field:
            key, value = field.split("=", 1)
        elif " " in field:
            key, value = field.split(" ", 1)
        else:
            key, value = field, ""
        attrs[key.strip()] = value.strip().strip('"')
    return attrs


def iter_paths(root: Path, suffixes: tuple[str, ...]) -> Iterator[Path]:
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        name = path.name.lower()
        if any(name.endswith(suf) for suf in suffixes):
            yield path


def index_genome_fastas(genome_dir: Path | None) -> dict[str, Path]:
    index: dict[str, Path] = {}
    if genome_dir is None:
        return index
    for path in iter_paths(genome_dir, FASTA_SUFFIXES):
        index[derive_native_genome_id(path)] = path
    return index


def match_genome_fasta(gff_path: Path, genome_index: dict[str, Path], token_index: int | None = None) -> Path | None:
    stem = derive_native_genome_id(gff_path, token_index)
    if stem in genome_index:
        return genome_index[stem]
    for key, value in genome_index.items():
        if stem == key or stem.startswith(key) or key.startswith(stem):
            return value
    return None


def autodetect_genome_fasta_dir(genome_qc_dir: Path) -> Path | None:
    candidates = [
        genome_qc_dir / "genome_atlas" / "rep_fastas",
        genome_qc_dir / "dedupe" / "fasta",
        genome_qc_dir / "genomes_subset",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def extract_rrna_from_gff(source: GenomeQcSource, skip_genomes: set[str] | None = None) -> list[ReferenceRecord]:
    barrnap_dir = source.barrnap_dir
    genome_dir = source.genome_fasta_dir
    allowed_genomes = source.allowed_genomes
    skip_genomes = skip_genomes or set()
    refs: list[ReferenceRecord] = []
    ref_counter = 0

    if genome_dir is None:
        return refs

    genome_index = index_genome_fastas(genome_dir)
    gff_paths: list[Path] = []
    for gff_path in iter_paths(barrnap_dir, GFF_SUFFIXES):
        native_genome_id = derive_native_genome_id(gff_path, source.id_token_index)
        if allowed_genomes is not None and native_genome_id not in allowed_genomes:
            continue
        genome_id = compose_genome_id(native_genome_id, source.source_label, source.multi_source)
        if genome_id in skip_genomes:
            continue
        gff_paths.append(gff_path)
    if not gff_paths:
        return refs

    info(f"Scanning {len(gff_paths)} barrnap GFF files for fallback 16S/SSU recovery.")
    for gff_path in gff_paths:
        exact_key = exact_match_key(gff_path)
        matched_genome_id = source.exact_barrnap_map.get(exact_key)
        if source.exact_barrnap_map and matched_genome_id is None:
            continue
        if matched_genome_id is not None:
            genome_fasta = source.exact_genome_fasta_map.get(matched_genome_id)
        else:
            genome_fasta = match_genome_fasta(gff_path, genome_index, source.id_token_index)
        if genome_fasta is None:
            warn(f"No matching genome FASTA found for barrnap GFF: {gff_path}")
            continue

        if matched_genome_id is not None:
            genome_id = matched_genome_id
            native_genome_id = genome_id.split("::", 1)[1] if "::" in genome_id else genome_id
        else:
            native_genome_id = derive_native_genome_id(gff_path, source.id_token_index)
            genome_id = compose_genome_id(native_genome_id, source.source_label, source.multi_source)
        seq_records = SeqIO.to_dict(SeqIO.parse(open_fasta(genome_fasta), "fasta"))
        found_any = False

        with open_text(gff_path) as handle:
            for line in handle:
                if not line or line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 9:
                    continue
                seq_id, gff_source, feature_type, start, end, score, strand, phase, attrs = parts
                attrs_map = parse_attributes(attrs)
                feature_text = " ".join(
                    [
                        feature_type,
                        attrs_map.get("Name", ""),
                        attrs_map.get("product", ""),
                        attrs_map.get("note", ""),
                    ]
                )
                if not looks_like_rrna(feature_text):
                    continue
                if seq_id not in seq_records:
                    warn(f"Sequence '{seq_id}' from {gff_path.name} not found in genome FASTA {genome_fasta.name}")
                    continue

                start_i = int(start)
                end_i = int(end)
                seq = seq_records[seq_id].seq[start_i - 1 : end_i]
                if strand == "-":
                    seq = seq.reverse_complement()
                seq_str = str(seq).upper()
                if not seq_str:
                    continue

                ref_counter += 1
                ref_id = f"ref{ref_counter:06d}"
                desc = attrs_map.get("product") or attrs_map.get("Name") or feature_type
                refs.append(
                    ReferenceRecord(
                        ref_id=ref_id,
                        genome_id=genome_id,
                        native_genome_id=native_genome_id,
                        source_label=source.source_label,
                        genome_qc_dir=str(source.genome_qc_dir) if source.genome_qc_dir is not None else "",
                        source_type="gff",
                        source_file=str(gff_path),
                        seq_id=seq_id,
                        start=start_i,
                        end=end_i,
                        strand=strand,
                        length=len(seq_str),
                        description=desc,
                        sequence=seq_str,
                    )
                )
                found_any = True
        if not found_any:
            warn(f"No 16S/SSU rRNA features extracted from {gff_path}")
    return refs


def collect_rrna_fastas(source: GenomeQcSource) -> list[ReferenceRecord]:
    barrnap_dir = source.barrnap_dir
    allowed_genomes = source.allowed_genomes
    refs: list[ReferenceRecord] = []
    ref_counter = 0
    fasta_paths = [
        path
        for path in iter_paths(barrnap_dir, FASTA_SUFFIXES)
        if "rrna" in path.name.lower() or "barrnap" in path.name.lower()
    ]
    if not fasta_paths:
        return refs

    info(f"Scanning {len(fasta_paths)} barrnap FASTA-like files for reference sequences.")
    for fasta_path in fasta_paths:
        exact_key = exact_match_key(fasta_path)
        matched_genome_id = source.exact_barrnap_map.get(exact_key)
        if source.exact_barrnap_map and matched_genome_id is None:
            continue
        if matched_genome_id is not None:
            genome_id = matched_genome_id
            native_genome_id = genome_id.split("::", 1)[1] if "::" in genome_id else genome_id
        else:
            native_genome_id = derive_native_genome_id(fasta_path, source.id_token_index)
            if allowed_genomes is not None and native_genome_id not in allowed_genomes:
                continue
            genome_id = compose_genome_id(native_genome_id, source.source_label, source.multi_source)
        try:
            with open_fasta(fasta_path) as handle:
                records = list(SeqIO.parse(handle, "fasta"))
        except Exception as exc:
            warn(f"Skipping unreadable FASTA file {fasta_path}: {exc}")
            continue

        found_any = False
        for record in records:
            if not looks_like_rrna(record.description):
                continue
            seq_str = str(record.seq).upper()
            if not seq_str:
                continue
            ref_counter += 1
            refs.append(
                ReferenceRecord(
                    ref_id=f"ref{ref_counter:06d}",
                    genome_id=genome_id,
                    native_genome_id=native_genome_id,
                    source_label=source.source_label,
                    genome_qc_dir=str(source.genome_qc_dir) if source.genome_qc_dir is not None else "",
                    source_type="fasta",
                    source_file=str(fasta_path),
                    seq_id=record.id,
                    start=None,
                    end=None,
                    strand=None,
                    length=len(seq_str),
                    description=record.description,
                    sequence=seq_str,
                )
            )
            found_any = True
        if not found_any and records:
            warn(f"No 16S/SSU rRNA sequences retained from barrnap FASTA: {fasta_path}")
    return refs


def write_reference_outputs(refs: list[ReferenceRecord], outdir: Path) -> tuple[Path, Path]:
    ref_dir = outdir / "references"
    ref_dir.mkdir(parents=True, exist_ok=True)
    fasta_path = ref_dir / "barrnap_16s_sequences.fasta"
    catalog_path = ref_dir / "barrnap_16s_reference_catalog.tsv"

    seq_records: list[SeqRecord] = []
    catalog_rows: list[dict[str, object]] = []
    for idx, ref in enumerate(refs, start=1):
        ref_id = f"ref{idx:06d}"
        seq_records.append(
            SeqRecord(
                Seq(ref.sequence),
                id=ref_id,
                description=f"{ref.genome_id} {sanitize_token(ref.seq_id)}",
            )
        )
        catalog_rows.append(
            {
                "ref_id": ref_id,
                "genome_id": ref.genome_id,
                "genome_uid": normalize_join_id(ref.genome_id),
                "native_genome_id": ref.native_genome_id,
                "source_label": ref.source_label,
                "genome_qc_dir": ref.genome_qc_dir,
                "source_type": ref.source_type,
                "source_file": ref.source_file,
                "seq_id": ref.seq_id,
                "start": ref.start,
                "end": ref.end,
                "strand": ref.strand,
                "length": ref.length,
                "description": ref.description,
            }
        )

    with fasta_path.open("w") as handle:
        SeqIO.write(seq_records, handle, "fasta")
    pd.DataFrame(catalog_rows).to_csv(catalog_path, sep="\t", index=False)
    info(f"Wrote {len(refs)} barrnap reference sequences.")
    return fasta_path, catalog_path


def read_asv_ids(asv_fasta: Path) -> list[str]:
    ids: list[str] = []
    with open_fasta(asv_fasta) as handle:
        for record in SeqIO.parse(handle, "fasta"):
            ids.append(record.id)
    return ids


def require_tool(name: str) -> str:
    path = shutil.which(name)
    if not path:
        die(f"Required executable not found in PATH: {name}")
    return path


def run_blast(
    asv_fasta: Path,
    ref_fasta: Path,
    outdir: Path,
    threads: int,
) -> Path:
    blast_dir = outdir / "blast"
    blast_dir.mkdir(parents=True, exist_ok=True)
    db_prefix = blast_dir / "barrnap_16s_db"
    blast_out = blast_dir / "asv_vs_barrnap_16s.tsv"
    query_fasta = blast_dir / "asv_query.fasta"

    makeblastdb = require_tool("makeblastdb")
    blastn = require_tool("blastn")

    if asv_fasta.suffix == ".gz":
        with gzip.open(asv_fasta, "rt") as src, query_fasta.open("w") as dst:
            shutil.copyfileobj(src, dst)
    else:
        query_fasta = asv_fasta

    subprocess.run(
        [
            makeblastdb,
            "-in",
            str(ref_fasta),
            "-dbtype",
            "nucl",
            "-parse_seqids",
            "-out",
            str(db_prefix),
        ],
        check=True,
    )

    outfmt = "6 qseqid sseqid pident length qlen slen mismatch gapopen qstart qend sstart send evalue bitscore"
    subprocess.run(
        [
            blastn,
            "-query",
            str(query_fasta),
            "-db",
            str(db_prefix),
            "-outfmt",
            outfmt,
            "-num_threads",
            str(max(1, threads)),
            "-out",
            str(blast_out),
        ],
        check=True,
    )
    return blast_out


def load_blast_hits(blast_path: Path, catalog_path: Path) -> pd.DataFrame:
    cols = [
        "ASV_ID",
        "ref_id",
        "pident",
        "align_length",
        "qlen",
        "slen",
        "mismatch",
        "gapopen",
        "qstart",
        "qend",
        "sstart",
        "send",
        "evalue",
        "bitscore",
    ]
    if blast_path.stat().st_size == 0:
        return pd.DataFrame(columns=cols + ["qcov", "scov", "genome_id"])
    hits = pd.read_csv(blast_path, sep="\t", header=None, names=cols)
    hits["qcov"] = 100.0 * hits["align_length"] / hits["qlen"].replace(0, pd.NA)
    hits["scov"] = 100.0 * hits["align_length"] / hits["slen"].replace(0, pd.NA)
    catalog = pd.read_csv(catalog_path, sep="\t")
    if "length" in catalog.columns:
        catalog = catalog.rename(columns={"length": "ref_length"})
    return hits.merge(catalog, on="ref_id", how="left")


def discover_mag_metadata_path(genome_qc_dir: Path | None, barrnap_dir: Path) -> Path | None:
    candidates: list[Path] = []
    if genome_qc_dir is not None:
        candidates.extend(
            [
                genome_qc_dir / "genome_quality_atlas" / "genome_quality_annotated.tsv",
                genome_qc_dir / "Master_genome_QC.atlas.tsv",
                genome_qc_dir / "Master_genome_QC.tsv",
            ]
        )
    inferred_root = barrnap_dir.parent if barrnap_dir.name.lower() == "barrnap" else barrnap_dir
    candidates.extend(
        [
            inferred_root / "genome_quality_atlas" / "genome_quality_annotated.tsv",
            inferred_root / "Master_genome_QC.atlas.tsv",
            inferred_root / "Master_genome_QC.tsv",
        ]
    )
    seen: set[Path] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        if path.exists() and path.is_file() and path.stat().st_size > 0:
            return path
    return None


def load_mag_metadata(
    genome_qc_dir: Path | None,
    barrnap_dir: Path,
    source_label: str = "source",
    multi_source: bool = False,
    id_token_index: int | None = None,
) -> tuple[pd.DataFrame, Path | None]:
    path = discover_mag_metadata_path(genome_qc_dir, barrnap_dir)
    if path is None:
        warn("No MAG metadata table found; enriched ASV-MAG summary tables will not be written.")
        return pd.DataFrame(), None

    info(f"Loading MAG metadata from: {path}")
    meta = pd.read_csv(path, sep="\t", low_memory=False)
    if meta.empty:
        warn(f"MAG metadata table is empty: {path}")
        return pd.DataFrame(), path

    join_col = None
    for candidate in ("Genome_Id", "Bin Id", "genome_id"):
        if candidate in meta.columns:
            join_col = candidate
            break
    if join_col is None:
        warn(f"MAG metadata table lacks a joinable genome identifier column: {path}")
        return pd.DataFrame(), path

    meta = meta.copy()
    meta["mag_native_genome_id_raw"] = meta[join_col].astype(str)
    meta["mag_native_genome_id"] = meta[join_col].map(lambda x: derive_native_genome_id_from_stem(str(x), id_token_index))
    meta["genome_id"] = meta["mag_native_genome_id"].map(lambda x: compose_genome_id(x, source_label, multi_source))
    meta["mag_genome_uid"] = meta["genome_id"].map(normalize_join_id)
    meta = meta.loc[meta["genome_id"] != ""].drop_duplicates(subset=["genome_id"], keep="first")

    preferred = [
        "Genome_Id",
        "Bin Id",
        "Completeness",
        "Contamination",
        "Strain heterogeneity",
        "num_seqs",
        "sum_len",
        "N50",
        "qscore",
        "pass_BARRNAP",
        "contains_16S",
        "has_16S",
        "16S_rRNA",
        "rrna_16S_score",
        "mimag_tier",
        "integrity_score",
        "recoverability_score",
        "mimag_quality_index",
        "recovered_feature_count",
        "recovery_pattern_label",
        "Domain",
        "Phylum",
        "Class",
        "Order",
        "Family",
        "Genus",
        "Species",
        "sample",
        "category",
        "fasta_path",
        "source_dir",
    ]
    keep = ["genome_id", "mag_genome_uid", "mag_native_genome_id", "mag_native_genome_id_raw"] + [col for col in preferred if col in meta.columns]
    meta = meta.loc[:, keep]
    rename_map = {}
    for col in meta.columns:
        if col in {"genome_id", "mag_genome_uid", "mag_native_genome_id", "mag_native_genome_id_raw"}:
            continue
        rename_map[col] = f"mag_{sanitize_token(col).lower()}"
    meta = meta.rename(columns=rename_map)
    meta["mag_source_label"] = source_label
    meta["mag_genome_qc_dir"] = str(genome_qc_dir) if genome_qc_dir is not None else ""
    meta["mag_metadata_source"] = str(path)
    return meta, path


def prepare_mag_metadata_frame(
    meta: pd.DataFrame,
    metadata_source: Path | str,
    source_label: str = "source",
    multi_source: bool = False,
    id_token_index: int | None = None,
    genome_qc_dir: Path | None = None,
) -> pd.DataFrame:
    join_col = None
    for candidate in ("Genome_Id", "Bin Id", "genome_id"):
        if candidate in meta.columns:
            join_col = candidate
            break
    if join_col is None:
        warn(f"MAG metadata table lacks a joinable genome identifier column: {metadata_source}")
        return pd.DataFrame()

    meta = meta.copy()
    meta["mag_native_genome_id_raw"] = meta[join_col].astype(str)
    meta["mag_native_genome_id"] = meta[join_col].map(lambda x: derive_native_genome_id_from_stem(str(x), id_token_index))
    meta["genome_id"] = meta["mag_native_genome_id"].map(lambda x: compose_genome_id(x, source_label, multi_source))
    meta["mag_genome_uid"] = meta["genome_id"].map(normalize_join_id)
    meta = meta.loc[meta["genome_id"] != ""].drop_duplicates(subset=["genome_id"], keep="first")

    preferred = [
        "Genome_Id", "Bin Id", "Completeness", "Contamination", "Strain heterogeneity",
        "num_seqs", "sum_len", "N50", "qscore", "pass_BARRNAP", "contains_16S",
        "has_16S", "16S_rRNA", "rrna_16S_score", "mimag_tier", "integrity_score",
        "recoverability_score", "mimag_quality_index", "recovered_feature_count",
        "recovery_pattern_label", "Domain", "Phylum", "Class", "Order", "Family",
        "Genus", "Species", "sample", "category", "fasta_path", "fasta_path_normalized",
        "copied_fasta_path", "copied_fasta_path_normalized", "ani_fasta_path",
        "ani_fasta_path_normalized", "source_dir", "source_dir_normalized",
    ]
    keep = ["genome_id", "mag_genome_uid", "mag_native_genome_id", "mag_native_genome_id_raw"] + [col for col in preferred if col in meta.columns]
    meta = meta.loc[:, keep]
    rename_map = {}
    for col in meta.columns:
        if col in {"genome_id", "mag_genome_uid", "mag_native_genome_id", "mag_native_genome_id_raw"}:
            continue
        rename_map[col] = f"mag_{sanitize_token(col).lower()}"
    meta = meta.rename(columns=rename_map)
    meta["mag_source_label"] = source_label
    meta["mag_genome_qc_dir"] = str(genome_qc_dir) if genome_qc_dir is not None else ""
    meta["mag_metadata_source"] = str(metadata_source)
    return meta


def load_master_metadata(master_tsv: Path) -> pd.DataFrame:
    info(f"Loading selected-set metadata from: {master_tsv}")
    meta = pd.read_csv(master_tsv, sep="\t", low_memory=False)
    if meta.empty:
        warn(f"Selected-set metadata table is empty: {master_tsv}")
        return pd.DataFrame()

    required_cols = ["source_dir", "copied_fasta_path"]
    missing = [col for col in required_cols if col not in meta.columns]
    if missing:
        die(f"Selected-set metadata is missing required columns {missing}: {master_tsv}")

    meta = meta.copy()
    meta["source_dir_normalized"] = meta["source_dir"].map(normalize_existing_path_string)
    meta["copied_fasta_path_normalized"] = meta["copied_fasta_path"].map(normalize_existing_path_string)
    if "fasta_path" in meta.columns:
        meta["fasta_path_normalized"] = meta["fasta_path"].map(normalize_existing_path_string)
    if "ani_fasta_path" in meta.columns:
        meta["ani_fasta_path_normalized"] = meta["ani_fasta_path"].map(normalize_existing_path_string)

    return meta


def build_exact_barrnap_map(mag_metadata: pd.DataFrame) -> tuple[dict[str, str], dict[str, Path]]:
    exact_map: dict[str, str] = {}
    exact_genome_fastas: dict[str, Path] = {}
    if mag_metadata.empty:
        return exact_map, exact_genome_fastas

    eligible = mag_metadata.copy()
    if "mag_eligible_for_linking" in eligible.columns:
        eligible = eligible.loc[truthy_series(eligible["mag_eligible_for_linking"])].copy()

    candidate_cols = [c for c in ("mag_bin_id", "mag_genome_id") if c in eligible.columns]
    candidate_path_cols = [
        c
        for c in (
            "mag_copied_fasta_path_normalized",
            "mag_copied_fasta_path",
            "mag_fasta_path_normalized",
            "mag_fasta_path",
            "mag_ani_fasta_path_normalized",
            "mag_ani_fasta_path",
        )
        if c in eligible.columns
    ]
    for _, row in eligible.iterrows():
        genome_id = str(row.get("genome_id", "")).strip()
        if not genome_id:
            continue
        for col in candidate_cols:
            value = row.get(col)
            if pd.isna(value):
                continue
            key = exact_match_key_from_stem(str(value))
            if key:
                exact_map[key] = genome_id
        for path_col in candidate_path_cols:
            fasta_path = row.get(path_col)
            if pd.isna(fasta_path):
                continue
            fasta_file = Path(str(fasta_path))
            if fasta_file.exists():
                exact_genome_fastas[genome_id] = fasta_file
                key = exact_match_key_from_stem(fasta_file.stem)
                if key:
                    exact_map[key] = genome_id
                break

    return exact_map, exact_genome_fastas


def truthy_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    numeric = pd.to_numeric(series, errors="coerce")
    numeric_truth = numeric.notna() & (numeric != 0)
    lowered = series.astype(str).str.strip().str.lower()
    text_truth = lowered.isin({"true", "t", "1", "1.0", "yes", "y", "pass", "passed"})
    return numeric_truth | text_truth


def build_sources(
    barrnap_dirs: list[Path],
    genome_fasta_dirs: list[Path] | None,
    genome_qc_dirs: list[Path] | None,
    id_token_indexes: list[int] | None,
    master_tsv: Path | None = None,
) -> list[GenomeQcSource]:
    barrnap_dirs = [p.resolve() for p in barrnap_dirs]
    genome_qc_dirs = [p.resolve() for p in (genome_qc_dirs or [])]
    genome_fasta_dirs = [p.resolve() for p in (genome_fasta_dirs or [])]

    sources: list[GenomeQcSource] = []
    if master_tsv is not None:
        if barrnap_dirs or genome_qc_dirs or genome_fasta_dirs:
            die("--master-tsv cannot be combined with --barrnap-dir, --genome-fasta-dir, or --genome-qc-dir.")

        master_meta = load_master_metadata(master_tsv.resolve())
        if master_meta.empty:
            die(f"No rows found in selected-set metadata: {master_tsv}")

        source_dir_values = [Path(p) for p in master_meta["source_dir_normalized"].dropna().astype(str).tolist() if p]
        unique_source_dirs = list(dict.fromkeys(source_dir_values))
        n_sources = len(unique_source_dirs)
        multi_source = n_sources > 1
        source_id_token_indexes = resolve_aligned_option(id_token_indexes, n_sources, "id_token_indexes")
        source_labels = make_source_labels(unique_source_dirs)

        for idx, source_dir in enumerate(unique_source_dirs):
            barrnap_dir = source_dir / "barrnap"
            if not barrnap_dir.exists() or not barrnap_dir.is_dir():
                die(f"Selected-set source_dir is missing barrnap/: {source_dir}")

            source_meta = master_meta.loc[master_meta["source_dir_normalized"] == str(source_dir)].copy()
            source_label = source_labels[source_dir]
            mag_metadata = prepare_mag_metadata_frame(
                meta=source_meta,
                metadata_source=master_tsv,
                source_label=source_label,
                multi_source=multi_source,
                id_token_index=source_id_token_indexes[idx],
                genome_qc_dir=source_dir,
            )
            if mag_metadata.empty:
                warn(f"[{source_label}] No usable genome metadata rows found in {master_tsv}; skipping source.")
                continue

            if "mag_copied_fasta_path" in mag_metadata.columns:
                mag_metadata["mag_in_final_fasta_set"] = mag_metadata["mag_copied_fasta_path"].map(
                    lambda value: bool(str(value).strip()) and str(value).strip().lower() != "nan"
                )
            else:
                mag_metadata["mag_in_final_fasta_set"] = False

            if "mag_pass_barrnap" in mag_metadata.columns:
                mag_metadata["mag_eligible_for_linking"] = mag_metadata["mag_in_final_fasta_set"] & truthy_series(
                    mag_metadata["mag_pass_barrnap"]
                )
            else:
                mag_metadata["mag_eligible_for_linking"] = mag_metadata["mag_in_final_fasta_set"]

            allowed_genomes = set(
                mag_metadata.loc[truthy_series(mag_metadata["mag_eligible_for_linking"]), "mag_native_genome_id"]
                .dropna()
                .astype(str)
            )
            exact_barrnap_map, exact_genome_fastas = build_exact_barrnap_map(mag_metadata)
            if exact_barrnap_map:
                info(f"[{source_label}] Using {len(exact_barrnap_map)} representative-specific barrnap keys from selected-set metadata.")

            sources.append(
                GenomeQcSource(
                    source_label=source_label,
                    genome_qc_dir=source_dir,
                    barrnap_dir=barrnap_dir,
                    genome_fasta_dir=None,
                    allowed_genomes=allowed_genomes,
                    exact_barrnap_map=exact_barrnap_map,
                    exact_genome_fasta_map=exact_genome_fastas,
                    mag_metadata=mag_metadata,
                    multi_source=multi_source,
                    id_token_index=source_id_token_indexes[idx],
                )
            )

        return sources

    n_sources = len(genome_qc_dirs) if genome_qc_dirs else len(barrnap_dirs)
    multi_source = n_sources > 1
    source_id_token_indexes = resolve_aligned_option(id_token_indexes, n_sources, "id_token_indexes")

    if genome_qc_dirs:
        source_labels = make_source_labels(genome_qc_dirs)
        for idx, qc_dir in enumerate(genome_qc_dirs):
            barrnap_dir = qc_dir / "barrnap"
            if not barrnap_dir.exists() or not barrnap_dir.is_dir():
                die(f"Genome QC directory is missing barrnap/: {qc_dir}")
            genome_dir = autodetect_genome_fasta_dir(qc_dir)
            if genome_dir is None:
                warn(f"No genome FASTA directory autodetected under {qc_dir}; GFF fallback will be skipped.")
            source_label = source_labels[qc_dir]
            mag_metadata, _ = load_mag_metadata(
                qc_dir,
                barrnap_dir,
                source_label=source_label,
                multi_source=multi_source,
                id_token_index=source_id_token_indexes[idx],
            )

            allowed_genomes = None
            if genome_dir is not None:
                allowed_genomes = set(index_genome_fastas(genome_dir).keys())
                info(f"[{source_label}] Restricting barrnap references to {len(allowed_genomes)} genomes present in: {genome_dir}")
            if not mag_metadata.empty and "mag_pass_barrnap" in mag_metadata.columns:
                passed_genomes = set(
                    mag_metadata.loc[truthy_series(mag_metadata["mag_pass_barrnap"]), "mag_native_genome_id"]
                    .dropna()
                    .astype(str)
                )
                allowed_genomes = passed_genomes if allowed_genomes is None else (allowed_genomes & passed_genomes)
                info(f"[{source_label}] Restricting barrnap references to {len(allowed_genomes)} genomes that pass genome_qc barrnap checks.")
            if not mag_metadata.empty:
                mag_metadata = mag_metadata.copy()
                if allowed_genomes is not None:
                    mag_metadata["mag_in_final_fasta_set"] = mag_metadata["mag_native_genome_id"].isin(allowed_genomes)
                else:
                    mag_metadata["mag_in_final_fasta_set"] = pd.NA
                if "mag_pass_barrnap" in mag_metadata.columns:
                    pass_mask = truthy_series(mag_metadata["mag_pass_barrnap"])
                    if allowed_genomes is not None:
                        mag_metadata["mag_eligible_for_linking"] = mag_metadata["mag_native_genome_id"].isin(allowed_genomes) & pass_mask
                    else:
                        mag_metadata["mag_eligible_for_linking"] = pass_mask
                else:
                    mag_metadata["mag_eligible_for_linking"] = mag_metadata["mag_in_final_fasta_set"]

            exact_barrnap_map, exact_genome_fastas = build_exact_barrnap_map(mag_metadata)
            if exact_barrnap_map:
                info(f"[{source_label}] Using {len(exact_barrnap_map)} representative-specific barrnap keys from genome QC metadata.")

            sources.append(
                GenomeQcSource(
                    source_label=source_label,
                    genome_qc_dir=qc_dir,
                    barrnap_dir=barrnap_dir,
                    genome_fasta_dir=genome_dir,
                    allowed_genomes=allowed_genomes,
                    exact_barrnap_map=exact_barrnap_map,
                    exact_genome_fasta_map=exact_genome_fastas,
                    mag_metadata=mag_metadata,
                    multi_source=multi_source,
                    id_token_index=source_id_token_indexes[idx],
                )
            )
    else:
        if not barrnap_dirs:
            die("Provide at least one --barrnap-dir or one --genome-qc-dir.")
        if genome_fasta_dirs and len(genome_fasta_dirs) not in {1, len(barrnap_dirs)}:
            die("--genome-fasta-dir must be provided once or once per --barrnap-dir.")
        label_paths = [barrnap_dir.parent if barrnap_dir.parent else barrnap_dir for barrnap_dir in barrnap_dirs]
        source_labels = make_source_labels(label_paths)
        for idx, barrnap_dir in enumerate(barrnap_dirs):
            genome_dir = None
            if genome_fasta_dirs:
                genome_dir = genome_fasta_dirs[0] if len(genome_fasta_dirs) == 1 else genome_fasta_dirs[idx]
            label_path = barrnap_dir.parent if barrnap_dir.parent else barrnap_dir
            source_label = source_labels[label_path]
            sources.append(
                GenomeQcSource(
                    source_label=source_label,
                    genome_qc_dir=None,
                    barrnap_dir=barrnap_dir,
                    genome_fasta_dir=genome_dir,
                    allowed_genomes=set(index_genome_fastas(genome_dir).keys()) if genome_dir is not None else None,
                    exact_barrnap_map={},
                    exact_genome_fasta_map={},
                    mag_metadata=pd.DataFrame(),
                    multi_source=multi_source,
                    id_token_index=source_id_token_indexes[idx],
                )
            )
    return sources


def summarize_pairings(
    asv_ids: list[str],
    hits: pd.DataFrame,
    ref_catalog: pd.DataFrame,
    mag_metadata: pd.DataFrame,
    min_pident: float,
    min_qcov: float,
    outdir: Path,
) -> None:
    tables_dir = outdir / "tables"
    plots_dir = outdir / "plots"
    tables_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    if hits.empty:
        pairing = pd.DataFrame({"ASV_ID": asv_ids})
        pairing["asv_uid"] = pairing["ASV_ID"].map(sanitize_token)
        pairing["pairing_status"] = "unpaired"
        pairing["genome_id"] = pd.NA
        pairing["genome_uid"] = pd.NA
        if not mag_metadata.empty:
            empty_pairing_meta = pairing.copy()
            empty_pairing_meta.to_csv(tables_dir / "asv2mag_pairing.tsv", sep="\t", index=False)
        else:
            pairing.to_csv(tables_dir / "asv2mag_pairing.tsv", sep="\t", index=False)
        pd.DataFrame(
            [{"pairing_status": "unpaired", "n_asvs": len(asv_ids)}]
        ).to_csv(tables_dir / "asv2mag_summary.tsv", sep="\t", index=False)
        ref_summary = (
            ref_catalog.groupby("genome_id", dropna=False)
            .agg(
                n_retained_refs=("ref_id", "nunique"),
                source_label=("source_label", "first"),
                native_genome_id=("native_genome_id", "first"),
                genome_qc_dir=("genome_qc_dir", "first"),
            )
            .reset_index()
        )
        ref_summary["has_retained_ref"] = ref_summary["n_retained_refs"] > 0
        ref_summary["genome_uid"] = ref_summary["genome_id"].map(normalize_join_id)
        if not mag_metadata.empty:
            genome_summary = mag_metadata.merge(ref_summary, on="genome_id", how="outer")
            genome_summary["n_best_asvs"] = 0
            genome_summary["n_unique_asvs"] = 0
            genome_summary["n_ambiguous_asvs"] = 0
            genome_summary["median_pident"] = pd.NA
            genome_summary["median_qcov"] = pd.NA
            genome_summary["median_bitscore"] = pd.NA
            genome_summary["source_type"] = pd.NA
            genome_summary["source_file"] = pd.NA
            genome_summary.to_csv(tables_dir / "asv2mag_genome_summary.tsv", sep="\t", index=False)
        else:
            ref_summary["n_best_asvs"] = 0
            ref_summary["n_unique_asvs"] = 0
            ref_summary["n_ambiguous_asvs"] = 0
            ref_summary["median_pident"] = pd.NA
            ref_summary["median_qcov"] = pd.NA
            ref_summary["median_bitscore"] = pd.NA
            ref_summary["source_type"] = pd.NA
            ref_summary["source_file"] = pd.NA
            ref_summary.to_csv(tables_dir / "asv2mag_genome_summary.tsv", sep="\t", index=False)
        plot_pairing_status(pd.DataFrame({"pairing_status": ["unpaired"], "n_asvs": [len(asv_ids)]}), plots_dir)
        return

    hits = hits.copy()
    hits["passes_filters"] = (hits["pident"] >= min_pident) & (hits["qcov"] >= min_qcov)
    filtered = hits.loc[hits["passes_filters"]].copy()
    filtered.sort_values(
        by=["ASV_ID", "bitscore", "pident", "qcov", "evalue", "align_length"],
        ascending=[True, False, False, False, True, False],
        inplace=True,
    )
    filtered["rank_within_asv"] = filtered.groupby("ASV_ID").cumcount() + 1

    pair_rows: list[dict[str, object]] = []
    grouped = {k: v.copy() for k, v in filtered.groupby("ASV_ID", sort=False)}
    for asv_id in asv_ids:
        grp = grouped.get(asv_id)
        if grp is None or grp.empty:
            pair_rows.append(
                {
                    "ASV_ID": asv_id,
                    "asv_uid": sanitize_token(asv_id),
                    "pairing_status": "unpaired",
                    "genome_id": pd.NA,
                    "genome_uid": pd.NA,
                    "link_pident": pd.NA,
                    "link_qcov": pd.NA,
                    "link_bitscore": pd.NA,
                    "n_hits_supporting_link": 0,
                    "n_refs_supporting_link": 0,
                    "n_candidate_genomes": 0,
                }
            )
            continue

        candidate_genomes = sorted({str(x) for x in grp["genome_id"].dropna() if str(x).strip()})
        status = "paired_unique" if len(candidate_genomes) == 1 else "paired_ambiguous"
        per_genome = (
            grp.groupby("genome_id", dropna=False)
            .agg(
                link_pident=("pident", "max"),
                link_qcov=("qcov", "max"),
                link_bitscore=("bitscore", "max"),
                n_hits_supporting_link=("ref_id", "size"),
                n_refs_supporting_link=("ref_id", "nunique"),
                source_label=("source_label", "first"),
                native_genome_id=("native_genome_id", "first"),
                genome_qc_dir=("genome_qc_dir", "first"),
            )
            .reset_index()
        )
        per_genome["ASV_ID"] = asv_id
        per_genome["asv_uid"] = sanitize_token(asv_id)
        per_genome["pairing_status"] = status
        per_genome["n_candidate_genomes"] = len(candidate_genomes)
        pair_rows.extend(per_genome.to_dict(orient="records"))

    pairing = pd.DataFrame(pair_rows)
    pairing["genome_uid"] = pairing["genome_id"].map(lambda x: normalize_join_id(x) if pd.notna(x) and str(x).strip() else pd.NA)
    if not mag_metadata.empty:
        pairing_with_meta = pairing.merge(mag_metadata, on="genome_id", how="left")
        pairing_with_meta.to_csv(tables_dir / "asv2mag_pairing.tsv", sep="\t", index=False)
    else:
        pairing.to_csv(tables_dir / "asv2mag_pairing.tsv", sep="\t", index=False)

    summary = (
        pairing.groupby("pairing_status", dropna=False)["ASV_ID"]
        .nunique()
        .reset_index(name="n_asvs")
        .sort_values("n_asvs", ascending=False)
    )
    summary["fraction_asvs"] = summary["n_asvs"] / max(1, len(asv_ids))
    summary.to_csv(tables_dir / "asv2mag_summary.tsv", sep="\t", index=False)

    genome_summary_hits = (
        pairing.loc[pairing["pairing_status"] != "unpaired"].dropna(subset=["genome_id"])
        .groupby("genome_id", dropna=False)
        .agg(
            n_best_asvs=("ASV_ID", "nunique"),
            median_pident=("link_pident", "median"),
            median_qcov=("link_qcov", "median"),
            median_bitscore=("link_bitscore", "median"),
        )
        .reset_index()
    )
    unique_counts = (
        pairing.loc[pairing["pairing_status"] == "paired_unique", ["genome_id", "ASV_ID"]]
        .dropna()
        .groupby("genome_id")["ASV_ID"]
        .nunique()
        .reset_index(name="n_unique_asvs")
    )
    ambiguous_counts = (
        pairing.loc[pairing["pairing_status"] == "paired_ambiguous", ["genome_id", "ASV_ID"]]
        .dropna()
        .groupby("genome_id")["ASV_ID"]
        .nunique()
        .reset_index(name="n_ambiguous_asvs")
    )
    genome_summary_hits = genome_summary_hits.merge(unique_counts, on="genome_id", how="left")
    genome_summary_hits = genome_summary_hits.merge(ambiguous_counts, on="genome_id", how="left")
    genome_summary_hits[["n_unique_asvs", "n_ambiguous_asvs"]] = genome_summary_hits[
        ["n_unique_asvs", "n_ambiguous_asvs"]
    ].fillna(0).astype(int)
    ref_summary = (
        ref_catalog.groupby("genome_id", dropna=False)
        .agg(
            n_retained_refs=("ref_id", "nunique"),
            source_label=("source_label", "first"),
            native_genome_id=("native_genome_id", "first"),
            genome_qc_dir=("genome_qc_dir", "first"),
        )
        .reset_index()
    )
    ref_summary["has_retained_ref"] = ref_summary["n_retained_refs"] > 0
    ref_summary["genome_uid"] = ref_summary["genome_id"].map(normalize_join_id)

    if not mag_metadata.empty:
        genome_summary = mag_metadata.merge(ref_summary, on="genome_id", how="outer")
    else:
        genome_summary = ref_summary.copy()

    genome_summary = genome_summary.merge(genome_summary_hits, on="genome_id", how="outer")
    for col in ["n_retained_refs", "n_best_asvs", "n_unique_asvs", "n_ambiguous_asvs"]:
        if col in genome_summary.columns:
            genome_summary[col] = genome_summary[col].fillna(0).astype(int)
    if "has_retained_ref" in genome_summary.columns:
        genome_summary["has_retained_ref"] = genome_summary["has_retained_ref"].fillna(False).astype(bool)
    genome_summary.sort_values(
        ["n_best_asvs", "n_retained_refs", "median_bitscore"],
        ascending=[False, False, False],
        inplace=True,
        na_position="last",
    )
    genome_summary.to_csv(tables_dir / "asv2mag_genome_summary.tsv", sep="\t", index=False)

    plot_pairing_status(summary, plots_dir)
    plot_top_genomes(genome_summary, plots_dir)
    plot_identity_vs_coverage(pairing, plots_dir)


def plot_pairing_status(summary: pd.DataFrame, plots_dir: Path) -> None:
    if summary.empty:
        return
    plt.figure(figsize=(6, 4))
    order = summary.sort_values("n_asvs", ascending=False)["pairing_status"]
    ax = sns.barplot(data=summary, x="pairing_status", y="n_asvs", order=order, color="#4C78A8")
    ax.set_xlabel("Pairing status")
    ax.set_ylabel("Number of ASVs")
    ax.set_title("ASV-to-genome pairing status")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    save_png_and_svg(plots_dir / "asv2mag_pairing_status.png")
    plt.close()


def plot_top_genomes(genome_summary: pd.DataFrame, plots_dir: Path, top_n: int = 20) -> None:
    if genome_summary.empty or "n_best_asvs" not in genome_summary.columns:
        return
    plot_df = genome_summary.loc[genome_summary["n_best_asvs"] > 0].head(top_n).copy()
    if plot_df.empty:
        return
    plt.figure(figsize=(8, max(4, 0.35 * len(plot_df))))
    ax = sns.barplot(data=plot_df, y="genome_id", x="n_best_asvs", color="#72B7B2")
    ax.set_xlabel("ASVs paired")
    ax.set_ylabel("Genome/MAG")
    ax.set_title("Top genome/MAG pairings")
    plt.tight_layout()
    save_png_and_svg(plots_dir / "asv2mag_top_genomes.png")
    plt.close()


def plot_identity_vs_coverage(pairing: pd.DataFrame, plots_dir: Path) -> None:
    plot_df = pairing.dropna(subset=["link_pident", "link_qcov", "genome_id"]).copy()
    if plot_df.empty:
        return
    plt.figure(figsize=(6, 5))
    ax = sns.scatterplot(
        data=plot_df,
        x="link_qcov",
        y="link_pident",
        hue="genome_id",
        legend=False,
        s=40,
        alpha=0.8,
    )
    ax.set_xlabel("Query coverage (%)")
    ax.set_ylabel("Percent identity")
    ax.set_title("Best ASV-to-genome alignments")
    plt.tight_layout()
    save_png_and_svg(plots_dir / "asv2mag_identity_vs_coverage.png")
    plt.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Link ASVs to barrnap-derived 16S references and summarize ASV-to-genome pairings."
    )
    p.add_argument("--asv-fasta", required=True, type=Path, help="Filtered ASV FASTA (optionally gzipped).")
    p.add_argument(
        "--barrnap-dir",
        dest="barrnap_dirs",
        action="append",
        type=Path,
        help="Directory containing barrnap outputs. Use once per source if not using --genome-qc-dir.",
    )
    p.add_argument(
        "--genome-fasta-dir",
        dest="genome_fasta_dirs",
        action="append",
        type=Path,
        default=[],
        help="Genome/MAG FASTA directory used with barrnap GFF outputs. Can be supplied once or once per barrnap source.",
    )
    p.add_argument(
        "--genome-qc-dir",
        dest="genome_qc_dirs",
        action="append",
        type=Path,
        default=[],
        help="Genome QC result directory. When provided, the script autodetects barrnap/, genome_atlas/rep_fastas (preferred), dedupe/fasta, or genomes_subset, plus MAG metadata. Can be supplied multiple times.",
    )
    p.add_argument(
        "--id-token-index",
        dest="id_token_indexes",
        action="append",
        type=int,
        default=[],
        help="Optional token index used to derive representative genome IDs from barrnap filenames after splitting the canonical stem on '.'. Provide once or once per source in the same order as genome_qc_dirs/barrnap_dirs.",
    )
    p.add_argument(
        "--master-tsv",
        type=Path,
        help="Selected-set master.tsv with source_dir and copied_fasta_path columns. Mutually exclusive with --barrnap-dir, --genome-fasta-dir, and --genome-qc-dir.",
    )
    p.add_argument("--outdir", required=True, type=Path, help="Output directory.")
    p.add_argument("--threads", type=int, default=1, help="Threads for blastn.")
    p.add_argument("--min-pident", type=float, default=97.0, help="Minimum percent identity.")
    p.add_argument("--min-qcov", type=float, default=90.0, help="Minimum query coverage (%).")
    p.add_argument("--top-n", type=int, default=5, help="Top hits per ASV to retain in the all-hits table.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    sources = build_sources(
        barrnap_dirs=args.barrnap_dirs or [],
        genome_fasta_dirs=args.genome_fasta_dirs or [],
        genome_qc_dirs=args.genome_qc_dirs or [],
        id_token_indexes=args.id_token_indexes or [],
        master_tsv=args.master_tsv,
    )
    mag_metadata_frames = [src.mag_metadata for src in sources if not src.mag_metadata.empty]
    mag_metadata = pd.concat(mag_metadata_frames, ignore_index=True) if mag_metadata_frames else pd.DataFrame()

    refs: list[ReferenceRecord] = []
    for source in sources:
        fasta_refs = collect_rrna_fastas(source)
        seen_genomes = {ref.genome_id for ref in fasta_refs}
        gff_refs = extract_rrna_from_gff(source, skip_genomes=seen_genomes)
        refs.extend(fasta_refs)
        refs.extend(gff_refs)
    if not refs:
        die(
            "No barrnap reference sequences were recovered. Provide barrnap FASTA outputs or "
            "barrnap GFF/GFF3 outputs plus genome FASTA directories, or provide one or more --genome-qc-dir roots."
        )

    ref_fasta, catalog_path = write_reference_outputs(refs, args.outdir)
    blast_path = run_blast(args.asv_fasta, ref_fasta, args.outdir, args.threads)
    hits = load_blast_hits(blast_path, catalog_path)
    asv_ids = read_asv_ids(args.asv_fasta)
    summarize_pairings(
        asv_ids=asv_ids,
        hits=hits,
        ref_catalog=pd.read_csv(catalog_path, sep="\t"),
        mag_metadata=mag_metadata,
        min_pident=args.min_pident,
        min_qcov=args.min_qcov,
        outdir=args.outdir,
    )
    info(f"Done. Outputs written to: {args.outdir}")


if __name__ == "__main__":
    main()
