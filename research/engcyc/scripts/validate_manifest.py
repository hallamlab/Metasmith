"""Check data/engcyc/manifest.tsv against SCHEMAS.md, and any file tables (manifest.csv) given as
arguments against the received-data schema. Exits non-zero on any error.

Warnings (an n_samples that differs from its table's row count) do not fail the check,
because a source table may count runs or plate copies rather than samples.
"""
import csv
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "data/engcyc/manifest.tsv"
METADATA = ROOT / "data/engcyc/metadata"

COLUMNS = ["dataset_id", "name", "collection", "modal", "description", "origin", "accession",
           "locations", "n_samples", "size_bytes", "size_source", "status", "metadata_table",
           "existing_results", "notes"]
ENUMS = {
    "modal": {"metagenome", "metatranscriptome", "genome", "fosmid", "amplicon"},
    "origin": {"internal", "external", "collaborator"},
    "size_source": {"measured", "recorded", ""},
    "status": {"available", "archived", "missing", "candidate"},
}
REQUIRED = ["dataset_id", "name", "collection", "modal", "description", "origin", "status",
            "existing_results"]


def check(m):
    errors, warnings = [], []
    if list(m.columns) != COLUMNS:
        errors.append(f"columns are {list(m.columns)}, expected {COLUMNS}")
        return errors, warnings
    for d in m.dataset_id[m.dataset_id.duplicated()]:
        errors.append(f"{d}: duplicate dataset_id")
    for _, r in m.iterrows():
        d = r.dataset_id
        for col in REQUIRED:
            if not r[col]:
                errors.append(f"{d}: {col} is empty")
        for col, allowed in ENUMS.items():
            if r[col] not in allowed:
                errors.append(f"{d}: {col}={r[col]!r} is not one of {sorted(allowed)}")
        for loc in filter(None, r.locations.split("; ")):
            node, _, path = loc.partition(":")
            if not node or not path or "://" in loc:
                errors.append(f"{d}: location {loc!r} is not node:path")
        if r.locations.startswith(";") or r.locations.endswith(";"):
            errors.append(f"{d}: locations has an empty entry")
        for col in ("n_samples", "size_bytes"):
            if r[col] and not r[col].isdigit():
                errors.append(f"{d}: {col}={r[col]!r} is not an integer")
        if r.size_bytes and not r.size_source:
            errors.append(f"{d}: size_bytes has no size_source")
        t = r.metadata_table
        if not t:
            continue
        if "/" in t or t != f"{d}.tsv":
            errors.append(f"{d}: metadata_table {t!r} is not {d}.tsv")
            continue
        path = METADATA / t
        if not path.is_file():
            errors.append(f"{d}: {t} does not exist in {METADATA}")
            continue
        rows = len(pd.read_csv(path, sep="\t", dtype=str))
        if r.n_samples and int(r.n_samples) != rows:
            warnings.append(f"{d}: n_samples={r.n_samples}, {t} has {rows} rows")
    linked = set(m.metadata_table)
    for f in sorted(METADATA.glob("*.tsv")):
        if f.name not in linked:
            errors.append(f"{f.name}: no manifest row links it")
    return errors, warnings


ROLES = {"raw", "qc_filtered", "report", "derived", "assembly", "alignment"}
MD5 = re.compile(r"^[0-9a-f]{32}$")
SAMPLE_ID = re.compile(r"^[A-Za-z0-9_]*$")


def check_files(path, dataset_ids):
    from manifest_jgi import COLUMNS
    errors = []
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != COLUMNS:
            return [f"{path}: columns are {reader.fieldnames}, expected manifest_jgi.COLUMNS"]
        seen = set()
        for n, r in enumerate(reader, start=2):
            where = f"{path.name}:{n}"
            if r["dataset_id"] not in dataset_ids:
                errors.append(f"{where}: dataset_id {r['dataset_id']!r} is not in manifest.tsv")
            if r["file"] in seen:
                errors.append(f"{where}: file {r['file']!r} is listed twice")
            seen.add(r["file"])
            if r["role"] not in ROLES:
                errors.append(f"{where}: role {r['role']!r} is not one of {sorted(ROLES)}")
            if not r["bytes"].isdigit():
                errors.append(f"{where}: bytes {r['bytes']!r} is not an integer")
            if r["md5"] and not MD5.match(r["md5"]):
                errors.append(f"{where}: md5 {r['md5']!r} is not 32 lowercase hex digits")
            if not SAMPLE_ID.match(r["sample_id"]):
                errors.append(f"{where}: sample_id {r['sample_id']!r} has characters outside [A-Za-z0-9_]")
            if r["role"] == "raw" and not r["sample_id"]:
                errors.append(f"{where}: raw file has no sample_id")
    return errors


def main():
    m = pd.read_csv(MANIFEST, sep="\t", dtype=str, keep_default_na=False)
    errors, warnings = check(m)
    for path in map(Path, sys.argv[1:]):
        errors += check_files(path, set(m.dataset_id))
    for w in warnings:
        print("WARN ", w)
    for e in errors:
        print("ERROR", e)
    print(f"{len(m)} datasets, {(m.metadata_table != '').sum()} with metadata, "
          f"{len(errors)} errors, {len(warnings)} warnings")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
