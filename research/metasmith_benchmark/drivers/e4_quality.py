#!/usr/bin/env python3
"""Tabulate MEMOTE's per-model results for E4's two lanes and for metaGEM's published GEMs.

Writes one row per bin per source into <out>/quality_<source>.tsv:
  repro    MEMOTE 0.9.13 result JSONs in the reproduction lane's chunk archives
  metagem  metaGEM's published per-test MEMOTE tables under published/<study>/
  modern   MEMOTE 0.17 section and total scores in the modern lane's chunk archives.
           The modern lane archived its score only, so it has no per-test columns.
Per-test columns carry MEMOTE's metric (a fraction) and, as <name>_n, the count or value it
was computed from, flattened the way metaGEM's tables flatten them.
Run it on fir, where the archives and the published tables are.
"""

import argparse
import csv
import gzip
import io
import json
import sys
from pathlib import Path

from e4_tally import ARCHIVE, PUBLISHED, bins_of, members, read_index

METAGEM_TABLES = {
    "bissett_base": "memote_straya.csv",
    "karlsson2013": "memote_gut_GEMs.csv.gz",
    "korem2015": "memote_korem.csv",
    "li2019": "memote_china_soil.csv",
    "sunagawa2015": "metagem_tara.csv.gz",
}
TESTS = {
    "reactions": "test_reactions_presence",
    "metabolites": "test_metabolites_presence",
    "genes": "test_genes_presence",
    "compartments": "test_compartments_presence",
    "metabolic_coverage": "test_metabolic_coverage",
    "pure_metabolic": "test_find_pure_metabolic_reactions",
    "transport": "test_find_transport_reactions",
    "medium": "test_find_medium_metabolites",
    "stoich_inconsistent": "test_stoichiometric_consistency",
    "mass_unbalanced": "test_reaction_mass_balance",
    "charge_unbalanced": "test_reaction_charge_balance",
    "formula_missing": "test_metabolites_formula_presence",
    "charge_missing": "test_metabolites_charge_presence",
    "disconnected": "test_find_disconnected",
    "unbounded_default": "test_find_reactions_unbounded_flux_default_condition",
    "blocked": "test_blocked_reactions",
    "deadends": "test_find_deadends",
    "orphans": "test_find_orphans",
    "balanced_cycles": "test_find_stoichiometrically_balanced_cycles",
    "duplicate_reactions": "test_find_duplicate_reactions",
    "growth_default": "test_biomass_default_production",
    "growth_open": "test_biomass_open_production",
    "fast_growth_default": "test_fast_growth_default",
    "biomass_consistency": "test_biomass_consistency",
    "precursors_blocked_default": "test_biomass_precursors_default_production",
    "precursors_blocked_open": "test_biomass_precursors_open_production",
    "precursors_missing": "test_essential_precursors_not_in_biomass",
    "rxn_no_gpr": "test_gene_protein_reaction_rule_presence",
    "transport_no_gpr": "test_transport_reaction_gpr_presence",
    "complexes": "test_protein_complex_presence",
    "identical_genes": "test_find_reactions_with_identical_genes",
    "met_unannotated": "test_metabolite_annotation_presence",
    "rxn_unannotated": "test_reaction_annotation_presence",
    "gene_unannotated": "test_gene_product_annotation_presence",
    "degrees_of_freedom": "test_degrees_of_freedom",
    "rank": "test_matrix_rank",
    "conservation_relations": "test_number_independent_conservation_relations",
}
SECTIONS = ["consistency", "annotation_met", "annotation_rxn", "annotation_gene", "annotation_sbo"]


def as_number(value):
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return ""


# A single-key dict is a biomass-parametrised test on a one-biomass model, and metaGEM's
# tables keep its value rather than its length.
def unwrap(value):
    if isinstance(value, dict) and len(value) == 1:
        return next(iter(value.values()))
    return value


def flatten(tests):
    row = {}
    for name, test in TESTS.items():
        result = tests.get(test)
        if result is None or unwrap(result.get("result")) in ("skipped", "error", None):
            row[name] = row[f"{name}_n"] = ""
            continue
        metric = unwrap(result.get("metric"))
        row[name] = metric if isinstance(metric, (int, float)) else ""
        row[f"{name}_n"] = as_number(unwrap(result.get("data")))
    return row


def lane_rows(lane, type_, parse):
    rows = {}
    for archive in sorted((ARCHIVE / lane).glob("chunk*.tar.zst")):
        manifest = read_index(archive)
        owner = bins_of(manifest)
        wanted = {path: owner[path] for path, entry in manifest.items() if entry.get("type") == type_}
        for member, tar in members(archive):
            b = wanted.get(member.name.removeprefix("results/"))
            if b is not None:
                if b in rows:
                    sys.exit(f"{lane}: {b} has more than one {type_}")
                rows[b] = parse(tar.extractfile(member).read())
        print(f"{lane} {archive.name}: {len(rows)} bins so far", file=sys.stderr)
    return rows


def parse_results(raw):
    return flatten(json.loads(gzip.decompress(raw))["tests"])


def parse_score(raw):
    score = json.loads(raw)
    row = {"total_score": score["total_score"]}
    row.update({s["section"]: s["score"] for s in score["sections"]})
    return row


def metagem_rows():
    rows = {}
    for study, table in METAGEM_TABLES.items():
        path = PUBLISHED / study / table
        raw = gzip.open(path, "rt") if table.endswith(".gz") else open(path)
        tests = {}
        with raw as fh:
            for r in csv.DictReader(fh):
                if r["status"] in ("skipped", "error"):
                    continue
                entry = tests.setdefault(r["model"], {})
                entry[r["test"]] = {"metric": _float(r["metric"]), "data": _float(r["numeric"]), "result": r["status"]}
        for model, entry in tests.items():
            if model in rows:
                sys.exit(f"metaGEM: {model} appears in more than one table")
            rows[model] = _published(entry)
        print(f"metagem {study}: {len(tests)} models", file=sys.stderr)
    return rows


def _float(text):
    try:
        return float(text)
    except ValueError:
        return None


# metaGEM's tables already hold the flattened count, so pass it through as a scalar.
def _published(entry):
    row = {}
    for name, test in TESTS.items():
        result = entry.get(test)
        if result is None:
            row[name] = row[f"{name}_n"] = ""
            continue
        row[name] = "" if result["metric"] is None else result["metric"]
        row[f"{name}_n"] = "" if result["data"] is None else result["data"]
    return row


def write(path, rows, study_of, columns):
    with open(path, "w", newline="") as fh:
        out = csv.writer(fh, delimiter="\t")
        out.writerow(["study", "bin"] + columns)
        for b in sorted(rows):
            out.writerow([study_of.get(b, ""), b] + [rows[b].get(c, "") for c in columns])
    stray = sorted(set(rows) - set(study_of))
    print(f"{path.name}: {len(rows)} bins, {len(stray)} outside the manifest {stray[:5]}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", default=Path(__file__).resolve().parent / "e4_published_proteins.tsv")
    ap.add_argument("--sources", nargs="*", default=["metagem", "repro", "modern"])
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with open(args.manifest) as fh:
        study_of = {row["bin"]: row["study"] for row in csv.DictReader(fh, delimiter="\t")}
    args.out.mkdir(parents=True, exist_ok=True)
    per_test = [c for name in TESTS for c in (name, f"{name}_n")]

    if "metagem" in args.sources:
        write(args.out / "quality_metagem.tsv", metagem_rows(), study_of, per_test)
    if "repro" in args.sources:
        write(args.out / "quality_repro.tsv", lane_rows("repro", "modelling::memote_results", parse_results), study_of, per_test)
    if "modern" in args.sources:
        write(args.out / "quality_modern.tsv", lane_rows("modern", "modelling::memote_score", parse_score), study_of,
              ["total_score"] + SECTIONS)


if __name__ == "__main__":
    main()
