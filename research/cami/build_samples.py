#!/usr/bin/env python3
"""Enumerate every CAMI sample unpacked under CAMI_ROOT/work into samples.tsv --
short- and long-read, CAMI II and CAMI III alike.

Two directory SHAPES exist on fir, not one, and a glob for either alone silently
drops the other with no error:

  Shape A (CAMI II, both short AND long read): reads sit in a directory literally
  named `reads` beneath a `*_sample_N/` directory that also holds a sibling
  `contigs/` with the gold standard. Nesting depth under `work/<top>/` varies --
  marine and plant_associated nest under `.../simulation_*/*_sample_N/`, strain
  under `.../long_read|short_read/*_sample_N/`, the rest directly under
  `<dataset>_short_read/*_sample_N/` -- so this walks `work/**/reads/
  anonymous_reads.fq.gz` and climbs back up to the first directory under `work/`
  rather than assuming one depth.

  Shape B (CAMI III only): NO shared sample directory at all. Reads live
  directly under `sample_<N>_reads/`, and the gold standard -- roughly 311
  per-source-genome FASTAs, not a single `binning_gs.tsv` or
  `gsa_mapping.tsv.gz` -- lives in the SIBLING `sample_<N>_gsa/` directory. This
  walks `work/**/sample_*_reads/anonymous_reads.fq.gz` separately because Shape
  A's glob (parent literally named `reads`) never matches it.

Run on fir, where the corpus lives, and stdlib only so it needs no environment:

    cat research/cami/build_samples.py | ssh fir python3 - > research/cami/samples.tsv

`has_binning_gs` accepts EITHER spelling of the Shape A bin gold standard: CAMI's
own Bioboxes `binning_gs.tsv` (what
metasmith_libraries/transforms/metagenomics/binning/amber.py actually reads), or
`gsa_mapping.tsv.gz` (the contig-to-genome mapping every Shape A dataset ships,
convertible to Bioboxes with gsa_to_bioboxes.py). Checking only the first
spelling undercounts on CAMI II short read: 110 of 229 of those rows ship
`binning_gs.tsv` directly, but all 229 ship one form or the other. Every CAMI
III (Shape B) row reads `has_binning_gs = 0` honestly -- it has neither file, by
design of the archive, not by a gap in this script -- which is fine: AMBER here
scores through the per-read-truth bridge (`has_truth`, see
run_cami_metag.py's module docstring and `cami_contig_truth.py`), not through
`binning_gs.tsv`.

`has_truth` is read off whether `reads_mapping.tsv.gz` actually exists beside
the reads, per row, never assumed from which dataset a row belongs to: the
CAMI II long-read subtrees and CAMI III do not obviously agree on this axis, so
guessing it from the dataset label would get some rows wrong silently. Read the
regenerated table's own `has_truth` column rather than a comment for the
current answer per dataset.

`dataset` values are DELIBERATELY distinct per work/ subtree rather than
reused between a short- and long-read version of "the same" community (e.g.
`marine` vs `marine_long`, `toy_humangut` vs `toy_humangut_long`) -- reusing a
label would collide two different physical samples onto one
`{dataset}_{sample_id}` key downstream. `read_type` ("short"/"long") is carried
as its own column anyway, so a consumer never has to parse it back out of the
dataset string.
"""

import csv
import re
import sys
from pathlib import Path

CAMI_ROOT = Path("/scratch/phyberos/cami")

# work/<top-level dir name> -> (samples.tsv `dataset` label, `read_type`).
#
# Checked unpacked and non-empty on fir 2026-09-12 (orchestrator's report from the
# standing data agent, cross-checked directly): marine_long_read (48G),
# strain_long_read (198G), plant_long_read_nano (39G), plant_long_read_pacbio
# (100G), toy_humangut_long_read (90G, Shape B, CAMI III), toy_humangut_short_read
# (94G, Shape A, CAMI III). Do not guess a work/ subdirectory name here: a wrong
# guess silently enumerates zero samples for that dataset rather than failing.
DATASET_LABELS = {
    "marine_short_read":            ("marine",                          "short"),
    "strain_short_read":            ("strain",                          "short"),
    "mousegut_short_read":          ("toy_mousegut",                    "short"),
    "airskinurogenital_short_read": ("toy_hmp_airskinurogenital",       "short"),
    "gastrooral_short_read":        ("toy_hmp_gastrooral",              "short"),
    "plant_short_read":             ("plant_associated",                "short"),
    "toy_humangut_short_read":      ("toy_humangut",                    "short"),  # CAMI III, Shape A

    "marine_long_read":             ("marine_long",                     "long"),   # CAMI II
    "strain_long_read":             ("strain_long",                     "long"),   # CAMI II
    "plant_long_read_nano":         ("plant_associated_long_nano",      "long"),   # CAMI II
    "plant_long_read_pacbio":       ("plant_associated_long_pacbio",    "long"),   # CAMI II
    "toy_humangut_long_read":       ("toy_humangut_long",               "long"),   # CAMI III, Shape B
}

_SAMPLE_RE = re.compile(r"sample_\d+")


def _climb_to_top(path: Path, work: Path) -> Path:
    top = path
    while top.parent != work:
        top = top.parent
    return top


def _sample_number(name: str) -> str:
    m = _SAMPLE_RE.search(name)
    return m.group(0) if m else name


def _enumerate_shape_a(work: Path) -> list[dict]:
    """reads/anonymous_reads.fq.gz under a `*_sample_N/` dir with a sibling contigs/."""
    rows = []
    for reads in sorted(work.glob("**/reads/anonymous_reads.fq.gz")):
        sample_dir = reads.parent.parent
        top = _climb_to_top(sample_dir, work)
        labeled = DATASET_LABELS.get(top.name)
        if labeled is None:
            sys.stderr.write(f"WARNING: unrecognized dataset dir {top}, skipping {sample_dir}\n")
            continue
        dataset, read_type = labeled

        truth = reads.parent / "reads_mapping.tsv.gz"
        contigs = sample_dir / "contigs"
        has_binning_gs = (contigs / "binning_gs.tsv").exists() or (contigs / "gsa_mapping.tsv.gz").exists()

        rows.append(dict(
            dataset=dataset,
            sample_id=_sample_number(sample_dir.name),
            read_type=read_type,
            reads_path=str(reads),
            has_truth=1 if truth.exists() else 0,
            has_binning_gs=1 if has_binning_gs else 0,
            reads_bytes=reads.stat().st_size,
        ))
    return rows


def _enumerate_shape_b(work: Path) -> list[dict]:
    """CAMI III: sample_<N>_reads/anonymous_reads.fq.gz, gold standard in sample_<N>_gsa/."""
    rows = []
    for reads in sorted(work.glob("**/sample_*_reads/anonymous_reads.fq.gz")):
        sample_reads_dir = reads.parent
        top = _climb_to_top(sample_reads_dir, work)
        labeled = DATASET_LABELS.get(top.name)
        if labeled is None:
            sys.stderr.write(f"WARNING: unrecognized dataset dir {top}, skipping {sample_reads_dir}\n")
            continue
        dataset, read_type = labeled

        truth = sample_reads_dir / "reads_mapping.tsv.gz"
        gsa_dir = sample_reads_dir.parent / sample_reads_dir.name.replace("_reads", "_gsa")
        has_binning_gs = (gsa_dir / "binning_gs.tsv").exists() or any(gsa_dir.glob("*gsa_mapping.tsv.gz"))

        rows.append(dict(
            dataset=dataset,
            sample_id=_sample_number(sample_reads_dir.name),
            read_type=read_type,
            reads_path=str(reads),
            has_truth=1 if truth.exists() else 0,
            has_binning_gs=1 if has_binning_gs else 0,
            reads_bytes=reads.stat().st_size,
        ))
    return rows


def enumerate_rows(root: Path) -> list[dict]:
    work = root / "work"
    # Shape B's dirs (`sample_*_reads/`) never match Shape A's glob (parent
    # literally named `reads`), and vice versa, so no row can double-count
    # between the two passes.
    return _enumerate_shape_a(work) + _enumerate_shape_b(work)


def main() -> None:
    rows = enumerate_rows(CAMI_ROOT)
    rows.sort(key=lambda r: (r["dataset"], int(re.search(r"\d+", r["sample_id"]).group())))

    fieldnames = ["dataset", "sample_id", "read_type", "reads_path",
                  "has_truth", "has_binning_gs", "reads_bytes"]
    w = csv.DictWriter(sys.stdout, delimiter="\t", fieldnames=fieldnames, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)

    by_dataset = {}
    for r in rows:
        d = by_dataset.setdefault(r["dataset"], dict(n=0, truth=0, gs=0))
        d["n"] += 1
        d["truth"] += r["has_truth"]
        d["gs"] += r["has_binning_gs"]
    for dataset, d in sorted(by_dataset.items()):
        sys.stderr.write(
            f"  {dataset:32s} n={d['n']:4d} has_truth={d['truth']:4d} has_binning_gs={d['gs']:4d}\n")
    sys.stderr.write(
        f"{len(rows)} samples total, {sum(r['has_truth'] for r in rows)} with per-read truth, "
        f"{sum(r['has_binning_gs'] for r in rows)} with a bin gold standard on disk (either spelling)\n")


if __name__ == "__main__":
    main()
