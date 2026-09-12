#!/usr/bin/env python3
"""Enumerate every CAMI short-read sample unpacked under CAMI_ROOT/work into samples.tsv.

Walks `work/**/reads/anonymous_reads.fq.gz` rather than one glob per dataset --
the six subtrees nest reads at different depths (marine and plant_associated
under `.../simulation_short_read/*_sample_N/`, strain under
`.../short_read/*_sample_N/`, the rest directly under `<dataset>_short_read/
*_sample_N/`), and a glob that only matches one shape silently drops the rest
with no error.

Run on fir, where the corpus lives, and stdlib only so it needs no environment:

    cat research/cami/build_samples.py | ssh fir python3 - > research/cami/samples.tsv

`has_binning_gs` accepts EITHER spelling of the bin gold standard: CAMI's own
Bioboxes `binning_gs.tsv` (what
metasmith_libraries/transforms/metagenomics/binning/amber.py actually reads),
or `gsa_mapping.tsv.gz` (the contig-to-genome mapping every dataset ships,
convertible to Bioboxes with gsa_to_bioboxes.py). Checking only the first
spelling undercounts: 110 of 229 samples ship `binning_gs.tsv` directly, but
all 229 ship one form or the other.
"""

import csv
import re
import sys
from pathlib import Path

CAMI_ROOT = Path("/scratch/phyberos/cami")

# work/<top-level dir name> -> samples.tsv `dataset` label.
#
# CAMI III (cami3_toy_humangut, 20 samples) has no entry yet: it was never
# unpacked into work/ on fir as of this writing (another lane is landing it
# over Globus). Its own gsa_mapping.tsv.gz already ships closer to Bioboxes
# shape than CAMI II's does -- see gsa_to_bioboxes.py's module docstring --
# and that converter's header-based column lookup handles it unchanged. Add
# the real work/ subdirectory name here once it lands and is unpacked; do not
# guess it, since a wrong guess here silently enumerates zero samples rather
# than failing.
DATASET_LABELS = {
    "marine_short_read": "marine",
    "strain_short_read": "strain",
    "mousegut_short_read": "toy_mousegut",
    "airskinurogenital_short_read": "toy_hmp_airskinurogenital",
    "gastrooral_short_read": "toy_hmp_gastrooral",
    "plant_short_read": "plant_associated",
}

_SAMPLE_RE = re.compile(r"(sample_\d+)$")


def enumerate_rows(root: Path) -> list[dict]:
    rows = []
    for reads in sorted((root / "work").glob("**/reads/anonymous_reads.fq.gz")):
        sample_dir = reads.parent.parent
        top = sample_dir
        while top.parent != root / "work":
            top = top.parent
        dataset = DATASET_LABELS.get(top.name)
        if dataset is None:
            sys.stderr.write(f"WARNING: unrecognized dataset dir {top}, skipping {sample_dir}\n")
            continue

        m = _SAMPLE_RE.search(sample_dir.name)
        sample_id = m.group(1) if m else sample_dir.name

        truth = reads.parent / "reads_mapping.tsv.gz"
        contigs = sample_dir / "contigs"
        has_binning_gs = (contigs / "binning_gs.tsv").exists() or (contigs / "gsa_mapping.tsv.gz").exists()

        rows.append(dict(
            dataset=dataset,
            sample_id=sample_id,
            reads_path=str(reads),
            has_truth=1 if truth.exists() else 0,
            has_binning_gs=1 if has_binning_gs else 0,
            reads_bytes=reads.stat().st_size,
        ))
    return rows


def main() -> None:
    rows = enumerate_rows(CAMI_ROOT)
    rows.sort(key=lambda r: (r["dataset"], int(re.search(r"\d+", r["sample_id"]).group())))

    w = csv.DictWriter(
        sys.stdout, delimiter="\t",
        fieldnames=["dataset", "sample_id", "reads_path", "has_truth", "has_binning_gs", "reads_bytes"],
        lineterminator="\n",
    )
    w.writeheader()
    w.writerows(rows)

    scoreable = sum(r["has_binning_gs"] for r in rows)
    sys.stderr.write(f"{len(rows)} samples, {scoreable} with a bin gold standard on disk (either spelling)\n")


if __name__ == "__main__":
    main()
