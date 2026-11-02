#!/usr/bin/env python3
"""One measured-phenotype sidecar per declared axis: the signed z-score every deletion
produced on that axis's ion, and whether it clears the paper's own threshold.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fuhrer/parse/write_y_sidecars.py
    ... --publish        # writes data/fabfos/benchmarks/fuhrer/Y/

SAME SHAPE AS THE GLYCOGEN SIDECARS -- keyed on `condition_id`, one file per axis, a `#`
provenance line above the header. What differs is that this screen MEASURED EVERY GENE. The
eydallin sidecars carry 65 rows because 65 mutants were stained; these carry 3,806 because
the mass spectrometer read every strain on the plate. That is the whole reason this arm
exists: `is_positive=False` here means measured and did not move, not unmeasured.

THE THRESHOLD IS THE PAPER'S 2.765 AND IT IS NOT TUNED. Fuhrer's Methods derive it from the
absolute z-score difference between two biological replicates of each mutant: above 2.765
there is a 1% probability of a false positive. It is a property of the assay's
reproducibility, established before any gene was looked at, which is exactly what a label
threshold should be.

`direction` IS THE SIGN OF THE Z-SCORE, AND IT IS THE COLUMN THIS CAMPAIGN HAS NEVER HAD.
The other three arms label one-sided: a clone made more glycogen, more fatty acid, survived
more ethanol. Here a deletion can raise a metabolite or lower it, both reproducibly, and a
probe that predicts WHICH is doing something a magnitude-only probe is not. `analyse` must
report agreement against the majority-class base rate of each axis's own label set -- three
of this campaign's sign figures previously sat below their own base rates and read as
results until the base rate was printed beside them.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[5]

SCREEN = REPO / "data/fabfos/runs/fuhrer_clones/parse/screen"
AXES = REPO / "data/fabfos/benchmarks/fuhrer/axes.tsv"
OUT = REPO / "data/fabfos/benchmarks/fuhrer/Y"

CONDITION_PREFIX = "fuhrer"
WT_COLUMN = "wt"
Z_THRESHOLD = 2.765
ELEMENT = "C"
COLS = ("condition_id", "element", "gene", "z", "abs_z", "is_positive", "direction")
PROVENANCE = ("# Fuhrer et al. Mol Syst Biol 13:907 (2017), doi:10.15252/msb.20167150. "
              "Modified z-score of {mode} ion {ion} ({name}, {kegg}) across the Keio "
              "deletion collection in M9 glucose. A deletion is positive at |z| > "
              f"{Z_THRESHOLD}, the paper's own 1%-false-positive reproducibility "
              "threshold.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--axes", type=Path, default=AXES,
                    help="the declared panel whose targets get a sidecar")
    ap.add_argument("--out-dir", type=Path, default=OUT)
    ap.add_argument("--quiet", action="store_true",
                    help="suppress the per-axis line; the wide panel has 105 of them")
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}")
    a = ap.parse_args()
    out_dir = a.out_dir.resolve()

    axes = pd.read_csv(a.axes, sep="\t", dtype=str).fillna("")
    targets = axes[(axes.role == "target") & (axes.ion_index != "")]
    roster = pd.read_csv(SCREEN / "gene_roster.tsv", sep="\t", dtype=str).fillna("")
    genes = [g for g in roster.gene if g != WT_COLUMN]
    print(f"{len(targets)} target axes x {len(genes):,} deletions")

    written = []
    for mode, group in targets.groupby("mode"):
        z = pd.read_parquet(SCREEN / f"zscore_{mode}.parquet", columns=genes)
        arr = z.to_numpy()
        for r in group.itertuples():
            row = arr[int(r.ion_index) - 1]
            pos = np.abs(row) > Z_THRESHOLD
            df = pd.DataFrame({
                "condition_id": [f"{CONDITION_PREFIX}:{g}" for g in genes],
                "element": ELEMENT, "gene": genes,
                "z": np.round(row, 5), "abs_z": np.round(np.abs(row), 5),
                "is_positive": pos,
                "direction": np.where(row > 0, "+", np.where(row < 0, "-", "0")),
            })
            up = int(((row > 0) & pos).sum())
            down = int(((row < 0) & pos).sum())
            base = max(up, down) / max(up + down, 1)
            if not a.quiet:
                print(f"   {r.axis:24} {r.sink_name[:34]:34} {int(pos.sum()):>4} positive "
                      f"({up} up / {down} down, majority-class base rate {base:.3f})")
            written.append((r, df))
        del z, arr

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {out_dir.relative_to(REPO)})")
        return 0
    out_dir.mkdir(parents=True, exist_ok=True)
    for r, df in written:
        path = out_dir / f"measured_{r.axis}.tsv"
        path.unlink(missing_ok=True)
        with path.open("w") as fh:
            fh.write(PROVENANCE.format(mode=r.mode, ion=r.ion_index,
                                       name=r.sink_name, kegg=r.kegg_id) + "\n")
            df[list(COLS)].to_csv(fh, sep="\t", index=False)
        if not a.quiet:
            print(f"-> {path.relative_to(REPO)}: {len(df):,} rows")
    print(f"\n-> {out_dir.relative_to(REPO)}: {len(written)} sidecars")
    return 0


if __name__ == "__main__":
    sys.exit(main())
