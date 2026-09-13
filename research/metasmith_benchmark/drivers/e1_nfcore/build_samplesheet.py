#!/usr/bin/env python3
"""Write E1's two nf-core/mag 5.5.0 sample sheets from E2's own sample selection.

E1 and E2 must score the same samples, so both sheets come from `e2_cami.enumerate_arms`:
  samplesheet.short.csv  208 samples, split R1/R2 pairs that stage_split_reads.py writes
  samplesheet.long.csv    41 samples, one long-read file each, platform from e2_cami.PLATFORM

`group` is the CAMI dataset. control.config's `binning_map_mode = 'own'` keeps a group from
cross-mapping its samples.

CAUTION nf-core/mag's schema stats every read path at validation, so one missing file fails the
whole launch before a task submits. The sheets are written only when every path exists on fir.

Usage: PYTHONPATH=src python research/metasmith_benchmark/drivers/e1_nfcore/build_samplesheet.py [--limit N] [--no-check]
"""

import argparse
import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import _common as c  # noqa: E402
import e2_cami  # noqa: E402

SPLIT_ROOT = "/scratch/phyberos/cami_nfcore_split_reads"


def split_pair(dataset, sample):
    return f"{SPLIT_ROOT}/{dataset}/{sample}_R1.fastq.gz", f"{SPLIT_ROOT}/{dataset}/{sample}_R2.fastq.gz"


def build_sheets(limit=None):
    dataset_of = {f"{r['dataset']}_{r['sample_id']}": r["dataset"] for r in c.cami_rows()}
    arms = e2_cami.enumerate_arms()
    short, long_ = [], []
    for sid, _reads in arms["short"][:limit]:
        r1, r2 = split_pair(dataset_of[sid], sid)
        short.append(dict(sample=sid, group=dataset_of[sid], short_reads_1=r1, short_reads_2=r2,
                          short_reads_platform="ILLUMINA"))
    for sid, reads, _truth, dataset in arms["long"][:limit]:
        long_.append(dict(sample=sid, group=dataset, long_reads=str(reads),
                          long_reads_platform=e2_cami.PLATFORM[dataset]))
    return {"short": short, "long": long_}


def missing_on_fir(paths):
    out = c.host_sh("for p in " + " ".join(f"'{p}'" for p in paths) + '; do [ -s "$p" ] || echo "$p"; done; true',
                    timeout=600)
    return out.split()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, help="first N samples per sheet")
    ap.add_argument("--no-check", dest="check", action="store_false",
                    help="write without confirming the read files exist on fir")
    args = ap.parse_args()

    sheets = build_sheets(args.limit)
    if not args.limit:
        for arm, rows in sheets.items():
            assert len(rows) == e2_cami.EXPECTED[arm], f"{arm}: {len(rows)} rows, E2 has {e2_cami.EXPECTED[arm]}"
    if args.check:
        paths = [row[k] for rows in sheets.values() for row in rows
                 for k in ("short_reads_1", "short_reads_2", "long_reads") if k in row]
        missing = missing_on_fir(paths)
        if missing:
            sys.exit(f"{len(missing)} of {len(paths)} read files are missing on fir, first: {missing[:5]}")

    for arm, rows in sheets.items():
        out = HERE / f"samplesheet.{arm}.csv"
        with out.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {len(rows)} samples to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
