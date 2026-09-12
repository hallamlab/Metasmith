#!/usr/bin/env python3
"""Deinterleave a RUNG of CAMI samples' reads for nf-core/mag, never the whole corpus.

CAMI ships one interleaved `anonymous_reads.fq.gz` per sample; nf-core/mag has no interleaved
ingestion path (see build_samplesheet.py's module docstring for why -- that finding holds and
was independently re-verified). Something has to materialise split R1/R2 pairs before a real
launch's samplesheet can validate. Splitting is real work (~0.87 TB across all 229 samples,
sample sizes 1-6 GB compressed) and it is a *decision*, not a mechanical default, how much of
the corpus to commit to disk before the pipeline itself has been proven on any of it -- that
decision is the campaign's, not this script's: one sample, then ten, then the rest. This
script only ever emits and (optionally) submits a job for an explicit RUNG; it refuses to
build one for the whole corpus without an explicit, separate opt-in (--rung all --i-mean-it).

Each array task:
  1. Skips a sample whose split R1/R2 already exist, are both non-empty, and whose read counts
     match each other (an interrupted prior split leaves exactly a size-mismatched pair).
  2. Runs bbmap's reformat.sh (module bbmap/39.06 on fir) to split the interleaved file.
  3. Re-checks the output the same way before declaring success, so a truncated split reports
     a failed Slurm task rather than a `sample staged` that silently is not.

Usage:
    # Plan only -- prints the rung's sample list and the disk-space check, writes no sbatch file.
    python research/cami/nfcore/stage_split_reads.py --rung 1

    # Write the array sbatch script for the rung (does not submit).
    python research/cami/nfcore/stage_split_reads.py --rung 10 --write-sbatch

    # Write and submit it (ssh's to --host, which must be able to sbatch).
    python research/cami/nfcore/stage_split_reads.py --rung 10 --write-sbatch --submit

The disk check sums `reads_bytes` (samples.tsv) for exactly the rung's *not-yet-split* samples,
applies a safety factor for reformat.sh's uncompressed working set, and refuses to write or
submit anything if `df` on --host reports less than that much available under --split-root's
filesystem.
"""

import argparse
import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "cami"
DEFAULT_SAMPLES_TSV = ROOT / "samples.tsv"
DEFAULT_SPLIT_ROOT = "/scratch/phyberos/cami_nfcore_split_reads"
SBATCH_DIR = Path(__file__).resolve().parent / "cluster"

# Working space reformat.sh needs beyond the final compressed R1+R2 output: it streams-decompresses
# the source and streams-recompresses two outputs concurrently, so peak usage on the destination
# filesystem (source, dest, and any -qz temp all land on the same /scratch) is bounded well under
# 3x the source size; 2.5x leaves headroom without being alarmist for an 11 TB filesystem.
SAFETY_FACTOR = 2.5


def load_rows(samples_tsv: Path) -> list[dict]:
    rows = list(csv.DictReader(samples_tsv.open(), delimiter="\t"))
    if not rows:
        sys.exit(f"ERROR: {samples_tsv} has no data rows")
    return rows


def select_rung(rows: list[dict], rung: str, i_mean_it: bool) -> list[dict]:
    if rung == "all":
        if not i_mean_it:
            sys.exit(
                "REFUSED: --rung all stages all 229 samples in one job, which is exactly what "
                "this script exists to prevent (see module docstring). Ramp instead: --rung 1, "
                "then --rung 10, then --rung all --i-mean-it once those have actually run clean."
            )
        return rows
    n = int(rung)
    if n < 1 or n > len(rows):
        sys.exit(f"ERROR: --rung {rung} out of range (samples.tsv has {len(rows)} rows)")
    return rows[:n]


def check_remote_disk(host: str, split_root: str, need_bytes: int) -> None:
    """Aborts (sys.exit) if `df` on `host` reports less than `need_bytes` available."""
    # split_root may not exist yet on a first rung; create it before `df` needs a real path,
    # and -B1 asks df for bytes directly instead of its default 1K blocks.
    r = subprocess.run(
        ["ssh", host, f"mkdir -p {split_root} && df --output=avail -B1 {split_root} 2>/dev/null | tail -1"],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode != 0 or not r.stdout.strip():
        sys.exit(f"ERROR: could not read free space on {host}:{split_root}: {r.stderr}")
    avail = int(r.stdout.strip())
    if avail < need_bytes:
        sys.exit(
            f"REFUSED: {host}:{split_root} has {avail / 1e9:.1f} GB free, need "
            f"{need_bytes / 1e9:.1f} GB ({SAFETY_FACTOR}x the rung's compressed input) -- "
            f"free space or shrink the rung before staging."
        )
    print(f"disk check OK: {avail / 1e9:.1f} GB free on {host}:{split_root}, "
          f"need {need_bytes / 1e9:.1f} GB", file=sys.stderr)


def render_sbatch(rung_rows: list[dict], split_root: str, account: str) -> str:
    lines = [row["dataset"] for row in rung_rows]
    ids = [row["sample_id"] for row in rung_rows]
    srcs = [row["reads_path"] for row in rung_rows]
    n = len(rung_rows)
    array_ds = " ".join(f'"{d}"' for d in lines)
    array_ids = " ".join(f'"{i}"' for i in ids)
    array_srcs = " ".join(f'"{s}"' for s in srcs)
    return f"""#!/bin/bash
#SBATCH --job-name=cami_split_reads
#SBATCH --account={account}
#SBATCH --array=0-{n - 1}
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=2:00:00
#SBATCH --output=/scratch/phyberos/wave2_b3_nfcore/logs/split_reads.%A_%a.out
# Generated by research/cami/nfcore/stage_split_reads.py -- one array task per rung sample.
# Do not hand-edit; regenerate from samples.tsv if the rung changes.
set -euo pipefail
module load bbmap/39.06

DATASETS=({array_ds})
IDS=({array_ids})
SRCS=({array_srcs})
i=$SLURM_ARRAY_TASK_ID
ds="${{DATASETS[$i]}}"
id="${{IDS[$i]}}"
src="${{SRCS[$i]}}"
sample="${{ds}}_${{id}}"
outdir="{split_root}/${{ds}}"
r1="$outdir/${{sample}}_R1.fastq.gz"
r2="$outdir/${{sample}}_R2.fastq.gz"
mkdir -p "$outdir"

count_reads() {{ zcat "$1" | wc -l; }}

already_done() {{
    [ -s "$r1" ] && [ -s "$r2" ] || return 1
    c1=$(count_reads "$r1"); c2=$(count_reads "$r2")
    [ "$c1" -gt 0 ] && [ "$c1" -eq "$c2" ]
}}

if already_done; then
    echo "SKIP $sample: R1/R2 already present and read-count-matched"
    exit 0
fi

echo "SPLIT-START $sample <- $src"
reformat.sh in="$src" out1="$r1.tmp.fastq.gz" out2="$r2.tmp.fastq.gz" \\
    threads=4 -Xmx6g overwrite=true
mv "$r1.tmp.fastq.gz" "$r1"
mv "$r2.tmp.fastq.gz" "$r2"

if ! already_done; then
    echo "SPLIT-FAIL $sample: post-split read counts do not match or are empty" >&2
    exit 1
fi
echo "SPLIT-OK $sample"
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--samples-tsv", type=Path, default=DEFAULT_SAMPLES_TSV)
    ap.add_argument("--split-root", default=DEFAULT_SPLIT_ROOT)
    ap.add_argument("--rung", default="1", help="'1', '10', an integer row count, or 'all' (requires --i-mean-it).")
    ap.add_argument("--i-mean-it", action="store_true", help="Required alongside --rung all.")
    ap.add_argument("--host", default="fir")
    ap.add_argument("--account", default="rrg-shallam-ab")
    ap.add_argument("--write-sbatch", action="store_true")
    ap.add_argument("--submit", action="store_true", help="ssh to --host and sbatch the written script.")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    rows = load_rows(args.samples_tsv)
    rung_rows = select_rung(rows, args.rung, args.i_mean_it)
    names = [f"{r['dataset']}_{r['sample_id']}" for r in rung_rows]
    print(f"rung '{args.rung}': {len(rung_rows)} sample(s): {', '.join(names)}", file=sys.stderr)

    need_bytes = int(sum(int(r["reads_bytes"]) for r in rung_rows) * SAFETY_FACTOR)
    check_remote_disk(args.host, args.split_root, need_bytes)

    if not args.write_sbatch:
        print("plan only (pass --write-sbatch to emit the array script)", file=sys.stderr)
        return

    script = render_sbatch(rung_rows, args.split_root, args.account)
    out = args.out or (SBATCH_DIR / f"split_reads.rung{args.rung}.sbatch")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(script)
    print(f"wrote {out}", file=sys.stderr)

    if args.submit:
        remote = f"/scratch/phyberos/wave2_b3_nfcore/stage_split_reads.rung{args.rung}.sbatch"
        subprocess.run(["scp", "-q", str(out), f"{args.host}:{remote}"], check=True)
        r = subprocess.run(
            ["ssh", args.host,
             f"mkdir -p /scratch/phyberos/wave2_b3_nfcore/logs && sbatch {remote}"],
            capture_output=True, text=True,
        )
        sys.stdout.write(r.stdout)
        sys.stderr.write(r.stderr)
        if r.returncode != 0:
            sys.exit(r.returncode)


if __name__ == "__main__":
    main()
