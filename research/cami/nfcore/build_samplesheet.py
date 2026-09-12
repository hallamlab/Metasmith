#!/usr/bin/env python3
"""Build an nf-core/mag 5.5.0 samplesheet from research/cami/samples.tsv.

samples.tsv (229 rows, six CAMI datasets: marine, strain, toy_mousegut,
toy_hmp_airskinurogenital, toy_hmp_gastrooral, plant_associated) carries only SHORT reads --
every `reads_path` was verified against the tracked manifest to contain no long-read entry
(`grep -c long_read samples.tsv` is 0). So the sheet this script writes is short-read-only by
construction, not by an assumption this script makes: it never emits `long_reads` /
`long_reads_platform` columns, and control.config's `skip_flye = false` simply never finds
long reads to act on here. A long-read CAMI II sample sheet is a separate, not-yet-built input
-- see the module docstring's note on `long_reads_platform` below for the value it would need.

`group` is set to the CAMI dataset name (marine, plant_associated, ...), not left one-per-sample.
Each dataset's samples are a time series from the same simulated environment, and nf-core/mag's
own default `binning_map_mode=group` pools exactly that shape for co-abundance binning without
co-assembling (see docs/usage.md's `group` column section) -- one group per sample would silently
throw away the co-binning signal these datasets were built to give it.

`sample` is `f"{dataset}_{sample_id}"`, not the bare `sample_id` samples.tsv itself uses --
`sample_id` recurs across datasets ("sample_0" in six different places), and the pipeline's own
`schema_input.json` requires `sample` (with no `run` column here) to be globally unique
(`uniqueEntries: ["sample", "run"]`). run_cami_metag.py's `enumerate_samples()` hits this same
collision and resolves it the same way; kept consistent on purpose so a row here and a row there
name the same sample.

THE INTERLEAVED-READS GAP -- flagged, not solved, here:
CAMI ships one interleaved `anonymous_reads.fq.gz` per sample. nf-core/mag has no interleaved
ingestion path: `short_reads_2` is genuinely optional in the schema, but its absence means
single-end (confirmed in modules/nf-core/megahit/main.nf: `meta.single_end || !reads2 ? "-r
${reads1}" : "-1 ... -2 ..."` -- there is no third, interleaved-aware branch). Pointing
`short_reads_1` at the interleaved file and setting `--single_end` would run every read as if
unpaired, discarding the mate relationship MEGAHIT and Flye both rely on for a real assembly.
This is a real staging gap, owned by whoever runs the real corpus (L3 for run/launch, or a data
agent for the split itself) -- NOT solved by this script, and NOT something a small pilot job on
the pipeline's own bundled test data would ever surface, since that data is already split.
Deinterleaving CAMI's files (e.g. `bbtools`'s reformat.sh: `in=<interleaved> out1=<R1>
out2=<R2>`) is a one-time, ~1TB-across-229-samples job, out of scope for a config-authoring pass.

This script therefore always computes SPLIT_ROOT/<dataset>/<sample>_R{1,2}.fastq.gz as the
sheet's short_reads_1/short_reads_2 paths -- a staging convention, not a promise the files exist.
Pass --check-remote to ssh into fir and confirm the split files are actually there before
trusting the sheet for a real launch.

CORRECTED CLAIM (this docstring previously said the opposite): nf-core/mag's own
`schema_input.json` requires more than this script first assumed, not less. Both
`short_reads_1` and `short_reads_2` carry `"exists": true` -- the schema itself stats the path
at validation time, before any task runs -- and the object-level `anyOf` requires at least one
of `short_reads_1` / `long_reads` to be present at all, with `dependentRequired` chaining
`short_reads_2` and `short_reads_platform` off `short_reads_1`. So an empty or absent reads
column does not "fail much later, mid-run": it fails at nf-schema's validation step, before the
first task ever submits. That makes `--check-remote` load-bearing, not a nicety -- a sheet
this script writes without it is a sheet nf-core/mag itself would refuse outright, not one that
degrades gracefully. --check-remote therefore now defaults ON; pass --no-check-remote only to
draft a sheet whose split files you already know don't exist yet (e.g. while writing the sheet
before running stage_split_reads.py for that rung).

RUNG SELECTION: pass --rung to emit a sheet naming only the samples a ramp step actually staged
(see stage_split_reads.py, which the campaign lead has directed be run one sample, then ten,
then the rest -- never the whole corpus in one deinterleave job). `--rung 1` and `--rung 10`
take samples.tsv's own row order (the same order stage_split_reads.py's default rung selection
uses, so the two scripts agree on which samples a given rung number means); `--rung all`
requires every one of those samples to already exist remotely (via --check-remote, which an
`all` rung cannot skip) since that is the point past which nothing should be running that has
not first run clean at 1 and 10 samples.

Usage:
    PYTHONPATH="$PWD/src" mamba run -n msm python research/cami/nfcore/build_samplesheet.py \
        [--samples-tsv PATH] [--split-root PATH] [--out PATH] [--rung {1,10,all,N}] \
        [--no-check-remote] [--host fir]
"""

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "cami"
DEFAULT_SAMPLES_TSV = ROOT / "samples.tsv"
DEFAULT_SPLIT_ROOT = "/scratch/phyberos/cami_nfcore_split_reads"
DEFAULT_OUT = Path(__file__).resolve().parent / "samplesheet.cami_short_read.csv"

KNOWN_DATASETS = {
    "marine", "strain", "toy_mousegut",
    "toy_hmp_airskinurogenital", "toy_hmp_gastrooral", "plant_associated",
}
FASTQ_RE = re.compile(r"\.f(ast)?q\.gz$")


def load_rows(samples_tsv: Path) -> list[dict]:
    rows = list(csv.DictReader(samples_tsv.open(), delimiter="\t"))
    if not rows:
        sys.exit(f"ERROR: {samples_tsv} has no data rows")
    required_cols = {"dataset", "sample_id", "reads_path"}
    missing = required_cols - set(rows[0].keys())
    if missing:
        sys.exit(f"ERROR: {samples_tsv} is missing column(s) {sorted(missing)}")
    return rows


def validate_row(row: dict, lineno: int) -> list[str]:
    """Checks the schema itself does not: reads_path shape and a recognised dataset.

    schema_input.json's `pattern` on short_reads_1 already rejects a non-fastq path at
    validation time -- duplicated here so a bad samples.tsv row is caught before it is even
    written into a sheet, not after a real launch fails deep into staging.
    """
    problems = []
    ds = row.get("dataset", "")
    if ds not in KNOWN_DATASETS:
        problems.append(f"line {lineno}: unrecognized dataset {ds!r}")
    if not row.get("sample_id", "").strip():
        problems.append(f"line {lineno}: empty sample_id")
    reads_path = row.get("reads_path", "")
    if not reads_path.strip():
        problems.append(f"line {lineno}: empty reads_path")
    elif not FASTQ_RE.search(reads_path):
        problems.append(f"line {lineno}: reads_path {reads_path!r} does not end in .fastq.gz/.fq.gz")
    if row.get("has_truth") != "1":
        problems.append(f"line {lineno}: has_truth != 1 for {ds}/{row.get('sample_id')} (samples.tsv claims every row does)")
    return problems


def split_paths(split_root: str, dataset: str, sample: str) -> tuple[str, str]:
    r1 = f"{split_root}/{dataset}/{sample}_R1.fastq.gz"
    r2 = f"{split_root}/{dataset}/{sample}_R2.fastq.gz"
    return r1, r2


def check_remote_exists(host: str, paths: list[str]) -> set[str]:
    """Returns the subset of `paths` that exist on `host`. One ssh round trip, not one per path.

    The loop's own exit status is the *last* path's `[ -s ]` result, which is 1 (false) whenever
    the last requested path happens to be missing -- a normal, expected outcome here, not an ssh
    failure. `; true` pins the script's exit code to ssh/connectivity only, so a legitimately
    missing file at the end of the list can't be misreported as "remote existence check failed".
    """
    script = "for p in " + " ".join(f"'{p}'" for p in paths) + '; do [ -s "$p" ] && echo "$p"; done; true'
    r = subprocess.run(["ssh", host, script], capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        sys.exit(f"ERROR: remote existence check failed: {r.stderr}")
    return set(line.strip() for line in r.stdout.splitlines() if line.strip())


def select_rung(rows: list[dict], rung: str) -> list[dict]:
    """Same row-order convention stage_split_reads.py uses, so rung N means the same N samples."""
    if rung == "all":
        return rows
    try:
        n = int(rung)
    except ValueError:
        sys.exit(f"ERROR: --rung must be an integer row count or 'all', got {rung!r}")
    if n < 1 or n > len(rows):
        sys.exit(f"ERROR: --rung {rung} out of range (samples.tsv has {len(rows)} rows)")
    return rows[:n]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--samples-tsv", type=Path, default=DEFAULT_SAMPLES_TSV)
    ap.add_argument("--split-root", default=DEFAULT_SPLIT_ROOT,
                     help="Staging root for deinterleaved R1/R2 pairs (see module docstring: "
                          "these files are NOT created by this script).")
    ap.add_argument("--rung", default="all",
                     help="'1', '10', another row count, or 'all' (samples.tsv row order, same "
                          "convention stage_split_reads.py uses). Selects which samples the "
                          "sheet names; does not stage anything itself.")
    ap.add_argument("--out", type=Path, default=None,
                     help="Default: samplesheet.cami_short_read.csv for --rung all, "
                          "samplesheet.cami_short_read.rung<N>.csv otherwise.")
    ap.add_argument("--check-remote", action="store_true", default=True,
                     help="ssh to --host and confirm every split R1/R2 pair actually exists "
                          "before writing the sheet. ON by default: schema_input.json requires "
                          "short_reads_1/short_reads_2 to exist at validation time, so a sheet "
                          "naming files that are not there yet is not a softer failure mode, "
                          "it is the same failure mode moved earlier.")
    ap.add_argument("--no-check-remote", dest="check_remote", action="store_false",
                     help="Skip the existence check -- only for drafting a sheet before running "
                          "stage_split_reads.py for this rung.")
    ap.add_argument("--host", default="fir")
    args = ap.parse_args()

    out = args.out or (
        DEFAULT_OUT if args.rung == "all"
        else Path(__file__).resolve().parent / f"samplesheet.cami_short_read.rung{args.rung}.csv"
    )

    rows = load_rows(args.samples_tsv)
    rung_rows = select_rung(rows, args.rung)

    problems = []
    out_rows = []
    seen_samples = set()
    for i, row in enumerate(rung_rows, start=2):  # header is line 1 of the full samples.tsv
        problems.extend(validate_row(row, i))
        dataset = row["dataset"]
        sample = f"{dataset}_{row['sample_id']}"
        if sample in seen_samples:
            problems.append(f"line {i}: duplicate sample id {sample!r} (schema requires uniqueEntries on sample)")
            continue
        seen_samples.add(sample)
        r1, r2 = split_paths(args.split_root, dataset, sample)
        out_rows.append(dict(
            sample=sample,
            group=dataset,
            short_reads_1=r1,
            short_reads_2=r2,
            short_reads_platform="ILLUMINA",
        ))

    if problems:
        sys.stderr.write(f"{len(problems)} problem(s) found in {args.samples_tsv}:\n")
        for p in problems:
            sys.stderr.write(f"  {p}\n")
        sys.exit(1)

    if args.check_remote:
        want = [p for r in out_rows for p in (r["short_reads_1"], r["short_reads_2"])]
        present = check_remote_exists(args.host, want)
        missing = [p for p in want if p not in present]
        if missing:
            sys.stderr.write(
                f"{len(missing)} of {len(want)} split read file(s) do not exist yet on {args.host} "
                f"(expected under {args.split_root} -- run stage_split_reads.py for this rung "
                f"first, or pass --no-check-remote to draft the sheet ahead of staging). "
                f"First few missing:\n"
            )
            for p in missing[:10]:
                sys.stderr.write(f"  {p}\n")
            sys.exit(1)

    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["sample", "group", "short_reads_1", "short_reads_2", "short_reads_platform"])
        w.writeheader()
        w.writerows(out_rows)

    sys.stderr.write(f"wrote {len(out_rows)} sample(s) (rung={args.rung}) to {out}\n")
    if not args.check_remote:
        sys.stderr.write(
            "NOTE: split read paths were not checked for existence (--no-check-remote). "
            "nf-core/mag WILL refuse this sheet at validation until they exist.\n"
        )


if __name__ == "__main__":
    main()
