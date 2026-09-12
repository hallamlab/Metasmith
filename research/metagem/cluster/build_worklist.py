"""List the manifest rows that are not yet on disk at their published byte count.

One row per array task for fetch_array.sbatch, in the manifest's own column order minus
the drop flag. An empty worklist is the finished condition.

A file fetched through the AWS .sra mirror and converted with fasterq-dump is NOT
byte-identical to the manifest's row: fasterq-dump writes a different read header than
ENA's own fastq (` length=82` vs `/1`), so the gzip differs and the file runs 6-8%
larger. The published byte count is the wrong gate for such a file, and a plain
byte-count check reports it "outstanding" forever -- 20 GB of a needless re-fetch to
close a gap that was already closed the right way (published .sra md5 + vdb-validate +
matching ENA read_count, done once at conversion time, not re-derived here).

`sra_derived.tsv`, if present beside the manifest (same directory, columns
`dataset\trelpath\treason`), names exactly those rows. For them this script checks only
that the file exists and is non-empty -- not a byte match -- and reports it satisfied
under a distinct label so a worklist reader never mistakes "exempted" for "verified
here". The real verification for an exempted row lives wherever it was actually done
(a job log, a report); this script does not repeat it.
"""

import argparse
import csv
from pathlib import Path


def load_sra_derived(manifest: Path) -> set[tuple[str, str]]:
    ledger = manifest.parent / "sra_derived.tsv"
    if not ledger.exists():
        return set()
    with ledger.open() as f:
        return {(r["dataset"], r["relpath"]) for r in csv.DictReader(f, delimiter="\t")}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--dataset", nargs="*", default=None)
    args = ap.parse_args()

    with args.manifest.open() as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    if args.dataset:
        rows = [r for r in rows if r["dataset"] in set(args.dataset)]

    sra_derived = load_sra_derived(args.manifest)

    todo = []
    exempted = 0
    for r in rows:
        dest = args.root/r["dataset"]/r["relpath"]
        key = (r["dataset"], r["relpath"])
        if key in sra_derived:
            if dest.exists() and dest.stat().st_size > 0:
                exempted += 1
                continue
            # An sra_derived row with nothing on disk is still genuinely outstanding.
            todo.append(r)
            continue
        if dest.exists() and dest.stat().st_size == int(r["bytes"]):
            continue
        todo.append(r)

    with args.out.open("w") as f:
        for r in todo:
            f.write("\t".join([r["dataset"], r["relpath"], r["url"], r["bytes"], r["md5"]]) + "\n")
    msg = f"{len(todo)} of {len(rows)} objects outstanding -> {args.out}"
    if exempted:
        msg += (f"  ({exempted} more satisfied via the .sra route, NOT byte-matched"
                f" against the manifest -- see sra_derived.tsv beside the manifest)")
    print(msg)


if __name__ == "__main__":
    main()
