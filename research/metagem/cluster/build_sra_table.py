"""Per-study run table for the AWS .sra route: the object to fetch, and the gates on it.

One row per RUN rather than per file, because the S3 object is a run-level .sra that both
mates come out of. Three facts have to be joined to fetch it safely:

  * NCBI's SDL API for the .sra object's size and md5. ENA's file report carries sra_bytes
    and sra_md5 columns and both are EMPTY for all four of these projects, so the published
    checksum on the bytes actually transferred is only available from SDL.
  * ENA's read_count and base_count, which is what the converted fastq is checked against.
    The manifest's md5 is over ENA's GENERATED fastq and cannot gate a file that came
    through the .sra -- measured on ERR969493, the sequence and quality streams are
    byte-identical but the headers differ (`/1` vs ` length=82`), so the gzip differs.
  * the manifest's own relpaths, which are where the converted mates have to land.
"""

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

SDL = "https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve"
ENA = "https://www.ebi.ac.uk/ena/portal/api/filereport"


def post(url: str, fields: list[tuple[str, str]]) -> bytes:
    body = urllib.parse.urlencode(fields).encode()
    with urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=180) as r:
        return r.read()


def get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=180) as r:
        return r.read().decode()


def sdl_batch(runs: list[str]) -> dict[str, tuple[int, str]]:
    fields = [("acc", r) for r in runs]
    fields += [("location-type", "forced"), ("location", "s3.us-east-1")]
    doc = json.loads(post(SDL, fields))
    out = {}
    for bundle in doc.get("result", []):
        for f in bundle.get("files", []):
            if f.get("type") != "sra":
                continue
            out[bundle["bundle"]] = (int(f["size"]), f["md5"])
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--outdir", type=Path, required=True)
    ap.add_argument("--dataset", nargs="+", required=True)
    args = ap.parse_args()

    with args.manifest.open() as f:
        rows = [r for r in csv.DictReader(f, delimiter="\t") if r["dataset"] in set(args.dataset)]

    by_ds_run: dict[tuple[str, str], list[str]] = defaultdict(list)
    project: dict[str, str] = {}
    for r in rows:
        parts = r["relpath"].split("/")
        project[r["dataset"]] = parts[0]
        by_ds_run[(r["dataset"], parts[1])].append(r["relpath"])

    counts: dict[str, tuple[str, str]] = {}
    for ds in args.dataset:
        prj = project[ds]
        text = get(f"{ENA}?accession={prj}&result=read_run"
                   f"&fields=run_accession,read_count,base_count&limit=0")
        for line in text.splitlines()[1:]:
            c = line.split("\t")
            if len(c) >= 3:
                counts[c[0]] = (c[1], c[2])

    args.outdir.mkdir(parents=True, exist_ok=True)
    for ds in args.dataset:
        runs = sorted({run for (d, run) in by_ds_run if d == ds})
        sra: dict[str, tuple[int, str]] = {}
        # SDL answers a batch, but a batch that is too wide returns a 414 rather than a
        # partial answer, so the width is fixed rather than derived from the study size.
        for i in range(0, len(runs), 40):
            sra.update(sdl_batch(runs[i:i + 40]))
        out = args.outdir/f"runs.{ds}.tsv"
        missing = 0
        with out.open("w") as f:
            f.write("run\tsra_bytes\tsra_md5\tread_count\tbase_count\tmates\trelpaths\n")
            for run in runs:
                if run not in sra:
                    missing += 1
                    continue
                size, md5 = sra[run]
                reads, bases = counts.get(run, ("", ""))
                rel = sorted(by_ds_run[(ds, run)])
                f.write(f"{run}\t{size}\t{md5}\t{reads}\t{bases}\t{len(rel)}\t{';'.join(rel)}\n")
        print(f"{ds}: {len(runs)} runs, {len(sra)} with an .sra object, {missing} without "
              f"-> {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
