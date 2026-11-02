"""Enumerate every Pratama 2026 object into one manifest: url, exact size, checksum, drop order.

Emits manifest.tsv and runs.tsv beside itself. Stdlib only, so it runs on a login node too.

Three sources. The two read projects enumerate through the ENA portal filereport API, which
mirrors the NCBI project as well as its own, and publishes a per-file MD5. The Zenodo record
holds the study's processed products and publishes a per-file checksum too.

The 2019 accession is the one in the paper's prose, not the one it hyperlinks: the hyperlinked
project (PRJEB36523) holds that study's 2019 metaSPAdes assemblies and zero read runs, and this
paper re-assembled anyway.

Only the ENA-normalised `fastq_ftp` files are taken. The `submitted_ftp` originals are the same
reads under the submitters' own names, and taking both would double the transfer.

runs.tsv exists for one job: relating a run accession to a sampling site. Nothing in the read
projects does that, and Supplementary Data 1 is the only place it is written down. The two years
need different keys for it, which is why runs.tsv carries three name columns rather than one:

- 2019 joins on the submitted file names. Its sample_alias is ambiguous, repeating across the two
  filter fractions of a well.
- 2022 joins on sample_alias, because ENA publishes no submitted_ftp at all for that project --
  every one of its 40 runs has the field empty.

Neither key matches the supplementary sheet literally. It writes 2019 as `Hain_H14_01um_R3_1`
where ENA submits `H14_0_1_3_R1`, the same well, fraction, replicate and mate in another order.
"""

import argparse
import csv
import io
import json
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ENA_FILEREPORT = (
    "https://www.ebi.ac.uk/ena/portal/api/filereport"
    "?accession={accession}&result=read_run"
    "&fields=run_accession,sample_accession,sample_alias,run_alias,library_layout,"
    "instrument_model,read_count,base_count,fastq_bytes,fastq_ftp,fastq_md5,submitted_ftp"
    "&format=tsv&limit=0"
)

# (project accession, dataset label, expected run count, expected total fastq bytes).
# The two totals are the figures reconnaissance recorded; a mismatch means the release
# changed under us and the manifest should not be trusted until that is understood.
READ_PROJECTS = [
    ("PRJEB36505", "reads_2019", 32, 388_551_767_888),
    ("PRJNA1236243", "reads_2022", 40, 432_978_333_648),
]

ZENODO_RECORD = "17897233"
ZENODO_API = f"https://zenodo.org/api/records/{ZENODO_RECORD}"

# One file for a single-ended run, two for a paired one. ENA sometimes also publishes an
# orphan-read file alongside a pair; that is a third entry and is worth failing on rather
# than silently naming _1 and _2 and dropping it.
FILES_PER_LAYOUT = {"SINGLE": 1, "PAIRED": 2}


def _get(url: str, retries: int = 4, via: str | None = None, timeout: int = 180) -> bytes:
    """Fetch a URL, optionally from another host.

    `via` is a shell prefix such as "ssh fir". Zenodo has refused this workstation's network
    with a traffic-reputation block, and the only fix that worked was fetching from elsewhere.
    """
    for attempt in range(retries):
        try:
            if via:
                cmd = shlex.split(via) + [
                    "curl -sS --fail --location --max-time %d %s" % (timeout, shlex.quote(url))
                ]
                r = subprocess.run(cmd, capture_output=True, timeout=timeout + 60)
                if r.returncode == 0 and r.stdout:
                    return r.stdout
                raise urllib.error.URLError(r.stderr.decode()[:200] or f"rc={r.returncode}")
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError, subprocess.SubprocessError) as e:
            sys.stderr.write(f"  attempt {attempt + 1}/{retries} failed: {e}\n")
            if attempt == retries - 1:
                raise
    raise AssertionError("unreachable")


def _split(field: str) -> list[str]:
    return [p for p in field.split(";") if p]


def read_runs(accession: str, dataset: str, via: str | None) -> tuple[list[dict], list[dict]]:
    """Return (manifest rows, run rows) for one read project."""
    sys.stderr.write(f"enumerating {dataset} ({accession}) ...\n")
    text = _get(ENA_FILEREPORT.format(accession=accession), via=via).decode()
    runs = list(csv.DictReader(io.StringIO(text), delimiter="\t"))

    rows: list[dict] = []
    run_rows: list[dict] = []
    for r in runs:
        run = r["run_accession"]
        layout = r["library_layout"]
        urls, md5s, sizes = _split(r["fastq_ftp"]), _split(r["fastq_md5"]), _split(r["fastq_bytes"])
        assert len(urls) == len(md5s) == len(sizes), f"{run}: ragged fastq_* fields"
        want = FILES_PER_LAYOUT.get(layout)
        assert want is not None, f"{run}: unknown library_layout {layout!r}"
        # A SINGLE run is delivered as one _1.fastq.gz with no mate. Building a mate path
        # from the run accession instead of reading fastq_ftp would 404 on every one of them.
        assert len(urls) == want, f"{run}: {layout} run has {len(urls)} fastq files, expected {want}"

        for url, md5, size in zip(urls, md5s, sizes):
            name = url.rsplit("/", 1)[-1]
            rows.append(dict(dataset=dataset, relpath=f"{run}/{name}",
                             url="https://" + url, bytes=int(size), md5=md5, drop=1))
        run_rows.append(dict(
            dataset=dataset, run_accession=run, sample_accession=r["sample_accession"],
            sample_alias=r["sample_alias"], run_alias=r["run_alias"],
            library_layout=layout, instrument_model=r["instrument_model"],
            read_count=r["read_count"], base_count=r["base_count"],
            n_files=len(urls), bytes=sum(int(s) for s in sizes),
            submitted_names=";".join(u.rsplit("/", 1)[-1] for u in _split(r["submitted_ftp"])),
        ))
    return rows, run_rows


def zenodo_files(via: str | None, cached: Path | None) -> list[dict]:
    """Enumerate the Zenodo record's files."""
    if cached and cached.exists():
        sys.stderr.write(f"zenodo: reading cached metadata from {cached}\n")
        record = json.loads(cached.read_text())
    else:
        sys.stderr.write(f"enumerating zenodo record {ZENODO_RECORD} ...\n")
        raw = _get(ZENODO_API, via=via)
        record = json.loads(raw)
        if cached:
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(raw)

    rows = []
    for f in record.get("files", []):
        # Zenodo qualifies its checksum with an algorithm, e.g. "md5:1a2b...". Anything
        # that is not md5 goes in blank, so verify.py holds the file to its size only
        # rather than comparing an md5 against a hash of some other function.
        algo, _, digest = f["checksum"].partition(":")
        rows.append(dict(dataset=f"zenodo_{ZENODO_RECORD}", relpath=f["key"],
                         url=f["links"]["self"], bytes=int(f["size"]),
                         md5=digest if algo == "md5" else "", drop=0))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    here = Path(__file__).parent
    ap.add_argument("--out", type=Path, default=here / "manifest.tsv")
    ap.add_argument("--runs-out", type=Path, default=here / "runs.tsv")
    ap.add_argument("--via", default=None,
                    help='shell prefix to fetch Zenodo through, e.g. "ssh fir"')
    ap.add_argument("--zenodo-json", type=Path, default=here / ".cache" / "zenodo.json",
                    help="cache file; read if present, written on a successful fetch")
    ap.add_argument("--skip-zenodo", action="store_true",
                    help="emit the read projects alone; the manifest is then incomplete")
    args = ap.parse_args()

    rows: list[dict] = []
    run_rows: list[dict] = []
    incomplete: list[str] = []

    for accession, dataset, want_runs, want_bytes in READ_PROJECTS:
        r, rr = read_runs(accession, dataset, args.via)
        got_bytes = sum(x["bytes"] for x in r)
        sys.stderr.write(f"  {len(rr)} runs, {len(r)} files, {got_bytes} bytes\n")
        if len(rr) != want_runs or got_bytes != want_bytes:
            sys.stderr.write(f"  WARNING: expected {want_runs} runs / {want_bytes} bytes\n")
            incomplete.append(f"{dataset} disagrees with the recorded run count or byte total")
        rows += r
        run_rows += rr

    if args.skip_zenodo:
        incomplete.append("zenodo record not enumerated (--skip-zenodo)")
    else:
        try:
            z = zenodo_files(args.via, args.zenodo_json)
            sys.stderr.write(f"  {len(z)} files, {sum(x['bytes'] for x in z)} bytes\n")
            rows += z
        except Exception as e:                                    # noqa: BLE001
            sys.stderr.write(f"  ZENODO FAILED: {e}\n")
            incomplete.append(f"zenodo record {ZENODO_RECORD} not enumerated: {e}")

    with args.out.open("w") as f:
        f.write("dataset\trelpath\turl\tbytes\tmd5\tdrop\n")
        for r in sorted(rows, key=lambda r: (r["dataset"], r["relpath"])):
            f.write("{dataset}\t{relpath}\t{url}\t{bytes}\t{md5}\t{drop}\n".format(**r))

    cols = ["dataset", "run_accession", "sample_accession", "sample_alias", "run_alias",
            "library_layout", "instrument_model", "read_count", "base_count", "n_files", "bytes",
            "submitted_names"]
    with args.runs_out.open("w") as f:
        f.write("\t".join(cols) + "\n")
        for r in sorted(run_rows, key=lambda r: (r["dataset"], r["run_accession"])):
            f.write("\t".join(str(r[c]) for c in cols) + "\n")

    total = sum(r["bytes"] for r in rows)
    sys.stderr.write(f"\n{len(rows)} objects, {total} bytes ({total / 1024**3:.1f} GiB) "
                     f"-> {args.out}\n{len(run_rows)} runs -> {args.runs_out}\n")
    for r in sorted({r["dataset"] for r in rows}):
        n = [x for x in rows if x["dataset"] == r]
        sys.stderr.write(f"  {r}: {len(n)} files, {sum(x['bytes'] for x in n)} bytes\n")
    if any(not r["md5"] for r in rows):
        sys.stderr.write(f"WARNING: {sum(1 for r in rows if not r['md5'])} objects have no md5\n")
    for msg in incomplete:
        sys.stderr.write(f"INCOMPLETE: {msg}\n")
    sys.exit(1 if incomplete else 0)


if __name__ == "__main__":
    main()
