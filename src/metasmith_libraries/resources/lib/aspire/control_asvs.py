#!/usr/bin/env python3
"""Build a contaminant list from published negative controls, the stages around vsearch's denoising.

literature_controls.tsv beside this script pins every control: its source study, sample, read
layout, URL and MD5. Each source is denoised alone, so `blanks` counts the controls an ASV was
seen in within its own study. Across sources, a shorter ASV that prefixes a longer one is the
same sequence read to a different length, and `studies` counts the sources behind each entry.

An entry is kept only when it was seen in at least MIN_PREVALENCE of its sources' controls. A
control also catches what leaks from the study's own samples, and those ASVs sit in one or two
controls: unfiltered, the list removed 44% of the lab's V4-V5 reads, its dominant Halomonas
among them.
"""

import argparse
import csv
import gzip
import hashlib
import re
import shutil
import sys
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CONTROLS = Path(__file__).resolve().parent / "literature_controls.tsv"

# Every source is cut to the span that starts right after 515F. Weyrich's and KatharoSeq's reads
# already start there. KatharoSeq is cut to 150 nt, one base short of its 151-nt reads, so its
# ASVs prefix the longer ones from the other two sources.
SOURCES = {
    "weyrich2019": {"primer_in_read": False, "truncate": None,
                    "citation": "Weyrich et al. 2019, Mol Ecol Resour, doi:10.1111/1755-0998.13011"},
    "katharoseq2018": {"primer_in_read": False, "truncate": 150,
                       "citation": "Minich et al. 2018, mSystems, doi:10.1128/mSystems.00218-17"},
    "dyrhovden2021": {"primer_in_read": True, "truncate": None,
                      "citation": "Dyrhovden et al. 2021, mBio, doi:10.1128/mBio.00598-21"},
}

PRIMER_515F = re.compile(r"GTG[CT]CAGC[AC]GCCGCGGTAA")
PRIMER_806R_RC = re.compile(r"ATTAGA[AT]ACCC[CGT][ACGT]GTAGTCC")
ADAPTERS = ("AGATCGGAAGAGC", "CTCGTATGCCGTC", "CTGTCTCTTATACACATCT")
MIN_LENGTH = 100
MIN_PREVALENCE = 0.25


def controls(source):
    with CONTROLS.open() as f:
        return [r for r in csv.DictReader(f, delimiter="\t") if r["source"] == source]


def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def download(url, md5, dest, attempts=4):
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=120) as r, open(dest, "wb") as f:
                shutil.copyfileobj(r, f, 1 << 20)
            if md5sum(dest) == md5:
                return
            print(f"[WARN] {dest.name}: MD5 mismatch, attempt {attempt + 1}", file=sys.stderr)
        except OSError as e:
            print(f"[WARN] {dest.name}: {e}, attempt {attempt + 1}", file=sys.stderr)
    raise SystemExit(f"[ERROR] could not fetch {url}")


def weyrich_sample(header):
    name = re.sub(r"_\d+$", "", header[1:].split()[0])
    return re.sub(r"_[ACGTN]+\.join$", "", name)


def split_weyrich(rows, raw):
    """Stream the study's one all-sample FASTA once, keeping the control samples' reads."""
    url, md5 = rows[0]["url"], rows[0]["md5"]
    wanted = {r["sample"] for r in rows}
    outs = {s: open(raw / f"{s}.fasta", "w") for s in wanted}
    h, keep = hashlib.md5(), None
    with urllib.request.urlopen(url, timeout=300) as r:
        for line in r:
            h.update(line)
            text = line.decode()
            if text.startswith(">"):
                keep = outs.get(weyrich_sample(text))
            if keep is not None:
                keep.write(text)
    for f in outs.values():
        f.close()
    if h.hexdigest() != md5:
        raise SystemExit(f"[ERROR] {url}: MD5 {h.hexdigest()} is not the pinned {md5}")


def fetch(args):
    rows = controls(args.source)
    raw = args.dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    manifest = []
    if args.source == "weyrich2019":
        split_weyrich(rows, raw)
        manifest = [(r["sample"], "fasta", f"raw/{r['sample']}.fasta", "") for r in rows]
    else:
        jobs = []
        for r in rows:
            files = []
            for url, md5 in zip(r["url"].split(";"), r["md5"].split(";")):
                dest = raw / url.rsplit("/", 1)[-1]
                jobs.append((url, md5, dest))
                files.append(f"raw/{dest.name}")
            manifest.append((r["sample"], r["layout"], files[0], files[1] if len(files) > 1 else ""))
        with ThreadPoolExecutor(args.threads) as pool:
            list(pool.map(lambda j: download(*j), jobs))
    with open(args.dir / "manifest.tsv", "w") as f:
        for row in manifest:
            f.write("\t".join(row) + "\n")


def read_fasta(path):
    seq, header = [], None
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if line.startswith(">"):
                if header is not None:
                    yield header, "".join(seq)
                header, seq = line[1:], []
            else:
                seq.append(line)
    if header is not None:
        yield header, "".join(seq)


def cut(seq, spec):
    seq = seq.upper()
    if spec["primer_in_read"]:
        m = PRIMER_515F.search(seq)
        if not m:
            return None
        seq = seq[m.end():]
    m = PRIMER_806R_RC.search(seq)
    if m:
        seq = seq[:m.start()]
    if spec["truncate"]:
        if len(seq) < spec["truncate"]:
            return None
        seq = seq[:spec["truncate"]]
    if len(seq) < MIN_LENGTH or "N" in seq or any(a in seq for a in ADAPTERS):
        return None
    return seq


def trim(args):
    spec = SOURCES[args.source]
    kept = total = 0
    with open(args.dir / "manifest.tsv") as m, open(args.out, "w") as out:
        for sample, *_ in csv.reader(m, delimiter="\t"):
            path = args.dir / "qc" / f"{sample}.fasta"
            if not path.exists():
                continue
            n = 0
            for _header, seq in read_fasta(path):
                total += 1
                seq = cut(seq, spec)
                if seq:
                    n += 1
                    out.write(f">{sample}.{n};sample={sample}\n{seq}\n")
            kept += n
    print(f"[INFO] {args.source}: kept {kept} of {total} quality-filtered reads")


def pool(args):
    with open(args.out, "w") as out:
        for source in SOURCES:
            for header, seq in read_fasta(args.work / source / "asvs.fasta"):
                out.write(f">{source}|{header.split(';')[0]}\n{seq}\n")


def seen_in(work, source):
    """ASV -> the control samples it has reads in."""
    seen = defaultdict(set)
    with open(work / source / "otutab.tsv") as f:
        samples = f.readline().rstrip("\n").split("\t")[1:]
        for line in f:
            asv, *counts = line.rstrip("\n").split("\t")
            seen[asv] = {s for s, c in zip(samples, counts) if float(c) > 0}
    return seen


def assemble(args):
    totals = {s: sum(1 for _ in open(args.work / s / "manifest.tsv")) for s in SOURCES}
    seen = {s: seen_in(args.work, s) for s in SOURCES}
    members = defaultdict(list)
    with open(args.uc) as f:
        for line in f:
            kind, *cols = line.rstrip("\n").split("\t")
            if kind == "S":
                members[cols[7]].append(cols[7])
            elif kind == "H":
                members[cols[8]].append(cols[7])
    seqs = dict(read_fasta(args.fasta))

    entries = []
    for centroid, labels in members.items():
        sources = sorted({label.split("|")[0] for label in labels})
        blanks = {(label.split("|")[0], sample) for label in labels
                  for sample in seen[label.split("|")[0]].get(label.split("|")[1], ())}
        total = sum(totals[s] for s in sources)
        if len(blanks) >= MIN_PREVALENCE * total:
            entries.append((len(sources), len(blanks), total, sources, seqs[centroid]))
    print(f"[INFO] {len(entries)} of {len(members)} ASVs seen in at least {MIN_PREVALENCE:.0%} of their controls")
    entries.sort(key=lambda e: (-e[0], -e[1], e[4]))

    with open(args.out, "w") as out:
        for i, (studies, blanks, total, sources, seq) in enumerate(entries, 1):
            out.write(f">lit_{i:06d} studies={studies};blanks={blanks}/{total};"
                      f"sources={','.join(sources)}\n{seq}\n")
    by_studies = defaultdict(int)
    for e in entries:
        by_studies[e[0]] += 1
    print(f"[INFO] {len(entries)} entries; by studies: {dict(sorted(by_studies.items()))}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="verb", required=True)
    p = sub.add_parser("fetch", help="download one source's controls and write its manifest")
    p.add_argument("--source", choices=SOURCES, required=True)
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--threads", type=int, default=8)
    p.set_defaults(run=fetch)
    p = sub.add_parser("trim", help="cut one source's quality-filtered reads to the post-515F span")
    p.add_argument("--source", choices=SOURCES, required=True)
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.set_defaults(run=trim)
    p = sub.add_parser("pool", help="tag every source's ASVs with the source and concatenate them")
    p.add_argument("--work", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.set_defaults(run=pool)
    p = sub.add_parser("assemble", help="write the list from the prefix-dereplicated pool")
    p.add_argument("--work", type=Path, required=True)
    p.add_argument("--uc", type=Path, required=True)
    p.add_argument("--fasta", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.set_defaults(run=assemble)
    args = ap.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
