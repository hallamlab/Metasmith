#!/usr/bin/env python3
"""Score every rung of E4's ablation ladder, and metaGEM's GEM, with MEMOTE 0.17.

stage    write metaGEM's published GEM of every subset bin beside the rungs e4_ablation.py extract wrote
         under <work>/models/<rung>/, then pack them into one tar per block of BLOCK bins,
         <work>/models/block_NN.tar, and delete the loose models. Lists the blocks in <work>/blocks.tsv.
retry    pack every (source, bin) the scored blocks lack into <work>/models/retry_NN.tar.
collect  read the task tars e4_ablation_memote.sbatch wrote under <work>/memote/ and write one row per
         bin and source. M is not rescored: its rows are the modern lane's archived MEMOTE 0.17 scores.
         --fidelity writes one row per rewritten model from sbml_rewrite.py's reports.

By default every model is scored as built. CarveMe 1.2.2's SBML writer (framed 0.5.1) drops each species'
formula and charge, so R0 to U lose consistency points the later rungs and metaGEM keep. The sbatch's
REWRITE_IMAGE instead rewrites each model through CarveMe 1.6.6's writer before scoring it. --sources
narrows stage and collect to the sources that run rescores, and leaves M out.
"""

import argparse
import csv
import gzip
import io
import json
import sys
import tarfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from e4_ablation import BINS, RUNGS, read_bins
from e4_quality import SECTIONS, parse_score

BLOCK = 32
SCORED = [r for r in RUNGS if r != "M"]
SOURCES = ["metagem"] + SCORED
COLUMNS = ["study", "bin", "source", "total_score"] + SECTIONS + ["note"]


def blocks(bins):
    return [bins[i:i + BLOCK] for i in range(0, len(bins), BLOCK)]


def pack(path, entries):
    part = path.with_suffix(".tar.part")
    with tarfile.open(part, "w") as tar:
        for arcname, data in entries:
            info = tarfile.TarInfo(arcname)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    part.rename(path)


def cmd_stage(args):
    import e4_parity

    e4_parity.PUBLISHED = args.root / "published"
    models = args.work / "models"
    bins = read_bins(args.bins_file)
    sources = args.sources or SOURCES
    for rung in [s for s in sources if s != "metagem"]:
        if not (models / rung / ".done").exists():
            sys.exit(f"{rung}: not extracted, run e4_ablation.py extract --out {models} first")
    gems = models / "metagem"
    if "metagem" in sources and not (gems / ".done").exists():
        gems.mkdir(exist_ok=True)
        n = 0
        for study in sorted({s for s, _ in bins}):
            for name, sbml in e4_parity.published_models(study, [b for s, b in bins if s == study]):
                (gems / f"{name}.xml.gz").write_bytes(gzip.compress(sbml))
                n += 1
        (gems / ".done").write_text(f"{n}\n")
    counts = {src: 0 for src in sources}
    with open(args.work / "blocks.tsv", "w") as fh:
        fh.write("block\tstudy\tbin\n")
        for i, block in enumerate(blocks(bins), 1):
            entries = []
            for study, b in block:
                fh.write(f"{i}\t{study}\t{b}\n")
                for src in sources:
                    model = models / src / f"{b}.xml.gz"
                    if model.exists():
                        entries.append((f"{src}/{b}.xml.gz", model.read_bytes()))
                        counts[src] += 1
            pack(models / f"block_{i:02d}.tar", entries)
    print("\t".join(f"{src} {n}" for src, n in counts.items()), file=sys.stderr)
    for src in sources:
        for model in (models / src).glob("*.xml.gz"):
            model.unlink()
        (models / src / ".done").unlink()
        (models / src).rmdir()


def scored(work):
    have, notes, rewrites = {}, {}, {}
    for out in sorted((work / "memote").glob("*.tar")):
        with tarfile.open(out) as tar:
            for member in tar:
                src, _, name = member.name.removeprefix("./").partition("/")
                if name.endswith(".score.json"):
                    have[(src, name.removesuffix(".score.json"))] = parse_score(tar.extractfile(member).read())
                elif name.endswith(".rewrite.json"):
                    rewrites[(src, name.removesuffix(".rewrite.json"))] = json.loads(tar.extractfile(member).read())
                elif member.name.endswith("status.tsv"):
                    for line in tar.extractfile(member).read().decode().splitlines():
                        src, b, rc = line.split("\t")[:3]
                        if rc != "0":
                            notes[(src, b)] = "timeout" if rc == "124" else f"exit {rc}"
    return have, notes, rewrites


def packed(work):
    models = {}
    for path in sorted((work / "models").glob("block_*.tar")):
        with tarfile.open(path) as tar:
            for member in tar:
                src, _, name = member.name.partition("/")
                models[(src, name.removesuffix(".xml.gz"))] = path
    return models


def cmd_retry(args):
    have, _, _ = scored(args.work)
    missing = {}
    for key, path in packed(args.work).items():
        if key not in have:
            missing.setdefault(path, []).append(key)
    n = 0
    for path, keys in sorted(missing.items()):
        with tarfile.open(path) as tar:
            entries = [(f"{src}/{b}.xml.gz", tar.extractfile(f"{src}/{b}.xml.gz").read()) for src, b in keys]
        for i in range(0, len(entries), BLOCK):
            n += 1
            pack(args.work / "models" / f"retry_{n:02d}.tar", entries[i:i + BLOCK])
    print(f"{sum(map(len, missing.values()))} models missing, packed into {n} retry tars", file=sys.stderr)


def cmd_collect(args):
    bins = read_bins(args.bins_file)
    have, notes, rewrites = scored(args.work)
    models = packed(args.work)
    sources = args.sources or SOURCES + ["M"]
    modern = {}
    if "M" in sources:
        with open(args.tables / "quality_modern.tsv") as fh:
            modern = {r["bin"]: r for r in csv.DictReader(fh, delimiter="\t")}
    rows, missing = [], []
    for study, b in bins:
        for src in sources:
            if src == "M":
                score = modern.get(b)
                note = "" if score else "no archived score"
            else:
                score = have.get((src, b))
                note = "" if score else notes.get((src, b), "not scored" if (src, b) in models else "no model")
            if note:
                missing.append((src, b, note))
            rows.append([study, b, src] + [score[c] if score else "" for c in ["total_score"] + SECTIONS] + [note])
    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(COLUMNS)
        w.writerows(rows)
    print(f"{len(rows)} rows, {len(missing)} without a score", file=sys.stderr)
    for src, b, note in missing:
        print(f"  {src}\t{b}\t{note}", file=sys.stderr)
    if args.fidelity:
        write_fidelity(args.fidelity, bins, sources, rewrites)


def write_fidelity(path, bins, sources, rewrites):
    checks = sorted({k for r in rewrites.values() for k in r["diffs"]})
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(["study", "bin", "source", "content_identical"] + checks + [
            f"{side}_{key}" for side in ("before", "after") for key in ("formula", "charge", "sbo", "namespaces")])
        for study, b in bins:
            for src in sources:
                r = rewrites.get((src, b))
                if r:
                    w.writerow([study, b, src, r["content_identical"]]
                               + [r["diffs"].get(k, {}).get("n", 0) for k in checks]
                               + [json.dumps(r[side][key], sort_keys=True) if key in ("sbo", "namespaces") else r[side][key]
                                  for side in ("before", "after") for key in ("formula", "charge", "sbo", "namespaces")])
    n = sum(r["content_identical"] for r in rewrites.values())
    print(f"{len(rewrites)} rewrites, {n} with identical content", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=["stage", "retry", "collect"])
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--root", type=Path, help="stage: holds published/<study>/ with metaGEM's GEM tarballs")
    ap.add_argument("--bins-file", type=Path, default=BINS)
    ap.add_argument("--tables", type=Path, default=HERE.parent / "results" / "e4",
                    help="collect: where quality_modern.tsv is")
    ap.add_argument("--out", type=Path, help="collect: the TSV to write")
    ap.add_argument("--fidelity", type=Path, help="collect: the TSV of rewrite checks to write")
    ap.add_argument("--sources", nargs="+", choices=SOURCES, help="stage, collect: only these sources")
    args = ap.parse_args()
    {"stage": cmd_stage, "retry": cmd_retry, "collect": cmd_collect}[args.step](args)


if __name__ == "__main__":
    main()
