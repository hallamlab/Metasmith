#!/usr/bin/env python3
"""Archive E2's products out of fir's task cache, under nf-core/mag's names.

  census --cache <task_cache> --out <manifest.tsv>
      Read-only. Selects each sample's final products by walking back from its AMBER shard,
      names every file the way nf-core/mag 5.5.0 would, and writes one manifest row per file.

  pack --manifest <manifest.tsv> --cache <task_cache> --dest <dir> (--sample S ... | --index I ...)
      Hardlinks one sample's files into <dest>/.stage under their archive names, writes one
      relabelled CheckM2 report per binner, tars it to <dest>/<arm>/<sample>.tar with a listing
      beside it, and checks every member's name and size against the manifest.

Reads manifest.cbor files and the store's sqlite (opened immutable) directly. Never open this
store through CacheStore or the `metasmith cache` CLI: both write to it.
"""

import argparse
import csv
import hashlib
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

import cbor2

AMBER_TK = "4UdrtQp8"
BINNERS = {"metabat2": "MetaBAT2", "semibin2": "SemiBin2", "comebin": "COMEBin", "das_tool": "DASTool"}
ASSEMBLER = {"e2::megahit_assembly": "MEGAHIT", "e2::flye_assembly": "FLYE"}
ARM = {"MEGAHIT": "short", "FLYE": "long"}
FIELDS = ["arm", "sample", "dataset", "kind", "binner", "shard", "relpath", "bytes", "archive_name", "label"]


def canonical(relpath):
    return re.sub(r"^\d+-", "1-", Path(relpath).name, count=1)


def file_id(slot_id, relpath):
    return hashlib.md5(f"{slot_id}::{canonical(relpath)}".encode()).hexdigest()


class Store:
    def __init__(self, root):
        self.root = root
        self.shards = {}
        self.producer = {}
        self.tomb_db = {}
        db = sqlite3.connect(f"file:{root / 'cache.sqlite'}?immutable=1", uri=True)
        for key, tomb in db.execute("select key, tombstoned_at from entries"):
            self.tomb_db[key.hex()] = tomb
        for base in (root, root / "imported"):
            for prefix in sorted(p for p in base.iterdir() if p.is_dir() and len(p.name) == 2):
                for entry in os.scandir(prefix):
                    self._load(prefix.name + entry.name, Path(entry.path))

    def _load(self, key, path):
        try:
            m = cbor2.loads((path / "manifest.cbor").read_bytes())
        except (OSError, ValueError, cbor2.CBORDecodeError):
            return
        m["_path"] = path
        self.shards[key] = m
        for f in m.get("files") or []:
            fid = f["slot_id"] if m.get("origin") == "imported" else file_id(f["slot_id"], f["relpath"])
            f["_fid"] = fid
            self.producer[fid] = (key, f)

    def servable(self, key):
        m = self.shards[key]
        if (m["_path"] / "tombstone").exists() or self.tomb_db.get(key) is not None:
            return False
        return all((m["_path"] / f["relpath"]).exists() for f in m.get("files") or [] if "relpath" in f)

    def consumed(self, key):
        return [i for ids in (self.shards[key].get("consumes") or {}).values() for i in ids]

    def ancestors(self, key):
        seen, stack = {}, [key]
        while stack:
            k = stack.pop()
            if k in seen:
                continue
            seen[k] = self.shards[k]
            for fid in self.consumed(k):
                if fid in self.producer:
                    stack.append(self.producer[fid][0])
        return seen


def first_contig(path):
    with open(path) as f:
        return f.readline()[1:].split()[0]


def read_table(path):
    table = {}
    with open(path) as f:
        for line in f:
            if line.strip():
                contig, label = line.rstrip("\n").split("\t")[:2]
                table[contig.split()[0]] = label
    return table


def bin_name(asm, binner, sample, label, bin_set=None):
    if binner == "COMEBin":
        return f"{asm}-COMEBin-{sample}.{label}"
    if binner == "DASTool":
        core, sub = (label[:-4], "_sub") if label.endswith("_sub") else (label, "")
        n = re.search(r"(\d+)$", core).group(1)
        return f"{asm}-{bin_set}Refined-{sample}.{n}{sub}"
    return f"{asm}-{label}"


def sample_rows(store, amber_key, checkm2_of):
    lineage = store.ancestors(amber_key)
    by_step = defaultdict(list)
    for k, m in lineage.items():
        by_step[m.get("step_name") or m.get("origin")].append(k)
    meta = [store.producer[i][1] for k in lineage for i in store.consumed(k)
            if i in store.producer and store.producer[i][1]["dtype_name"] == "e2::read_metadata"]
    samples = {Path(f["abspath"]).parent.name for f in meta}
    assert len(samples) == 1, (amber_key, samples)
    sample = samples.pop()
    dataset = sample.rsplit("_sample_", 1)[0]

    files = [(k, f) for k, m in lineage.items() for f in m.get("files") or [] if m.get("origin") == "lineage"]
    asm_files = [(k, f) for k, f in files if f["dtype_name"] in ASSEMBLER]
    assert len(asm_files) == 1, (sample, asm_files)
    asm = ASSEMBLER[asm_files[0][1]["dtype_name"]]
    arm = ARM[asm]
    rows, problems = [], []

    def row(kind, binner, key, f, name, label=""):
        rows.append(dict(arm=arm, sample=sample, dataset=dataset, kind=kind, binner=binner, shard=key,
                         relpath=f["relpath"], bytes=f["size"], archive_name=name, label=label))

    for key in lineage:
        if store.shards[key].get("origin") == "lineage" and not store.servable(key):
            problems.append(f"{sample}: shard {key[:14]} ({store.shards[key].get('step_name')}) not servable")

    fixed = {
        "e2::trimmed_short_reads": ("reads", f"reads/{sample}.fastp.fastq.gz"),
        "e2::fastp_json": ("qc", f"qc/{sample}.fastp.json"),
        "e2::fastp_html": ("qc", f"qc/{sample}.fastp.html"),
        "e2::filtered_long_reads": ("reads", f"reads/{sample}.chopper.fastq.gz"),
        "e2::megahit_assembly": ("assembly", f"assembly/MEGAHIT-{sample}.contigs.fa"),
        "e2::flye_assembly": ("assembly", f"assembly/FLYE-{sample}.assembly.fa"),
        "e2::binning_bam": ("alignment", f"alignment/{asm}-{sample}-{sample}.bam"),
        "e2::contig_gold_standard": ("gold_standard", f"gold_standard/{asm}-{sample}.gsa_mapping.tsv"),
        "e2::das_tool_summary": ("das_tool_summary", f"bins/DASTool/{asm}-DASTool-{sample}_summary.tsv"),
        "e2::amber_results": ("amber", f"amber/{sample}.amber_results.tsv"),
        "e2::amber_bin_metrics": ("amber", f"amber/{sample}.amber_bin_metrics.tsv"),
    }
    for key, f in files:
        if f["dtype_name"] in fixed:
            kind, name = fixed[f["dtype_name"]]
            row(kind, "", key, f, name)

    for step, binner in BINNERS.items():
        keys = by_step.get(step, [])
        if len(keys) != 1:
            problems.append(f"{sample}: {len(keys)} {step} shards in lineage")
            continue
        key = keys[0]
        shard = store.shards[key]
        c2b = [f for f in shard["files"] if f["dtype_name"] == f"e2::{step}_contig_to_bin"]
        bins = [f for f in shard["files"] if f["dtype_name"] == f"e2::{step}_bin"]
        table = read_table(shard["_path"] / c2b[0]["relpath"])
        row("contig_to_bin", binner, key, c2b[0], f"contig_to_bin/{asm}-{binner}-{sample}.contig_to_bin.tsv")
        bin_set = {}
        if step == "das_tool":
            summary = [f for f in shard["files"] if f["dtype_name"] == "e2::das_tool_summary"][0]
            with open(shard["_path"] / summary["relpath"]) as fh:
                bin_set = {r["bin"]: r["bin_set"].removesuffix(".tsv") for r in csv.DictReader(fh, delimiter="\t")}
        for f in bins:
            label = table[first_contig(shard["_path"] / f["relpath"])]
            name = bin_name(asm, binner, sample, label, bin_set.get(label))
            row("bin", binner, key, f, f"bins/{binner}/{name}.fa", label)
            quality = checkm2_of.get(f["_fid"], [])
            if len(quality) != 1:
                problems.append(f"{sample}: bin {name} has {len(quality)} checkm2 shards")
            for qkey, qf in quality[:1]:
                row("checkm2", binner, qkey, qf, f"checkm2/{asm}-{binner}-{sample}.quality_report.tsv", name)
    return sample, rows, problems


def census(args):
    store = Store(Path(args.cache))
    print(f"shards read: {len(store.shards)}; file ids: {len(store.producer)}", flush=True)

    checkm2_of = defaultdict(list)
    for key, m in store.shards.items():
        if m.get("step_name") == "checkm2" and store.servable(key):
            for fid in store.consumed(key):
                if fid in store.producer and store.producer[fid][1]["dtype_name"].endswith("_bin"):
                    checkm2_of[fid].append((key, m["files"][0]))

    ambers = [k for k, m in store.shards.items() if m.get("step_name") == "amber" and m.get("tk") == AMBER_TK]
    print(f"AMBER {AMBER_TK} shards: {len(ambers)}")

    all_rows, problems, seen = [], [], Counter()
    for key in sorted(ambers):
        sample, rows, issues = sample_rows(store, key, checkm2_of)
        seen[sample] += 1
        all_rows += rows
        problems += issues
    problems += [f"{s}: {n} AMBER shards" for s, n in seen.items() if n > 1]

    out = Path(args.out)
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, delimiter="\t")
        w.writeheader()
        w.writerows(sorted(all_rows, key=lambda r: (r["arm"], r["sample"], r["kind"], r["archive_name"])))

    names = Counter((r["sample"], r["archive_name"], r["label"]) for r in all_rows)
    problems += [f"duplicate archive row: {k}" for k, n in names.items() if n > 1]
    report(all_rows, seen)
    print(f"\nproblems: {len(problems)}")
    for p in problems[:200]:
        print("  " + p)
    return 1 if problems else 0


def report(rows, seen):
    print(f"samples: {len(seen)}  " + str(Counter(r["arm"] for r in rows if r["kind"] == "assembly")))
    counts = Counter((r["arm"], r["binner"]) for r in rows if r["kind"] == "bin")
    quality = Counter((r["arm"], r["binner"]) for r in rows if r["kind"] == "checkm2")
    print("\narm    binner    bins   checkm2")
    for (arm, binner), n in sorted(counts.items()):
        print(f"{arm:6} {binner:9} {n:6d} {quality[(arm, binner)]:7d}")
    per_sample = defaultdict(Counter)
    for r in rows:
        per_sample[r["sample"]][r["kind"]] += 1
    for kind in ("reads", "assembly", "alignment", "gold_standard", "amber"):
        missing = sorted(s for s in per_sample if not per_sample[s][kind])
        print(f"samples missing {kind}: {len(missing)} {missing[:5]}")
    total = Counter()
    for r in rows:
        total[r["kind"]] += int(r["bytes"])
    print("\nkind              files        GB")
    for kind, b in sorted(total.items(), key=lambda kv: -kv[1]):
        print(f"{kind:16} {sum(1 for r in rows if r['kind'] == kind):7d} {b / 1e9:9.1f}")
    print(f"{'total':16} {len(rows):7d} {sum(total.values()) / 1e9:9.1f}")


def pack(args):
    with open(args.manifest) as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    samples = sorted({r["sample"] for r in rows})
    chosen = [samples[i] for i in args.index] if args.index else args.sample
    cache, dest = Path(args.cache), Path(args.dest)
    failed = 0
    for sample in chosen:
        failed += pack_sample(sample, [r for r in rows if r["sample"] == sample], cache, dest)
    return 1 if failed else 0


def pack_sample(sample, rows, cache, dest):
    arm = rows[0]["arm"]
    stage = dest / ".stage" / sample
    if stage.exists():
        shutil.rmtree(stage)
    root = stage / sample
    expected = {}
    reports = defaultdict(list)
    for r in rows:
        src = cache / r["shard"][:2] / r["shard"][2:] / r["relpath"]
        if r["kind"] == "checkm2":
            reports[r["archive_name"]].append((r["label"], src))
            continue
        target = root / r["archive_name"]
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(src, target)
        expected[r["archive_name"]] = int(r["bytes"])

    for name, members in reports.items():
        lines = []
        for label, src in sorted(members):
            header, row = src.read_text().splitlines()[:2]
            lines = lines or [header]
            lines.append("\t".join([label] + row.split("\t")[1:]))
        text = "\n".join(lines) + "\n"
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
        expected[name] = len(text.encode())

    with open(root / "MANIFEST.tsv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    expected["MANIFEST.tsv"] = (root / "MANIFEST.tsv").stat().st_size

    tarball = dest / arm / f"{sample}.tar"
    tarball.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["tar", "-cf", tarball, "-C", stage, sample], check=True)
    listing = subprocess.run(["tar", "-tvf", tarball], check=True, capture_output=True, text=True).stdout
    Path(f"{tarball}.list").write_text(listing)

    found = {}
    for line in listing.splitlines():
        if line.startswith("-"):
            f = line.split(None, 5)
            found[f[5].removeprefix(f"{sample}/")] = int(f[2])
    shutil.rmtree(stage)
    bad = sorted(set(expected) ^ set(found)) + sorted(k for k in expected if k in found and expected[k] != found[k])
    print(f"{sample}: {len(found)} members, {sum(found.values()) / 1e9:.2f} GB, {'OK' if not bad else f'{len(bad)} MISMATCHES'}",
          flush=True)
    for k in bad[:20]:
        print(f"  {k}: expected {expected.get(k)} found {found.get(k)}")
    return 1 if bad else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("census")
    p.add_argument("--cache", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=census)
    p = sub.add_parser("pack")
    p.add_argument("--manifest", required=True)
    p.add_argument("--cache", required=True)
    p.add_argument("--dest", required=True)
    p.add_argument("--sample", nargs="*", default=[])
    p.add_argument("--index", nargs="*", type=int, default=[], help="positions in the sorted sample list")
    p.set_defaults(fn=pack)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
