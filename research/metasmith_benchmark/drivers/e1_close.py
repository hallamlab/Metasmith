#!/usr/bin/env python3
"""Closes E1's gaps on fir: sample sheet, per-sample contig-to-bin maps, bin FASTAs, result tables.

Verbs:
  sheet    one row per E1 sample: assembly, BAM, E2's QC'd reads and the CAMI read truth
  split    cut nf-core's run-wide contig_to_bin_map.tsv into one map per assembly
  derive   write one sample's bins for the named binners as FASTAs, from its map and assembly
  patch    replace one binner's rows in a sample's map with a two-column contig-to-bin table
  collect  gather the AMBER outputs into E2's two table layouts
"""

import argparse
import csv
import gzip
import sys
from pathlib import Path

LONG_DATASETS = {"plant_associated_long_nano": "plant_associated", "toy_humangut_long": "toy_humangut"}
MAP_HEADER = ["assembly_id", "contig_id", "binner", "bin_id"]
BINNERS = ("COMEBin", "DASTool", "MetaBAT2", "SemiBin2")
SHEET = ["idx", "arm", "sample", "dataset", "assembly_id", "assembly", "bam", "reads", "truth"]


def read_tsv(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def write_tsv(path, header, rows):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def sheet_rows(path):
    return {r["sample"]: r for r in read_tsv(path)}


def sheet(args):
    reads = {r["sample"]: r for r in read_tsv(args.e2_manifest) if r["kind"] == "reads"}
    long_replaced = set(LONG_DATASETS.values())
    rows = []
    for r in read_tsv(args.cami_samples):
        sample = f"{r['dataset']}_{r['sample_id']}"
        truth = str(Path(r["reads_path"]).parent / "reads_mapping.tsv.gz")
        if r["read_type"] == "short" and r["dataset"] not in long_replaced:
            asm_id = f"MEGAHIT-{sample}"
            rows.append(["short", sample, r["dataset"], asm_id,
                         f"{args.short_assemblies}/{asm_id}.contigs.fa.gz",
                         f"{args.work}/bam/{asm_id}-{sample}.bam"])
        elif r["read_type"] == "long" and r["dataset"] in LONG_DATASETS:
            asm_id = f"FLYE-{sample}"
            rows.append(["long", sample, r["dataset"], asm_id,
                         f"{args.kept_inputs}/{asm_id}.assembly.fasta",
                         f"{args.kept_inputs}/{asm_id}-{sample}.bam"])
        else:
            continue
        e2 = reads[sample]
        rows[-1] += [f"{args.cache}/{e2['shard'][:2]}/{e2['shard'][2:]}/{e2['relpath']}", truth]
    rows.sort(key=lambda x: (x[0] != "short", x[1]))
    assert sum(r[0] == "short" for r in rows) == 208 and sum(r[0] == "long" for r in rows) == 41, len(rows)
    write_tsv(args.out, SHEET, [[i + 1, *r] for i, r in enumerate(rows)])


def split(args):
    wanted = {r["assembly_id"]: r["sample"] for r in sheet_rows(args.sheet).values()}
    out = {a: [] for a in wanted}
    for path in args.maps:
        with open(path, newline="") as fh:
            reader = csv.reader(fh, delimiter="\t")
            assert next(reader) == MAP_HEADER, path
            for row in reader:
                if row and row[0] in out:
                    out[row[0]].append(row)
    args.outdir.mkdir(parents=True, exist_ok=True)
    counts = []
    for asm_id, rows in sorted(out.items()):
        write_tsv(args.outdir / f"{wanted[asm_id]}.tsv", MAP_HEADER, rows)
        for b in BINNERS:
            bins = {r[3] for r in rows if r[2] == b and "unbinned" not in r[3].lower()}
            counts.append([wanted[asm_id], b, len(bins)])
    write_tsv(args.outdir / "bin_counts.tsv", ["sample", "binner", "bins"], counts)


def read_fasta(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    seqs, name, buf = {}, None, []
    with opener(path, "rt") as fh:
        for line in fh:
            if line.startswith(">"):
                if name is not None:
                    seqs[name] = "".join(buf)
                name, buf = line[1:].split()[0], []
            else:
                buf.append(line.strip())
    if name is not None:
        seqs[name] = "".join(buf)
    return seqs


def bin_stem(bin_id):
    return bin_id[:-3] if bin_id.endswith(".fa") else bin_id


def derive(args):
    s = sheet_rows(args.sheet)[args.sample]
    seqs = read_fasta(s["assembly"])
    members = {}
    for r in read_tsv(args.maps / f"{args.sample}.tsv"):
        if r["binner"] == args.binner and "unbinned" not in r["bin_id"].lower():
            members.setdefault(bin_stem(r["bin_id"]), []).append(r["contig_id"].split()[0])
    assert members, f"{args.sample} has no {args.binner} bins"
    args.outdir.mkdir(parents=True, exist_ok=True)
    for name, contigs in members.items():
        missing = [c for c in contigs if c not in seqs]
        assert not missing, f"{name}: {len(missing)} contigs absent from {s['assembly']}"
        with open(args.outdir / f"{name}.fa", "w") as fh:
            for c in contigs:
                fh.write(f">{c}\n{seqs[c]}\n")
    print(f"{args.sample} {args.binner}: {len(members)} bins")


def patch(args):
    s = sheet_rows(args.sheet)[args.sample]
    path = args.maps / f"{args.sample}.tsv"
    kept = [[r[k] for k in MAP_HEADER] for r in read_tsv(path) if r["binner"] != args.binner]
    with open(args.contig2bin) as fh:
        new = [[s["assembly_id"], c.split()[0], args.binner, b] for c, b in
               (line.rstrip("\n").split("\t")[:2] for line in fh if line.strip())]
    write_tsv(path, MAP_HEADER, kept + new)
    print(f"{args.sample} {args.binner}: {len(new)} rows patched in")


def collect(args):
    summary, metrics = [], []
    s_header = m_header = None
    for s in sheet_rows(args.sheet).values():
        for b in BINNERS:
            d = args.amber / s["sample"] / b
            res = read_tsv(d / "results.tsv")
            assert len(res) == 1, d
            s_header = s_header or list(res[0])
            summary.append([s["arm"], args.run_key, s["sample"], *res[0].values()])
            for row in read_tsv(d / "metrics_per_bin.tsv"):
                m_header = m_header or list(row)
                metrics.append([s["arm"], args.run_key, s["sample"], b, *row.values()])
    write_tsv(args.out_prefix.with_name(args.out_prefix.name + "_summary.tsv"),
              ["arm", "run_key", "sample", *s_header], summary)
    write_tsv(args.out_prefix.with_name(args.out_prefix.name + "_bin_metrics.tsv"),
              ["arm", "run_key", "sample", "Tool", *m_header], metrics)
    print(f"{len(summary)} summary rows, {len(metrics)} bin rows")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="verb", required=True)

    p = sub.add_parser("sheet")
    p.add_argument("--cami-samples", type=Path, required=True)
    p.add_argument("--e2-manifest", type=Path, required=True)
    p.add_argument("--cache", required=True)
    p.add_argument("--short-assemblies", required=True)
    p.add_argument("--kept-inputs", required=True)
    p.add_argument("--work", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.set_defaults(fn=sheet)

    p = sub.add_parser("split")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--maps", type=Path, nargs="+", required=True)
    p.add_argument("--outdir", type=Path, required=True)
    p.set_defaults(fn=split)

    p = sub.add_parser("derive")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--maps", type=Path, required=True)
    p.add_argument("--sample", required=True)
    p.add_argument("--binner", required=True, choices=BINNERS)
    p.add_argument("--outdir", type=Path, required=True)
    p.set_defaults(fn=derive)

    p = sub.add_parser("patch")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--maps", type=Path, required=True)
    p.add_argument("--sample", required=True)
    p.add_argument("--binner", required=True, choices=BINNERS)
    p.add_argument("--contig2bin", type=Path, required=True)
    p.set_defaults(fn=patch)

    p = sub.add_parser("collect")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--amber", type=Path, required=True)
    p.add_argument("--run-key", default="E1")
    p.add_argument("--out-prefix", type=Path, required=True)
    p.set_defaults(fn=collect)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
