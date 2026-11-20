#!/usr/bin/env python3
"""AMBER on E5's DAS Tool bins for every CAMI sample, outside any plan: a supplementary table.

E5 adds no measurement of its own, so this scores E5's CAMI bins exactly as E1's close-out scored
nf-core's: research/cami/score_reference_amber.py, with E2's gold-standard vote
(library/resources/e2/cami_gold_standard.py) and the same AMBER image. The gold standard votes CAMISIM's
read truth onto each run's own contigs, so a score is conditional on that run's assembly.

Verbs:
  sheet    on fir: one row per sample of the named runs, with its assembly, binning BAM, DAS Tool table
           and the read truth of the reads that BAM mapped
  collect  gather the AMBER outputs into E2's two table layouts
"""

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from e5_assemblies import ASSEMBLY_DIRS, HOME, read_index, sample_of  # noqa: E402

CAMI_LONG = {"toy_humangut": "toy_humangut_long", "plant_associated": "plant_associated_long_nano",
             "marine": "marine_long", "strain": "strain_long"}
SHEET = ["idx", "run", "study", "sample", "assembly", "bam", "das_tool", "reads", "truth"]
DAS_TOOL_DIR = "binning-das_tool_contig_to_bin_table"


def read_types(path):
    """{instance key: type} from a results index."""
    types, key = {}, None
    with open(path) as f:
        for line in f:
            if line.startswith("  ") and not line.startswith("   "):
                key = line.strip().rstrip(":")
            elif line.startswith("    type: "):
                types[key] = line.split(": ", 1)[1].strip()
    return types


def shard_files(names):
    """{file name: path in the task cache} for the unevicted shards holding any of the names."""
    cache = HOME / "task_cache"
    db = sqlite3.connect(f"file:{cache / 'cache.sqlite'}?mode=ro", uri=True)
    pattern = re.compile(rb"out/[0-9-]+\.[0-9a-f]+-[A-Za-z0-9]+\.bam")
    found = {}
    for root, payload in db.execute("select output_root, payload from entries where tombstoned_at is null"):
        for m in pattern.finditer(payload):
            rel = m.group().decode()
            name = rel.rsplit("/", 1)[1]
            if name in names and (cache / root / rel).exists():
                found[name] = cache / root / rel
    return found


def truth_of(study, sample, long_reads):
    dataset = CAMI_LONG[study] if long_reads else study
    sample_id = sample.removeprefix(f"{CAMI_LONG.get(study, study)}_").removeprefix(f"{study}_")
    with open(HERE.parent.parent / "cami" / "samples.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["dataset"] == dataset and r["sample_id"] == sample_id and r["read_type"] == ("long" if long_reads else "short"):
                assert r["has_truth"] == "1", f"{dataset} {sample_id} has no read truth"
                return Path(r["reads_path"]).parent / "reads_mapping.tsv.gz"
    raise SystemExit(f"no samples.tsv row for {dataset} {sample_id}")


def sheet(args):
    rows, bams = [], {}
    for run in args.runs:
        results = HOME / "runs" / run / "results"
        index = results / "_metadata" / "index.yml"
        parents, types = read_index(index), read_types(index)
        children = {}
        for k, ps in parents.items():
            for p in ps:
                children.setdefault(p, []).append(k)
        das_of = {sample_of(f"{DAS_TOOL_DIR}/{f.name}", parents)[1]: f
                  for f in (results / DAS_TOOL_DIR).glob("*")}
        for d in ASSEMBLY_DIRS:
            for f in sorted((results / d).glob("*")) if (results / d).is_dir() else ():
                asm = f"{d}/{f.name}"
                study, sample = sample_of(asm, parents)
                bam = [k for k in children.get(asm, ()) if types.get(k) == "alignment::bam"]
                assert len(bam) == 1, f"{run} {sample}: {len(bam)} BAMs"
                # Flye's BAM maps the long reads it assembled. Every other BAM maps the trimmed short reads:
                # a hybrid sample's Nanopore reads are e3::nanopore_reads, which assembly_stats cannot take.
                long_reads = ASSEMBLY_DIRS[d] == "flye"
                bams[bam[0].rsplit("/", 1)[1]] = len(rows)
                rows.append({"run": run, "study": study, "sample": sample, "assembly": f, "bam": None,
                             "das_tool": das_of.get(sample, ""), "reads": "long" if long_reads else "short",
                             "truth": truth_of(study, sample, long_reads)})
        print(f"{run}: {sum(r['run'] == run for r in rows)} samples", file=sys.stderr)
    found = shard_files(set(bams))
    missing = [n for n in bams if n not in found]
    assert not missing, f"{len(missing)} BAMs are not in the task cache, e.g. {missing[:3]}"
    for name, i in bams.items():
        rows[i]["bam"] = found[name]
    w = csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")
    w.writerow(SHEET)
    for i, r in enumerate(rows):
        w.writerow([i, *(r[k] for k in SHEET[1:])])


def read_tsv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def collect(args):
    summary, metrics, s_header, m_header, absent = [], [], None, None, []
    for s in read_tsv(args.sheet):
        d = args.amber / s["sample"] / "DASTool"
        if not (d / "results.tsv").exists():
            absent.append(s["sample"])
            continue
        res = read_tsv(d / "results.tsv")
        assert len(res) == 1, d
        s_header = s_header or list(res[0])
        summary.append([s["run"], s["study"], s["sample"], *res[0].values()])
        for row in read_tsv(d / "metrics_per_bin.tsv"):
            m_header = m_header or list(row)
            metrics.append([s["run"], s["study"], s["sample"], "DASTool", *row.values()])
    for suffix, header, rows in (("summary", ["run", "study", "sample", *s_header], summary),
                                 ("bin_metrics", ["run", "study", "sample", "Tool", *m_header], metrics)):
        with open(f"{args.out_prefix}_{suffix}.tsv", "w", newline="") as f:
            w = csv.writer(f, delimiter="\t", lineterminator="\n")
            w.writerow(header)
            w.writerows(rows)
    print(f"{len(summary)} summary rows, {len(metrics)} bin rows; no AMBER output for {len(absent)}: {absent}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="verb", required=True)
    p = sub.add_parser("sheet")
    p.add_argument("runs", nargs="+")
    p.set_defaults(fn=sheet)
    p = sub.add_parser("collect")
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--amber", type=Path, required=True)
    p.add_argument("--out-prefix", required=True)
    p.set_defaults(fn=collect)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
