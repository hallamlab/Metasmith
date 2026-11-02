#!/usr/bin/env python3
"""Score Pratama's published vOTUs against an E3 skani table, as a curve over ANI and alignment fraction.

Stdlib only. Two steps:

  tally  <published names>            the published catalogue's headers, one per line, to
                                      published_votu_tally.tsv (sample, lane, caller, name)
  curve  <skani table> --scope NAME   recovery per published lane and caller, for the runs given

A published name carries its sample, assembler and caller (Hain_H43_02um_R3_2019-genomad-spades_...,
H14R101um_2022_longread-vibrant_...). The denominator is the published vOTUs of the selected runs'
samples. A published vOTU counts as recovered at (ANI, AF) when some E3 sequence reaches both, with AF
the fraction of the published vOTU that aligns. `same_sample` also requires that sequence to come from
the published vOTU's own sample, read from the E3 frozen_id `sample|lane|caller|contig|start_end`.
The merge labels a sample by its imported read pair (`read_pair@<hash>`), so `--labels` maps each label
to its run: `ls -d <imports>/e3/*/read_pair@*` lists them as `<run>/<label>`.
"""

import argparse
import csv
import gzip
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
RUNS_TSV = REPO / "research" / "pratama2026" / "runs.tsv"
TALLY = HERE / "published_votu_tally.tsv.gz"
EXCLUDED_RUNS = {"ERR3858126"}

ANI_GRID = (80, 85, 90, 95, 97, 99)
AF_GRID = (15, 30, 50, 70, 85)

CALLERS = {"genomad": "genomad", "vibrant": "vibrant", "vs2": "virsorter2", "dvf": "deepvirfinder"}
PUB_2019 = re.compile(r"^Hain_(H\d+)_(0\d)um_R(\d)")
PUB_2022 = re.compile(r"^(H\d+)R(\d)(0\d)um_2022")
RUN_2019 = re.compile(r"^(H\d+)_0_(\d)_(\d)_R1")
RUN_2022 = re.compile(r"^(H\d+)R(\d)(0\d)um_2022$")


def published_key(name):
    if m := PUB_2019.match(name):
        return f"{m[1]}_{m[2]}um_R{m[3]}_2019"
    if m := PUB_2022.match(name):
        return f"{m[1]}_{m[3]}um_R{m[2]}_2022"
    return None


def published_lane_caller(name):
    low = name.lower()
    caller = next((v for k, v in CALLERS.items() if k in low), None)
    lane = "hybrid" if "longread" in low else "spades" if "spades" in low else "megahit" if "megahit" in low else None
    return lane, caller


def run_keys():
    keys = {}
    with open(RUNS_TSV, newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            run = row["run_accession"]
            if run in EXCLUDED_RUNS or "Nanopore" in row["sample_alias"]:
                continue
            if m := RUN_2019.match(row["submitted_names"]):
                keys[run] = f"{m[1]}_0{m[2]}um_R{m[3]}_2019"
            elif m := RUN_2022.match(row["sample_alias"]):
                keys[run] = f"{m[1]}_{m[3]}um_R{m[2]}_2022"
    return keys


def cmd_tally(args):
    rows, bad = [], []
    for line in open(args.names):
        name = line.strip().lstrip(">").split()[0]
        if not name:
            continue
        key = published_key(name)
        lane, caller = published_lane_caller(name)
        if None in (key, lane, caller):
            bad.append(name)
            continue
        rows.append((key, lane, caller, name))
    assert not bad, f"{len(bad)} names parse to no sample, lane or caller, e.g. {bad[:3]}"
    unmapped = {r[0] for r in rows} - set(run_keys().values())
    assert not unmapped, f"published samples with no run: {sorted(unmapped)}"
    with gzip.open(TALLY, "wt", newline="") as f:
        f.write("sample\tlane\tcaller\tname\n")
        for r in sorted(rows):
            f.write("\t".join(r) + "\n")
    for dim, i in (("lane", 1), ("caller", 2)):
        print(dim, dict(Counter(r[i] for r in rows)))
    print(f"{len(rows)} published vOTUs -> {TALLY}")


def load_published(samples):
    by_name = {}
    with gzip.open(TALLY, "rt", newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            if row["sample"] in samples:
                by_name[row["name"]] = row
    return by_name


def load_labels(path):
    """Label -> run, from the `<run>/<label>` lines of an imports listing."""
    labels = {}
    for line in open(path) if path else ():
        run, _, label = line.strip().rstrip("/").rpartition("/")
        if label:
            labels[label] = run.rsplit("/", 1)[-1]
    return labels


def best_hits(skani_path, published, keys, labels):
    """Per published name, the (ANI, AF) pairs reached by any E3 sequence and by one from its own sample."""
    hits_any, hits_same, unmapped = defaultdict(list), defaultdict(list), set()
    with open(skani_path, newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            pub = row["Query_name"].split()[0]
            if pub not in published:
                continue
            point = (float(row["ANI"]), float(row["Align_fraction_query"]))
            hits_any[pub].append(point)
            label = row["Ref_name"].split()[0].split("|")[0]
            run = labels.get(label, label)
            if run not in keys:
                unmapped.add(label)
            if keys.get(run) == published[pub]["sample"]:
                hits_same[pub].append(point)
    assert not unmapped, f"E3 sample labels with no run, pass --labels: {sorted(unmapped)[:5]}"
    return hits_any, hits_same


def recovered(points, ani, af):
    return any(a >= ani and f >= af for a, f in points)


def cmd_curve(args):
    keys = run_keys()
    runs = args.runs or sorted(keys)
    unknown = set(runs) - set(keys)
    assert not unknown, f"not a scored short-read run: {sorted(unknown)}"
    published = load_published({keys[r] for r in runs})
    labels = load_labels(args.labels)
    hits_any, hits_same = best_hits(args.skani, published, keys, labels)

    groups = defaultdict(list)
    for name, row in published.items():
        for lane in (row["lane"], "all"):
            for caller in (row["caller"], "all"):
                groups[(lane, caller)].append(name)

    out = csv.writer(sys.stdout if args.out == "-" else open(args.out, "w", newline=""), delimiter="\t")
    out.writerow(["scope", "runs", "lane", "caller", "ani", "af", "published", "recovered_any",
                  "recovered_same_sample", "fraction_any", "fraction_same_sample"])
    for (lane, caller), names in sorted(groups.items()):
        for ani in ANI_GRID:
            for af in AF_GRID:
                n_any = sum(recovered(hits_any[n], ani, af) for n in names)
                n_same = sum(recovered(hits_same[n], ani, af) for n in names)
                out.writerow([args.scope, len(runs), lane, caller, ani, af, len(names), n_any, n_same,
                              f"{n_any / len(names):.4f}", f"{n_same / len(names):.4f}"])


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("tally")
    t.add_argument("names", help="published headers, one per line (grep '>' of the catalogue FASTA)")
    t.set_defaults(func=cmd_tally)
    c = sub.add_parser("curve")
    c.add_argument("skani", help="an E3 recovery table (skani dist, published as query)")
    c.add_argument("--scope", required=True, help="label for the table, e.g. pool or final")
    c.add_argument("--runs", nargs="*", help="run accessions scored; default all 65")
    c.add_argument("--labels", help="imports listing mapping read_pair labels to runs, one <run>/<label> per line")
    c.add_argument("--out", default="-")
    c.set_defaults(func=cmd_curve)
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
