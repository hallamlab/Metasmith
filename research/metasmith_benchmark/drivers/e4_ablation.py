#!/usr/bin/env python3
"""E4's ablation ladder over a stratified 10% subset of its bins.

pick     sample 10% of each study's bins that have a metaGEM GEM and a model in both archived lanes,
         and write them to e4_ablation_bins.tsv.
extract  write every rung's model of every subset bin out of the lane archives, as <out>/<rung>/<bin>.xml.gz.
         R0 and M come from E4's archives, the other rungs from the ablation's. Run it on fir.
compare  compare each rung's model with metaGEM's GEM and with the previous rung's, one row per bin,
         rung and reference.
summary  medians and interquartile ranges of compare's table, pooled and per study.
"""

import argparse
import csv
import math
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
TALLY = HERE.parent / "results/e4/tally.tsv"
BINS = HERE / "e4_ablation_bins.tsv"
SEED = 20260928
FRACTION = 0.10
ROOT = Path("/scratch/phyberos/e4_ablation")

# rung: (archive directory under ROOT, model type). Each rung adds one change to the one before it.
LADDER = {
    "R0": ("e4_gems_archive/repro", "modelling::carveme_model_cplex"),
    "R1": ("archive/repro", "modelling::carveme_model_cplex"),
    "B": ("archive/bigg", "e4abl::model_bigg"),
    "U": ("archive/universe", "e4abl::model_universe"),
    "V": ("archive/version", "e4abl::model_version"),
    "G": ("archive/gapfill", "e4abl::model_gapfill"),
    "D": ("archive/diamond", "e4abl::model_diamond"),
    "S": ("archive/scip", "e4abl::model_scip"),
    "M": ("e4_gems_archive/modern", "modelling::carveme_model"),
}
RUNGS = list(LADDER)
KINDS = ("reactions", "metabolites", "genes")


def read_bins(path=BINS):
    with open(path) as f:
        return [(r["study"], r["bin"]) for r in csv.DictReader(f, delimiter="\t")]


def cmd_pick(args):
    eligible = defaultdict(list)
    with open(TALLY) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["metagem_gem"] == r["repro_model"] == r["modern_model"] == "1":
                eligible[r["study"]].append(r["bin"])
    rng = random.Random(SEED)
    picked = []
    for study in sorted(eligible):
        pool = sorted(eligible[study])
        n = math.floor(len(pool) * FRACTION + 0.5)
        picked += [(study, b) for b in sorted(rng.sample(pool, n))]
        print(f"{study}\t{len(pool)}\t{n}")
    print(f"total\t{sum(map(len, eligible.values()))}\t{len(picked)}")
    with open(args.out, "w") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["study", "bin"])
        w.writerows(picked)


def cmd_extract(args):
    import e4_tally

    wanted = {b for _, b in read_bins(args.bins_file)}
    for rung in args.rungs:
        subdir, dtype = LADDER[rung]
        out = args.out / rung
        if (out / ".done").exists():
            print(f"{rung}: already extracted", file=sys.stderr)
            continue
        out.mkdir(parents=True, exist_ok=True)
        archives = sorted((args.root / subdir).glob("chunk*.tar.zst"))
        if not archives:
            sys.exit(f"{rung}: no archive under {args.root / subdir}")
        n = 0
        for archive in archives:
            manifest = e4_tally.read_index(archive)
            owner = e4_tally.bins_of(manifest)
            models = {p: owner[p] for p, e in manifest.items() if e.get("type") == dtype and owner[p] in wanted}
            if len(set(models.values())) != len(models):
                sys.exit(f"{archive}: a bin has more than one {dtype}")
            if models:
                e4_tally.extract_models(archive, models, out)
            n += len(models)
            print(f"{rung} {archive.name}: {len(models)} models", file=sys.stderr)
        (out / ".done").write_text(f"{n}\n")
        print(f"{rung}: {n} of {len(wanted)} bins", file=sys.stderr)


def jaccard(a, b):
    union = len(a | b)
    return len(a & b) / union if union else 1.0


def metrics(ours, theirs):
    row = {f"{k}_jaccard": round(jaccard(set(ours[k]), set(theirs[k])), 4) for k in KINDS}
    shared = set(ours["reactions"]) & set(theirs["reactions"])
    differs = sum(ours["reactions"][r][0] != theirs["reactions"][r][0] for r in shared)
    row["gpr_differs_share"] = round(differs / len(shared), 4) if shared else ""
    return row


def cmd_compare(args):
    import e4_parity

    e4_parity.PUBLISHED = args.root / "published"
    bins = read_bins(args.bins_file)
    by_study = defaultdict(list)
    for study, b in bins:
        by_study[study].append(b)
    columns = ["study", "bin", "rung", "against", "reactions", "metabolites", "genes",
               *(f"{k}_jaccard" for k in KINDS), "gpr_differs_share"]
    missing = defaultdict(list)
    with open(args.out, "w", newline="") as fh:
        out = csv.DictWriter(fh, columns, delimiter="\t", lineterminator="\n")
        out.writeheader()
        for study in sorted(by_study):
            for b, published in e4_parity.published_models(study, by_study[study]):
                theirs = e4_parity.parse_model(published)
                models = {}
                for rung in RUNGS:
                    path = args.models / rung / f"{b}.xml.gz"
                    if path.exists():
                        models[rung] = e4_parity.parse_model(e4_parity.read_sbml(path))
                    else:
                        missing[rung].append(b)
                for i, rung in enumerate(RUNGS):
                    if rung not in models:
                        continue
                    ours = models[rung]
                    base = {"study": study, "bin": b, "rung": rung, **{k: len(ours[k]) for k in KINDS}}
                    out.writerow({**base, "against": "metagem", **metrics(ours, theirs)})
                    if i and RUNGS[i - 1] in models:
                        out.writerow({**base, "against": "previous", **metrics(ours, models[RUNGS[i - 1]])})
    for rung in RUNGS:
        print(f"{rung}: {len(missing[rung])} bins with no model", file=sys.stderr)
        for b in missing[rung][:20]:
            print(f"  {b}", file=sys.stderr)


def quartiles(values):
    if len(values) < 2:
        return (values[0],) * 3 if values else ("",) * 3
    q1, q2, q3 = statistics.quantiles(values, n=4)
    return q1, statistics.median(values), q3


def cmd_summary(args):
    rows = list(csv.DictReader(open(args.table), delimiter="\t"))
    measures = [f"{k}_jaccard" for k in KINDS] + ["gpr_differs_share"]
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        for scope in ("pooled", r["study"]):
            for m in measures:
                if r[m] != "":
                    groups[(r["against"], r["rung"], scope)][m].append(float(r[m]))
    print("against\trung\tscope\tn\t" + "\t".join(f"{m}_median\t{m}_iqr" for m in measures))
    studies = sorted({r["study"] for r in rows})
    for against in ("previous", "metagem"):
        for rung in RUNGS:
            for scope in ("pooled", *studies):
                g = groups.get((against, rung, scope))
                if not g:
                    continue
                cells = []
                for m in measures:
                    q1, med, q3 = quartiles(g[m])
                    cells += [f"{med:.3f}", f"{q1:.3f}-{q3:.3f}"] if g[m] else ["", ""]
                n = len(g[measures[0]])
                print(f"{against}\t{rung}\t{scope}\t{n}\t" + "\t".join(cells))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    pick = sub.add_parser("pick")
    pick.add_argument("--out", type=Path, default=BINS)
    pick.set_defaults(func=cmd_pick)
    extract = sub.add_parser("extract")
    extract.add_argument("--out", type=Path, required=True)
    extract.add_argument("--root", type=Path, default=ROOT)
    extract.add_argument("--rungs", nargs="*", choices=RUNGS, default=RUNGS)
    extract.add_argument("--bins-file", type=Path, default=BINS)
    extract.set_defaults(func=cmd_extract)
    compare = sub.add_parser("compare")
    compare.add_argument("--models", type=Path, required=True, help="extract's --out")
    compare.add_argument("--out", type=Path, required=True)
    compare.add_argument("--root", type=Path, default=ROOT)
    compare.add_argument("--bins-file", type=Path, default=BINS)
    compare.set_defaults(func=cmd_compare)
    summary = sub.add_parser("summary")
    summary.add_argument("table", type=Path)
    summary.set_defaults(func=cmd_summary)
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
