#!/usr/bin/env python3
"""Score `e4_gems.py --lane evidence`: up to four GEMs per bin, carved from DIAMOND's, KOfam's and both tables'
hits, and from each ORF's best-supported reaction across the two (diluted_top1).

extract    write each lane's GEM and each bin's DIAMOND and KOfam hits out of a run's results, as
           <out>/<lane>/<bin>.xml.gz and <out>/hits_<source>/<bin>.tsv.gz, and the BiGG gene KO table.
           Run it once per run, smoke run first: a product the planner answered from another run's cache
           has no bin in its lineage, so it is skipped and read from that run instead.
compare    each lane against metaGEM's GEM, the DIAMOND lane and E4's rung M (e4_ablation.py extract --rungs M),
           one row per bin, lane and reference.
sources    per bin of the union lane, its reactions by which tables hold a hit on one of their BiGG genes.
reference  the KO coverage of CarveMe 1.6.6's BiGG genes, and how far each ORF's KOs expand.
stage      pack the lanes' GEMs into e4_ablation_memote.sbatch's blocks, beside its blocks.tsv.
collect    one row per bin and lane of the MEMOTE scores e4_ablation_memote.sbatch wrote.
summary    medians and interquartile ranges of compare's, sources' and collect's tables.
fig2c      one row per lane, study and metric for Figure 2C: reactions per model, MEMOTE 0.17 total and
           reaction Jaccard against metaGEM. E4's rungs R0 (reproduction) and M (modern) come from its
           ablation tables, so every lane covers the same 1,408 bins.
"""

import argparse
import csv
import gzip
import shutil
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from e4_ablation import BINS, KINDS, metrics, quartiles, read_bins  # noqa: E402

LANES = ("diamond", "kofam", "both", "diluted_top1")
ROOT = Path("/scratch/phyberos/e4_ablation")
GPRS = ROOT / "refs" / "carveme166_bigg_gprs.csv.gz"


# Only a run's targets are published under results/. Every other product's index path names its task's
# shard by the last 16 hex of the task hash, and the home's cache index maps that to the shard.
def cache_shards(home):
    import sqlite3

    db = sqlite3.connect(home / "task_cache" / "cache.sqlite")
    return {k[-16:]: home / "task_cache" / root
            for k, root in db.execute("select lower(hex(key)), output_root from entries where tombstoned_at is null")}


def product_file(results, path, shards):
    published = results / path
    if published.exists():
        return published
    return shards[Path(path).name.split(".")[1].split("-")[0]] / path


def cmd_extract(args):
    import yaml
    from e4_tally import bins_of

    index = yaml.load(open(args.results / "_metadata" / "index.yml"), Loader=yaml.CSafeLoader)["manifest"]
    owner = bins_of(index)
    shards = cache_shards(args.results.parents[2])
    wanted = {f"bench::gem_{lane}": lane for lane in LANES}
    wanted.update({"bench::bigg_hits_diamond": "hits_diamond", "bench::bigg_hits_kofam": "hits_kofam"})
    counts, seen = Counter(), set()
    for path, entry in index.items():
        kind = wanted.get(entry.get("type"))
        if entry.get("type") == "bench::bigg_gene_kos":
            shutil.copyfile(product_file(args.results, path, shards), args.out / "bigg_gene_kos.tsv")
        if kind is None:
            continue
        b = owner[path]
        if b is None:
            counts["no bin in lineage"] += 1
            continue
        if (kind, b) in seen:
            sys.exit(f"{b} has more than one {entry['type']}")
        seen.add((kind, b))
        dest = args.out / kind / (f"{b}.xml.gz" if kind in LANES else f"{b}.tsv.gz")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(gzip.compress(product_file(args.results, path, shards).read_bytes(), compresslevel=1))
        counts[kind] += 1
    print("\t".join(f"{k} {n}" for k, n in sorted(counts.items())), file=sys.stderr)


def pair(ours, theirs):
    return {**metrics(ours, theirs), "reactions_changed": len(set(ours["reactions"]) ^ set(theirs["reactions"]))}


def cmd_compare(args):
    import e4_parity

    e4_parity.PUBLISHED = args.root / "published"
    by_study = defaultdict(list)
    for study, b in read_bins(args.bins_file):
        by_study[study].append(b)
    columns = ["study", "bin", "lane", "against", *KINDS, *(f"{k}_jaccard" for k in KINDS), "gpr_differs_share",
               "reactions_changed"]
    missing = Counter()
    with open(args.out, "w", newline="") as fh:
        out = csv.DictWriter(fh, columns, delimiter="\t", lineterminator="\n")
        out.writeheader()
        for study in sorted(by_study):
            for b, published in e4_parity.published_models(study, by_study[study]):
                theirs = e4_parity.parse_model(published)
                models = {}
                for lane in LANES:
                    path = args.models / lane / f"{b}.xml.gz"
                    if path.exists():
                        models[lane] = e4_parity.parse_model(e4_parity.read_sbml(path))
                    else:
                        missing[lane] += 1
                rung_m = args.rung_m / f"{b}.xml.gz"
                rung_m = e4_parity.parse_model(e4_parity.read_sbml(rung_m)) if rung_m.exists() else None
                for lane, ours in models.items():
                    base = {"study": study, "bin": b, "lane": lane, **{k: len(ours[k]) for k in KINDS}}
                    out.writerow({**base, "against": "metagem", **metrics(ours, theirs)})
                    if lane != "diamond" and "diamond" in models:
                        out.writerow({**base, "against": "diamond", **pair(ours, models["diamond"])})
                    if rung_m is not None:
                        out.writerow({**base, "against": "M", **pair(ours, rung_m)})
    for lane in LANES:
        print(f"{lane}: {missing[lane]} bins with no model", file=sys.stderr)


def read_hits(path):
    with gzip.open(path, "rt") as fh:
        return {line.split("\t", 2)[1] for line in fh if line.strip()}


def read_gprs(path):
    genes_of = defaultdict(set)
    with gzip.open(path, "rt") as fh:
        for row in csv.DictReader(fh):
            genes_of[row["reaction"]].add(f"{row['model']}.{row['gene'][2:]}")
    return genes_of


# A reaction counts for a source when that source's table holds a hit on any BiGG gene of its rule.
# One with neither came from the gap fill, or is spontaneous or a transport CarveMe adds without a rule.
# Rung M is the DIAMOND lane, so its models stand in for the lane's own, which exist only for the smoke bins.
def cmd_sources(args):
    import e4_parity

    genes_of = read_gprs(args.gprs)
    columns = ["study", "bin", "reactions", "diamond_only", "kofam_only", "both_sources", "neither",
               "in_diamond_lane", "in_kofam_lane", "in_neither_lane"]
    with open(args.out, "w", newline="") as fh:
        out = csv.writer(fh, delimiter="\t", lineterminator="\n")
        out.writerow(columns)
        for study, b in read_bins(args.bins_file):
            union = args.models / "both" / f"{b}.xml.gz"
            hits = {s: args.models / f"hits_{s}" / f"{b}.tsv.gz" for s in ("diamond", "kofam")}
            if not union.exists() or not all(p.exists() for p in hits.values()):
                continue
            reactions = set(e4_parity.parse_model(e4_parity.read_sbml(union))["reactions"])
            diamond, kofam = read_hits(hits["diamond"]), read_hits(hits["kofam"])
            tally = Counter()
            for r in reactions:
                genes = genes_of.get(r, set())
                d, k = bool(genes & diamond), bool(genes & kofam)
                tally["both_sources" if d and k else "diamond_only" if d else "kofam_only" if k else "neither"] += 1
            lanes = {}
            for lane, path in (("diamond", args.rung_m / f"{b}.xml.gz"), ("kofam", args.models / "kofam" / f"{b}.xml.gz")):
                lanes[lane] = set(e4_parity.parse_model(e4_parity.read_sbml(path))["reactions"]) if path.exists() else set()
            out.writerow([study, b, len(reactions), tally["diamond_only"], tally["kofam_only"], tally["both_sources"],
                          tally["neither"], len(reactions & lanes["diamond"]), len(reactions & lanes["kofam"]),
                          len(reactions - lanes["diamond"] - lanes["kofam"])])


def cmd_reference(args):
    kos_of = defaultdict(set)
    with open(args.models / "bigg_gene_kos.tsv") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            kos_of[row["bigg_gene"]].add(row["KO"])
    genes_of_ko = defaultdict(set)
    for gene, kos in kos_of.items():
        for ko in kos:
            genes_of_ko[ko].add(gene)
    total = sum(1 for line in open(args.proteins) if line.startswith(">"))
    print(f"BiGG genes with a KO\t{len(kos_of)} of {total} ({len(kos_of) / total:.1%})")
    print(f"KOs on a BiGG gene\t{len(genes_of_ko)}")
    print(f"BiGG genes per KO, median\t{statistics.median(len(g) for g in genes_of_ko.values())}")
    per_orf = []
    for path in sorted((args.models / "hits_kofam").glob("*.tsv.gz")):
        genes = defaultdict(set)
        with gzip.open(path, "rt") as fh:
            for line in fh:
                orf, gene = line.split("\t", 2)[:2]
                genes[orf].add(gene)
        per_orf += [len(g) for g in genes.values()]
    if per_orf:
        q1, med, q3 = quartiles(per_orf)
        print(f"BiGG genes per ORF with a mapped KO, median (IQR)\t{med} ({q1}-{q3}) over {len(per_orf)} ORFs")


def cmd_stage(args):
    from e4_ablation_memote import blocks, pack

    bins = read_bins(args.bins_file)
    (args.work / "models").mkdir(parents=True, exist_ok=True)
    with open(args.work / "blocks.tsv", "w") as fh:
        fh.write("block\tstudy\tbin\n")
        for i, block in enumerate(blocks(bins), 1):
            entries = []
            for study, b in block:
                fh.write(f"{i}\t{study}\t{b}\n")
                for lane in LANES:
                    model = args.models / lane / f"{b}.xml.gz"
                    if model.exists():
                        entries.append((f"{lane}/{b}.xml.gz", model.read_bytes()))
            pack(args.work / "models" / f"block_{i:02d}.tar", entries)
    print(f"{i} blocks", file=sys.stderr)


def cmd_collect(args):
    from e4_ablation_memote import COLUMNS, SECTIONS, scored

    have, notes, _ = scored(args.work)
    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(COLUMNS)
        for study, b in read_bins(args.bins_file):
            for lane in LANES:
                score = have.get((lane, b))
                note = "" if score else notes.get((lane, b), "no score")
                w.writerow([study, b, lane] + [score[c] if score else "" for c in ["total_score"] + SECTIONS] + [note])


def summarise(rows, keys, measures):
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        for m in measures:
            if r.get(m, "") != "":
                groups[tuple(r[k] for k in keys)][m].append(float(r[m]))
    print("\t".join([*keys, "n", *(f"{m}_median\t{m}_iqr" for m in measures)]))
    for key in sorted(groups):
        g = groups[key]
        cells = []
        for m in measures:
            q1, med, q3 = quartiles(g[m]) if g[m] else ("", "", "")
            cells += [f"{med:.3f}", f"{q1:.3f}-{q3:.3f}"] if g[m] else ["", ""]
        print("\t".join([*key, str(max(len(v) for v in g.values())), *cells]))


def cmd_summary(args):
    rows = list(csv.DictReader(open(args.table), delimiter="\t"))
    header = rows[0].keys() if rows else []
    if "against" in header:
        summarise(rows, ("against", "lane"), [*KINDS, *(f"{k}_jaccard" for k in KINDS), "gpr_differs_share",
                                               "reactions_changed"])
    elif "diamond_only" in header:
        summarise([{**r, "lane": "both"} for r in rows], ("lane",),
                  ["reactions", "diamond_only", "kofam_only", "both_sources", "neither",
                   "in_diamond_lane", "in_kofam_lane", "in_neither_lane"])
    else:
        from e4_ablation_memote import SECTIONS
        summarise(rows, ("source",), ["total_score", *SECTIONS, *(["grows"] if "grows" in header else [])])


def cmd_fig2c(args):
    values = defaultdict(list)

    def add(lane, study, metric, value):
        if value != "":
            for scope in (study, "pooled"):
                values[(lane, scope, metric)].append(float(value))

    with open(args.tables / "ablation_parity.tsv") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["rung"] in ("R0", "M") and r["against"] == "metagem":
                add(r["rung"], r["study"], "reactions", r["reactions"])
                add(r["rung"], r["study"], "reactions_jaccard_metagem", r["reactions_jaccard"])
    with open(args.tables / "ablation_memote.tsv") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["source"] in ("R0", "M"):
                add(r["source"], r["study"], "memote_total", r["total_score"])
    with open(args.parity) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["against"] == "metagem":
                add(r["lane"], r["study"], "reactions", r["reactions"])
                add(r["lane"], r["study"], "reactions_jaccard_metagem", r["reactions_jaccard"])
    with open(args.memote) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            add(r["source"], r["study"], "memote_total", r["total_score"])
    order = {lane: i for i, lane in enumerate(("R0", "M", *LANES))}
    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(["lane", "study", "metric", "median", "q1", "q3", "n"])
        for (lane, study, metric), v in sorted(values.items(), key=lambda kv: (order[kv[0][0]], kv[0][1] == "pooled", kv[0][1], kv[0][2])):
            q1, med, q3 = quartiles(v)
            w.writerow([lane, study, metric, round(med, 4), round(q1, 4), round(q3, 4), len(v)])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("extract")
    p.add_argument("--results", type=Path, required=True, help="the run's results/ directory")
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("compare")
    p.add_argument("--models", type=Path, required=True, help="extract's --out")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--root", type=Path, default=ROOT)
    p.add_argument("--rung-m", type=Path, default=ROOT / "evidence" / "rung_models" / "M")
    p = sub.add_parser("sources")
    p.add_argument("--models", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--gprs", type=Path, default=GPRS)
    p.add_argument("--rung-m", type=Path, default=ROOT / "evidence" / "rung_models" / "M")
    p = sub.add_parser("reference")
    p.add_argument("--models", type=Path, required=True)
    p.add_argument("--proteins", type=Path, default=ROOT / "refs" / "carveme166_bigg_proteins.faa")
    p = sub.add_parser("stage")
    p.add_argument("--models", type=Path, required=True)
    p.add_argument("--work", type=Path, required=True)
    p = sub.add_parser("collect")
    p.add_argument("--work", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("fig2c")
    p.add_argument("--tables", type=Path, default=HERE.parent / "results" / "e4")
    p.add_argument("--parity", type=Path, required=True, help="compare's table")
    p.add_argument("--memote", type=Path, required=True, help="collect's table")
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("summary")
    p.add_argument("table", type=Path)
    for name, s in sub.choices.items():
        if name not in ("extract", "summary", "reference", "fig2c"):
            s.add_argument("--bins-file", type=Path, default=BINS)
    args = ap.parse_args()
    if getattr(args, "out", None) and args.cmd == "extract":
        args.out.mkdir(parents=True, exist_ok=True)
    {"extract": cmd_extract, "compare": cmd_compare, "sources": cmd_sources, "reference": cmd_reference,
     "stage": cmd_stage, "collect": cmd_collect, "summary": cmd_summary, "fig2c": cmd_fig2c}[args.cmd](args)


if __name__ == "__main__":
    main()
