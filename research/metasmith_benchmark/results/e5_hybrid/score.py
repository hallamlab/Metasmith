#!/usr/bin/env python3
# Score the E5 hybrid pilot from what collect.py and sample_genomes.py pulled off fir: one TSV per metric,
# one row per sample and assembly, then a per-dataset summary of each.
import csv, io, statistics, sys, tarfile
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ASSEMBLIES = ["megahit", "opera_ms", "flye", "flye_polca"]
# The steps each assembly's lane runs from raw reads. POLCA reads the clean short reads, so it pays for QC too.
LANES = {"megahit": ["qc", "megahit"], "opera_ms": ["qc", "megahit", "opera_ms"],
         "flye": ["flye"], "flye_polca": ["qc", "flye", "polca"]}
# CAMI takes JGI's bbduk, which reads its minimum length off seqkit's stats. Pratama takes its own bbduk.
QC = {"cami": ["seqkit_reads", "bbduk"], "pratama": ["bbduk_pratama"]}
DATASETS = [("marine_long", "marine PacBio"), ("plant_associated_long_nano", "plant Nanopore"),
            ("plant_associated_long_pacbio", "plant PacBio"), ("strain_long", "strain PacBio"),
            ("toy_humangut_long", "toy humangut Nanopore"), ("SRR", "Pratama Nanopore")]


def dataset(sample):
    return next(name for prefix, name in DATASETS if sample.startswith(prefix))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def table(tar, name):
    member = next((m for m in tar.getmembers() if m.name.lstrip("./") == name), None)
    if member is None:
        return {}
    rows = list(csv.reader(io.TextIOWrapper(tar.extractfile(member)), delimiter="\t"))
    head = rows[0]
    return {r[0]: dict(zip(head[1:], r[1:])) for r in rows[1:]}


def write(name, cols, rows):
    with (HERE / name).open("w") as f:
        w = csv.DictWriter(f, cols, delimiter="\t", lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def fmt(v, digits=1):
    return "" if v is None else f"{v:.{digits}f}"


def sample_genomes():
    refs = defaultdict(set)
    for r in csv.DictReader((HERE / "sample_genomes.tsv").open(), delimiter="\t"):
        refs[r["sample"]].add(r["reference"])
    return refs


def contiguity(samples, genomes):
    rows = []
    for s in samples:
        with tarfile.open(HERE / "reports" / f"{s}.quast.tar.gz") as tar:
            q = table(tar, "report.tsv")
        mq, nga = {}, {}
        if (HERE / "reports" / f"{s}.metaquast.tar.gz").exists():
            with tarfile.open(HERE / "reports" / f"{s}.metaquast.tar.gz") as tar:
                mq = table(tar, "combined_reference/report.tsv")
                nga = table(tar, "summary/TSV/NGA50.tsv")
        for a in ASSEMBLIES:
            # NGA50 exists only for a genome the assembly covers to half its length, never for CAMI's pooled reference.
            per_genome = [v for v in (num(nga.get(r, {}).get(a)) for r in genomes[s]) if v is not None]
            rows.append({"sample": s, "dataset": dataset(s), "assembly": a,
                         "contigs": q["# contigs"][a], "total_length": q["Total length"][a], "N50": q["N50"][a],
                         "NA50": mq.get("NA50", {}).get(a, ""), "largest_alignment": mq.get("Largest alignment", {}).get(a, ""),
                         "genomes_with_NGA50": len(per_genome) if mq else "",
                         "median_NGA50": fmt(statistics.median(per_genome), 0) if per_genome else ""})
    return rows


# metaQUAST's combined-reference run counts every alignment of an ambiguous contig (--ambiguity-usage all) against
# one copy of its length, so near-identical strains push its base error rates past 100%. Its per-reference runs keep
# one alignment each, and pooling them weights every reference by the bases aligned to it.
def pooled_errors(tar):
    pooled = {a: {"mismatches": 0.0, "indels": 0.0, "aligned": 0.0, "misassemblies": 0.0} for a in ASSEMBLIES}
    for m in tar.getmembers():
        parts = m.name.lstrip("./").split("/")
        if len(parts) != 3 or parts[0] != "runs_per_reference" or parts[2] != "report.tsv":
            continue
        rows = list(csv.reader(io.TextIOWrapper(tar.extractfile(m)), delimiter="\t"))
        t = {r[0]: dict(zip(rows[0][1:], r[1:])) for r in rows[1:]}
        for a in ASSEMBLIES:
            aligned = num(t.get("Total aligned length", {}).get(a))
            if not aligned:
                continue
            pooled[a]["aligned"] += aligned
            pooled[a]["mismatches"] += num(t["# mismatches per 100 kbp"][a]) * aligned / 1e5
            pooled[a]["indels"] += num(t["# indels per 100 kbp"][a]) * aligned / 1e5
            pooled[a]["misassemblies"] += num(t["# misassemblies"][a]) or 0
    return pooled


def recovery_and_errors(samples, genomes):
    rec, err = [], []
    for s in samples:
        path = HERE / "reports" / f"{s}.metaquast.tar.gz"
        if not path.exists():
            continue
        with tarfile.open(path) as tar:
            gf = table(tar, "summary/TSV/Genome_fraction.tsv")
            comb = table(tar, "combined_reference/report.tsv")
            pooled = pooled_errors(tar)
        present = genomes[s]
        for a in ASSEMBLIES:
            fractions = [num(gf.get(r, {}).get(a)) or 0.0 for r in present]
            rec.append({"sample": s, "dataset": dataset(s), "assembly": a, "genomes": len(present),
                        "mean_genome_fraction": fmt(statistics.mean(fractions), 2),
                        "genomes_over_50pct": sum(f >= 50 for f in fractions),
                        "genomes_over_90pct": sum(f >= 90 for f in fractions)})
            aligned = num(comb["Total aligned length"][a])
            mis = num(comb["# misassemblies"][a])
            err.append({"sample": s, "dataset": dataset(s), "assembly": a,
                        "mismatches_per_100kb": fmt(pooled[a]["mismatches"] / pooled[a]["aligned"] * 1e5, 1),
                        "indels_per_100kb": fmt(pooled[a]["indels"] / pooled[a]["aligned"] * 1e5, 1),
                        "misassemblies": comb["# misassemblies"][a],
                        "misassemblies_per_Mb": fmt(mis / aligned * 1e6, 2) if aligned and mis is not None else "",
                        "aligned_length": comb["Total aligned length"][a],
                        # Per-reference runs see only one genome each, so they miss a join between two genomes.
                        "ref_misassemblies_per_Mb": fmt(pooled[a]["misassemblies"] / pooled[a]["aligned"] * 1e6, 2) if pooled[a]["aligned"] else "",
                        "mean_genome_fraction": rec[-1]["mean_genome_fraction"]})
    return rec, err


def cost(samples):
    steps = defaultdict(dict)
    for r in csv.DictReader((HERE / "steps.tsv").open(), delimiter="\t"):
        if r["state"] == "COMPLETED" and r["sample"] in samples:
            steps[r["sample"]][r["step"]] = r
    rows = []
    for s in samples:
        for step, r in sorted(steps[s].items()):
            el, cpus = int(r["elapsed_s"]), int(r["cpus"])
            rows.append({"sample": s, "dataset": dataset(s), "step": step, "wall_h": fmt(el / 3600, 2),
                         "cpus": cpus, "cpu_h": fmt(el * cpus / 3600, 1),
                         "max_rss_gb": fmt(int(r["max_rss_kb"]) / 1024 ** 2, 1), "req_gb": req_gb(r),
                         "at_cap": at_cap(r), "job": r["job"]})
    return rows, steps


def req_gb(r):
    return int(r["req_mem"].rstrip("G"))


# fir's MaxRSS counts page cache, so a step that streams large files reads at its memory request.
def at_cap(r):
    return int(r["max_rss_kb"]) >= 0.98 * req_gb(r) * 1024 ** 2


def lane_cost(samples, steps):
    rows = []
    for s in samples:
        qc = QC["pratama" if s.startswith("SRR") else "cami"]
        for a in ASSEMBLIES:
            lane = [x for st in LANES[a] for x in (qc if st == "qc" else [st])]
            parts = [steps[s].get(st) for st in lane]
            if not all(parts):
                missing = [st for st, p in zip(lane, parts) if not p]
                print(f"{s} {a}: no sacct row for {missing}", file=sys.stderr)
                continue
            wall = sum(int(p["elapsed_s"]) for p in parts)
            rows.append({"sample": s, "dataset": dataset(s), "assembly": a, "wall_h": fmt(wall / 3600, 2),
                         "cpu_h": fmt(sum(int(p["elapsed_s"]) * int(p["cpus"]) for p in parts) / 3600, 1),
                         "max_rss_gb": fmt(max(int(p["max_rss_kb"]) for p in parts) / 1024 ** 2, 1),
                         "steps_at_cap": ",".join(st for st, p in zip(lane, parts) if at_cap(p))})
    return rows


def summary(rows, metrics):
    by = defaultdict(list)
    for r in rows:
        by[(r["dataset"], r["assembly"])].append(r)
    out = []
    for (ds, a), rs in sorted(by.items(), key=lambda kv: ([n for _, n in DATASETS].index(kv[0][0]), ASSEMBLIES.index(kv[0][1]))):
        row = {"dataset": ds, "assembly": a, "samples": len(rs)}
        for m in metrics:
            vals = [v for v in (num(r[m]) for r in rs) if v is not None]
            row[m] = fmt(statistics.median(vals), 2) if vals else ""
        out.append(row)
    return out


def main():
    samples = sorted({p.name.split(".")[0] for p in (HERE / "reports").glob("*.quast.tar.gz")})
    genomes = sample_genomes()
    cont = contiguity(samples, genomes)
    rec, err = recovery_and_errors(samples, genomes)
    steps_rows, steps = cost(samples)
    lanes = lane_cost(samples, steps)
    key = ["sample", "dataset", "assembly"]
    write("contiguity.tsv", key + ["contigs", "total_length", "N50", "NA50", "largest_alignment",
                                   "genomes_with_NGA50", "median_NGA50"], cont)
    write("recovery.tsv", key + ["genomes", "mean_genome_fraction", "genomes_over_50pct", "genomes_over_90pct"], rec)
    write("errors.tsv", key + ["mismatches_per_100kb", "indels_per_100kb", "misassemblies", "misassemblies_per_Mb",
                               "ref_misassemblies_per_Mb", "aligned_length", "mean_genome_fraction"], err)
    write("cost_steps.tsv", ["sample", "dataset", "step", "wall_h", "cpus", "cpu_h", "max_rss_gb", "req_gb", "at_cap", "job"], steps_rows)
    write("cost_lanes.tsv", key + ["wall_h", "cpu_h", "max_rss_gb", "steps_at_cap"], lanes)
    m = {"contiguity": ["contigs", "N50", "NA50", "genomes_with_NGA50", "median_NGA50"], "recovery": ["mean_genome_fraction", "genomes_over_50pct", "genomes_over_90pct"],
         "errors": ["mismatches_per_100kb", "indels_per_100kb", "misassemblies_per_Mb", "ref_misassemblies_per_Mb"], "cost": ["wall_h", "cpu_h", "max_rss_gb"]}
    src = {"contiguity": cont, "recovery": rec, "errors": err, "cost": lanes}
    rows = defaultdict(dict)
    for name, metrics in m.items():
        for r in summary(src[name], metrics):
            rows[(r["dataset"], r["assembly"])].update({k: v for k, v in r.items()})
    cols = ["dataset", "assembly", "samples"] + [x for ms in m.values() for x in ms]
    order = [n for _, n in DATASETS]
    write("summary.tsv", cols, sorted(rows.values(), key=lambda r: (order.index(r["dataset"]), ASSEMBLIES.index(r["assembly"]))))


if __name__ == "__main__":
    main()
