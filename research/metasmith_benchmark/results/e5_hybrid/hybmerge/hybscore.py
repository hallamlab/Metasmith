#!/usr/bin/env python3
# Score the Flye/MEGAHIT joins beside the pilot's four assemblies, per CAMI sample, then a per-dataset median.
# Reads reports/<sample>.metaquast.tar.gz (the pilot) and hybmerge/<sample>.{hybmerge,hybsub}.tar.gz (the joins),
# all from metaQUAST over the same sample references and flags.
import csv, io, statistics, sys, tarfile
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import score  # noqa: E402

SOURCES = [(HERE.parent / "reports", "metaquast", ["megahit", "opera_ms", "flye", "flye_polca"]),
           (HERE, "hybmerge", ["unaligned_raw", "unaligned_polca"]),
           (HERE, "hybsub", ["sub_raw", "sub_polca"])]
METRICS = ["total_Mb", "Mb_in_50kb", "N50", "NA50", "mean_genome_fraction", "mismatches_per_100kb", "indels_per_100kb",
           "misassemblies_per_Mb"]


def tables(tar):
    out = {"comb": score.table(tar, "combined_reference/report.tsv"),
           "gf": score.table(tar, "summary/TSV/Genome_fraction.tsv"), "refs": defaultdict(dict)}
    for m in tar.getmembers():
        parts = m.name.lstrip("./").split("/")
        if len(parts) == 3 and parts[0] == "runs_per_reference" and parts[2] == "report.tsv":
            rows = list(csv.reader(io.TextIOWrapper(tar.extractfile(m)), delimiter="\t"))
            out["refs"][parts[1]] = {r[0]: dict(zip(rows[0][1:], r[1:])) for r in rows[1:]}
    return out


def assembly_row(t, a, genomes):
    comb, num = t["comb"], score.num
    aligned = mism = ind = 0.0
    for rep in t["refs"].values():
        al = num(rep.get("Total aligned length", {}).get(a))
        if al:
            aligned += al
            mism += num(rep["# mismatches per 100 kbp"][a]) * al / 1e5
            ind += num(rep["# indels per 100 kbp"][a]) * al / 1e5
    total_aligned = num(comb["Total aligned length"][a])
    # A union keeps every long contig but adds short ones, which drags N50 down: count the sequence in long contigs too.
    return {"total_Mb": num(comb["Total length"][a]) / 1e6, "Mb_in_50kb": num(comb["Total length (>= 50000 bp)"][a]) / 1e6, "N50": num(comb["N50"][a]), "NA50": num(comb["NA50"][a]),
            "mean_genome_fraction": statistics.mean(num(t["gf"].get(r, {}).get(a)) or 0.0 for r in genomes),
            "mismatches_per_100kb": mism / aligned * 1e5 if aligned else None,
            "indels_per_100kb": ind / aligned * 1e5 if aligned else None,
            "misassemblies_per_Mb": num(comb["# misassemblies"][a]) / total_aligned * 1e6 if total_aligned else None}


def main():
    genomes = score.sample_genomes()
    rows = []
    for s in sorted(p.name.split(".")[0] for p in (HERE / ".").glob("*.hybmerge.tar.gz")):
        for d, kind, assemblies in SOURCES:
            path = d / f"{s}.{kind}.tar.gz"
            if not path.exists():
                continue
            with tarfile.open(path) as tar:
                t = tables(tar)
            for a in assemblies:
                if a in t["comb"].get("Total length", {}):
                    rows.append({"sample": s, "dataset": score.dataset(s), "assembly": a, **assembly_row(t, a, genomes[s])})
    cols = ["sample", "dataset", "assembly"] + METRICS
    with (HERE / "hybmerge.tsv").open("w") as f:
        w = csv.DictWriter(f, cols, delimiter="\t", lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: (f"{v:.2f}" if isinstance(v, float) else v) for k, v in r.items()})
    by = defaultdict(list)
    for r in rows:
        by[(r["dataset"], r["assembly"])].append(r)
    print("\t".join(["dataset", "assembly", "n"] + METRICS))
    for (ds, a), rs in by.items():
        med = [statistics.median(x[m] for x in rs if x[m] is not None) for m in METRICS]
        print("\t".join([ds, a, str(len(rs))] + [f"{v:,.1f}" for v in med]))


if __name__ == "__main__":
    main()
