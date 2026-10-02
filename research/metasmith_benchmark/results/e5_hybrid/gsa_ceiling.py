#!/usr/bin/env python3
# The genome fraction CAMI's own gold-standard assembly of each sample reaches, per genome: the ceiling a single-sample
# assembly can approach at that sample's depth. Runs on fir, beside sample_genomes.tsv and genome_map.<study>.tsv.
# CAMI II maps each gold-standard contig to a genome interval; CAMI III's toy humangut ships one FASTA per genome.
import csv, gzip, re, sys, tarfile
from collections import defaultdict
from pathlib import Path

G = Path("/scratch/phyberos/cami")
W = G / "work"
HERE = Path(__file__).resolve().parent
TARBALLS = {"marine": G / "cami2_challenge/marine/marmgCAMI2_genomes.tar.gz",
            "plant_associated": G / "cami2_challenge/plant_associated/rhimgCAMI2_genomes.tar.gz",
            "strain": G / "cami2_challenge/strain/strmgCAMI2_genomes.tar.gz",
            "toy_humangut": G / "cami3_toy_humangut/long/source_genomes.tar.gz"}
SHORT = {"marine": "marine_short_read", "plant_associated": "plant_short_read",
         "strain": "strain_short_read", "toy_humangut": "toy_humangut_short_read"}
FASTA = (".fna", ".fa", ".fasta")


def reference_lengths(study, wanted):
    lengths = {}
    with tarfile.open(TARBALLS[study], "r|gz") as tar:
        for m in tar:
            name = Path(m.name).name
            stem = name.rsplit(".", 1)[0]
            if m.isfile() and name.endswith(FASTA) and stem in wanted:
                lengths[stem] = sum(len(l.strip()) for l in tar.extractfile(m) if not l.startswith(b">"))
    return lengths


def merged(intervals):
    total, end = 0, -1
    for a, b in sorted(intervals):
        if b > end:
            total += b - max(a, end + 1) + 1
            end = b
    return total


def gsa_bases(study, n):
    if study == "toy_humangut":
        out = {}
        for p in (W / SHORT[study] / f"sample_{n}_gsa").glob(f"sample{n}_*_gsa.fasta.gz"):
            gid = re.match(rf"sample{n}_(.+)_gsa\.fasta\.gz$", p.name)[1]
            with gzip.open(p, "rb") as f:
                out[gid] = sum(len(l.strip()) for l in f if not l.startswith(b">"))
        return out
    spans = defaultdict(list)
    for m in (W / SHORT[study]).glob(f"*/*sample_{n}/contigs/gsa_mapping.tsv.gz"):
        with gzip.open(m, "rt") as f:
            for row in csv.reader(f, delimiter="\t"):
                if not row[0].startswith("#"):
                    spans[(row[1], row[3])].append((int(row[5]), int(row[6])))
    out = defaultdict(int)
    for (gid, _), iv in spans.items():
        out[gid] += merged(iv)
    return out


def main():
    rows = list(csv.DictReader((HERE / "sample_genomes.tsv").open(), delimiter="\t"))
    by_study = defaultdict(list)
    for r in rows:
        by_study[r["study"]].append(r)
    with (HERE / "gsa_ceiling.tsv").open("w") as out:
        out.write("sample\tstudy\tgenome_id\tgenome_length\tgsa_bases\tgsa_fraction\n")
        for study, rs in by_study.items():
            lengths = reference_lengths(study, {r["reference"] for r in rs if r["reference"]})
            for sample in sorted({r["sample"] for r in rs}):
                n = re.search(r"_sample_(\d+)$", sample)[1]
                gsa = gsa_bases(study, n)
                glen = defaultdict(int)
                for r in rs:
                    if r["sample"] == sample and r["reference"] in lengths:
                        glen[r["genome_id"]] += lengths[r["reference"]]
                for gid, length in sorted(glen.items()):
                    out.write(f"{sample}\t{study}\t{gid}\t{length}\t{gsa.get(gid, 0)}\t{100 * min(gsa.get(gid, 0) / length, 1):.2f}\n")
            print(study, "done", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
