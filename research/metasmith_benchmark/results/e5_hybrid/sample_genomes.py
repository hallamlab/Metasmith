#!/usr/bin/env python3
# Write the reference files of the genomes present in each CAMI sample of the E5 hybrid draw. Runs on fir,
# beside genome_map.<study>.tsv. CAMI II names a sample's genomes in its gsa_mapping; CAMI III's toy
# humangut ships one gold-standard FASTA per genome and sample instead.
import csv, gzip, re
from collections import defaultdict
from pathlib import Path

W = Path("/scratch/phyberos/cami/work")
HERE = Path(__file__).resolve().parent
SHORT = {"marine": "marine_short_read", "plant_associated": "plant_short_read",
         "strain": "strain_short_read", "toy_humangut": "toy_humangut_short_read"}


def genomes(study, n):
    if study == "toy_humangut":
        return {re.match(rf"sample{n}_(.+)_gsa\.fasta\.gz$", p.name)[1]
                for p in (W / SHORT[study] / f"sample_{n}_gsa").glob(f"sample{n}_*_gsa.fasta.gz")}
    ids = set()
    for m in (W / SHORT[study]).glob(f"*/*sample_{n}/contigs/gsa_mapping.tsv.gz"):
        with gzip.open(m, "rt") as f:
            ids |= {row[1] for row in csv.reader(f, delimiter="\t") if not row[0].startswith("#")}
    return ids


def main():
    samples = sorted({l.split("\t")[0] for l in (HERE / "steps.tsv").read_text().splitlines()[1:]})
    with (HERE / "sample_genomes.tsv").open("w") as out:
        out.write("sample\tstudy\tgenome_id\treference\n")
        for s in samples:
            m = re.match(r"(marine|plant_associated|strain|toy_humangut)_long\w*_sample_(\d+)$", s)
            if not m:
                continue
            study, n = m[1], m[2]
            refs = defaultdict(list)
            for row in csv.DictReader((HERE / f"genome_map.{study}.tsv").open(), delimiter="\t"):
                refs[row["genome_id"]].append(row["reference"])
            ids = genomes(study, n)
            for g in sorted(ids):
                for r in refs.get(g) or [""]:
                    out.write(f"{s}\t{study}\t{g}\t{r}\n")
            print(s, len(ids), "genomes,", sum(1 for g in ids if g not in refs), "without a reference file")


if __name__ == "__main__":
    main()
