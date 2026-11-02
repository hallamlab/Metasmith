#!/usr/bin/env python3
# Map each study's source-genome FASTA to its CAMI genome_id by majority vote of its contig headers
# against gsa_mapping (through sequence_id_map where CAMI renamed contigs). Runs on fir.
import csv, gzip, sys, tarfile
from collections import Counter, defaultdict
from pathlib import Path

G = Path("/scratch/phyberos/cami")
W = G / "work"
STUDIES = {
    "marine": (G / "cami2_challenge/marine/marmgCAMI2_genomes.tar.gz", "marine_short_read"),
    "plant_associated": (G / "cami2_challenge/plant_associated/rhimgCAMI2_genomes.tar.gz", "plant_short_read"),
    "strain": (G / "cami2_challenge/strain/strmgCAMI2_genomes.tar.gz", "strain_short_read"),
    "toy_humangut": (G / "cami3_toy_humangut/long/source_genomes.tar.gz", "toy_humangut_short_read"),
}
FASTA = (".fna", ".fa", ".fasta")


def contig_to_genome(short_dir):
    out = {}
    # CAMI III ships a pooled mapping instead: @@SEQUENCEID BINID TAXID LENGTH contig_id ...
    for m in sorted((G / "cami3_toy_humangut").glob("*/gsa_pooled_mapping.tsv.gz")) if short_dir.startswith("toy") else []:
        with gzip.open(m, "rt") as f:
            for row in csv.reader(f, delimiter="\t"):
                if not row[0].startswith("@"):
                    out[row[4]] = row[1]
    for m in sorted((W / short_dir).glob("*/*/contigs/gsa_mapping.tsv.gz")):
        with gzip.open(m, "rt") as f:
            for row in csv.reader(f, delimiter="\t"):
                if row[0].startswith("#"):
                    continue
                out[row[3]] = row[1]
    return out


def main(study):
    tarball, short_dir = STUDIES[study]
    c2g = contig_to_genome(short_dir)
    idmap = {}
    rows = []
    with tarfile.open(tarball, "r:gz") as tar:
        for m in tar:
            name = Path(m.name).name
            if name == "sequence_id_map.txt":
                for line in tar.extractfile(m).read().decode().splitlines():
                    p = line.split("\t")
                    if len(p) == 3:
                        idmap[p[2]] = (p[0], p[1])
                continue
            if not m.isfile() or not name.endswith(FASTA):
                continue
            heads = [l[1:].split()[0].decode() for l in tar.extractfile(m) if l.startswith(b">")]
            votes = Counter()
            for h in heads:
                if h in c2g:
                    votes[c2g[h]] += 1
                elif h in idmap and idmap[h][1] in c2g:
                    votes[c2g[idmap[h][1]]] += 1
                elif h in idmap:
                    votes[idmap[h][0]] += 1
            stem = name.rsplit(".", 1)[0]
            gid, n = votes.most_common(1)[0] if votes else ("", 0)
            rows.append((stem, gid, n, len(heads)))
    out = Path(__file__).with_name(f"genome_map.{study}.tsv")
    with out.open("w") as f:
        f.write("reference\tgenome_id\tmatched\theaders\n")
        for r in rows:
            f.write("\t".join(map(str, r)) + "\n")
    mapped = sum(1 for r in rows if r[1])
    print(f"{study}: {mapped}/{len(rows)} files mapped, {len(set(c2g.values()))} genome ids in gsa, idmap {len(idmap)}")


if __name__ == "__main__":
    for s in sys.argv[1:] or STUDIES:
        main(s)
