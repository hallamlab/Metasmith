import argparse
import re
from collections import defaultdict
from pathlib import Path

import fabfos_evidence as fe

CELLULAR_ORGANISMS = 131567
# Pathway Tools refuses a frame name over 60 characters, and it names a gene's protein <ID>-MONOMER.
# The refusal is fatal to the whole build yet still exits 0. NAME keeps the ORF ID, and
# pathway_table.py reads that.
MAX_ID = 60 - len("-MONOMER")


def org_id(name: str) -> str:
    ident = re.sub(r"[^A-Za-z0-9]", "", name) or "pgdb"
    return ("P" + ident if ident[0].isdigit() else ident)[:40]


def contig_ids(fasta):
    with open(fasta) as fh:
        return {line[1:].split()[0] for line in fh if line.startswith(">")}


def contig_of(orf: str) -> str:
    return re.sub(r"[_-]\d+$", "", orf)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--annotations", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--bin-fasta")
    p.add_argument("--taxon", type=int, default=CELLULAR_ORGANISMS)
    a = p.parse_args()

    df = fe.read_parquet(a.annotations, columns=["orf_id", "kind", "value"])
    df = df[df["kind"] == "EC"]
    keep = contig_ids(a.bin_fasta) if a.bin_fasta else None
    ecs = defaultdict(list)
    for orf, ec in zip(df["orf_id"], df["value"]):
        if (keep is None or contig_of(orf) in keep) and ec not in ecs[orf]:
            ecs[orf].append(ec)

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    pf_ids = {orf: orf if len(orf) <= MAX_ID else orf[-MAX_ID:].lstrip("_-") for orf in ecs}
    assert len(set(pf_ids.values())) == len(pf_ids), "truncated PathoLogic gene IDs collide"
    with open(out / "0.pf", "w") as fh:
        for orf in sorted(ecs):
            fh.write(f"ID\t{pf_ids[orf]}\nNAME\t{orf}\nPRODUCT-TYPE\tP\n")
            fh.writelines(f"EC\t{ec}\n" for ec in ecs[orf])
            fh.write("//\n")
    (out / "genetic-elements.dat").write_text("ID\t0\nNAME\t0\nTYPE\t:CONTIG\nANNOT-FILE\t0.pf\n//\n")
    ident = org_id(a.name)
    (out / "organism-params.dat").write_text(
        f"ID\t{ident}\nNAME\t{a.name}\nABBREV-NAME\t{ident}\nSTORAGE\tFILE\nSTRAIN\t1\n"
        f"RANK\t|species|\nNCBI-TAXON-ID\t{a.taxon}\n")
    print(f"[build_pf] {ident}: {len(ecs):,} ORFs with an EC", flush=True)


if __name__ == "__main__":
    main()
