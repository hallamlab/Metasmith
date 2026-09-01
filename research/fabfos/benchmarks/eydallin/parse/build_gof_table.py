#!/usr/bin/env python3
"""The 86 Eydallin GOF clones as one flat table: gene, insert locus tag, insert
product, measured phenotype -- a true accounting of what the paper's screen actually
was, not of the model this benchmark scores it against.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/eydallin/parse/build_gof_table.py
    ... --publish        # writes data/fabfos/runs/eydallin_clones/parse/gof/gof.csv

WHY W3110 AND NOT DH1/AG1 -- THIS IS THE PART THAT IS EASY TO GET BACKWARDS. Eydallin
et al. screened the ASKA library. The paper's own Methods (eydallin2010_fulltext, S2.1)
says the library is "a set of 4123 different clones of the AG1 E. coli K-12 strain
... each expressing one of all predicted E. coli K-12 ORFs", and separately calls AG1
"the ASKA library['s] plasmids recipient" -- AG1 is the HOST the plasmids are grown
and expressed in, nothing more. The ORFs themselves were PCR-amplified from W3110
genomic DNA before being cloned and transformed into AG1 (Kitagawa et al. 2005, the
paper's ref. 34, which built the library); AG1 never contributes a sequence to any
clone. So a table describing what a clone actually IS has to be keyed on W3110 -- the
insert -- not on AG1 or on DH1, which isn't in this experiment's lineage at all and
only enters the benchmark later, as the proxy host GEM below.

THIS IS A DIFFERENT QUESTION FROM WHAT THE GPR/ECSPr TABLES USE, AND THAT IS
DELIBERATE, NOT AN INCONSISTENCY. Running ECSPr needs a metabolic model, and AG1 has
none of its own -- `resolve_gene_manual.py`, `build_clone_gpr.py` and the
`gpr_gem.parquet`/`gpr_denovo.parquet` tables under `runs/eydallin_clones/gpr/` all
key genes against DH1's curated GEM (`iECDH1ME8569_1439`) as AG1's model proxy,
because that is the only curated model this lineage has, not because DH1 is where the
sequence came from. Two different reference genomes for two different jobs:
    W3110  -- what was overexpressed (this table, and `eydallin_clones.faa`)
    DH1    -- the model AG1 borrows to make ECSPr answerable (the `gpr_*` tables)
Do not let a locus tag from one stand in for the other.

RESOLUTION REUSES `build_clone_orfs.py`'s OWN WORK rather than re-deriving it: that
script already did the hard part (paper name -> b-number -> W3110 protein by exact
sequence identity, falling back to symbol only where identity fails) to build
`eydallin_clones.faa`, and wrote every step to `clone_resolution.tsv` alongside it.
This script just joins that table's `source_strain`/`*_accession` columns against the
matching proteome FASTA header to recover a current locus tag and product string --
it does not re-resolve anything `build_clone_orfs.py` already settled.

85/86 genes resolve a protein this way: 84 from W3110 directly, 1 (`tnaA`) from
MG1655 instead because W3110's own copy is a pseudogene (`build_clone_orfs.py`'s
`mg1655_w3110_pseudo` fallback) -- a real protein, just not W3110's. The 86th
(`yhcE`) is a pseudogene in BOTH strains and carries no protein sequence anywhere;
its row is written with `locus_tag`/`product` empty rather than dropped, so 86 rows
in means 86 rows out.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]

EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
MEASURED = REPO / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
CLONE_RESOLUTION = REPO / "data/fabfos/runs/eydallin_clones/annotations/clone_resolution.tsv"
W3110_FAA = REPO / "data/fabfos/originals/genomes/e_coli_w3110/genome/CP165600.1.faa"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
OUT = REPO / "data/fabfos/runs/eydallin_clones/parse/gof"

OUT_COLS = ("gene", "source_genome", "locus_tag", "product", "pct_glycogen_production")

ACCESSION_COL = {"w3110": "w3110_accession", "mg1655": "mg1655_accession"}
GENOME_LABEL = {"w3110": "W3110", "mg1655": "MG1655"}


def read_proteome_by_accession(path: Path) -> dict[str, dict]:
    """NCBI protein FASTA header token (`lcl|...`) -> {locus_tag, product}, read
    straight from the `[locus_tag=...]`/`[protein=...]` bracket fields every header
    carries -- the same accession `clone_resolution.tsv`'s `*_accession` columns
    already record, so this is a lookup, not a re-resolution."""
    out: dict[str, dict] = {}
    accession = None
    for line in path.open():
        if not line.startswith(">"):
            continue
        head = line[1:].rstrip("\n")
        accession = head.split()[0]
        locus_start = head.find("[locus_tag=")
        locus_tag = head[locus_start + 11:head.find("]", locus_start)] if locus_start >= 0 else ""
        prot_start = head.find("[protein=")
        product = head[prot_start + 9:head.find("]", prot_start)] if prot_start >= 0 else ""
        out[accession] = {"locus_tag": locus_tag, "product": product}
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                     help=f"write into {OUT.relative_to(REPO)}/gof.csv")
    a = ap.parse_args()

    rows = list(csv.DictReader(EXTRACTION.open(), delimiter="\t"))
    # keyed case-insensitively: `ppK` in extraction.tsv is `ppk` in measured_glycogen.tsv
    # -- same gene, and condition_id (`eydallin:ppk`) agrees with the lowercase spelling,
    # so a case-insensitive join is unambiguous here rather than a guess.
    measured = {r["gene"].lower(): r for r in csv.DictReader(MEASURED.open(), delimiter="\t")}
    resolution = {r["gene"]: r for r in csv.DictReader(CLONE_RESOLUTION.open(), delimiter="\t")}
    w3110 = read_proteome_by_accession(W3110_FAA)
    mg1655 = read_proteome_by_accession(MG1655_FAA)
    proteomes = {"w3110": w3110, "mg1655": mg1655}

    print(f"{len(rows)} genes in extraction.tsv, {len(measured)} rows in "
          f"measured_glycogen.tsv, {len(resolution)} rows in {CLONE_RESOLUTION.name}, "
          f"{len(w3110)} W3110 proteins, {len(mg1655)} MG1655 proteins")

    counts: dict[str, int] = {}
    out_rows, n_unresolved = [], []
    for r in rows:
        gene = r["gene"].strip()
        res = resolution.get(gene, {})
        strain = res.get("source_strain", "")
        accession = res.get(ACCESSION_COL.get(strain, ""), "") if strain else ""
        hit = proteomes.get(strain, {}).get(accession) if accession else None

        if hit:
            counts[strain] = counts.get(strain, 0) + 1
        else:
            n_unresolved.append(gene)

        mrow = measured.get(gene.lower())
        pct = mrow["pct_wt"] if mrow else ""

        out_rows.append({
            "gene": gene,
            "source_genome": GENOME_LABEL.get(strain, "") if hit else "",
            "locus_tag": hit["locus_tag"] if hit else "",
            "product": hit["product"] if hit else "",
            "pct_glycogen_production": pct,
        })

    summary = ", ".join(f"{n} from {GENOME_LABEL[s]}" for s, n in sorted(counts.items()))
    print(f"\n{summary}, {len(n_unresolved)} unresolved (no protein for this gene in "
          f"either strain -- a pseudogene on both sides): {sorted(n_unresolved)}")
    n_no_pct = sum(1 for r in out_rows if not r["pct_glycogen_production"])
    if n_no_pct:
        print(f"!! {n_no_pct} gene(s) have no row in measured_glycogen.tsv -- pct left blank")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(REPO)}/gof.csv)")
        return 0

    OUT.mkdir(parents=True, exist_ok=True)
    out_path = OUT / "gof.csv"
    with out_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(OUT_COLS))
        w.writeheader()
        w.writerows(out_rows)
    print(f"\n-> {out_path}: {len(out_rows)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
