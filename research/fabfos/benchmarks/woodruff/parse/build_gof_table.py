#!/usr/bin/env python3
"""The SCALEs library as one flat table: gene, insert locus tag, insert product,
measured phenotype -- a true accounting of what the screen actually was, not of the
model this benchmark scores it against. Same schema as the eydallin benchmark's
`parse/build_gof_table.py`, with one column renamed for its units.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/woodruff/parse/build_gof_table.py
    ... --publish        # writes data/fabfos/runs/woodruff_clones/parse/gof/gof.csv

WHICH POPULATION THIS TABLE DESCRIBES, AND WHY IT IS NOT THE PRODUCTION PAPER'S.
Woodruff et al. Metab Eng 17:1-11 (2013) -- the production study, host LW06 -- ranked
4,103 genes by fitness under ethanol production. Its gene symbols lived in a workbook
that survives in the supplementary PowerPoint only as a dead OLE link, so
`parse/decode_mmc1_chart.py` recovers the DISTRIBUTION from the chart's embedded cache
and 4,100 of the 4,103 rows are anonymous (`scales_prod_r0001`...). Exactly three genes
are nameable, and only because the prose names them: betA (rank 2243), betB (1993),
betI (1052).

A gene-keyed table of three rows is not a screen. So this file keys on the COMPANION
paper instead -- Woodruff et al. Metab Eng 15:124-133 (2013), the ethanol-TOLERANCE
screen of the SAME SCALEs library in BW25113 delta-recA -- which ships a complete named
workbook: 4,225 genes, every one with a b-number and two fitness values.

THE TWO ARMS' FITNESS VALUES MAY NOT BE JOINED and this table does not join them. The
later paper recalculated the earlier selections; the ranges differ sixfold and a trial
join matched 337 of 4,103. `REPORT.md` carries that finding. What survives the refusal
is the LIBRARY: both papers screen the same genomic BW25113 fragment library, so the
GENE POPULATION is shared even though the phenotype is not. This table therefore says:
these are the genes the SCALEs library carries, and this is the only per-gene phenotype
either paper leaves nameable. The production readout the LW06 ratio sweep is aimed at
is NOT in this table, because it does not exist per gene anywhere on disk.

`fitness_30gL_ethanol` IS THE RENAMED PHENOTYPE COLUMN. Fitness is
freq_final/freq_initial in the companion paper's own arithmetic and its threshold is
> 1 (487 genes clear it at 30 g/L, 158 at 15 g/L -- both reproduced on the first read
with no tuning). The 15 g/L column is a second selection of the same library and is
deliberately NOT carried here: one phenotype column is the schema, and
`data/fabfos/benchmarks/scales_tol/Y/measured_fitness.tsv` remains the source of truth
for both.

RESOLUTION REUSES `gpr_build/build_scales_gpr.py`'s OWN WORK rather than re-deriving
it. That script already resolved every screened gene against the BW25113 proteome and
wrote the winning FASTA header token to `gene_census.tsv`'s `denovo_feature` column.
This script joins that token against the proteome FASTA to recover a current locus tag
and product string -- it does not re-resolve anything the census already settled.

The library was built from BW25113 genomic DNA, so BW25113 is what a table describing
what a clone actually IS has to be keyed on. That is a DIFFERENT question from what the
ECSPr sweep uses, and the difference is deliberate: the ratio sweep runs against
`runs/e_coli_lw06/gpr/gpr_gem.parquet`, LW06's edited iML1515, because LW06 is the host
the production selection ran in. Two reference genomes for two jobs --
    BW25113 -- what was cloned and overexpressed (this table)
    LW06    -- the organism the conductance is measured on (the sweep)
Do not let a locus tag from one stand in for the other.

EVERY SCREENED GENE KEEPS A ROW. A gene the proteome lookup cannot place gets blank
`locus_tag`/`product` rather than being dropped, so 4,225 rows in means 4,225 rows out.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]

CENSUS = REPO / "data/fabfos/runs/woodruff_clones/gpr/gene_census.tsv"
MEASURED = REPO / "data/fabfos/benchmarks/scales_tol/Y/measured_fitness.tsv"
BW25113_FAA = REPO / "data/fabfos/originals/genomes/e_coli_bw25113/genome/CP193896.1.faa"
OUT = REPO / "data/fabfos/runs/woodruff_clones/parse/gof"

OUT_COLS = ("gene", "source_genome", "locus_tag", "product", "fitness_30gL_ethanol")

GENOME_LABEL = "BW25113"


def read_proteome_by_accession(path: Path) -> dict[str, dict]:
    """NCBI protein FASTA header token (`lcl|...`) -> {locus_tag, product}, read straight
    from the `[locus_tag=...]`/`[protein=...]` bracket fields every header carries -- the
    same accession `gene_census.tsv`'s `denovo_feature` column already records, so this is
    a lookup, not a re-resolution."""
    out: dict[str, dict] = {}
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


def read_tsv(path: Path) -> list[dict]:
    """The Y sidecars carry a `#` provenance line above the header; the census does not."""
    lines = [l for l in path.open() if not l.startswith("#")]
    return list(csv.DictReader(lines, delimiter="\t"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}/gof.csv")
    a = ap.parse_args()

    census = read_tsv(CENSUS)
    measured = {r["gene_norm"]: r for r in read_tsv(MEASURED)}
    proteome = read_proteome_by_accession(BW25113_FAA)

    print(f"{len(census)} genes in {CENSUS.name}, {len(measured)} rows in "
          f"{MEASURED.name}, {len(proteome):,} BW25113 proteins")

    out_rows, unresolved, ambiguous, no_pct = [], [], [], []
    for r in census:
        gene, norm = r["gene"].strip(), r["gene_norm"].strip()
        # `denovo_feature` is `;`-joined where the census's symbol lookup landed on more
        # than one protein -- BW25113's RefSeq annotation carries paralogous entries under
        # one symbol (`asd`, `argF`, `fimA`, ...). Sorted-first is a deterministic
        # representative, not a resolution of the paralogy, and the count is printed so
        # the arbitrariness is visible rather than inferred.
        accessions = sorted(x for x in r.get("denovo_feature", "").split(";") if x.strip())
        hits = [proteome[x] for x in accessions if x in proteome]
        if len(hits) > 1:
            ambiguous.append(gene)
        hit = hits[0] if hits else None
        if hit is None:
            unresolved.append(gene)

        mrow = measured.get(norm)
        if mrow is None:
            no_pct.append(gene)

        out_rows.append({
            "gene": gene,
            "source_genome": GENOME_LABEL if hit else "",
            "locus_tag": hit["locus_tag"] if hit else "",
            "product": hit["product"] if hit else "",
            "fitness_30gL_ethanol": mrow["fitness_30"] if mrow else "",
        })

    print(f"\n{len(out_rows) - len(unresolved)} genes carry a {GENOME_LABEL} protein "
          f"({len(ambiguous)} of them through a symbol this proteome annotates on more "
          f"than one protein), {len(unresolved)} unresolved (no proteome hit -- "
          f"locus_tag/product left blank, the row kept): {sorted(unresolved)[:20]}"
          + (" ..." if len(unresolved) > 20 else ""))
    if no_pct:
        print(f"!! {len(no_pct)} gene(s) have no row in {MEASURED.name} -- "
              f"fitness left blank: {sorted(no_pct)[:20]}")

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
