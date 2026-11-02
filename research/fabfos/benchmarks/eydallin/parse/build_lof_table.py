#!/usr/bin/env python3
"""The 65 Eydallin 2007 deletion mutants as one flat table: gene, deleted locus tag,
deleted product, measured phenotype -- the LOF counterpart to `build_gof_table.py`.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/eydallin/parse/build_lof_table.py
    ... --publish        # writes data/fabfos/runs/eydallin_clones/parse/lof/lof.csv

WHY BW25113, AND WHY THAT IS A DIFFERENT ANSWER FROM THE GOF HALF'S. Eydallin et al.
2007 screened the Keio collection, and the paper's own Methods say so directly: "mutants
from the systematic, single-gene knockout mutant collection of the nonessential genes of
the W3110 derivative BW25113". A deletion mutant IS its chromosome, so the thing that was
perturbed is a BW25113 locus -- there is no plasmid and no insert, which is the entire
difference from the 2010 ASKA screen. `build_gof_table.py` keys on W3110 because W3110 is
where an ASKA insert's sequence came from; this keys on BW25113 because BW25113 is where
the deletion happened. Neither is the other's strain, and neither is DH1, which enters
that half only as AG1's model proxy.

Here the model and the strain agree for once. BW25113's curated GEM is iML1515
(`data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet`), the same pairing this tree's keio
benchmark already declares per row, so `build_lof_reactions.py` scores these genes against
the strain they were deleted from rather than against a borrowed model.

RESOLUTION IS THE SAME THREE LEGS `build_clone_orfs.py` USES, pointed at a different
target strain, and its `read_faa` is imported rather than re-implemented:

  1. name -> b-number, through MG1655's GenBank. The paper is from 2007 and six of its
     names have since been retired: `ybhE` is now `pgl`, `yhbG` is `lptB`, `yobG` is
     `mgrB`, `cspC` is `cspE`, `cysU` is `cysT`, `deoT` is `yciT`. A lookup against any
     current proteome's `[gene=]` field misses those; the GenBank record carries
     `/gene_synonym` and misses none of the 65. That table is `build_extraction.py`'s and
     it is IMPORTED -- two synonym resolvers is how two cohorts come to disagree about
     which gene a name means.
  2. b-number -> the MG1655 protein.
  3. MG1655 protein -> the BW25113 protein BY SEQUENCE. BW25113's RefSeq annotation uses
     `ACROUG_RS*` locus tags and carries no b-numbers, so there is no id to join on; what
     the two strains share is the protein. Exact sequence identity is the strong leg, the
     gene symbol is the fallback, and which one answered is recorded per gene rather than
     averaged into a coverage number.

NOTHING IS SILENTLY DROPPED. A gene that resolves no way keeps its row with blank identity
cells and its reason in `lof_resolution.tsv`, so 65 rows in is 65 rows out and a partial
answer arrives labelled rather than as a file that looks complete.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "research" / "fabfos" / "benchmarks" / "aska"))
sys.path.insert(0, str(HERE / "gpr_build"))
from build_extraction import gene_to_bnumber                             # noqa: E402
from build_clone_orfs import read_faa, one_faa                           # noqa: E402

EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin_2007/extraction.tsv"
MEASURED = REPO / "data/fabfos/benchmarks/eydallin_2007/Y/measured_glycogen.tsv"
MG1655 = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome"
BW25113 = REPO / "data/fabfos/originals/genomes/e_coli_bw25113/genome"
OUT = REPO / "data/fabfos/runs/eydallin_clones/parse/lof"

OUT_COLS = ("gene", "source_genome", "locus_tag", "product", "pct_glycogen_production")

RESOLUTION_COLS = ("gene", "gene_norm", "b_number", "mg1655_accession",
                   "bw25113_accession", "bw25113_gene", "locus_tag", "method",
                   "identical_to_mg1655", "aa_len", "note")

GENOME_LABEL = "BW25113"

PRODUCT_RE = re.compile(r"\[protein=([^\]]+)\]")


def product_of(rec: dict) -> str:
    """`read_faa` keeps every header's bracket fields it needs and the whole header
    besides; `[protein=]` is the one this table wants and that one does not."""
    m = PRODUCT_RE.search(rec["header"])
    return m.group(1).strip() if m else ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}/")
    a = ap.parse_args()

    out_dir = OUT if a.publish else (HERE / "out")
    rows = list(csv.DictReader(EXTRACTION.open(), delimiter="\t"))
    measured = {r["gene"].lower(): r
                for r in csv.DictReader(MEASURED.open(), delimiter="\t")}

    to_bnum = gene_to_bnumber(one_faa(MG1655).with_suffix(".gbk"))
    mg = read_faa(one_faa(MG1655))
    bw = read_faa(one_faa(BW25113))
    print(f"{len(rows)} genes from {EXTRACTION.relative_to(REPO)}, "
          f"{len(measured)} measured rows, MG1655 {len(mg):,} proteins, "
          f"BW25113 {len(bw):,} proteins ({one_faa(BW25113).stem})")

    mg_by_locus = {r["locus_tag"]: r for r in mg if r["locus_tag"]}
    bw_by_seq: dict[str, dict] = {}
    for r in bw:
        bw_by_seq.setdefault(r["seq"], r)
    bw_by_gene: dict[str, dict] = {}
    for r in sorted(bw, key=lambda x: x["pseudo"]):
        if r["gene"]:
            bw_by_gene.setdefault(r["gene"].lower(), r)

    out_rows, table, methods = [], [], {}
    for r in rows:
        gene = r["gene"].strip()
        norm = (r.get("gene_norm") or "").strip() or gene
        rec = dict.fromkeys(RESOLUTION_COLS, "")
        rec.update(gene=gene, gene_norm=norm)

        b = to_bnum.get(gene) or to_bnum.get(norm) or ""
        rec["b_number"] = b
        hit, method = None, "unresolved"

        if not b:
            rec["note"] = ("no b-number for this name in MG1655's GenBank, synonyms "
                           "included")
        elif (src := mg_by_locus.get(b)) is None:
            rec["note"] = (f"{b} is in the GenBank record but has no protein in the "
                           f"MG1655 proteome -- non-coding, or a pseudogene")
        else:
            rec["mg1655_accession"] = src["accession"]
            hit, method = bw_by_seq.get(src["seq"]), "bw25113_exact_protein"
            if hit is None:
                for key in (src["gene"], norm, gene):
                    if key and key.lower() in bw_by_gene:
                        hit, method = bw_by_gene[key.lower()], "bw25113_symbol"
                        break
            if hit is not None and hit["pseudo"]:
                rec["note"] = (f"BW25113's {hit['gene'] or hit['locus_tag']} is a "
                               f"pseudogene; no protein to describe the deleted locus")
                hit, method = None, "unresolved"
            elif hit is None:
                method = "unresolved"
                rec["note"] = (f"{b} / {src['gene'] or '?'} has neither an identical "
                               f"protein nor a matching symbol in BW25113")

        if hit is not None:
            rec.update(bw25113_accession=hit["accession"],
                       bw25113_gene=hit["gene"],
                       locus_tag=hit["locus_tag"],
                       identical_to_mg1655="yes" if method.endswith("exact_protein")
                                           else "no",
                       aa_len=str(len(hit["seq"])))
        rec["method"] = method
        methods[method] = methods.get(method, 0) + 1
        table.append(rec)

        mrow = measured.get(gene.lower())
        out_rows.append({
            "gene": gene,
            "source_genome": GENOME_LABEL if hit else "",
            "locus_tag": hit["locus_tag"] if hit else "",
            "product": product_of(hit) if hit else "",
            "pct_glycogen_production": mrow["pct_wt"] if mrow else "",
        })

    print("\n" + ", ".join(f"{n} {m}" for m, n in sorted(methods.items())))
    unresolved = [r["gene"] for r in table if r["method"] == "unresolved"]
    if unresolved:
        print(f"!! {len(unresolved)} unresolved: {sorted(unresolved)}")
    renamed = [(r["gene"], r["bw25113_gene"]) for r in table
               if r["bw25113_gene"] and r["bw25113_gene"] != r["gene"]]
    print(f"{len(renamed)} of the paper's names have been retired since 2007: "
          + ", ".join(f"{a}->{b}" for a, b in sorted(renamed)))
    n_no_pct = sum(1 for r in out_rows if not r["pct_glycogen_production"])
    if n_no_pct:
        print(f"!! {n_no_pct} gene(s) have no row in {MEASURED.name} -- pct left blank")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(REPO)}/lof.csv)")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "lof.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(OUT_COLS))
        w.writeheader()
        w.writerows(out_rows)
    with (out_dir / "lof_resolution.tsv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(RESOLUTION_COLS), delimiter="\t")
        w.writeheader()
        w.writerows(table)
    print(f"\n-> {out_dir / 'lof.csv'}: {len(out_rows)} rows")
    print(f"-> {out_dir / 'lof_resolution.tsv'}: {len(table)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
