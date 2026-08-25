#!/usr/bin/env python3
"""The 86 Eydallin clones as protein sequences, taken from W3110.

    mamba run -n figure-net python main/benchmarks/eydallin/build_clone_orfs.py
    mamba run -n figure-net python main/benchmarks/eydallin/build_clone_orfs.py --publish

Eydallin et al. screened the ASKA library, and an ASKA clone is a W3110 ORF on a
high-copy plasmid -- so the sequence of the thing that was overexpressed is W3110's,
not MG1655's, even though the paper writes K-12 gene names and every other study in
this tree resolves against MG1655. This writes those 86 sequences out as one ORF set,
which is what the de-novo half of the cohort has to run on.

THE HEADER IS THE GENE NAME, and that is the deliverable rather than a convenience:
the cohort is keyed on the paper's names all the way through -- `extraction.tsv`,
`conditions.tsv` (`eydallin:<gene>`) and the digitised bar heights -- so an ORF set
keyed on anything else needs a join to get back to the measurement.

RESOLUTION IS THREE-LEGGED, AND THE LEGS ARE NOT INTERCHANGEABLE.

  1. name -> b-number, through MG1655's GenBank. The paper is from 2010 and several
     of its names have since been retired -- `pfs` is `mtnN`, `yncC` is `mlrA` -- so a
     lookup against any current proteome's `[gene=]` field misses 11 of the 86 while a
     lookup against the GenBank record, which carries `/gene_synonym`, misses none.
     This is `build_extraction.py`'s table and it is IMPORTED rather than re-derived:
     two copies of a synonym resolver is how the two ASKA cohorts come to disagree
     about which gene a name means.
  2. b-number -> the MG1655 protein. The b-number is the id both K-12 substrains'
     literature is keyed on and the only one the model uses.
  3. MG1655 protein -> the W3110 protein BY SEQUENCE. W3110's 2025 RefSeq annotation
     has its own locus tags and no b-numbers, so there is no id to join on; what the
     two strains share is the protein itself. Exact sequence identity is the strong
     leg, the gene symbol is the fallback for the handful that differ, and which one
     answered is recorded per gene rather than averaged into a coverage number. That
     is `check_epi300_identity.py`'s rule -- the join that means something is the
     protein, exact where it matches and symbol otherwise -- applied to a second pair
     of strains.

A PSEUDOGENE IS A CDS WITH A TRANSLATION AND NO PROTEIN, and both strains have one in
this cohort. MG1655's `yhcE`/`b4569` is `[pseudo=true]`, and W3110's `tnaA` is
`[pseudo=true] [partial=5']` -- a truncated tryptophanase where MG1655 carries the full
471 aa. Taking the pseudo record because its symbol matched would put a fragment in the
ORF set under the name of the enzyme, which every lane downstream would then annotate as
best it could. So a pseudo record never wins a symbol match: the resolver falls back to
MG1655's protein and says which record it took and why. Where BOTH sides are pseudo
there is no protein to take at all, and the gene is reported unresolved rather than
given a fragment.

NOTHING IS SILENTLY DROPPED. A gene that resolves no way is absent from the FASTA and
present in `clone_resolution.tsv` with the reason, so "85 of 86" survives to the reader
instead of arriving as a file that looks complete.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "main" / "benchmarks" / "aska"))
from build_extraction import gene_to_bnumber                          # noqa: E402

EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
MG1655 = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome"
W3110 = REPO / "data/fabfos/originals/genomes/e_coli_w3110/genome"
OUT = REPO / "data/fabfos/runs/eydallin_clones/annotations"

ORF_SET = "eydallin_clones"

RESOLUTION_COLS = (
    "gene", "gene_norm", "b_number", "mg1655_accession", "w3110_accession",
    "w3110_gene", "source_strain", "method", "identical_to_mg1655", "aa_len", "note",
)


def read_faa(path: Path) -> list[dict]:
    out, cur, seq = [], None, []
    for line in path.open():
        if line.startswith(">"):
            if cur:
                cur["seq"] = "".join(seq)
                out.append(cur)
            head = line[1:].rstrip("\n")
            cur = dict(accession=head.split()[0], header=head)
            for key, pat in (("gene", r"\[gene=([^\]]+)\]"),
                             ("locus_tag", r"\[locus_tag=([^\]]+)\]"),
                             ("protein_id", r"\[protein_id=([^\]]+)\]"),
                             ("partial", r"\[partial=([^\]]+)\]")):
                m = re.search(pat, head)
                cur[key] = m.group(1).strip() if m else ""
            cur["pseudo"] = ("[pseudo=true]" in head) or not cur["protein_id"]
            seq = []
        elif cur is not None:
            seq.append(line.strip())
    if cur:
        cur["seq"] = "".join(seq)
        out.append(cur)
    return out


def one_faa(d: Path) -> Path:
    hits = sorted(d.glob("*.faa"))
    if len(hits) != 1:
        raise SystemExit(f"expected one proteome under {d}, found "
                         f"{[p.name for p in hits]}")
    return hits[0]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)} rather than beside this "
                         f"script")
    a = ap.parse_args()

    out_dir = OUT if a.publish else (HERE / "out")
    rows = list(csv.DictReader(EXTRACTION.open(), delimiter="\t"))
    print(f"{len(rows)} genes from {EXTRACTION.relative_to(REPO)}")

    to_bnum = gene_to_bnumber(one_faa(MG1655).with_suffix(".gbk"))
    mg = read_faa(one_faa(MG1655))
    w3 = read_faa(one_faa(W3110))
    print(f"MG1655 {len(mg):,} proteins   W3110 {len(w3):,} proteins "
          f"({one_faa(W3110).stem})")

    mg_by_locus = {r["locus_tag"]: r for r in mg if r["locus_tag"]}
    w3_by_seq: dict[str, dict] = {}
    dup_seqs = 0
    for r in w3:
        if r["seq"] in w3_by_seq:
            dup_seqs += 1
            continue
        w3_by_seq[r["seq"]] = r
    w3_by_gene = {}
    for r in sorted(w3, key=lambda x: x["pseudo"]):
        if r["gene"]:
            w3_by_gene.setdefault(r["gene"].lower(), r)
    print(f"W3110 index: {len(w3_by_seq):,} distinct sequences "
          f"({dup_seqs} duplicate), {len(w3_by_gene):,} gene symbols, "
          f"{sum(1 for r in w3 if r['pseudo']):,} pseudogene records")

    resolved, table = [], []
    for r in rows:
        gene = (r["gene"] or "").strip()
        norm = (r["gene_norm"] or "").strip() or gene
        rec = dict.fromkeys(RESOLUTION_COLS, "")
        rec.update(gene=gene, gene_norm=norm)

        b = to_bnum.get(norm) or to_bnum.get(gene) or ""
        rec["b_number"] = b
        if not b:
            rec.update(method="unresolved",
                       note="no b-number for this name in MG1655's GenBank, synonyms "
                            "included")
            table.append(rec)
            continue

        src = mg_by_locus.get(b)
        if src is None:
            rec.update(method="unresolved",
                       note=f"{b} is in the GenBank record but has no protein in the "
                            f"MG1655 proteome -- non-coding, or a pseudogene")
            table.append(rec)
            continue
        rec["mg1655_accession"] = src["accession"]

        hit = w3_by_seq.get(src["seq"])
        method = "w3110_exact_protein"
        if hit is None:
            for key in (src["gene"], norm, gene):
                if key and key.lower() in w3_by_gene:
                    hit = w3_by_gene[key.lower()]
                    method = "w3110_symbol"
                    break
        if hit is not None and hit["pseudo"]:
            rec["note"] = (f"W3110's {hit['gene'] or hit['locus_tag']} is a pseudogene"
                           + (f" ({hit['partial']} partial)" if hit["partial"] else "")
                           + "; took MG1655's protein instead")
            rec["w3110_accession"] = hit["accession"]
            hit = None
            method = "mg1655_w3110_pseudo"
        if hit is None and not src["pseudo"] and method == "mg1655_w3110_pseudo":
            hit = src
        if hit is None:
            if src["pseudo"]:
                rec.update(method="unresolved",
                           note=f"{b} / {src['gene'] or '?'} is a pseudogene in MG1655 "
                                f"and has no protein in W3110 either -- there is no "
                                f"protein for this name on either side")
            else:
                rec.update(method="unresolved",
                           note=f"{b} / {src['gene'] or '?'} has neither an identical "
                                f"protein nor a matching symbol in W3110")
            table.append(rec)
            continue

        from_w3110 = hit is not src
        identical = hit["seq"] == src["seq"]
        rec.update(w3110_accession=rec["w3110_accession"] or hit["accession"],
                   w3110_gene=hit["gene"] if from_w3110 else "",
                   source_strain="w3110" if from_w3110 else "mg1655",
                   method=method, identical_to_mg1655="yes" if identical else "no",
                   aa_len=str(len(hit["seq"])))
        if from_w3110 and not identical:
            n = min(len(hit["seq"]), len(src["seq"]))
            same = sum(1 for x, y in zip(hit["seq"][-n:], src["seq"][-n:]) if x == y)
            rec["note"] = (f"differs from MG1655 ({len(src['seq'])} aa there); "
                           f"C-terminal-aligned identity {same}/{n} ({same / n:.1%})")
        table.append(rec)
        resolved.append((gene, hit, rec))

    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "clone_resolution.tsv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(RESOLUTION_COLS), delimiter="\t")
        w.writeheader()
        w.writerows(table)

    names = [g for g, _, _ in resolved]
    if len(set(names)) != len(names):
        dupes = sorted({n for n in names if names.count(n) > 1})
        raise SystemExit(f"duplicate gene names would collide as FASTA headers: {dupes}")

    faa = out_dir / f"{ORF_SET}.faa"
    with faa.open("w") as fh:
        for gene, hit, rec in resolved:
            fh.write(f">{gene} accession={hit['accession']} b_number={rec['b_number']} "
                     f"strain={rec['source_strain']} method={rec['method']} "
                     f"identical={rec['identical_to_mg1655']}\n")
            s = hit["seq"]
            for i in range(0, len(s), 60):
                fh.write(s[i:i + 60] + "\n")

    by_method: dict[str, int] = {}
    for rec in table:
        by_method[rec["method"]] = by_method.get(rec["method"], 0) + 1
    n_diff = sum(1 for rec in table if rec["identical_to_mg1655"] == "no"
                 and rec["source_strain"] == "w3110")
    print(f"\n{len(resolved)}/{len(rows)} resolved -> {faa}")
    for m, n in sorted(by_method.items()):
        print(f"    {m:<22} {n}")
    print(f"    {n_diff} of the W3110 sequences differ from MG1655's")
    for rec in table:
        if rec["method"] == "mg1655_w3110_pseudo":
            print(f"    PSEUDO     {rec['gene']}: {rec['note']}")
    for rec in table:
        if rec["method"] == "unresolved":
            print(f"    UNRESOLVED {rec['gene']}: {rec['note']}")
    print(f"\nresolution -> {out_dir / 'clone_resolution.tsv'}")
    if len(table) != len(rows):
        raise SystemExit("the resolution table does not account for every gene")
    return 0


if __name__ == "__main__":
    sys.exit(main())
