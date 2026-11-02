#!/usr/bin/env python3
"""Fang's individually rebuilt ASKA clones as one flat table: gene, insert locus tag,
insert product, measured FFA titer -- what the paper's reverse-genetics panel actually
was, not the model this benchmark scores it against.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fang/parse/build_gof_table.py
    ... --publish        # writes data/fabfos/runs/fang_clones/parse/gof/

WHY W3110 AND NOT MG1655 -- SAME TRAP AS THE EYDALLIN ARM, DIFFERENT HOST. Fang et al.
screened the ASKA library, whose ORFs were PCR-amplified from W3110 genomic DNA before
being cloned into pCA24N (Kitagawa et al. 2005). The plasmids were then electroporated
into MG1655(DE3) delta-fadE, which is the EXPRESSION HOST and contributes no sequence to
any clone. Fang's own Methods confirm the insert lineage from the other end: the screen's
NGS reads were aligned to "the E. coli W3110 reference genome (GenBank ID: AP009048.1)".
So a table describing what a clone IS keys on W3110; the MG1655 GEM that makes ECSPr
answerable is a separate question, answered separately:

    W3110    -- what was overexpressed (this table)
    MG1655   -- the assay host, and iML1515 is ITS OWN curated model rather than a proxy
                (`runs/e_coli_k12/gpr/`, used by `build_gof_reactions.py` and the sweeps)

That second line is where this arm differs from eydallin's, which had to borrow DH1's
model for AG1. Nothing here is a proxy, so nothing here needs the eydallin arm's warning
about not letting one strain's locus tag stand in for the other's -- but the two reference
genomes are still two different jobs and must not be interchanged.

RESOLUTION IS THE THREE-LEGGED CHAIN `build_clone_orfs.py` ESTABLISHED, reused rather than
re-derived: paper name -> b-number through MG1655's GenBank (which carries `/gene_synonym`,
so the retired *rfa* spellings resolve -- `rfaY` is `waaY`/`b3625`, and a lookup against
any current proteome's `[gene=]` field misses every *waa* gene in this cohort);
b-number -> the MG1655 protein; MG1655 protein -> the W3110 protein BY EXACT SEQUENCE,
with the gene symbol as the fallback where the two strains' proteins differ. A pseudo
record never wins a symbol match -- taking a truncated CDS because its symbol matched puts
a fragment in the table under the name of the enzyme.

TWO TABLES, ONE BASELINE EACH, BECAUSE THE PAPER RAN TWO SCREENS AGAINST TWO CONTROLS.
Round 1 and the LPS panel were assayed in strain F (pF, tesA' alone) against F0 = 799.6
mg/L; round 2 was assayed in strain RF (pRF, tesA' + rfaY) against RF = 2240.3 mg/L, since
its whole question was what ADDS to rfaY. Putting both in one `ffa_titer_mg_l` column would
make a 2,200 mg/L round-2 clone look like a triumph beside an 800 mg/L round-1 control when
it is in fact exactly its own control. So `gof.csv` holds the F-background panel and
`gof_round2.csv` the RF-background one, on the identical schema. EACH TABLE IS COMPLETE FOR
ITS OWN PANEL, so the five genes assayed on both backgrounds (`ydeA`, `setA`, `setB`,
`yafL`, `rimM`) appear in both, each row carrying its own baseline. That is not a duplicate:
`yafL` alone in strain F is flat at 853 mg/L and `yafL` on rfaY is up 54% at 3448 mg/L, and
collapsing the pair onto one row deletes either the paper's second-round headline or the
control that makes it interesting.

THE TITERS ARE NOT RE-DIGITISED HERE. `data/fabfos/benchmarks/fang/extraction.tsv`
already carries them, read off the figure bars by this lane's own
`plots`-side pair -- `parse/digitise_ffa.py` and `parse/build_extraction.py` -- against
the four titers the paper states in prose (F0 799.6, rfaY 2461.3, RF 2240.3,
rfaY-yafL 3447.6). Re-measuring the same bars would be a second answer to a
settled question, and two digitisations of one figure is how two arms of this campaign
would come to disagree about what the screen measured.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin/gpr_build"))

from build_clone_orfs import read_faa                                       # noqa: E402
from build_extraction import gene_to_bnumber                                # noqa: E402

EXTRACTION = REPO / "data/fabfos/benchmarks/fang/extraction.tsv"
W3110_FAA = REPO / "data/fabfos/originals/genomes/e_coli_w3110/genome/CP165600.1.faa"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
OUT = REPO / "data/fabfos/runs/fang_clones/parse/gof"

OUT_COLS = ("gene", "source_genome", "locus_tag", "product", "ffa_titer_mg_l")

RESOLUTION_COLS = ("gene", "b_number", "mg1655_accession", "w3110_accession",
                   "source_genome", "locus_tag", "product", "method",
                   "identical_to_mg1655", "aa_len", "note")

# Which panels make up each table, and the control each is normalised against. `figS4` is
# excluded on purpose and is the one exclusion in this file: every figS4 strain carries TWO
# ASKA ORFs, so its titer belongs to a pair rather than to a gene and cannot be written to a
# one-row-per-gene table without inventing an attribution.
PANELS = {
    "gof.csv": (("fig1c", "fig3b", "figS5"), "F0", 799.6,
                "strain F (pF: tesA'), empty-vector control F0"),
    "gof_round2.csv": (("fig4c",), "RF", 2240.3,
                       "strain RF (pRF: tesA' + rfaY), rfaY-only control RF"),
}

# Panel controls carry `role=add` in the extraction because RF really does express an ASKA
# ORF (rfaY, from the backbone rather than from a pCA24N clone). They are the baseline, not
# a clone, and a row for one would be a gene scored against itself.
CONTROL_STRAINS = {"F0", "RF"}


def product_of(rec: dict) -> str:
    """`read_faa` keeps the raw header but not `[protein=...]`, which is the product string
    this table's fourth column is."""
    m = re.search(r"\[protein=([^\]]+)\]", rec.get("header", ""))
    return m.group(1).strip() if m else ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}/")
    a = ap.parse_args()

    obs = (pd.read_csv(EXTRACTION, sep="\t", dtype=str)
           .drop_duplicates("obs_id").fillna(""))
    obs["ffa_mg_L"] = pd.to_numeric(obs.ffa_mg_L, errors="coerce")
    # `strain_id` is the clone; `gene` on a round-2 row names the BACKGROUND's rfaY, not the
    # ORF that was added, so the clone's own gene is the strain id. The two agree on every
    # round-1 row and the assertion below is what keeps that true rather than assumed.
    obs["clone_gene"] = obs.strain_id
    single = obs[(obs.role == "add") & (~obs.strain_id.isin(CONTROL_STRAINS))]

    to_bnum = gene_to_bnumber(MG1655_GBK)
    aliases_of: dict[str, set[str]] = {}
    for nm, bn in to_bnum.items():
        aliases_of.setdefault(bn, set()).add(nm)
    mg = read_faa(MG1655_FAA)
    w3 = read_faa(W3110_FAA)
    mg_by_locus = {r["locus_tag"]: r for r in mg if r["locus_tag"]}
    w3_by_seq: dict[str, dict] = {}
    for r in w3:
        w3_by_seq.setdefault(r["seq"], r)
    w3_by_gene: dict[str, dict] = {}
    for r in sorted(w3, key=lambda x: x["pseudo"]):
        if r["gene"]:
            w3_by_gene.setdefault(r["gene"].lower(), r)
    print(f"MG1655 {len(mg):,} proteins, {len(to_bnum):,} names/synonyms -> b-number | "
          f"W3110 {len(w3):,} proteins, {len(w3_by_seq):,} distinct sequences")

    def resolve(gene: str) -> dict:
        rec = dict.fromkeys(RESOLUTION_COLS, "")
        rec["gene"] = gene
        b = to_bnum.get(gene, "")
        rec["b_number"] = b
        if not b:
            rec["method"] = "unresolved"
            rec["note"] = "no b-number for this name in MG1655's GenBank, synonyms included"
            return rec
        src = mg_by_locus.get(b)
        if src is None:
            rec["method"] = "unresolved"
            rec["note"] = f"{b} has no protein in the MG1655 proteome"
            return rec
        rec["mg1655_accession"] = src["accession"]
        hit, method = w3_by_seq.get(src["seq"]), "w3110_exact_protein"
        if hit is None:
            # The symbol fallback has to carry the SYNONYMS of the b-number, not just the
            # two spellings in hand: W3110's 2025 annotation still calls the LPS core genes
            # by their retired `rfa*` names while MG1655's calls them `waa*`, so `waaQ`
            # against `rfaQ` fails on both `src["gene"]` and the paper's own spelling and
            # would be written unresolved for a protein that is plainly there.
            for key in (src["gene"], gene, *sorted(aliases_of.get(b, ()))):
                if key and key.lower() in w3_by_gene:
                    hit, method = w3_by_gene[key.lower()], "w3110_symbol"
                    break
        if hit is not None and hit["pseudo"]:
            rec["w3110_accession"] = hit["accession"]
            rec["note"] = (f"W3110's {hit['gene'] or hit['locus_tag']} is a pseudogene; "
                           f"took MG1655's protein instead")
            hit, method = (None if src["pseudo"] else src), "mg1655_w3110_pseudo"
        if hit is None:
            rec["method"] = "unresolved"
            rec["note"] = (f"{b} / {src['gene'] or '?'} is a pseudogene in MG1655 and has "
                           f"no protein in W3110 either")
            return rec
        from_w3110 = hit is not src
        rec.update(w3110_accession=rec["w3110_accession"] or hit["accession"],
                   source_genome="W3110" if from_w3110 else "MG1655",
                   locus_tag=hit["locus_tag"], product=product_of(hit), method=method,
                   identical_to_mg1655="yes" if hit["seq"] == src["seq"] else "no",
                   aa_len=str(len(hit["seq"])))
        return rec

    tables, resolution = {}, []
    for fname, (figs, control, control_mg_l, desc) in PANELS.items():
        rows, panel = [], single[single.figure.isin(figs)]
        for gene in sorted(panel.clone_gene.unique()):
            vals = panel.loc[panel.clone_gene == gene, "ffa_mg_L"].dropna().unique()
            if len(vals) != 1:
                raise SystemExit(f"[gof] {gene} carries {len(vals)} distinct titers in "
                                 f"{figs}: {vals}")
            rec = resolve(gene)
            resolution.append({**rec, "note": (rec["note"] + "; " if rec["note"] else "")
                               + f"assayed in {desc} at {control_mg_l} mg/L"})
            rows.append({"gene": gene, "source_genome": rec["source_genome"],
                         "locus_tag": rec["locus_tag"], "product": rec["product"],
                         "ffa_titer_mg_l": f"{vals[0]:.1f}"})
        tables[fname] = (rows, control, control_mg_l, desc)

    for fname, (rows, control, control_mg_l, desc) in tables.items():
        n_un = sum(1 for r in rows if not r["locus_tag"])
        print(f"\n{fname}: {len(rows)} genes, control {control} = {control_mg_l} mg/L "
              f"({desc})")
        by = {}
        for r in rows:
            by[r["source_genome"] or "unresolved"] = by.get(r["source_genome"] or
                                                            "unresolved", 0) + 1
        print(f"    source genome: {by}")
        if n_un:
            print(f"    !! {n_un} gene(s) resolved no protein -- rows kept with empty "
                  f"locus_tag/product: "
                  f"{sorted(r['gene'] for r in rows if not r['locus_tag'])}")

    for fname, (figs, *_r) in PANELS.items():
        want = single[single.figure.isin(figs)].clone_gene.nunique()
        got = len(tables[fname][0])
        if want != got:
            raise SystemExit(f"[gof] {fname}: {want} genes assayed as single clones in "
                             f"{figs} but {got} rows written -- a gene has been dropped")
    both = sorted(set(r["gene"] for r in tables["gof.csv"][0])
                  & set(r["gene"] for r in tables["gof_round2.csv"][0]))
    print(f"\n{sum(len(rows) for rows, *_ in tables.values())} rows over "
          f"{len(set().union(*(set(r['gene'] for r in rows) for rows, *_ in tables.values())))}"
          f" distinct genes; {len(both)} assayed on BOTH backgrounds and carried in both "
          f"tables with their own baseline: {both}")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(REPO)}/)")
        return 0

    OUT.mkdir(parents=True, exist_ok=True)
    for fname, (rows, *_rest) in tables.items():
        with (OUT / fname).open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(OUT_COLS))
            w.writeheader()
            w.writerows(rows)
        print(f"-> {OUT / fname}: {len(rows)} rows")
    res_path = OUT.parent / "annotations" / "clone_resolution.tsv"
    res_path.parent.mkdir(parents=True, exist_ok=True)
    with res_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(RESOLUTION_COLS), delimiter="\t")
        w.writeheader()
        w.writerows(resolution)
    print(f"-> {res_path}: {len(resolution)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
