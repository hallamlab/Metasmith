#!/usr/bin/env python3
"""Per-model counts and per-reaction cross-references from the CarveMe SBMLs, as the models state them.

    carveme_inventory.py <data/mm_compare>

Walks e5_hmp_carveme/<sample>/<mag>/ and ecoli_carveme/<strain>/. Writes inventory/carveme_models.tsv
(one row per model) and inventory/carveme_reactions.tsv.gz (one row per reaction: gene rule present,
and the EC, MetaCyc and MetaNetX identifiers the SBML annotation carries). Nothing is translated.
"""
import csv
import gzip
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SBML = "{http://www.sbml.org/sbml/level3/version1/core}"
FBC = "{http://www.sbml.org/sbml/level3/version1/fbc/version2}"
RDF = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
XREF = {"ec": "identifiers.org/ec-code/", "metacyc": "identifiers.org/biocyc/META:",
        "mnxr": "identifiers.org/metanetx.reaction/"}


def model_rows(sbml: Path):
    root = ET.parse(sbml).getroot()
    m = root.find(f"{SBML}model")
    n_met = len(m.findall(f"{SBML}listOfSpecies/{SBML}species"))
    n_gene = len(m.findall(f"{FBC}listOfGeneProducts/{FBC}geneProduct"))
    rxns = []
    for r in m.iterfind(f"{SBML}listOfReactions/{SBML}reaction"):
        refs = [li.get(f"{RDF}resource", "") for li in r.iter(f"{RDF}li")]
        x = {k: sorted({u.split(p, 1)[1] for u in refs if p in u}) for k, p in XREF.items()}
        rxns.append((r.get("id"), r.find(f"{FBC}geneProductAssociation") is not None,
                     "|".join(x["ec"]), "|".join(x["metacyc"]), "|".join(x["mnxr"])))
    return n_met, n_gene, rxns


def models(root: Path):
    for s in sorted((root / "e5_hmp_carveme").iterdir()):
        for mag in sorted(s.iterdir()):
            yield "cami_hmp", s.name, mag.name, mag
    for s in sorted((root / "ecoli_carveme").iterdir()):
        yield "ecoli", s.name, s.name, s


def main(root: Path):
    out = root / "inventory"
    out.mkdir(exist_ok=True)
    cols = ["dataset", "sample", "subject", "reactions", "reactions_with_gene_rule", "exchange_reactions",
            "genes", "metabolites", "reactions_with_ec", "reactions_with_metacyc", "reactions_with_mnxr",
            "memote_total"]
    for f in ("carveme_models.tsv", "carveme_reactions.tsv.gz"):
        (out / f).unlink(missing_ok=True)
    with open(out / "carveme_models.tsv", "w") as fm, gzip.open(out / "carveme_reactions.tsv.gz", "wt") as fr:
        wm = csv.writer(fm, delimiter="\t", lineterminator="\n")
        wr = csv.writer(fr, delimiter="\t", lineterminator="\n")
        wm.writerow(cols)
        wr.writerow(["dataset", "sample", "subject", "reaction", "gene_rule", "ec", "metacyc", "mnxr"])
        for ds, sample, subject, d in models(root):
            n_met, n_gene, rxns = model_rows(d / "carveme_sbml.xml")
            memote = d / "memote.json"
            total = json.loads(memote.read_text())["total_score"] if memote.exists() else ""
            wm.writerow([ds, sample, subject, len(rxns), sum(r[1] for r in rxns),
                         sum(r[0].startswith("R_EX_") for r in rxns), n_gene, n_met,
                         sum(bool(r[2]) for r in rxns), sum(bool(r[3]) for r in rxns),
                         sum(bool(r[4]) for r in rxns), total])
            wr.writerows([ds, sample, subject, r[0], int(r[1]), r[2], r[3], r[4]] for r in rxns)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
