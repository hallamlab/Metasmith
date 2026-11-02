#!/usr/bin/env python3
"""Rewrite one SBML model through CarveMe 1.6.6's SBML writer (reframed), and check what changed.

    python sbml_rewrite.py <in.xml> <out.xml> <report.json>

Run it in the CarveMe 1.6.6 image. reframed reads the model in the fbc2 flavor, which is how CarveMe 1.2.2
and metaGEM wrote theirs, and writes it back the same way carve --fbc2 does. Bounds and reversibility are
loaded as written, so the rewrite changes how the model is serialised and nothing else. The report checks
that with a parser independent of reframed: reactions, stoichiometry, bounds, gene rules, species, genes
and the objective must match. It also counts the formulas, charges, annotations and SBO terms on each side.
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from e4_parity import FBC, SBML, gpr_text

RDF = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"
XHTML = "{http://www.w3.org/1999/xhtml}"
EXAMPLES = 5


def notes(elem):
    out = {}
    node = elem.find(SBML + "notes")
    if node is not None:
        for p in node.iter():
            text = "".join(p.itertext()) if p.tag == XHTML + "p" else ""
            if ":" in text:
                key, value = text.split(":", 1)
                out[key.strip()] = value.strip()
    return out


def namespaces(elem):
    found = set()
    annotation = elem.find(SBML + "annotation")
    if annotation is not None:
        for li in annotation.iter(RDF + "li"):
            uri = li.get(RDF + "resource", "")
            if "identifiers.org/" in uri:
                found.add(uri.split("identifiers.org/", 1)[1].split("/")[0].split(":")[0])
    return found


def parse(path):
    model = ET.parse(path).getroot().find(SBML + "model")
    params = {p.get("id"): float(p.get("value")) for p in model.iter(SBML + "parameter")}
    species, reactions, inventory = {}, {}, {"formula": 0, "charge": 0, "sbo": Counter(), "namespaces": {}}
    for kind, tag in (("species", SBML + "species"), ("reaction", SBML + "reaction"), ("gene", FBC + "geneProduct")):
        ns = Counter()
        for elem in model.iter(tag):
            ns.update(namespaces(elem))
            if elem.get("sboTerm"):
                inventory["sbo"][kind] += 1
        inventory["namespaces"][kind] = dict(ns)
    for s in model.iter(SBML + "species"):
        formula, charge = s.get(FBC + "chemicalFormula"), s.get(FBC + "charge")
        inventory["formula"] += formula is not None
        inventory["charge"] += charge is not None
        species[s.get("id")] = {"compartment": s.get("compartment"), "boundary": s.get("boundaryCondition"),
                                "formula": formula, "charge": None if charge is None else int(charge),
                                "notes": notes(s)}
    for r in model.iter(SBML + "reaction"):
        assoc = r.find(FBC + "geneProductAssociation")
        stoich = Counter()
        for side, sign in (("listOfReactants", -1), ("listOfProducts", 1)):
            refs = r.find(SBML + side)
            for ref in [] if refs is None else refs:
                stoich[ref.get("species")] += sign * float(ref.get("stoichiometry", 1))
        reactions[r.get("id")] = {
            "gpr": gpr_text(assoc[0]) if assoc is not None and len(assoc) else "",
            "bounds": (params.get(r.get(FBC + "lowerFluxBound")), params.get(r.get(FBC + "upperFluxBound"))),
            "reversible": r.get("reversible"),
            "stoich": {k: v for k, v in stoich.items() if v},
        }
    objective = {}
    for obj in model.iter(FBC + "objective"):
        for flux in obj.iter(FBC + "fluxObjective"):
            objective[flux.get(FBC + "reaction")] = float(flux.get(FBC + "coefficient"))
    genes = {g.get(FBC + "id") for g in model.iter(FBC + "geneProduct")}
    inventory["sbo"] = dict(inventory["sbo"])
    return {"species": species, "reactions": reactions, "genes": genes, "objective": objective}, inventory


def differ(a, b, field):
    shared = set(a) & set(b)
    return sorted(k for k in shared if a[k][field] != b[k][field])


def check(old, new):
    diffs = {
        "reactions_only_old": sorted(set(old["reactions"]) - set(new["reactions"])),
        "reactions_only_new": sorted(set(new["reactions"]) - set(old["reactions"])),
        "species_only_old": sorted(set(old["species"]) - set(new["species"])),
        "species_only_new": sorted(set(new["species"]) - set(old["species"])),
        "genes_only_old": sorted(old["genes"] - new["genes"]),
        "genes_only_new": sorted(new["genes"] - old["genes"]),
        "gpr": differ(old["reactions"], new["reactions"], "gpr"),
        "bounds": differ(old["reactions"], new["reactions"], "bounds"),
        "reversible": differ(old["reactions"], new["reactions"], "reversible"),
        "stoichiometry": differ(old["reactions"], new["reactions"], "stoich"),
        "compartment": differ(old["species"], new["species"], "compartment"),
        "boundary": differ(old["species"], new["species"], "boundary"),
        "objective": [] if old["objective"] == new["objective"] else sorted(set(old["objective"]) | set(new["objective"])),
    }
    # A formula or charge the rewrite adds must be the one the old notes held.
    shared = set(old["species"]) & set(new["species"])
    for key, note, cast in (("formula", "FORMULA", str), ("charge", "CHARGE", int)):
        wrong = []
        for s in shared:
            before, after = old["species"][s][key], new["species"][s][key]
            if after != before:
                held = old["species"][s]["notes"].get(note)
                try:
                    from_notes = before is None and held is not None and after == cast(held)
                except ValueError:
                    from_notes = False
                if not from_notes:
                    wrong.append(s)
        diffs[f"{key}_not_from_notes"] = sorted(wrong)
    return {k: {"n": len(v), "examples": v[:EXAMPLES]} for k, v in diffs.items() if v}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("report")
    args = ap.parse_args()
    src, dst, report = args.src, args.dst, args.report
    from reframed import load_cbmodel, save_cbmodel
    import reframed

    model = load_cbmodel(src, flavor="fbc2", use_infinity=False, reversibility_check=False)
    save_cbmodel(model, dst, flavor="fbc2")
    old, old_inv = parse(src)
    new, new_inv = parse(dst)
    diffs = check(old, new)
    Path(report).write_text(json.dumps({
        "reframed": reframed.__version__,
        "content_identical": not diffs,
        "diffs": diffs,
        "before": old_inv,
        "after": new_inv,
    }, indent=1))


if __name__ == "__main__":
    main()
