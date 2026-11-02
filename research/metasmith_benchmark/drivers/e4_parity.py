#!/usr/bin/env python3
"""Compare E4's GEMs with metaGEM's published GEMs for the same bins.

Reads a TSV of `bin<TAB>path to our SBML` (plain or .gz), or pairs a run's results directory with
its bins by gene content (--results). Pulls each bin's published model out of its study's GEM
archive, and reports per bin the reaction, metabolite and gene sets, the GPR of every shared
reaction and the flux bounds of every shared reaction. Run it on fir, where the archives are.
"""

import argparse
import csv
import gzip
import json
import os
import statistics
import sys
import tarfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
PUBLISHED = Path(os.environ.get("METAGEM_PUBLISHED", "/scratch/phyberos/metagem/published"))
GEM_ARCHIVE = {"sunagawa2015": "tara_gems.tar.gz"}
SBML = "{http://www.sbml.org/sbml/level3/version1/core}"
FBC = "{http://www.sbml.org/sbml/level3/version1/fbc/version2}"


def gpr_text(node):
    tag = node.tag.removeprefix(FBC)
    if tag == "geneProductRef":
        return node.get(FBC + "geneProduct")
    parts = sorted(gpr_text(child) for child in node)
    if len(parts) == 1:
        return parts[0]
    return "(" + f" {tag} ".join(parts) + ")"


def parse_model(data):
    root = ET.fromstring(data)
    model = root.find(SBML + "model")
    params = {p.get("id"): float(p.get("value")) for p in model.iter(SBML + "parameter")}
    reactions = {}
    for r in model.iter(SBML + "reaction"):
        assoc = r.find(FBC + "geneProductAssociation")
        gpr = gpr_text(assoc[0]) if assoc is not None and len(assoc) else ""
        bounds = (params.get(r.get(FBC + "lowerFluxBound")), params.get(r.get(FBC + "upperFluxBound")))
        reactions[r.get("id")] = (gpr, bounds)
    return {
        "reactions": reactions,
        "metabolites": {s.get("id") for s in model.iter(SBML + "species")},
        "genes": {g.get(FBC + "id") for g in model.iter(FBC + "geneProduct")},
    }


def read_sbml(path):
    raw = Path(path).read_bytes()
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


# Yielded one at a time: a study holds up to 9,371 models, too many to hold decompressed.
def published_models(study, bins):
    wanted = set(bins)
    with tarfile.open(PUBLISHED / study / GEM_ARCHIVE.get(study, "GEMs.tar.gz"), "r|gz") as tar:
        for member in tar:
            name = os.path.basename(member.name).removesuffix(".gz").removesuffix(".xml")
            if member.isfile() and name in wanted:
                raw = tar.extractfile(member).read()
                yield name, gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
                wanted.discard(name)
                if not wanted:
                    break


def sanitised(protein_id):
    return "G_" + "".join(ch if ch.isalnum() else "_" for ch in protein_id)


# A model's genes are its bin's protein ids, so the bin whose proteins hold all of them made it.
# CarveMe's spontaneous pseudo-gene is in no FASTA.
def pair_by_genes(results, bins, study_of):
    proteins = {}
    for b in bins:
        faa = PUBLISHED / study_of[b] / "proteins" / f"{b}.faa"
        proteins[b] = {sanitised(line[1:].split()[0]) for line in open(faa) if line.startswith(">")}
    candidates = {}
    for xml in sorted(Path(results).iterdir()):
        genes = {g for g in parse_model(read_sbml(xml))["genes"] if "spontaneous" not in g.lower()}
        candidates[xml] = sorted(b for b in bins if genes and genes <= proteins[b])
    pairs, taken = {}, set()
    for xml, found in sorted(candidates.items(), key=lambda kv: len(kv[1])):
        free = [b for b in found if b not in taken]
        if not free:
            print(f"# no bin for {xml.name} (candidates {found})", file=sys.stderr)
            continue
        if len(found) > 1:
            print(f"# {xml.name} fits {found}, assigned {free[0]}", file=sys.stderr)
        pairs[free[0]] = str(xml)
        taken.add(free[0])
    return pairs


def compare(ours, theirs):
    def sets(key):
        a, b = set(ours[key]), set(theirs[key])
        return {"ours": len(a), "theirs": len(b), "shared": len(a & b),
                "only_ours": sorted(a - b), "only_theirs": sorted(b - a)}

    out = {k: sets(k) for k in ("reactions", "metabolites", "genes")}
    shared = set(ours["reactions"]) & set(theirs["reactions"])
    out["gpr_differs"] = sorted(r for r in shared if ours["reactions"][r][0] != theirs["reactions"][r][0])
    out["bounds_differ"] = sorted(r for r in shared if ours["reactions"][r][1] != theirs["reactions"][r][1])
    out["identical"] = all(not out[k]["only_ours"] and not out[k]["only_theirs"]
                           for k in ("reactions", "metabolites", "genes")) \
        and not out["gpr_differs"] and not out["bounds_differ"]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pairs", nargs="?", help="TSV: bin, path to our SBML")
    ap.add_argument("--results", help="a run's results directory of models, paired with --bins by gene content")
    ap.add_argument("--bins", nargs="*", help="the bins the run carved")
    ap.add_argument("--manifest", default=HERE / "e4_published_proteins.tsv")
    ap.add_argument("--json", help="write the full per-bin differences here")
    args = ap.parse_args()

    study_of = {r["bin"]: r["study"] for r in csv.DictReader(open(args.manifest), delimiter="\t")}
    if args.results:
        ours = pair_by_genes(args.results, args.bins, study_of)
    else:
        ours = dict(line.rstrip("\n").split("\t")[:2] for line in open(args.pairs) if line.strip())
    by_study = defaultdict(list)
    for b in ours:
        by_study[study_of[b]].append(b)

    results = {}
    for study, bins in by_study.items():
        for b, theirs in published_models(study, bins):
            results[b] = compare(parse_model(read_sbml(ours[b])), parse_model(theirs))
        for b in bins:
            results.setdefault(b, {"missing_published": True})

    cols = ("reactions", "metabolites", "genes")
    print("bin\tstudy\t" + "\t".join(f"{k}_ours\t{k}_theirs\t{k}_shared" for k in cols)
          + "\tgpr_differs\tbounds_differ\tidentical")
    for b in sorted(results, key=lambda b: (study_of[b], b)):
        r = results[b]
        if r.get("missing_published"):
            print(f"{b}\t{study_of[b]}\tno published GEM")
            continue
        print(f"{b}\t{study_of[b]}\t" + "\t".join(f"{r[k]['ours']}\t{r[k]['theirs']}\t{r[k]['shared']}" for k in cols)
              + f"\t{len(r['gpr_differs'])}\t{len(r['bounds_differ'])}\t{r['identical']}")
    compared = [r for r in results.values() if not r.get("missing_published")]
    if compared:
        def spread(values):
            return f"{statistics.median(values):.3f} ({min(values):.3f}-{max(values):.3f})"

        def jaccard(r, k):
            union = r[k]["ours"] + r[k]["theirs"] - r[k]["shared"]
            return r[k]["shared"] / union if union else 1.0

        print(f"# {len(compared)} bins compared, {len(results) - len(compared)} with no published GEM", file=sys.stderr)
        for k in cols:
            print(f"# {k} Jaccard median {spread([jaccard(r, k) for r in compared])}", file=sys.stderr)
        gpr = [100 * len(r["gpr_differs"]) / r["reactions"]["shared"] for r in compared if r["reactions"]["shared"]]
        print(f"# shared reactions with a different GPR, median {statistics.median(gpr):.1f}%", file=sys.stderr)
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=1))
    return 0 if all(r.get("identical") for r in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
