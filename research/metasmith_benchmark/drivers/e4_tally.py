#!/usr/bin/env python3
"""Count E4's archived GEMs per bin against metaGEM's published GEMs.

Reads each lane's chunk archives (e4_chain.sbatch's <archive>/<lane>/chunk*.tar.zst), traces every
model and MEMOTE product through the lineage index back to its protein bin, and lists every
published bin by whether metaGEM, the reproduction lane and the modern lane each have a model.
With --extract it also writes each model out under its bin's name, for e4_parity.py.
Run it on fir, where the archives and metaGEM's GEM tarballs are.
"""

import argparse
import csv
import gzip
import os
import subprocess
import sys
import tarfile
from collections import Counter
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
PUBLISHED = Path(os.environ.get("METAGEM_PUBLISHED", "/scratch/phyberos/metagem/published"))
ARCHIVE = Path(os.environ.get("E4_ARCHIVE", "/scratch/phyberos/metagem/e4_gems_archive"))
GEM_ARCHIVE = {"sunagawa2015": "tara_gems.tar.gz"}
PRODUCTS = {
    "repro": {"model": "modelling::carveme_model_cplex", "memote": "modelling::memote_results"},
    "modern": {"model": "modelling::carveme_model", "memote": "modelling::memote_score"},
}
Loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def members(archive):
    zstd = subprocess.Popen(["zstd", "-dcq", str(archive)], stdout=subprocess.PIPE)
    with tarfile.open(fileobj=zstd.stdout, mode="r|") as tar:
        for member in tar:
            yield member, tar
    zstd.stdout.close()
    zstd.wait()


def read_index(archive):
    for member, tar in members(archive):
        if member.name == "results/_metadata/index.yml":
            return yaml.load(tar.extractfile(member), Loader=Loader)["manifest"]
    sys.exit(f"{archive} has no results/_metadata/index.yml")


# A second pass, because tar order puts the index anywhere among the products it describes.
def extract_models(archive, models, out):
    for member, tar in members(archive):
        b = models.get(member.name.removeprefix("results/"))
        if b is not None:
            (out / f"{b}.xml.gz").write_bytes(gzip.compress(tar.extractfile(member).read(), compresslevel=1))


def bins_of(manifest):
    """Map each product path in the index to the protein bin at the root of its lineage."""
    memo = {}

    def resolve(path):
        if path in memo:
            return memo[path]
        memo[path] = None
        entry = manifest.get(path)
        if entry is None:
            return None
        if entry.get("type") == "sequences::bin_orfs":
            memo[path] = Path(path).name.removesuffix(".faa")
            return memo[path]
        found = {resolve(parent.split("@", 1)[1]) for parent in entry.get("parents") or {}}
        found.discard(None)
        if len(found) > 1:
            sys.exit(f"{path} descends from {len(found)} bins: {sorted(found)}")
        memo[path] = found.pop() if found else None
        return memo[path]

    return {path: resolve(path) for path in manifest}


def tally_lane(lane, extract):
    have = {"model": Counter(), "memote": Counter()}
    for archive in sorted((ARCHIVE / lane).glob("chunk*.tar.zst")):
        manifest = read_index(archive)
        owner = bins_of(manifest)
        models = {}
        for kind, type_ in PRODUCTS[lane].items():
            for path, entry in manifest.items():
                if entry.get("type") == type_:
                    if owner[path] is None:
                        sys.exit(f"{archive}: {path} has no protein bin in its lineage")
                    have[kind][owner[path]] += 1
                    if kind == "model":
                        models[path] = owner[path]
        if extract:
            (extract / lane).mkdir(parents=True, exist_ok=True)
            extract_models(archive, models, extract / lane)
        print(f"{lane} {archive.name}: {sum(have['model'].values())} models so far", file=sys.stderr)
    for kind, counts in have.items():
        repeated = [b for b, n in counts.items() if n > 1]
        if repeated:
            sys.exit(f"{lane}: {len(repeated)} bins have more than one {kind}, e.g. {repeated[:5]}")
    return have


def published_gems(studies):
    found = set()
    for study in studies:
        with tarfile.open(PUBLISHED / study / GEM_ARCHIVE.get(study, "GEMs.tar.gz"), "r|gz") as tar:
            for member in tar:
                if member.isfile():
                    found.add(os.path.basename(member.name).removesuffix(".gz").removesuffix(".xml"))
    return found


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", default=HERE / "e4_published_proteins.tsv")
    ap.add_argument("--lanes", nargs="*", default=list(PRODUCTS))
    ap.add_argument("--out", required=True, help="per-bin TSV to write")
    ap.add_argument("--extract", type=Path, help="also write each lane's models here, as <lane>/<bin>.xml.gz")
    args = ap.parse_args()

    with open(args.manifest) as fh:
        study_of = {row["bin"]: row["study"] for row in csv.DictReader(fh, delimiter="\t")}
    published = published_gems(sorted(set(study_of.values())))
    lanes = {lane: tally_lane(lane, args.extract) for lane in args.lanes}

    columns = ["study", "bin", "metagem_gem"] + [f"{lane}_{kind}" for lane in lanes for kind in ("model", "memote")]
    with open(args.out, "w", newline="") as fh:
        out = csv.writer(fh, delimiter="\t")
        out.writerow(columns)
        for b in sorted(study_of):
            row = [study_of[b], b, int(b in published)]
            row += [lanes[lane][kind][b] for lane in lanes for kind in ("model", "memote")]
            out.writerow(row)

    print(f"bins {len(study_of)}, metaGEM GEMs {len(published & set(study_of))}")
    stray = published - set(study_of)
    if stray:
        print(f"metaGEM GEMs with no protein bin in the manifest: {len(stray)}: {sorted(stray)}")
    for lane, have in lanes.items():
        models, memotes = set(have["model"]), set(have["memote"])
        print(f"{lane}: models {len(models)}, MEMOTE {len(memotes)}")
        for label, missing in (("no model", sorted(set(study_of) - models)),
                               ("model but no MEMOTE", sorted(models - memotes))):
            print(f"  {label}: {len(missing)}")
            for b in missing:
                print(f"    {study_of[b]}\t{b}\tmetaGEM {'has' if b in published else 'has no'} GEM")
        unknown = models - set(study_of)
        if unknown:
            print(f"  models for bins outside the manifest: {sorted(unknown)}")


if __name__ == "__main__":
    main()
