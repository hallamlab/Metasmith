#!/usr/bin/env python3
"""Re-score a per-study sample of E4 bins so the MEMOTE comparison runs on common ground.

  stage    pick SAMPLE bins per study that all three sources scored, write each one's
           reproduction-lane model, that model with framed 0.5.2's attributes restored, and metaGEM's
           published GEM under <work>/models/, and list them in <work>/bins.txt.
  collect  read the MEMOTE outputs e4_quality_sample.sbatch wrote and tabulate them into
           <out>: one row per bin per run, per-test columns as e4_quality.py writes them.

The restored attributes are what framed 0.5.2 writes from each species' notes: fbc:chemicalFormula
from FORMULA and fbc:charge from an integer CHARGE. metaGEM's published GEMs carry both. framed
0.5.1, CarveMe 1.2.2's pin and the reproduction image's, writes neither.
"""

import argparse
import csv
import gzip
import json
import random
import re
import sys
import tarfile
from pathlib import Path

from e4_quality import SECTIONS, TESTS, flatten, parse_score
from e4_tally import ARCHIVE, GEM_ARCHIVE, PUBLISHED, bins_of, members, read_index

SAMPLE = 40
SEED = 4
SPECIES = re.compile(r"<species\b[^>]*?(/?)>")
FORMULA = re.compile(r"<p>FORMULA: ([^<]*)</p>")
CHARGE = re.compile(r"<p>CHARGE: ([^<]*)</p>")


def restore_fbc(sbml):
    out, pos, added = [], 0, 0
    for tag in SPECIES.finditer(sbml):
        if tag.group(1) or "chemicalFormula" in tag.group(0):
            continue
        notes = sbml[tag.end():sbml.index("</species>", tag.end())]
        formula, charge = FORMULA.search(notes), CHARGE.search(notes)
        attrs = ""
        if charge is not None:
            try:
                attrs += f' fbc:charge="{int(charge.group(1))}"'
            except ValueError:
                pass
        if formula is not None:
            attrs += f' fbc:chemicalFormula="{formula.group(1).strip()}"'
            added += 1
        close = tag.end() - 1
        out += [sbml[pos:close], attrs]
        pos = close
    out.append(sbml[pos:])
    return "".join(out), added


def pick(tables):
    have = []
    for source in ("metagem", "repro", "modern"):
        with open(tables / f"quality_{source}.tsv") as fh:
            have.append({r["bin"]: r["study"] for r in csv.DictReader(fh, delimiter="\t")})
    common = set(have[0]) & set(have[1]) & set(have[2])
    rng = random.Random(SEED)
    chosen = {}
    for study in sorted(set(have[0].values())):
        bins = sorted(b for b in common if have[0][b] == study)
        chosen.update({b: study for b in rng.sample(bins, min(SAMPLE, len(bins)))})
    return chosen


def stage(work, tables):
    chosen = pick(tables)
    models = work / "models"
    for sub in ("repro", "restored", "metagem"):
        (models / sub).mkdir(parents=True, exist_ok=True)
    for archive in sorted((ARCHIVE / "repro").glob("chunk*.tar.zst")):
        manifest = read_index(archive)
        owner = bins_of(manifest)
        wanted = {p: owner[p] for p, e in manifest.items()
                  if e.get("type") == "modelling::carveme_model_cplex" and owner[p] in chosen}
        for member, tar in members(archive):
            b = wanted.get(member.name.removeprefix("results/"))
            if b is None:
                continue
            sbml = tar.extractfile(member).read().decode()
            (models / "repro" / f"{b}.xml").write_text(sbml)
            restored, added = restore_fbc(sbml)
            if added != len(FORMULA.findall(sbml)):
                sys.exit(f"{b}: restored {added} formulas of {len(FORMULA.findall(sbml))}")
            (models / "restored" / f"{b}.xml").write_text(restored)
        print(f"repro {archive.name}: {len(list((models / 'repro').iterdir()))} models", file=sys.stderr)
    for study in sorted(set(chosen.values())):
        with tarfile.open(PUBLISHED / study / GEM_ARCHIVE.get(study, "GEMs.tar.gz"), "r|gz") as tar:
            for member in tar:
                name = Path(member.name).name.removesuffix(".gz").removesuffix(".xml")
                if member.isfile() and chosen.get(name) == study:
                    raw = tar.extractfile(member).read()
                    (models / "metagem" / f"{name}.xml").write_bytes(
                        gzip.decompress(raw) if member.name.endswith(".gz") else raw)
    missing = [b for b in chosen for sub in ("repro", "metagem") if not (models / sub / f"{b}.xml").exists()]
    if missing:
        sys.exit(f"models missing for {missing}")
    with open(work / "bins.txt", "w") as fh:
        fh.writelines(f"{b}\t{chosen[b]}\n" for b in sorted(chosen))
    print(f"staged {len(chosen)} bins", file=sys.stderr)


def collect(work, tables, out):
    with open(work / "bins.txt") as fh:
        study_of = dict(line.rstrip("\n").split("\t") for line in fh)
    with open(tables / "quality_repro.tsv") as fh:
        archived = {r["bin"]: r for r in csv.DictReader(fh, delimiter="\t") if r["bin"] in study_of}
    per_test = [c for name in TESTS for c in (name, f"{name}_n")]
    columns = ["total_score"] + SECTIONS + per_test
    rows, absent = [], []
    for b in sorted(study_of):
        runs = {"repro_0913": archived[b]}
        for run in ("restored_0913", "restored_017", "metagem_017"):
            results = work / "memote" / run / f"{b}.json.gz"
            if not results.exists():
                absent.append(f"{run}/{b}")
                continue
            runs[run] = flatten(json.loads(gzip.decompress(results.read_bytes()))["tests"])
            score = work / "memote" / run / f"{b}.score.json"
            if score.exists():
                runs[run].update(parse_score(score.read_bytes()))
        rows += [[study_of[b], b, run] + [row.get(c, "") for c in columns] for run, row in runs.items()]
    with open(out, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["study", "bin", "run"] + columns)
        w.writerows(rows)
    print(f"{len(study_of)} bins, {len(absent)} runs absent: {absent[:10]}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=["stage", "collect"])
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--tables", type=Path, default=Path(__file__).resolve().parents[1] / "results" / "e4",
                    help="where e4_quality.py's quality_<source>.tsv are")
    ap.add_argument("--out", type=Path, help="collect: the TSV to write")
    args = ap.parse_args()
    if args.step == "stage":
        stage(args.work, args.tables)
    else:
        collect(args.work, args.tables, args.out)


if __name__ == "__main__":
    main()
