#!/usr/bin/env python3
"""Summarise E4's MEMOTE tables against metaGEM's, per bin and paired.

Reads results/e4/quality_{metagem,repro,modern}.tsv (e4_quality.py) and, when present,
quality_sample.tsv (e4_quality_sample.py), and prints the medians the findings quote.
"""

import argparse
import csv
import statistics as st
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parents[1] / "results" / "e4"
# The reproduction lane's SBML carries no fbc:chemicalFormula, so MEMOTE's mass balance and its
# transport-reaction detection read nothing there. These rows are reported apart.
FORMULA_BOUND = ["mass_unbalanced", "transport_n", "pure_metabolic_n", "transport_no_gpr"]
COMPARED = [
    "reactions_n", "metabolites_n", "genes_n",
    "stoich_inconsistent", "charge_unbalanced", "unbounded_default", "blocked", "deadends_n",
    "orphans_n", "balanced_cycles", "growth_default_n", "rxn_no_gpr", "complexes",
    "identical_genes", "precursors_missing_n",
] + FORMULA_BOUND
SCORES = ["total_score", "consistency", "annotation_met", "annotation_rxn", "annotation_gene", "annotation_sbo"]


def load(name):
    path = RESULTS / name
    if not path.exists():
        return None
    with open(path) as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def num(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def fmt(x):
    return "" if x is None else f"{x:.4g}"


def median(values):
    values = [v for v in values if v is not None]
    return st.median(values) if values else None


def paired(ours, theirs, columns, label):
    print(f"\n## {label}: paired over bins both scored")
    common = sorted(set(ours) & set(theirs))
    print(f"bins {len(common)}")
    print("| metric | metaGEM median | ours median | median paired difference | bins equal |")
    print("|---|---|---|---|---|")
    for c in columns:
        pairs = [(num(theirs[b].get(c)), num(ours[b].get(c))) for b in common]
        pairs = [(t, o) for t, o in pairs if t is not None and o is not None]
        if not pairs:
            continue
        diffs = [o - t for t, o in pairs]
        print(f"| {c} | {fmt(median(t for t, _ in pairs))} | {fmt(median(o for _, o in pairs))} "
              f"| {fmt(median(diffs))} | {sum(abs(d) < 1e-9 for d in diffs) / len(diffs):.3f} |")


def growth(rows, label):
    by_study = defaultdict(list)
    for r in rows.values():
        g = num(r.get("growth_default_n"))
        if g is not None:
            by_study[r["study"]].append(g > 1e-6)
            by_study["all"].append(g > 1e-6)
    print(f"{label}: " + ", ".join(f"{s} {sum(v)}/{len(v)}" for s, v in sorted(by_study.items())))


def per_study(ours, theirs, columns, label):
    print(f"\n## {label}: median paired difference per study")
    common = sorted(set(ours) & set(theirs))
    studies = sorted({theirs[b]["study"] for b in common})
    print("| metric | " + " | ".join(studies) + " |")
    print("|---|" + "---|" * len(studies))
    for c in columns:
        cells = []
        for s in studies:
            d = [num(ours[b].get(c)) - num(theirs[b].get(c)) for b in common if theirs[b]["study"] == s
                 and num(ours[b].get(c)) is not None and num(theirs[b].get(c)) is not None]
            cells.append(fmt(median(d)))
        print(f"| {c} | " + " | ".join(cells) + " |")


def sample(rows):
    runs = defaultdict(dict)
    for r in rows:
        runs[r["run"]][r["bin"]] = r
    print(f"\n# Sample: {len(runs['repro_0913'])} bins, runs {sorted(runs)}")
    metagem = {r["bin"]: r for r in load("quality_metagem.tsv") if r["bin"] in runs["repro_0913"]}
    modern = {r["bin"]: r for r in load("quality_modern.tsv") if r["bin"] in runs["repro_0913"]}
    paired(runs["repro_0913"], metagem, FORMULA_BOUND, "Sample, MEMOTE 0.9.13, reproduction lane as run vs metaGEM")
    paired(runs["restored_0913"], metagem, COMPARED, "Sample, MEMOTE 0.9.13, framed 0.5.2 attributes restored vs metaGEM")
    print("\n## Sample, MEMOTE 0.17 scores, same script as the modern lane")
    print("| score | metaGEM GEM | reproduction, attributes restored | modern lane |")
    print("|---|---|---|---|")
    for c in SCORES:
        cells = [median(num(src[b].get(c)) for b in src) for src in (runs["metagem_017"], runs["restored_017"], modern)]
        print(f"| {c} | " + " | ".join(fmt(x) for x in cells) + " |")
    for c in ("total_score", "consistency"):
        d = [num(modern[b][c]) - num(runs["metagem_017"][b][c]) for b in runs["metagem_017"]
             if b in modern and num(runs["metagem_017"][b].get(c)) is not None]
        print(f"modern - metaGEM {c}: median {fmt(median(d))}, modern higher in {sum(x > 0 for x in d)}/{len(d)}")
        d = [num(runs["restored_017"][b][c]) - num(runs["metagem_017"][b][c]) for b in runs["metagem_017"]
             if num(runs["restored_017"].get(b, {}).get(c)) is not None and num(runs["metagem_017"][b].get(c)) is not None]
        print(f"restored - metaGEM {c}: median {fmt(median(d))}, max |d| {fmt(max(map(abs, d)) if d else None)}")


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    metagem = {r["bin"]: r for r in load("quality_metagem.tsv")}
    repro = {r["bin"]: r for r in load("quality_repro.tsv")}
    modern = {r["bin"]: r for r in load("quality_modern.tsv")}
    print(f"# Full set: metaGEM {len(metagem)}, reproduction {len(repro)}, modern {len(modern)} bins scored")
    paired(repro, metagem, COMPARED, "MEMOTE 0.9.13, reproduction lane vs metaGEM")
    per_study(repro, metagem, COMPARED, "MEMOTE 0.9.13, reproduction lane vs metaGEM")
    print("\n## Growth on the model's default medium (growth_default_n > 0)")
    growth(metagem, "metaGEM")
    growth(repro, "reproduction")
    print("\n## Modern lane, MEMOTE 0.17 scores, all bins")
    for c in SCORES:
        values = [num(r[c]) for r in modern.values()]
        print(f"{c}: median {fmt(median(values))}")
    rows = load("quality_sample.tsv")
    if rows:
        sample(rows)


if __name__ == "__main__":
    main()
