#!/usr/bin/env python3
"""One row per DAS Tool MAG of E5 runs: CheckM2 quality, CarveMe model size and memote scores.

Run on fir with plain python3: `e5_mags.py RUN... > mags.tsv`. Each product is joined to its MAG by walking
the run's results index up to the `sequences-das_tool_bin_fasta` instance it descends from. The table lets
the per-MAG cache shards leave fir once chinook holds them.
"""

import csv
import json
import sys
from collections import deque

from e5_assemblies import HOME, read_index, sample_of

BIN_DIR = "sequences-das_tool_bin_fasta/"
CHECKM2 = ["Completeness", "Contamination", "Genome_Size", "Contig_N50", "Total_Contigs", "GC_Content"]
MEMOTE = ["consistency", "annotation_met", "annotation_rxn", "annotation_gene", "annotation_sbo"]
MODEL = {"reactions": b"<reaction ", "metabolites": b"<species ", "genes": b"<fbc:geneProduct "}
COLUMNS = ["run", "study", "sample", "bin", *CHECKM2, *MODEL, "memote_total", *(f"memote_{s}" for s in MEMOTE)]


def bin_of(key, parents):
    seen, todo = {key}, deque([key])
    while todo:
        k = todo.popleft()
        if k.startswith(BIN_DIR):
            return k
        for p in parents.get(k, ()):
            if p not in seen:
                seen.add(p)
                todo.append(p)
    return None


def rows(run):
    results = HOME / "runs" / run / "results"
    parents = read_index(results / "_metadata" / "index.yml")
    mags = {k: {} for k in parents if k.startswith(BIN_DIR)}
    for key in parents:
        d = key.split("/", 1)[0]
        if d not in ("bench-checkm2_quality", "modelling-carveme_model", "modelling-memote_score"):
            continue
        b = bin_of(key, parents)
        assert b, f"{run}: no MAG above {key}"
        path, row = results / key, mags[b]
        if d == "bench-checkm2_quality":
            with open(path) as f:
                (q,) = csv.DictReader(f, delimiter="\t")
            row.update({c: q[c] for c in CHECKM2})
        elif d == "modelling-carveme_model":
            xml = path.read_bytes()
            row.update({c: xml.count(tag) for c, tag in MODEL.items()})
        elif path.suffix == ".json":
            m = json.loads(path.read_text())
            row["memote_total"] = m["total_score"]
            row.update({f"memote_{s['section']}": s["score"] for s in m["sections"]})
    for b, row in sorted(mags.items()):
        study, sample = sample_of(b, parents)
        yield {"run": run, "study": study, "sample": sample, "bin": b.split("/", 1)[1], **row}


def main(runs):
    w = csv.DictWriter(sys.stdout, COLUMNS, delimiter="\t", lineterminator="\n", restval="")
    w.writeheader()
    for run in runs:
        n = 0
        for r in rows(run):
            w.writerow(r)
            n += 1
        print(f"{run}: {n} MAGs", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
