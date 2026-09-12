#!/usr/bin/env python3
"""Re-derive the SCADC insert set from ./data with the pipeline's own algorithm.

    mamba run -n figure-net python tests/rebuild_insert_set.py --out <dir>
    mamba run -n figure-net python tests/rebuild_insert_set.py --out <dir> --pools pool01_ATTGAGCC

WHY A DRIVER AND NOT A METASMITH RUN
------------------------------------
`resolve_inserts` drives `src/fabfos/algorithm/fabfos_recovery.py`; it does not
reimplement it. Every input that transform consumes -- 35 pools of megahit and
spades contigs with their graphs, and the pCC1fos backbone -- is checked in under
`./data`, so the same three stages run here in minutes against the same code the
transform executes remotely. This driver exists so a change to the algorithm can
be measured before a cluster job is spent on it, and it states every stage's
arguments explicitly so the two paths can be diffed by eye.

The transform remains the definition. When a flag moves here it moves there in
the same change, or the two quietly produce different insert sets.

THE BACKBONE IS RECORD ONE
--------------------------
`data/originals/vector/pcc1.fna` carries the 7,930 bp vector AND a 4.69 Mb EPI300
chromosome. The algorithm refuses a multi-record backbone rather than calling a
junction on every host-derived contig, so record 1 is extracted here first.

PROVENANCE IS STATED, NEVER PARSED
----------------------------------
`--assembly PATH:ASSEMBLER:POOL` is the algorithm's contract: the assembler and
the pool are asserted by the caller. Here the pool name comes from the file
STEM, which is the one place a filename is trusted, and it is trusted because
`data/.../assemblies` is a chunk whose naming is pinned by its own MD5SUMS.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabfos.algorithm import fabfos_recovery as fr   # noqa: E402

ASSEMBLIES = REPO / "data/fabfos/runs/scadc_fosmids/assembly/assemblies"
VECTOR = REPO / "data/fabfos/originals/vector/pcc1.fna"
BLAST_BIN = Path(os.environ.get("FABFOS_BLAST_BIN",
                                "/home/tony/lib/miniforge3/envs/fabfos-bio/bin"))


def pools():
    found = {}
    for f in sorted(ASSEMBLIES.iterdir()):
        if f.suffix in (".md", "") or f.name == "MD5SUMS":
            continue
        pool, _, rest = f.name.partition(".")
        found.setdefault(pool, {})[rest] = f
    want = {"megahit.fna", "megahit.fastg", "spades.fna", "spades.gfa", "spades.paths"}
    complete = {p: d for p, d in found.items() if want <= set(d)}
    for p in sorted(set(found) - set(complete)):
        print(f"  skipping {p}: missing {sorted(want - set(found[p]))}", file=sys.stderr)
    return complete


def backbone(work):
    out = work / "backbone.fna"
    if not out.exists():
        name, desc, seq = next(iter(fr.read_fasta(VECTOR)))
        fr.write_fasta(out, [(name, desc, seq)])
        print(f"  backbone: {name} ({len(seq):,} bp)")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, required=True,
                    help="directory for the new inserts.fna + insert_metadata/")
    ap.add_argument("--pools", nargs="*", default=None, help="default: all")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--keep", action="store_true", help="reuse an existing work tree")
    a = ap.parse_args()

    if not (BLAST_BIN / "blastn").exists():
        raise SystemExit(f"no blastn at {BLAST_BIN}; set FABFOS_BLAST_BIN")
    os.environ["PATH"] = f"{BLAST_BIN}:{os.environ['PATH']}"

    out = a.out.resolve()
    work = out / "work"
    if work.exists() and not a.keep:
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)
    bb = backbone(work)

    selected = pools()
    if a.pools:
        selected = {p: d for p, d in selected.items() if p in set(a.pools)}
    print(f"{len(selected)} pools")

    splits = []
    for i, (pool, d) in enumerate(sorted(selected.items()), 1):
        w = work / pool
        w.mkdir(exist_ok=True)
        split = w / "split_contigs.fna"
        junctions = w / "junctions.tsv"
        if not (a.keep and split.exists()):
            asms = [(d["megahit.fna"], "megahit", pool), (d["spades.fna"], "spades", pool)]
            fr.call_map(asms, bb, junctions, threads=a.threads, work=w / "map")
            fr.rectify(asms, junctions, split, None, None, backbone=bb,
                       graphs={"megahit": (d["megahit.fastg"], None),
                               "spades": (d["spades.gfa"], d["spades.paths"])},
                       graph_hits={}, threads=a.threads, work=w / "rectify")
        splits.append((junctions, split))
        print(f"[{i}/{len(selected)}] {pool}", flush=True)

    allsplit = work / "all_split.fna"
    with open(allsplit, "w") as fh:
        for _j, s in splits:
            fh.write(s.read_text())

    meta_dir = out / "insert_metadata"
    fr.dedup(allsplit, out / "inserts.fna", meta_dir, threads=a.threads,
             work=work / "dedup")
    with open(meta_dir / "junctions.tsv", "w") as fh:
        for j, _s in splits:
            fh.write(j.read_text())

    ref = out / "mapping_reference.fna"
    recs = list(fr.read_fasta(out / "inserts.fna"))
    name, desc, seq = next(iter(fr.read_fasta(bb)))
    fr.write_fasta(ref, recs + [(name, desc, seq)])
    print(f"  mapping_reference.fna: {len(recs)} inserts + {name} ({len(seq):,} bp)")
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
