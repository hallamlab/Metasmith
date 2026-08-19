#!/usr/bin/env python3
"""Promote a retrieved fir results directory into a named, pinnable data chunk.

    python tests/promote_coverage_chunk.py inserts <results_dir> <chunk_dir>
    python tests/promote_coverage_chunk.py assemblies <results_dir> <chunk_dir>

WHAT THIS IS FOR
----------------
metasmith names every published output for its content, so a retrieved results
tree is 140-odd files called things like
`2_sequences-assembly_per_bp_coverage/1-1-1.hkZW85eM-NMXQpIPK.gz`. That is
correct and useless: a pinned directory has to say what each file measures
without a manifest join, because the thing reading it a year from now is a
person with `ls`.

The join back to a pool is the drivers' own `attribute()` -- each output's parents
in `_metadata/index.yml`, one of which is the reads file, whose name carries the
barcode -- and it REFUSES rather than guessing when an output cannot be named.
That refusal is the point: a mis-named coverage track is worse than a missing one.
This script only renames what attribution already resolved.

MD5SUMS is written last and over the finished tree, so `md5sum -c MD5SUMS` from
inside the chunk is a complete check of it. The chunk holds no prose: what this
script writes is the inventory and the run identifiers, and the commit that pins
both this script and the .dvc is the provenance record.
"""
from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

LAYOUT = {
    "assembly_stats": ("stats", ".json"),
    "per_contig_coverage": ("per_contig_coverage", ".tsv"),
    "per_bp_coverage": ("per_bp_coverage", ".bedgraph.gz"),
    "read_qc_stats": ("read_qc_stats", ".json"),
}

SUMMARIES = {
    "inserts": ["pool_summary.tsv", "insert_coverage_matrix.tsv",
                "insert_set.json", "pool_map.tsv"],
    "assemblies": ["assembly_summary.tsv", "job_map.tsv"],
}


def _which(path: Path) -> tuple[str, str] | None:
    s = str(path)
    hits = [v for k, v in LAYOUT.items() if k in s]
    if len(hits) != 1:
        return None
    return hits[0]


def promote(kind: str, results: Path, chunk: Path) -> int:
    if kind == "inserts":
        import assembly_stats_on_fir as drv
    else:
        import original_assembly_stats_on_fir as drv

    label_of = drv.attribute(results)

    if chunk.exists():
        raise SystemExit(f"{chunk} already exists; remove it first "
                         f"(`rm -rf {chunk}`) rather than writing into it")
    chunk.mkdir(parents=True)

    placed: dict[str, int] = {}
    skipped: list[Path] = []
    for rel, lab in sorted(label_of.items()):
        src = results / rel
        if not src.exists():
            skipped.append(rel)
            continue
        dest = _which(rel)
        if dest is None:
            skipped.append(rel)
            continue
        sub, ext = dest
        out = chunk / sub / f"{lab}{ext}"
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            raise SystemExit(f"two products would both become {out}: "
                             f"attribution is not one-to-one")
        shutil.copy(src, out)
        placed[sub] = placed.get(sub, 0) + 1

    for name in SUMMARIES[kind]:
        s = results.parent / name
        if s.exists():
            shutil.copy(s, chunk / name)
        else:
            print(f"  WARNING: summary {name} missing from {results.parent}",
                  file=sys.stderr)

    lines = []
    for p in sorted(chunk.rglob("*")):
        if not p.is_file():
            continue
        h = hashlib.md5(p.read_bytes()).hexdigest()
        lines.append(f"{h}  ./{p.relative_to(chunk)}")
    (chunk / "MD5SUMS").write_text("\n".join(lines) + "\n")

    print(f"=== {chunk} ===")
    for sub in sorted(placed):
        print(f"  {sub:24s} {placed[sub]}")
    print(f"  summaries               {len(SUMMARIES[kind])}")
    print(f"  MD5SUMS                 {len(lines)} files")
    if skipped:
        print(f"  not promoted            {len(skipped)} "
              f"(left on fir, e.g. {skipped[0]})")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kind", choices=sorted(SUMMARIES))
    ap.add_argument("results", help="the retrieved <run>/results directory")
    ap.add_argument("chunk", help="data/fabfos/runs/<run>/<name> to create")
    a = ap.parse_args()
    return promote(a.kind, Path(a.results).resolve(), Path(a.chunk).resolve())


if __name__ == "__main__":
    sys.exit(main())
