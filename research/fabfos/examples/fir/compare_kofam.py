#!/usr/bin/env python3
"""Is the REUSED KOfam evidence the same evidence a fresh run would produce?

    python compare_kofam.py <fresh.csv> <repacked.csv>

The campaign does not recompute KOfam. It repacks an earlier pass's per-assembly
hits into the shards and hands them to the planner as already-computed products,
which is where roughly half the compute saving comes from. Everything checked so
far only establishes that those hits are WELL FORMED and land in the right
shard: the header was verified across all 2,844 assemblies, the ids join to each
shard's own id set, and the repack is exactly conservative (82,116,616 rows in,
82,116,616 out).

None of that would notice if the earlier pass had used a different KOfam release
or a different score threshold. A wrong-but-well-formed hit table joins,
validates and delivers exactly like a right one -- so the only thing that
settles it is running KOfam from the same ORFs and comparing the rows.

What is compared, in descending order of what a difference would mean:

  1. The (gene, KO) call set. A call present in one and not the other is a
     different answer about that gene, whatever the scores say.
  2. `best` -- KOfam's own mark for the top assignment per gene. Two runs can
     agree on every call and disagree on which is the gene's primary one.
  3. The bitscore, for calls both made. Identical profiles and thresholds give
     identical scores; a systematic offset means a different profile release,
     and scattered differences mean a different hmmsearch build.

Exit 0 only when the call sets are identical. Anything else is a method change
and has to be reported as one, not averaged away.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

COLS = ["gene_name", "KO", "thrshld", "score", "E-value", "best"]


def load(path: Path, what: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if list(df.columns) != COLS:
        raise SystemExit(
            f"{what} ({path}) has columns {list(df.columns)}, not {COLS}. The "
            f"comparison is meaningless against a different schema.")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df


def main() -> int:
    fresh = load(Path(sys.argv[1]), "the fresh run")
    old = load(Path(sys.argv[2]), "the repacked lane")

    fg, og = set(fresh["gene_name"]), set(old["gene_name"])
    fk = set(map(tuple, fresh[["gene_name", "KO"]].values))
    ok = set(map(tuple, old[["gene_name", "KO"]].values))

    print(f"fresh    {len(fresh):>8,} rows  {len(fg):>7,} genes  {fresh['KO'].nunique():>6,} KOs")
    print(f"repacked {len(old):>8,} rows  {len(og):>7,} genes  {old['KO'].nunique():>6,} KOs")

    both, only_f, only_o = fk & ok, fk - ok, ok - fk
    jac = len(both) / max(len(fk | ok), 1)
    print(f"\n(gene,KO) calls: {len(both):,} shared, {len(only_f):,} fresh-only, "
          f"{len(only_o):,} repacked-only  (Jaccard {jac:.6f})")
    for label, s in (("fresh-only", only_f), ("repacked-only", only_o)):
        if s:
            print(f"  e.g. {label}: {sorted(s)[:3]}")

    # Scores, for the calls both runs made.
    m = fresh.merge(old, on=["gene_name", "KO"], suffixes=("_f", "_o"))
    if len(m):
        d = (m["score_f"] - m["score_o"]).abs()
        print(f"\nbitscore over {len(m):,} shared calls: "
              f"identical {int((d == 0).sum()):,} "
              f"({100.0 * (d == 0).mean():.4f}%), "
              f"max |diff| {d.max():.6g}, median |diff| {d.median():.6g}")
        bf = m["best_f"].astype(str).str.strip()
        bo = m["best_o"].astype(str).str.strip()
        print(f"`best` marker agrees on {int((bf == bo).sum()):,} of {len(m):,} "
              f"({100.0 * (bf == bo).mean():.4f}%)")

    if only_f or only_o:
        print(f"\nTHE REUSED LANE IS NOT THE SAME EVIDENCE. {len(only_f):,} calls "
              f"the fresh run makes are absent from the repacked lane and "
              f"{len(only_o):,} are the other way round. That is a method "
              f"difference -- a different KOfam release or score threshold -- "
              f"and it belongs in the provenance record, not in a rounding "
              f"argument. It does NOT by itself invalidate the tables: it means "
              f"the kofam channel is attributable to the earlier pass's method, "
              f"and is not comparable to a fresh one.", file=sys.stderr)
        return 1

    print(f"\nThe reused lane is the same evidence: identical call sets over "
          f"{len(fg):,} genes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
