#!/usr/bin/env python3
"""Split the retrieved 4-lane tables back to MAGs, and check that pooling a sample changed no MAG's rows.

    gpr_split.py <repo root>

Reads every parquet under data/scratch/mm_compare_gpr/results whose `source` column names an ORF set,
joins e5_hmp_orf_to_mag.tsv, and writes data/mm_compare/gpr_4lane/<e5_sample>.parquet (one row per call,
plus `mag`) and gpr_4lane/poolcheck.parquet. Writes gpr_4lane/poolcheck_diff.tsv: per lane, the
(orf, mnxr) calls only the pooled run made, only the single-MAG run made, and the largest score change.
"""
import sys
from pathlib import Path

import pandas as pd

POOLCHECK = ("e5_hmp_poolcheck", "toy_hmp_gastrooral_sample_4", "1-21-1.f3cadf8affb7669b-AufqCiBP")


def main(repo: Path):
    mm = repo / "data" / "mm_compare"
    results = repo / "data" / "scratch" / "mm_compare_gpr" / "results"
    wanted = {f.stem for f in (mm / "e5_hmp_orfs").glob("*.faa")} | {POOLCHECK[0]}
    tables = []
    for f in results.rglob("*.parquet"):
        t = pd.read_parquet(f)
        if "source" in t.columns and "mnxr" in t.columns and set(t.source.unique()) <= wanted:
            tables.append(t)
    calls = pd.concat(tables, ignore_index=True)
    missing = wanted - set(calls.source)
    if missing:
        sys.exit(f"no table for {sorted(missing)}")
    if calls.duplicated().any():
        sys.exit(f"{calls.duplicated().sum()} duplicate rows: two tables cover one ORF set")

    o2m = pd.read_csv(mm / "e5_hmp_orf_to_mag.tsv", sep="\t")
    out = mm / "gpr_4lane"
    out.mkdir(exist_ok=True)
    for f in out.glob("*"):
        f.unlink()
    for source, t in calls.groupby("source"):
        sample = POOLCHECK[1] if source == POOLCHECK[0] else source
        t = t.merge(o2m[o2m.e5_sample == sample][["orf", "mag"]], on="orf", how="left")
        if t.mag.isna().any():
            sys.exit(f"{source}: {t.mag.isna().sum()} calls on ORFs with no MAG")
        if source == POOLCHECK[0] and set(t.mag) != {POOLCHECK[2]}:
            sys.exit(f"poolcheck rows map to {set(t.mag)}")
        t.to_parquet(out / ("poolcheck.parquet" if source == POOLCHECK[0] else f"{source}.parquet"), index=False)

    single = pd.read_parquet(out / "poolcheck.parquet")
    pooled = pd.read_parquet(out / f"{POOLCHECK[1]}.parquet")
    pooled = pooled[pooled.mag == POOLCHECK[2]]
    key = ["channel", "orf", "mnxr", "intermediate_id"]
    m = pooled.merge(single, on=key, how="outer", suffixes=("_pooled", "_single"), indicator=True)
    rows = []
    for lane, d in m.groupby("channel"):
        both = d[d._merge == "both"]
        rows.append({"lane": lane, "calls_pooled": int((d._merge != "right_only").sum()),
                     "calls_single": int((d._merge != "left_only").sum()),
                     "only_pooled": int((d._merge == "left_only").sum()),
                     "only_single": int((d._merge == "right_only").sum()),
                     "max_score_change": float((both.raw_score_pooled - both.raw_score_single).abs().max())})
    diff = pd.DataFrame(rows)
    diff.to_csv(out / "poolcheck_diff.tsv", sep="\t", index=False)
    print(f"{calls.source.nunique()} ORF sets, {len(calls):,} calls")
    print(diff.to_string(index=False))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
