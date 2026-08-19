"""Turn a sweep into the tables the cap decision rests on.

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_cap/report.py \
        --results <dir>/results_20s.jsonl --outdir research/fabfos/benchmarks/aam_cap/results

WHAT COUNTS AS A HIT. `status == ok` means Indigo returned; it does not mean the result
is usable. A mapping with no atom-map numbers in it is an empty map, which `merge` drops
and which banks nothing -- so `mapped` is the column that matters and `ok` alone would
overstate the yield.

WHAT THE COST COLUMN IS. `hung` reactions dominate wall clock and they are the reason
the member lane has a sidecar rather than a loop. Reporting a mean over everything would
bury them; the tables keep the three outcomes apart and give the hang rate its own
number, because that -- not the median -- is what the routing decision costs.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def load(path: Path) -> pd.DataFrame:
    d = pd.DataFrame([json.loads(l) for l in path.read_text().splitlines()])
    d["has_map"] = d["mapped"].fillna("").str.contains(":")
    d["outcome"] = d["status"].where(d["status"].isin(("ok", "hung")), "timeout")
    d.loc[(d["outcome"] == "ok") & ~d["has_map"], "outcome"] = "empty_map"
    return d


def by_population(d: pd.DataFrame) -> pd.DataFrame:
    g = d.groupby("pop")
    out = pd.DataFrame(dict(
        n=g.size(),
        mapped=g["has_map"].sum(),
        timeout=g["outcome"].apply(lambda s: (s == "timeout").sum()),
        hung=g["outcome"].apply(lambda s: (s == "hung").sum()),
        median_secs=g["secs"].median(),
        max_secs=g["secs"].max(),
    ))
    out["hit_rate"] = (out["mapped"] / out["n"]).round(3)
    out["hang_rate"] = (out["hung"] / out["n"]).round(3)
    return out.reset_index()


def by_size(d: pd.DataFrame) -> pd.DataFrame:
    r = d[d["pop"] != "control_under_cap"].copy()
    r["band"] = pd.cut(r["atoms"], [600, 700, 800, 1000, 1400, 10 ** 9],
                       labels=["600-700", "700-800", "800-1000", "1000-1400", "1400+"])
    g = r.groupby("band", observed=True)
    out = pd.DataFrame(dict(n=g.size(), mapped=g["has_map"].sum(),
                            median_secs=g["secs"].median()))
    out["hit_rate"] = (out["mapped"] / out["n"]).round(3)
    return out.reset_index()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True, type=Path, nargs="+")
    ap.add_argument("--outdir", required=True, type=Path)
    a = ap.parse_args(argv)
    a.outdir.mkdir(parents=True, exist_ok=True)

    d = pd.concat([load(p) for p in a.results], ignore_index=True)
    pop, size = by_population(d), by_size(d)
    pop.to_csv(a.outdir / "indigo_above_the_cap.tsv", sep="\t", index=False)
    size.to_csv(a.outdir / "indigo_by_size.tsv", sep="\t", index=False)
    d[d["has_map"]][["key", "pop", "mnxr", "element", "atoms", "secs"]].sort_values(
        "atoms", ascending=False).to_csv(
        a.outdir / "indigo_reached.tsv", sep="\t", index=False)

    print("\n=== what Indigo does above the cap")
    print(pop.to_string(index=False))
    print("\n=== by reaction size")
    print(size.to_string(index=False))
    print(f"\nwrote three tables to {a.outdir}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
