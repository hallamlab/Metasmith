"""Every source table in, the integrated tables out: long, wide, coverage and gaps."""
import argparse
from pathlib import Path
import pandas as pd


def gap_reason(status, reason):
    if status == "not-attempted":
        return "not_applicable"
    return "no_hits" if reason.startswith("LookupError") else "failed"


def aggregate(tables):
    # every column as text: sources disagree on type (native_res_m is metres or "vector")
    t = pd.concat([pd.read_csv(p, dtype=str) for p in tables], ignore_index=True)
    t["value_num"] = pd.to_numeric(t.value_num, errors="coerce")
    found = t[t.status == "found"]
    long_ = found.drop(columns=["status", "reason"]).reset_index(drop=True)
    wide = found.pivot_table(index="aoi", columns="feature", values="value", aggfunc="first")
    wide = wide.reindex(sorted(wide.columns), axis=1).reset_index()
    coverage = t.groupby(["aoi", "source"]).status.first().unstack().reset_index()
    # a source registered by two adapter lanes (epa_echo) writes one status row per lane
    gaps = t[t.status != "found"].drop_duplicates(["aoi", "source"])[["aoi", "source", "family", "status", "reason"]].copy()
    gaps.insert(4, "gap", [gap_reason(s, r) for s, r in zip(gaps.status, gaps.reason.fillna(""))])
    gaps["reason"] = gaps.reason.str.replace(r"^[A-Z]\w*: ", "", regex=True)
    return long_, wide, coverage, gaps.sort_values(["source", "aoi"])


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tables", nargs="+", required=True)
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    long_, wide, coverage, gaps = aggregate(a.tables)
    long_.to_parquet(out / "observations_long.parquet", index=False)
    wide.to_parquet(out / "features_wide.parquet", index=False)
    coverage.to_csv(out / "coverage.csv", index=False)
    gaps.to_csv(out / "gaps.csv", index=False)
    print(f"{len(long_)} values, {wide.shape[0]} AOIs x {wide.shape[1] - 1} features, {len(gaps)} gaps")


if __name__ == "__main__":
    main()
