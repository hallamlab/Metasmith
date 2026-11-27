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
    """The driver writes it from the source tables, so the workflow needs no product type for it."""
    found = t[t.status == "found"]
    long_ = found.drop(columns=["status", "reason"]).reset_index(drop=True)
    return long_, widen(found), coverage(t), gaps(t)


def widen(found):
    """One row per AOI, a column per feature[class], and a <column>_unit beside any whose unit varies."""
    col = found.feature.where(found["class"].isna(), found.feature + "[" + found["class"].fillna("") + "]")
    f = found.assign(col=col)
    wide = f.pivot_table(index="aoi", columns="col", values="value", aggfunc="first")
    units = f.pivot_table(index="aoi", columns="col", values="SI_unit", aggfunc="first")
    varies = [c for c in units.columns if units[c].dropna().nunique() > 1]
    wide = wide.join(units[varies].add_suffix("_unit"))
    return wide.reindex(sorted(wide.columns), axis=1).rename_axis(columns=None).reset_index()


def coverage(t):
    return t.groupby(["aoi", "source"]).status.first().unstack().reset_index()


def gaps(t):
    # a source registered by two adapter lanes (epa_echo) writes one status row per lane
    g = t[t.status != "found"].drop_duplicates(["aoi", "source"])[["aoi", "source", "family", "status", "reason"]].copy()
    g.insert(4, "gap", [gap_reason(s, r) for s, r in zip(g.status, g.reason.fillna(""))])
    g["reason"] = g.reason.str.replace(r"^[A-Z]\w*: ", "", regex=True)
    return g.sort_values(["source", "aoi"])


def versions(t):
    """One row per source: the time asked for, the data times returned, and the edition and address read."""
    found = t[t.status == "found"].drop_duplicates(["aoi", "source"])
    join = lambda s: ";".join(sorted(set(s.dropna().astype(str))))
    v = found.groupby("source").agg(data_time=("time", join), release=("release", join), url=("url", join))
    v.insert(0, "requested_time", "source default")
    return v.reset_index()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tables", nargs="+", required=True)
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    long_, wide, cov, gap = aggregate(a.tables)
    long_.to_parquet(out / "observations_long.parquet", index=False)
    wide.to_parquet(out / "features_wide.parquet", index=False)
    cov.to_csv(out / "coverage.csv", index=False)
    gap.to_csv(out / "gaps.csv", index=False)
    print(f"{len(long_)} values, {wide.shape[0]} AOIs x {wide.shape[1] - 1} columns, {len(gap)} gaps")


if __name__ == "__main__":
    main()
