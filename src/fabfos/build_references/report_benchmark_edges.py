"""What the condition GPR reaches, per entry, against what the single-route lane reached.

    PATH="/home/tony/lib/miniforge3/envs/msm-fabfos/bin:$PATH" \\
        python build_references/report_benchmark_edges.py \\
            --gpr data/scratch/bench_conditions_local/condition_gpr.parquet

READ-ONLY. It opens the produced table and the givens it was produced from, writes
nothing, and is meant to be read BEFORE anything is published. Two things here are hard
failures rather than observations, and both are ways coverage can go up while the
benchmark gets worse:

  * an entry that had an edge set and no longer does
  * an emitted reaction MetaNetX does not define

THE BASELINE IS RECOMPUTED, NOT REMEMBERED. "Before" is the shipped lane -- accessions
from the het curation, inner-merged against the bridge's uniprot slice -- rebuilt here
from the same bytes. A number copied out of an old log stops being comparable the moment
an input moves; recomputing it costs one merge and stays true.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
BUILDLIB = Path(__file__).resolve().parent / "resources" / "buildlib"
sys.path.insert(0, str(BUILDLIB))

from bench_cohorts import load_all_cohorts                    # noqa: E402
import bench_universe as bu                                   # noqa: E402


def entry_set(df: pd.DataFrame) -> set:
    return set(zip(df["cohort"], df["condition_id"]))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--gpr", default="data/fabfos/scratch/bench_conditions_local/"
                                     "condition_gpr.parquet")
    ap.add_argument("--extracts", default="data/fabfos/benchmarks/_extractions")
    ap.add_argument("--het", default="data/fabfos/originals/benchmarks/het_screen")
    ap.add_argument("--bridge", default="data/fabfos/processed/mnxr_lookup/mnxr_lookup.parquet")
    ap.add_argument("--metanetx", default="data/fabfos/originals/metanetx")
    a = ap.parse_args()

    P = lambda p: (REPO / p) if not Path(p).is_absolute() else Path(p)
    gpr = pd.read_parquet(P(a.gpr))
    genes = load_all_cohorts(laser=P(a.extracts) / "laser", keio=P(a.extracts) / "keio",
                             eydallin=P(a.extracts) / "eydallin", het=P(a.het))
    entries = genes[["cohort", "condition_id"]].drop_duplicates()

    b = pd.read_parquet(P(a.bridge), columns=["id", "id_source", "mnxr"])
    up = (b[b["id_source"] == "uniprot"][["id", "mnxr"]].drop_duplicates()
          .rename(columns={"id": "uniprot"}))
    before = genes[genes["uniprot"].notna()].merge(up, on="uniprot", how="inner")

    reac_prop = bu.reac_prop_path(P(a.metanetx))
    universe = bu.all_mnxrs(reac_prop)
    transport = bu.transport_mnxrs(reac_prop)

    edges = gpr[gpr["mnxr"].notna()]
    prim = edges[edges["in_entry_set"]] if "in_entry_set" in edges.columns else edges
    ok_before, ok_after = entry_set(before), entry_set(edges)

    print("=" * 78)
    print("ENTRY COVERAGE -- entries carrying at least one MNXR")
    print("=" * 78)
    # "Before" is the SHIPPED LANE ON TODAY'S INPUTS, which isolates the code change: the
    # het curation has since resolved more accessions, so this is a few points above the
    # 203 the shipped lane produced on the shipped curation. Both numbers are true and
    # they answer different questions -- this one answers "what did the routes buy".
    print(f"  before (bridge_uniprot only): {len(ok_before):5,} / {len(entries):,}")
    print(f"  after  (all routes):          {len(ok_after):5,} / {len(entries):,}")
    for c, g in entries.groupby("cohort"):
        nb = sum(1 for k in ok_before if k[0] == c)
        na = sum(1 for k in ok_after if k[0] == c)
        print(f"    {c:12s} {nb:5,} -> {na:5,}   of {len(g):5,}")

    lost = sorted(ok_before - ok_after)
    print(f"\n  entries that HAD an edge set and no longer do: {len(lost):,}")
    for k in lost:
        print(f"    LOST {k}")

    print("\n" + "=" * 78)
    print("EDGE-SET SIZE, over the authoritative set (in_entry_set)")
    print("=" * 78)
    sz = prim.groupby(["cohort", "condition_id"])["mnxr"].nunique()
    print(sz.describe().to_string())
    print("\n  per cohort (median / mean / max):")
    for c, g in sz.groupby(level=0):
        print(f"    {c:12s} {g.median():6.0f} {g.mean():8.1f} {g.max():6.0f}")

    print("\n" + "=" * 78)
    print("PER ROUTE")
    print("=" * 78)
    for route, g in edges.groupby("route"):
        n_ent = g[["cohort", "condition_id"]].drop_duplicates().shape[0]
        n_prim = (g[g["in_entry_set"]][["cohort", "condition_id"]].drop_duplicates().shape[0]
                  if "in_entry_set" in g.columns else 0)
        outside = int((~g["mnxr"].isin(universe)).sum())
        n_trans = int(g["mnxr"].isin(transport).sum())
        print(f"  {route:<18} {len(g):>7,} rows  {g['mnxr'].nunique():>5,} reactions  "
              f"{n_ent:>4,} entries  primary for {n_prim:>4,}  "
              f"{n_trans:>5,} transport  {outside:>3,} outside MetaNetX")

    print("\n" + "=" * 78)
    print("ROUTE AGREEMENT -- how many routes independently produced each edge")
    print("=" * 78)
    print(edges["n_agree"].value_counts().sort_index().to_string())
    print("\n  top agreeing sets:")
    for k, v in edges["agreed_by"].value_counts().head(15).items():
        print(f"    {k:<48} {v:>7,}")

    print("\n" + "=" * 78)
    print("CURATED vs DERIVED -- where a curator's set and an inference disagree")
    print("=" * 78)
    # READ OFF `agreed_by`, NOT off a set intersection of the table. Layering means a
    # reaction the curated route supplied is ABSENT from every derived route's rows for
    # that entry -- by construction, which is the point -- so intersecting the emitted
    # sets measures the layering and says nothing about whether the routes agree.
    DERIVED = {"metacyc_rxn", "bridge_uniprot", "bridge_ec", "host_gem", "web"}
    cur = edges[edges["route"] == "curated"].copy()
    cur["_by"] = cur["agreed_by"].str.split("+")
    cur["_corrob"] = [bool(DERIVED & set(v)) for v in cur["_by"]]
    per = cur.groupby(["cohort", "condition_id"]).agg(
        n=("mnxr", "nunique"), n_corrob=("_corrob", "sum"))
    print(f"  {len(per):,} entries carry a curated set")
    print(f"    curated edges a derived route also found: {int(cur['_corrob'].sum()):,} "
          f"of {len(cur):,}")
    der = edges[edges["route"].isin(DERIVED)]
    print(f"    edges only a derived route names (the curator did not): {len(der):,}")
    worst = per[per["n_corrob"] == 0].sort_values("n", ascending=False).head(8)
    print("  entries whose curated set nothing else corroborates:")
    for k, r in worst.iterrows():
        print(f"    {int(r['n']):4,} reactions, 0 corroborated   {k}")

    print("\n" + "=" * 78)
    print("ENTRIES STILL AT ZERO")
    print("=" * 78)
    zero = sorted(entry_set(entries) - ok_after)
    by_route: dict = {}
    for k in zero:
        r = tuple(sorted(set(gpr[(gpr["cohort"] == k[0])
                                 & (gpr["condition_id"] == k[1])]["route"])))
        by_route.setdefault(r or ("absent from the table",), []).append(k)
    print(f"  {len(zero):,} of {len(entries):,}")
    for r, ks in sorted(by_route.items(), key=lambda x: -len(x[1])):
        print(f"    {'+'.join(r):<28} {len(ks):4,}")
        for k in ks[:6]:
            print(f"        {k[0]}/{k[1]}")
        if len(ks) > 6:
            print(f"        ... and {len(ks) - 6:,} more")

    outside = edges[~edges["mnxr"].isin(universe)]
    print("\n" + "=" * 78)
    fails = []
    if lost:
        fails.append(f"{len(lost):,} entries lost their edge set")
    if len(outside):
        fails.append(f"{len(outside):,} emitted edges name a reaction MetaNetX does not "
                     f"define: {sorted(set(outside['mnxr']))[:10]}")
    if fails:
        for f in fails:
            print(f"FAIL: {f}")
        return 1
    print("PASS: no entry lost its edge set; every emitted reaction is in MetaNetX")
    return 0


if __name__ == "__main__":
    sys.exit(main())
