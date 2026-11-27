#!/usr/bin/env python3
"""Pathway overlap with MetaPathways 4, before and after a PathoLogic input change.

    pathway_recheck.py SAMPLE NEW_PGDB_DIR [--tables-root DIR]

Reads export_pgdb.py output for MetaPathways (tables-root/SAMPLE/pgdb_mp), the published port
(tables-root/SAMPLE/pgdb_port) and the rerun (NEW_PGDB_DIR), and prints one TSV row per entity:
pathway Jaccard against MetaPathways, before and after.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


def pathways(d):
    p = Path(d) / "pgdb_genes.parquet"
    if not p.exists():
        return {}
    g = pd.read_parquet(p, columns=["entity", "pathway"])
    g = g[g.pathway != ""]
    return {e: set(s) for e, s in g.groupby("entity").pathway}


def jaccard(a, b):
    return round(len(a & b) / len(a | b), 3) if a | b else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sample")
    ap.add_argument("new")
    ap.add_argument("--tables-root", type=Path, default=Path("/scratch/phyberos/metapathways_mp/tables/v1"))
    a = ap.parse_args()
    T = a.tables_root / a.sample
    mp, old, new = pathways(T / "pgdb_mp"), pathways(T / "pgdb_port"), pathways(a.new)
    print("sample\tentity\tmp\told\tnew\tjaccard_old\tjaccard_new")
    for e in sorted(set(mp) & set(new), key=lambda e: (e != "community", e)):
        m, o, n = mp[e], old.get(e, set()), new[e]
        print(f"{a.sample}\t{e}\t{len(m)}\t{len(o)}\t{len(n)}\t{jaccard(m, o)}\t{jaccard(m, n)}")


if __name__ == "__main__":
    sys.exit(main())
