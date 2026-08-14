"""Fuse the EPI300 GEM lane and the fosmid insert lanes into one GPR table.

The sweep and the layout both take a single GPR parquet and read ``mnxr`` as the reaction
set, so "the host plus its clones" is expressed as one union table rather than as a second
code path. Every downstream mask -- a single clone, a lane subset, host-only -- is applied
to the ``origin`` column of this table, so the community network is measured once and
sliced afterwards.

``origin`` is ``host`` for GEM-lane rows and the insert id for fosmid rows, where the
insert id is the fosmid ORF name with its trailing ``_<n>`` stripped. ``host`` and
``build_id`` are carried because :mod:`gpr_ieff`'s packing step reads them for provenance.

The union is over reactions, not rows: the four fosmid lanes overlap heavily, so a reaction
kept once per (origin, lane) is what makes the origin mask meaningful without inflating the
table with duplicate evidence rows.
"""
import argparse
import re
from pathlib import Path

import pandas as pd

ORF_SUFFIX = re.compile(r"_\d+$")


def insert_of(orf: str) -> str:
    return ORF_SUFFIX.sub("", orf)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host-table", type=Path, required=True)
    ap.add_argument("--fosmid-table", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--host-name", default="e_coli_epi300")
    args = ap.parse_args()

    h = pd.read_parquet(args.host_table, columns=["mnxr", "host", "build_id", "channel"])
    h = h.dropna(subset=["mnxr"]).copy()
    h["origin"] = "host"
    h["insert"] = ""

    f = pd.read_parquet(args.fosmid_table, columns=["mnxr", "orf", "channel", "source"])
    f = f.dropna(subset=["mnxr"]).copy()
    f["insert"] = f["orf"].map(insert_of)
    f["origin"] = f["insert"]
    f["host"] = args.host_name
    f["build_id"] = "fosmid_" + f["source"].astype(str).str.slice(0, 12)

    cols = ["mnxr", "host", "build_id", "channel", "origin", "insert"]
    u = pd.concat([h[cols], f[cols]], ignore_index=True)
    # One row per reaction per origin per lane: the lanes overlap heavily and a duplicate
    # evidence row says nothing the mask can use.
    u = u.drop_duplicates(subset=["mnxr", "origin", "channel"]).reset_index(drop=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    u.to_parquet(args.out, index=False)
    n_host = u.loc[u.origin == "host", "mnxr"].nunique()
    n_ins = u.loc[u.origin != "host", "mnxr"].nunique()
    print(f"{len(u)} rows, {u.mnxr.nunique()} reactions "
          f"({n_host} host, {n_ins} insert, {n_ins - (n_ins + n_host - u.mnxr.nunique())} "
          f"insert-only), {u.loc[u.origin != 'host', 'origin'].nunique()} inserts")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
