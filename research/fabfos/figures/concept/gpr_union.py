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
