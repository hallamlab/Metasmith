import argparse
from pathlib import Path

import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", type=Path, required=True)
    ap.add_argument("--origin", action="append", required=True,
                    help="an origin to keep; repeat. 'host' is the GEM lane.")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    d = pd.read_parquet(args.table)
    keep = d[d.origin.isin(args.origin)].reset_index(drop=True)
    missing = set(args.origin) - set(keep.origin.unique())
    if missing:
        raise SystemExit(f"no rows for origin(s): {sorted(missing)}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    keep.to_parquet(args.out, index=False)
    print(f"{len(keep)} rows, {keep.mnxr.nunique()} reactions over "
          f"{keep.origin.nunique()} origins -> {args.out}")
    for o in args.origin:
        print(f"  {o[:60]:<62} {keep.loc[keep.origin == o, 'mnxr'].nunique()} reactions")


if __name__ == "__main__":
    main()
