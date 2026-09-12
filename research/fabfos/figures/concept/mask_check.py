import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def rows(path, keep=None):
    z = np.load(path, allow_pickle=False)
    indptr, indices, data = z["indptr"], z["indices"], z["data"]
    src = [str(s) for s in z["src"]]
    rxn = [str(r) for r in z["rxn"]]
    out = {}
    for i, s in enumerate(src):
        if keep is not None and s not in keep:
            continue
        a, b = indptr[i], indptr[i + 1]
        nb = [rxn[j] for j in indices[a:b]]
        d = data[a:b]
        o = np.argsort(d)[::-1]
        r = [nb[j] for j in o if (keep is None or nb[j] in keep) and nb[j] != s]
        out[s] = r
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--community", type=Path, required=True)
    ap.add_argument("--standalone", type=Path, required=True)
    ap.add_argument("--origin-table", type=Path, required=True)
    ap.add_argument("--origin", action="append", required=True,
                    help="the origins the standalone solve covers; repeat")
    ap.add_argument("--highlight", default=None,
                    help="report this origin's own rows separately")
    ap.add_argument("--k", type=int, default=30)
    args = ap.parse_args()

    d = pd.read_parquet(args.origin_table, columns=["mnxr", "origin"])
    keep = set(d.loc[d.origin.isin(args.origin), "mnxr"])
    hi = set(d.loc[d.origin == args.highlight, "mnxr"]) if args.highlight else set()

    A = rows(args.community, keep)
    B = rows(args.standalone, keep)
    shared = sorted(set(A) & set(B))
    print(f"community rows {len(A)}, standalone rows {len(B)}, shared {len(shared)}")

    def report(name, names):
        ov = []
        for r in names:
            a, b = set(A[r][:args.k]), set(B[r][:args.k])
            if not a and not b:
                continue
            ov.append(len(a & b) / max(len(a), len(b)))
        if not ov:
            print(f"  {name}: no rows")
            return
        ov = np.array(ov)
        print(f"  {name:<28} n={len(ov):<5} mean={ov.mean():.3f} median={np.median(ov):.3f} "
              f"p10={np.percentile(ov, 10):.3f} min={ov.min():.3f}")

    print(f"top-{args.k} neighbour overlap (1.0 = identical neighbourhood):")
    report("all shared reactions", shared)
    if hi:
        report(f"{args.highlight[:24]}... rows", [r for r in shared if r in hi])
        report("host rows", [r for r in shared if r not in hi])


if __name__ == "__main__":
    main()
