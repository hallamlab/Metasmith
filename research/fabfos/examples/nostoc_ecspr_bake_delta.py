#!/usr/bin/env python3
"""Compare two generations of the community measurement, bake against bake.

    python research/fabfos/examples/nostoc_ecspr_bake_delta.py OLD_RESULTS NEW_RESULTS

Both arguments are directories laid out as `<network>/ecspr-ground_results/*.parquet`.
`OLD_RESULTS` is the pinned chunk's `results/`; `NEW_RESULTS` is what
`nostoc_ecspr_measure.py --out` published.

The GPR tables and the conditions are IDENTICAL across the two generations by
construction -- only the bake moved -- so every difference reported here is attributable
to the atom-pair basis. That is what makes it worth being able to re-run.

TWO SCHEMAS, ONE MEASUREMENT
----------------------------
The pinned products are the WIDE shape: one row per (condition, metabolite) with `draw`,
`role`, `throughput`, `n_nodes`, `n_reactions_used`, `n_aam_gap` and the rest as columns.
`ecspr.model.probes` now emits the LONG shape -- `(condition_id, probe, orientation,
element, readout, value)` -- where a readout is a sink metabolite, `total`, or an
underscore-prefixed diagnostic (`_n_nodes`, `_n_reactions_used`, `_leak_frac`,
`_abstained`, ...). This module normalises both onto the long shape and compares only
what BOTH carry: `total`, the per-precursor draws, and the structural diagnostics.

What the long shape does not carry is the interior draw vector -- the old products'
8,401 `role=interior` rows per carbon condition -- because `--readouts sinks` is the
default. Nothing here needs it; `nostoc_ecspr_set4.py` does, and would need
`--readouts all` to be re-measured against.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

DIAG = {"n_nodes": "_n_nodes", "n_edges": "_n_edges",
        "n_reactions_used": "_n_reactions_used", "n_aam_gap": "_n_aam_gap",
        "leak_frac": "_leak_frac", "total": "total"}


def _from_wide(df: pd.DataFrame) -> pd.DataFrame:
    rows = [df[df.role.isin(("precursor", "source"))]
            .assign(readout=lambda d: d.metabolite, value=lambda d: d.draw)
            [["condition_id", "element", "readout", "value"]]]
    per_cond = df.groupby(["condition_id", "element"]).first().reset_index()
    for wide, long in DIAG.items():
        if wide in per_cond.columns:
            rows.append(per_cond.assign(readout=long, value=per_cond[wide])
                        [["condition_id", "element", "readout", "value"]])
    return pd.concat(rows, ignore_index=True)


def load(root: Path, probe="ground") -> pd.DataFrame:
    frames, seen = [], {}
    for d in sorted(p for p in root.iterdir() if p.is_dir()):
        for f in sorted((d / f"ecspr-{probe}_results").glob("*.parquet")):
            df = pd.read_parquet(f)
            if "role" in df.columns:
                df = _from_wide(df)
            df = df[["condition_id", "element", "readout", "value"]]
            # A unit measured on two hosts writes two files and one measurement; the
            # pinned chunk carries both for the networks that were. They agree to 4e-16,
            # so keep the first and assert rather than averaging or picking by name.
            key = (d.name, tuple(sorted(df.condition_id.unique())))
            if key in seen:
                assert len(df) == seen[key], f"{d.name}: two runs of {key[1]} differ"
                continue
            seen[key] = len(df)
            frames.append(df.assign(network=d.name))
    if not frames:
        raise SystemExit(f"no {probe} products under {root}")
    return pd.concat(frames, ignore_index=True)


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2:
        raise SystemExit(__doc__)
    old, new = (load(Path(p)) for p in argv)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 40)
    pd.set_option("display.max_rows", 200)

    for tag, df in (("old", old), ("new", new)):
        print(f"{tag}: {len(df):,} rows, {df.network.nunique()} networks, "
              f"{df.condition_id.nunique()} conditions")

    key = ["network", "condition_id", "element", "readout"]
    m = old.merge(new, on=key, suffixes=("_old", "_new"), how="outer", indicator=True)
    print(f"\nrow correspondence: {m._merge.value_counts().to_dict()}")
    both = m[m._merge == "both"]

    print("\n=== structure, per network (element C) ===")
    st = both[(both.element == "C")
              & both.readout.isin(["_n_nodes", "_n_reactions_used", "_n_aam_gap"])]
    piv = st.pivot_table(index="network", columns="readout",
                         values=["value_old", "value_new"])
    out = pd.DataFrame(index=piv.index)
    for r, label in (("_n_nodes", "nodes"), ("_n_reactions_used", "rxn_used"),
                     ("_n_aam_gap", "aam_gap")):
        o, n = piv[("value_old", r)], piv[("value_new", r)]
        out[f"{label}_old"], out[f"{label}_new"] = o.astype(int), n.astype(int)
        out[f"d_{label}"] = (n - o).astype(int)
    print(out.to_string())

    print("\n=== total injected current, per condition ===")
    t = both[both.readout == "total"].copy()
    t["d_%"] = (100 * (t.value_new - t.value_old) / t.value_old).round(2)
    print(t.sort_values(["element", "network"])
          [["network", "condition_id", "element", "value_old", "value_new", "d_%"]]
          .round(6).to_string(index=False))

    print("\n=== per-precursor draw: how far the measurement moved ===")
    d = both[~both.readout.str.startswith("_") & (both.readout != "total")]
    rows = []
    for el, g in d.groupby("element"):
        a, b = g.value_old.to_numpy(float), g.value_new.to_numpy(float)
        fin = np.isfinite(a) & np.isfinite(b)
        a, b = a[fin], b[fin]
        nz = (a > 0) | (b > 0)
        rows.append(dict(
            element=el, rows=len(a),
            pearson=round(float(np.corrcoef(a, b)[0, 1]), 6) if len(a) > 2 else np.nan,
            median_rel_change=round(float(np.median(
                (b[nz] - a[nz]) / np.maximum(np.abs(a[nz]), 1e-30))), 4) if nz.any() else np.nan,
            max_abs_rel=round(float(np.max(
                np.abs(b[nz] - a[nz]) / np.maximum(np.abs(a[nz]), 1e-30))), 3) if nz.any() else np.nan,
            newly_nonzero=int(((a == 0) & (b > 0)).sum()),
            newly_zero=int(((a > 0) & (b == 0)).sum()),
        ))
    print(pd.DataFrame(rows).to_string(index=False))

    ab = both[both.readout == "_abstained"]
    if len(ab):
        changed = ab[ab.value_old != ab.value_new]
        print(f"\n=== abstentions: {int(ab.value_old.sum())} -> {int(ab.value_new.sum())}"
              f" of {len(ab)} conditions ===")
        if len(changed):
            print(changed[["network", "condition_id", "element",
                           "value_old", "value_new"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
