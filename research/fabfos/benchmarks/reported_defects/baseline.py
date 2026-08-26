"""Pin the bake's own numbers for every reaction the two defect reports named.

`fabfos/bench-fba` and `fabfos/bench-eydallin` each cite specific accessions. Their own
drivers rest on run data this worktree does not hold, so the reproducible part -- and the
only part a re-bake moves -- is what the reference itself says about those reactions. Run
before and after a re-bake and diff.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

RT = 8.314e-3 * 298.15

# bench-eydallin: the two balanced anchors, the four-accession synonym family, the two
# unbalanced accessions that inherit the offset, glgB, and the rows they asked us NOT to
# change plus the one they recorded as open.
DIRECTION = [
    ("MNXR132476", "anchor: maltotriose + ADPG -> maltotetraose + ADP"),
    ("MNXR145632", "anchor: alpha-maltopentaose + Pi (family member 1)"),
    ("MNXR145633", "family member 2"),
    ("MNXR145634", "family member 3"),
    ("MNXR145635", "family member 4"),
    ("MNXR145046", "glgA, unbalanced, inherits the anchor value"),
    ("MNXR145036", "glgP/malP, unbalanced, inherits the anchor value"),
    ("MNXR145021", "glgB: Glycogen -> Branching glycogen"),
    ("MNXR145050", "glgC -- asked NOT to change (tier 1)"),
    ("MNXR103113", "ppK -- asked NOT to change"),
    ("MNXR146501", "talA -- recorded open, at the rail"),
    ("MNXR101036", "transketolase neighbour, same rail"),
    ("MNXR157776", "the polymer aliasing artifact -- must stay declined"),
    ("MNXR145038", "tier 0 today, cost of the MNXM8348 exclusion"),
    ("MNXR145039", "tier 0 today, cost of the MNXM8348 exclusion"),
]

# bench-fba: the reported reaction, its sibling, and the curated twin the crosswalk filed
# the identical chemistry under.
ATOM = ["MNXR166070", "MNXR206779", "MNXR116249"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--chunk", required=True, help="a metabolism_bake chunk directory")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)

    chunk = Path(a.chunk)
    sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "src"))
    from ecspr.bake.encoding import Vocab

    v = Vocab(pd.read_parquet(chunk / "vocab.parquet"))
    sym = v.symbols("rxn")
    code = {str(s): i for i, s in enumerate(sym)}

    d = pd.read_parquet(chunk / "direction.parquet")
    by_code = dict(zip(d["rxn"], zip(d["ratio"], d["dir_tier"])))

    ann_p = chunk / "seams/direction_annotation.parquet"
    ann = pd.read_parquet(ann_p).set_index("mnxr") if ann_p.exists() else None

    rows = []
    for mnxr, note in DIRECTION:
        c = code.get(mnxr)
        ratio, tier = by_code.get(c, (np.nan, -1)) if c is not None else (np.nan, -1)
        dg = RT * np.log(ratio) if ratio == ratio and ratio > 0 else np.nan
        rec = dict(lane="direction", mnxr=mnxr, ratio=ratio, dG_prime=dg,
                   dir_tier=tier, note=note)
        if ann is not None and mnxr in ann.index:
            r = ann.loc[mnxr]
            rec.update(dir_method=str(r.get("dir_method")),
                       sigma=float(r.get("sigma", np.nan)),
                       clamped=bool(r.get("clamped", False)))
        rows.append(rec)

    ap_p = chunk / "atom_pairs.parquet"
    pairs = pd.read_parquet(ap_p)
    met = v.symbols("met")
    meth = v.symbols("method") if "method" in v else None
    for mnxr in ATOM:
        c = code.get(mnxr)
        g = pairs[pairs["rxn"] == c] if c is not None else pairs.iloc[:0]
        for (el, tm, tr, hm), sub in g.groupby(
                ["element", "tail_met", "tail_rank", "head_met"], observed=True):
            rows.append(dict(
                lane="atom", mnxr=mnxr, element=str(v.symbols("element")[el]),
                tail_met=str(met[tm]), tail_rank=int(tr), head_met=str(met[hm]),
                n_rows=len(sub), pair_w=float(sub["pair_w"].sum()),
                note="atom correspondence"))

    out = pd.DataFrame(rows)
    out.to_csv(a.out, sep="\t", index=False)
    print(out.to_string(index=False))
    print(f"\nwrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
