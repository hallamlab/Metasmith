"""The three populations the atom cap has never been measured against.

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_cap/build_sets.py \
        --lookups <built lookups dir> --worklist <adjudicated worklist parquet> \
        --out <dir>/sets.jsonl

WHY THIS EXISTS. `ATOM_LIMIT` was warranted by a yield curve joining reaction size to
the reactions the deployed bake banked -- 98-99% up to 600 atoms, 12.8% from 600 to 800,
1.3% above. That curve is CENSORED. Every mapper method in the deployed table stops dead
at the cap (`localmapper_only` max 600, `indigo_only` 598, `rxnmapper_only` 563,
`consensus` 581) because the same cut sat upstream of all three; the only thing banked
above it is `curated`, which comes from MetaCyc and never sees a mapper. So above the
threshold the curve measures the threshold. What a mapper would actually do up there is
unknown, and this is what asks.

THE THREE POPULATIONS:

  * `refused_whole`    -- the reactions the worklist refuses for size, as the COLLAPSED
                          string a mapper would be handed.
  * `refused_reduced`  -- the partial lane's element-reduced submissions that are still
                          over the atom cap. Same question, one element at a time.
  * `control_under_cap`-- reactions from 400 to 600 atoms that the deployed bake banked.
                          Without it a slow result up top has nothing to be slow against.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "src"))

from ecspr.bake import atom_pairs as AP                      # noqa: E402
from ecspr.bake.aam import partial as P, worklist as W       # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lookups", required=True, type=Path)
    ap.add_argument("--worklist", required=True, type=Path,
                    help="an adjudicated worklist, post-collapse")
    ap.add_argument("--banked", type=Path, default=None,
                    help="optional parquet with `mnxr` + `banked`, for the control set")
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)

    wl = pd.read_parquet(a.worklist)
    rx = pd.read_parquet(a.lookups / "reactions.parquet", columns=["mnxr", "equation"])
    eq = dict(zip(rx["mnxr"], rx["equation"]))
    formulas, smiles_of, ranks_of = P.load_lookups(a.lookups)
    refused = wl[wl["verdict"].isin(("oversize", "too_long"))]
    print(f"[cap] {len(wl):,} adjudicated, {len(refused):,} refused for size", flush=True)

    rows = []
    for r in refused.itertuples(index=False):
        smi = W.collapse(r.rxn_smiles) if isinstance(r.rxn_smiles, str) else None
        if not smi or len(smi) > W.SMILES_LEN_LIMIT:
            continue
        pe = AP.parse_equation(eq.get(r.mnxr, "")) or ([], [])
        rows.append(dict(pop="refused_whole", key=r.mnxr, mnxr=r.mnxr, element=None,
                         atoms=W.count_atoms(smi), chars=len(smi), smiles=smi,
                         sub=P._uniq(pe[0]), prod=P._uniq(pe[1])))

    for r in refused.itertuples(index=False):
        pe = AP.parse_equation(eq.get(r.mnxr, ""))
        if not pe:
            continue
        subs, prods = pe
        settled = {X for (X, _s, _p) in AP.forced_pairs(subs, prods, formulas, ranks_of)}
        for X in AP.ELEMENTS:
            if X in settled:
                continue
            red = AP.reduce_for_element(subs, prods, formulas, X)
            if red is None:
                continue
            ks, kp = red
            if any(m not in smiles_of for m in ks + kp):
                continue
            cks, ckp = P._uniq(ks), P._uniq(kp)
            use = (cks, ckp) if P._still_balances(cks, ckp, formulas, X) else (ks, kp)
            smi = P._write(use[0], use[1], smiles_of)
            if len(smi) > W.SMILES_LEN_LIMIT:
                continue
            n = W.count_atoms(smi)
            if n is None or n <= W.ATOM_LIMIT:
                continue
            rows.append(dict(pop="refused_reduced", key=P.make_key(r.mnxr, X),
                             mnxr=r.mnxr, element=X, atoms=n, chars=len(smi),
                             smiles=smi, sub=list(use[0]), prod=list(use[1])))

    if a.banked is not None:
        b = pd.read_parquet(a.banked, columns=["mnxr", "banked"])
        w = wl.set_index("mnxr")
        ctl = b[b["banked"]].merge(wl[["mnxr", "atoms"]], on="mnxr")
        ctl = ctl[(ctl["atoms"] >= 400) & (ctl["atoms"] <= W.ATOM_LIMIT)].head(60)
        for m in ctl["mnxr"]:
            smi = w.at[m, "rxn_smiles"]
            if not isinstance(smi, str):
                continue
            pe = AP.parse_equation(eq.get(m, "")) or ([], [])
            rows.append(dict(pop="control_under_cap", key=m, mnxr=m, element=None,
                             atoms=int(w.at[m, "atoms"]), chars=len(smi), smiles=smi,
                             sub=pe[0], prod=pe[1]))

    a.out.parent.mkdir(parents=True, exist_ok=True)
    with a.out.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(pd.DataFrame(rows).groupby("pop").agg(
        n=("key", "size"), median_atoms=("atoms", "median"),
        max_atoms=("atoms", "max")).to_string())
    print(f"\nwrote {a.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
