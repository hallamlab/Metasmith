#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))

import bake_pairs  # noqa: E402
import ecspr.model.conditions as econd  # noqa: E402
import ecspr.model.probes as ep  # noqa: E402

N2 = "MNXM51036"
NH4 = "MNXM729302"
GPR = REPO / "data/fabfos/nostoc/annotation/NOS/gpr_4lane.parquet"
CACHE = Path(__file__).resolve().parent / "eydallin" / "cache"

PRECURSORS = {
    "L-alanine": "MNXM1105732", "L-arginine": "MNXM739527",
    "L-asparagine": "MNXM1107821", "L-aspartate": "MNXM1364497",
    "L-cysteine": "MNXM738068", "L-glutamate": "MNXM741173",
    "L-glutamine": "MNXM37", "glycine": "MNXM29", "L-histidine": "MNXM1107769",
    "L-isoleucine": "MNXM728337", "L-leucine": "MNXM1106761",
    "L-lysine": "MNXM1364268", "L-methionine": "MNXM738804",
    "L-phenylalanine": "MNXM741664", "L-proline": "MNXM114",
    "L-serine": "MNXM737787", "L-threonine": "MNXM142",
    "L-tryptophan": "MNXM741553", "L-tyrosine": "MNXM76", "L-valine": "MNXM199",
    "ATP": "MNXM3", "GTP": "MNXM1103553", "CTP": "MNXM1103718", "UTP": "MNXM1101474",
    "dATP": "MNXM286", "dGTP": "MNXM344", "dCTP": "MNXM360", "dTTP": "MNXM394",
    "UDP-GlcNAc": "MNXM1104529", "sn-glycerol-3-P": "MNXM66",
    "biotin": "MNXM304", "thiamine": "MNXM730135", "chorismate": "MNXM337",
}
BY_ID = {v: k for k, v in PRECURSORS.items()}


def decoded(chunk: str):
    bake_pairs.BAKE = REPO / "data/fabfos/processed" / chunk
    bake_pairs.CACHE = CACHE / chunk
    bake_pairs.CACHE.mkdir(parents=True, exist_ok=True)
    return bake_pairs.atom_pairs(), bake_pairs.direction_ratios()


def nostoc_gpr() -> pd.DataFrame:
    g = pd.read_parquet(GPR)
    return g.assign(unit_id=g.orf)


def run(chunk: str) -> dict:
    ap, dr = decoded(chunk)
    basis = ep.Basis(ap, dr, element="N", orientation="as_written")
    present = set(basis.pairs.substrate) | set(basis.pairs["product"])
    sinks = tuple(m for m in PRECURSORS.values() if m in present)
    c = econd.Condition(condition_id=f"V3|{chunk}", element="N", source_hub=N2,
                        sinks=sinks)
    d = {r["readout"]: r["value"]
         for r in ep.measure(basis, nostoc_gpr(), c, probe="two-point")}

    fix = basis.pairs[basis.pairs.substrate.isin((N2, NH4))
                      & basis.pairs["product"].isin((N2, NH4))
                      & (basis.pairs.substrate != basis.pairs["product"])]
    in_gpr = sorted(set(fix.mnxr) & set(pd.read_parquet(GPR).mnxr.astype(str)))

    abstained = bool(d.get("_abstained"))
    print(f"\n=== {chunk}")
    print(f"  N2 a node of the network : {'no' if d.get('_missing_source') else 'yes'}")
    print(f"  N2<->NH4 reactions in GPR: {len(in_gpr)} {in_gpr}")
    print(f"  endpoints offered        : {len(sinks)}")
    for k in ("_n_reactions", "_n_reactions_used", "_n_aam_gap", "_converged",
              "_conservation_error", "_missing_sinks"):
        if k in d:
            print(f"  {k:<25}: {d[k]}")
    print(f"  effective conductance    : {'ABSTAINED' if abstained else d.get('total')}")
    if not abstained:
        draws = sorted(((v, BY_ID.get(k, k)) for k, v in d.items()
                        if not k.startswith("_") and k != "total" and v is not None),
                       reverse=True)
        print("  largest draws            : "
              + ", ".join(f"{n} {v:.4f}" for v, n in draws[:5]))
    return d


if __name__ == "__main__":
    for chunk in sys.argv[1:] or ["metabolism_bake"]:
        run(chunk)
