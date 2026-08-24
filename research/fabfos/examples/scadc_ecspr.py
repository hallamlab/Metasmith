#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs
from ecspr.model.evidence import per_unit_weights
from ecspr.model.graph import Terminal, solve

ROOT = Path(__file__).resolve().parents[3]

REFS = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr" / "refs"
GPR = ROOT / "data" / "fabfos" / "runs" / "scadc_fosmids" / "gpr" / "gpr_4lane.parquet"
HOST_GEM = ROOT / "data" / "fabfos" / "runs" / "e_coli_epi300" / "gpr" / "gpr_gem.parquet"
CONDITIONS = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr" / "conditions.parquet"
OUT = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr" / "results.parquet"

ELEMENT = "C"
MEDIA = "glucose_minimal"


def clr(shares: np.ndarray) -> np.ndarray:
    eps = 1e-12
    x = np.log(shares + eps)
    return x - x.mean()


def main():
    pairs = load_pairs(REFS / "atom_pairs.parquet", element=ELEMENT)
    ratios = load_direction_ratios(REFS / "direction_ratios.parquet")
    conditions = pd.read_parquet(CONDITIONS)
    assert conditions.element.eq(ELEMENT).all()
    source_hub = conditions.source_hub.iloc[0]
    assert conditions.source_hub.nunique() == 1, "one glucose source hub, by construction"
    sink_hubs = conditions.sink_hub.tolist()
    sink_label = dict(zip(conditions.sink_hub, conditions.condition_id))

    host = pd.read_parquet(HOST_GEM)
    host_weights = {m: 1.0 for m in host.mnxr.dropna().unique()}

    gpr = pd.read_parquet(GPR)
    unit_weights = per_unit_weights(gpr, "contig")
    n_orfs_by_unit = gpr.assign(
        contig=gpr.orf.str.rsplit("_", n=1).str[0]
    ).groupby("contig")["orf"].nunique().to_dict()

    def build_and_solve(weights: dict):
        g = graph_from_pairs(pairs, ELEMENT, weights, ratios)
        src = Terminal.metabolite(g, source_hub, label="glucose")
        snk = Terminal.merge(g, sink_hubs, label="ground")
        sol = solve(g, src, snk)
        delivered = {m: sol.delivered(m) for m in sink_hubs}
        return sol.total, delivered

    print(f"[host] {len(host_weights)} reactions")
    host_total, host_delivered = build_and_solve(host_weights)
    host_sum = sum(host_delivered.values())
    host_share = np.array([host_delivered[m] / host_sum if host_sum > 0 else 0.0
                            for m in sink_hubs])
    host_clr = clr(host_share)
    print(f"[host] total={host_total:.6g} delivered={host_delivered}")

    rows = []

    def emit(unit: str, total: float, delivered: dict, n_orfs: int):
        s = sum(delivered.values())
        share = np.array([delivered[m] / s if s > 0 else 0.0 for m in sink_hubs])
        c = clr(share)
        delta_total = total - host_total
        for i, mnxm in enumerate(sink_hubs):
            cid = sink_label[mnxm]
            rows.append(dict(unit=unit, condition_id=cid, element=ELEMENT, media=MEDIA,
                              metabolite=mnxm, metric="delta_total",
                              direction="up", delta_obs=delta_total,
                              n_orfs=n_orfs, p=None, q=None, survives=None))
            rows.append(dict(unit=unit, condition_id=cid, element=ELEMENT, media=MEDIA,
                              metabolite=mnxm, metric="delta_clr",
                              direction="two_sided", delta_obs=float(c[i] - host_clr[i]),
                              n_orfs=n_orfs, p=None, q=None, survives=None))

    emit("epi300_host", host_total, host_delivered, int(gpr.orf.nunique()))

    units = sorted(unit_weights.keys())
    for i, contig in enumerate(units, 1):
        combined = dict(host_weights)
        for mnxr, e in unit_weights[contig].items():
            combined[mnxr] = combined.get(mnxr, 0.0) + e
        total, delivered = build_and_solve(combined)
        emit(f"epi300+{contig}", total, delivered, n_orfs_by_unit.get(contig, 0))
        print(f"[{i}/{len(units)}] {contig}: total={total:.6g} "
              f"delta_total={total - host_total:+.4g} delivered={delivered}")

    out = pd.DataFrame(rows)
    out.to_parquet(OUT, index=False)
    print(f"\nwrote {OUT} ({len(out)} rows, {out.unit.nunique()} units)")


if __name__ == "__main__":
    main()
