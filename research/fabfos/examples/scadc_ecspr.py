#!/usr/bin/env python3
"""First real ECSPr measurement for SCADC (plan T2/T3): epi300 host baseline vs
epi300 + each recovered fosmid insert, glucose -> {acetyl-CoA, malonyl-CoA,
oxaloacetate, AMP} under the universal leakage ground.

    python examples/scadc_ecspr.py

Writes `data/fabfos/scadc_ecspr/results.parquet` (`ecspr::results` schema, p/q/survives
left null -- no significance test this pass) directly, via
plain pandas/numpy/scipy over the `ecspr` package. Does NOT go through the
metasmith `TransformInstance`/container machinery -- it predates
`ecspr_measure.py` having a protocol at all, and is kept as the worked record of
the call sequence the CLI now offers as `ecspr ground`.

NOT the deployed reference basis: atom_pairs is tier4 (validated, but not the
canonical `.awm/data/ref/derived/mnxref-4_5/` release, unmaterialized here),
direction_ratios' dG_prime/sigma/dir_confidence are null (the bake only carries
ratio + dir_tier). Both substitutions are made in this file, above.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from ecspr.build import load_pairs, load_direction_ratios, graph_from_pairs
from ecspr.evidence import per_unit_weights
from ecspr.graph import Terminal, solve

ROOT = Path(__file__).resolve().parent.parent

REFS = ROOT / "data" / "fabfos" / "scadc_ecspr" / "refs"
GPR = ROOT / "data" / "fabfos" / "scadc_fosmids" / "gpr" / "gpr_4lane.parquet"
HOST_GEM = ROOT / "data" / "benchmarks" / "hosts" / "e_coli_epi300" / "gpr_gem.parquet"
CONDITIONS = ROOT / "data" / "fabfos" / "scadc_ecspr" / "conditions.parquet"
OUT = ROOT / "data" / "fabfos" / "scadc_ecspr" / "results.parquet"

ELEMENT = "C"
MEDIA = "glucose_minimal"


def clr(shares: np.ndarray) -> np.ndarray:
    """Centred log-ratio, zero-padded (delivered currents can be exactly zero for an
    unreached sink -- a pseudocount avoids -inf without hiding the zero elsewhere)."""
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
