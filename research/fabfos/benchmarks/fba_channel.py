#!/usr/bin/env python3
"""A flux balance result, rendered as one more CHANNEL of the cross-arm cut.

    research/fabfos/benchmarks/fba_channel.py --arm eydallin_lof \
        --results research/fabfos/benchmarks/eydallin/fba/cache/results.parquet \
        --like <the conductance sweep TSV> --out <the fba channel TSV>

`ratio_cross_arm.py` reads a TSV per (arm, channel) and iterates whatever channel keys its
`SWEEPS` dict carries, so the flux balance arm enters beside `gem` and `denovo` with no
change to that file. What it needs is this shim.

THE RATIO IS THE SAME SHAPE IN BOTH ARMS, WHICH IS WHY THEY CAN SHARE A COLUMN. The
conductance arm reports C(source -> axis) / C(source -> denominator); this reports
flux(axis) / flux(denominator) over the same two metabolites, each against the same
baseline condition. `delta_ratio_pct` therefore means "percent change of a ratio" on both,
and a rank statistic over it is comparable across them.

WHAT DOES **NOT** CROSS, and must not be read across the boundary:

  * `MOVER_PCT = 1.0` is calibrated on conductance-ratio percent change. A flux ratio
    steps rather than drifts -- a deletion either takes out a reaction the pathway needs
    or does not -- so `movers` is a within-channel count here and nothing else.
  * `rxns` HAS NO FLUX BALANCE ANALOGUE. The conductance arm names the reactions a gene
    nominates in the atom-pair universe; this arm's perturbation is a change of bounds on
    a stoichiometric frame. `rxns` is left EMPTY so `rxn_all` and `rxn_movers` come back
    as no-reactions rather than as zero, which would read as "nothing carried it".
  * An ABSTENTION is not a zero. A deletion that takes growth to zero has no target flux
    to report, and `ratio_state` says `abstained` for it. The conductance arm has no such
    state -- a graph always solves -- so those rows are dropped from a shared count rather
    than entered as unmoved.

THE LABELS ARE JOINED FROM THE CONDUCTANCE SWEEP, not re-derived. One label set, joined on
`condition_id`, is what keeps a difference between the channels a difference in what was
measured rather than in who was counted.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

#: The denominator axis per arm, by the frame metabolite a gem result names in `target`.
#: It is the arm's own -- pyruvate for the glycogen ratio, CO2 for fuhrer -- and it is the
#: column that says whether one axis moved or the whole carbon budget did.
DENOMINATOR = {"eydallin_lof": "pyr_c", "fuhrer": "co2_c"}

#: Which numerator axes to emit. `None` means every target that is not the denominator,
#: one stratum each, which is what a multi-axis panel needs.
NUMERATOR = {"eydallin_lof": ("glycogen_c",), "fuhrer": None}

LABEL_COLS = ("gene", "b_number", "eydallin_gene", "eydallin_phenotype", "assayed",
              "essential", "is_positive", "screened", "model_gene", "direction", "z",
              "fuhrer_category", "fuhrer_n_differential_ions", "axis")


def fluxes(results: Path) -> pd.DataFrame:
    """`condition_id x target metabolite` -> the maximised flux, NaN where it abstained."""
    d = pd.read_parquet(results)
    if "target" not in d.columns:
        raise SystemExit(
            f"{results} carries no `target` column, so its axes cannot be told apart. It "
            f"was written before the gem key carried one -- re-run `ecspr fba`.")
    m = d[d.readout == "target"].copy()
    m["condition_id"] = m.test_id.str.split(".", n=1).str[1]
    return m.pivot_table(index="condition_id", columns="target", values="value")


def build(results: Path, like: pd.DataFrame, arm: str, baseline: str) -> pd.DataFrame:
    f = fluxes(results)
    den = DENOMINATOR[arm]
    if den not in f.columns:
        raise SystemExit(f"the denominator axis {den!r} is not in {sorted(f.columns)}")
    if baseline not in f.index:
        raise SystemExit(f"baseline condition {baseline!r} is not in the results")
    nums = NUMERATOR[arm] or tuple(c for c in f.columns if c != den)

    keep = [c for c in LABEL_COLS if c in like.columns]
    lk = like.drop_duplicates("condition_id").set_index("condition_id")
    labels = lk[keep]
    # AN ABSTENTION HAS TO BE A ROW. A deletion that takes growth to zero emits no target
    # flux at all, so it is ABSENT from the pivot rather than NaN in it -- and a channel
    # that is quietly six rows shorter than the one beside it is compared over a different
    # population without saying so. Reindexing over the conductance sweep's own conditions
    # brings them back as `abstained`.
    f = f.reindex(sorted(set(f.index) | set(lk.index) | {baseline}))
    out = []
    for num in nums:
        r = f[num] / f[den]
        base = float(r.loc[baseline])
        d = pd.DataFrame({
            "condition_id": r.index,
            "fba_num": f[num].to_numpy(), "fba_den": f[den].to_numpy(),
            "fba_ratio": r.to_numpy(),
            "delta_ratio_pct": 100.0 * (r.to_numpy() / base - 1.0),
            "host_num": float(f[num].loc[baseline]), "host_den": float(f[den].loc[baseline]),
            "host_ratio": base,
        })
        d = d[d.condition_id != baseline]
        d["ratio_state"] = np.where(d.fba_ratio.isna(), "abstained", "finite")
        d.loc[d.ratio_state == "abstained", "delta_ratio_pct"] = np.nan
        # `n_rxn` IS THE CONDUCTANCE ARM'S REACH, kept so the two channels count the same
        # population. This arm's own reach is `ratio_state`.
        d = d.join(labels, on="condition_id")
        if "n_rxn" in like.columns:
            d["n_rxn"] = d.condition_id.map(
                like.drop_duplicates("condition_id").set_index("condition_id").n_rxn)
        d["rxns"] = ""
        d["channel"] = "fba"
        d["fba_axis"] = num
        if NUMERATOR[arm] is None:
            d["axis"] = num
        out.append(d)
    return pd.concat(out, ignore_index=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--arm", required=True, choices=sorted(DENOMINATOR))
    ap.add_argument("--results", type=Path, required=True,
                    help="the `ecspr fba` results table")
    ap.add_argument("--like", type=Path, required=True,
                    help="the conductance sweep TSV whose labels and rows this mirrors")
    ap.add_argument("--baseline", required=True,
                    help="the condition_id every ratio is taken against")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    like = pd.read_csv(a.like, sep="\t")
    d = build(a.results, like, a.arm, a.baseline)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    d.to_csv(a.out, sep="\t", index=False)
    fin = d[d.ratio_state == "finite"]
    print(f"[fba-channel] {len(d):,} rows ({len(fin):,} finite, "
          f"{len(d) - len(fin):,} abstained) -> {a.out}")
    print(f"[fba-channel] host ratio {d.host_ratio.iloc[0]:.6g} "
          f"({d.host_num.iloc[0]:.6g} / {d.host_den.iloc[0]:.6g})")
    print(f"[fba-channel] responders {int((fin.delta_ratio_pct.abs() > 1e-12).sum()):,} | "
          f"|delta| >= 1% {int((fin.delta_ratio_pct.abs() >= 1).sum()):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
