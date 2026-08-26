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
#:
#: **CAUTION** FUHRER'S DENOMINATOR IS AT ITS BOUND. Maximum flux to CO2 on the unperturbed
#: host is 1000, the exchange ceiling, and it stays there under every deletion the frame can
#: absorb. The normalisation is therefore a constant on that arm and its ratio is a rescaled
#: numerator, which is NOT true of the conductance arm's CO2 denominator. Read the two arms'
#: fuhrer ratios as ranks and never as the same quantity.
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


def build(results: Path, like: pd.DataFrame, arm: str, baseline: str,
          axis_of: dict = None) -> pd.DataFrame:
    axis_of = axis_of or {}
    f = fluxes(results)
    den = DENOMINATOR[arm]
    if den not in f.columns:
        raise SystemExit(f"the denominator axis {den!r} is not in {sorted(f.columns)}")
    if baseline not in f.index:
        raise SystemExit(f"baseline condition {baseline!r} is not in the results")
    nums = NUMERATOR[arm] or tuple(c for c in f.columns if c != den)

    keep = [c for c in LABEL_COLS if c in like.columns]
    # THE LABEL IS PER (CONDITION, AXIS) WHERE THE SCREEN HAS AXES. Fuhrer labels a mutant
    # positive on one ion and not on another, so collapsing the sweep to one row per
    # condition takes whichever axis sorted first and stamps its `is_positive`,
    # `direction` and `z` onto all sixteen. Every flux row then carried one axis's labels
    # and the cut reported the same ten "metabolically active" mutants on every axis.
    per_axis = "axis" in like.columns and NUMERATOR[arm] is None
    lk = like.drop_duplicates("condition_id").set_index("condition_id")
    if per_axis:
        by_axis = {ax: g.drop_duplicates("condition_id").set_index("condition_id")
                   for ax, g in like.groupby("axis")}
    labels = lk[keep]
    # AN ABSTENTION HAS TO BE A ROW. A deletion that takes growth to zero emits no target
    # flux at all, so it is ABSENT from the pivot rather than NaN in it -- and a channel
    # that is quietly six rows shorter than the one beside it is compared over a different
    # population without saying so. Reindexing over the conductance sweep's own conditions
    # brings them back as `abstained`.
    ids = {str(x) for x in set(f.index) | set(lk.index) | {baseline}
           if isinstance(x, str) or x == x}
    f = f.reindex(sorted(ids))
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
        # AN UNREACHABLE AXIS IS NOT A FLAT ONE. Where the unperturbed host carries no flux
        # to the target at all -- oxalate and butanal are not metabolites of iML1515 -- the
        # ratio is 0/0 and every delta is NaN. That is the frame refusing the question, and
        # it is the state fuhrer's two highest published AUCs are in, so it gets its own
        # name rather than being counted among the mutants that did not move.
        d["ratio_state"] = np.where(d.fba_ratio.isna(), "abstained", "finite")
        if not (base > 0):
            d["ratio_state"] = "unreachable"
        d.loc[d.ratio_state != "finite", "delta_ratio_pct"] = np.nan
        # `n_rxn` IS THE CONDUCTANCE ARM'S REACH, kept so the two channels count the same
        # population. This arm's own reach is `ratio_state`.
        src = by_axis.get(axis_of.get(num, num), lk) if per_axis else lk
        d = d.join(src[[c for c in keep if c in src.columns]], on="condition_id")
        if "n_rxn" in src.columns:
            d["n_rxn"] = d.condition_id.map(src.n_rxn)
        d["rxns"] = ""
        d["channel"] = "fba"
        d["fba_axis"] = num
        if NUMERATOR[arm] is None:
            # THE AXIS NAME HAS TO BE THE SCREEN'S, not the frame's. `ratio_cross_arm`
            # strata are the declared axes (`c00209_neg870`), while a gem result names the
            # frame metabolite the flux was taken to (`oxa_c`). Grouping on the second
            # gives one stratum per metabolite that joins to no label at all.
            d["axis"] = axis_of.get(num, num)
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
    ap.add_argument("--axes", type=Path, default=None,
                    help="the arm's `probe_axes.tsv`, which maps the frame metabolite a "
                         "gem result names back to the axis the screen declared")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    like = pd.read_csv(a.like, sep="\t")
    axis_of = {}
    if a.axes:
        ax = pd.read_csv(a.axes, sep="\t")
        axis_of = dict(zip(ax.metabolite.astype(str), ax.axis.astype(str)))
    d = build(a.results, like, a.arm, a.baseline, axis_of)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    d.to_csv(a.out, sep="\t", index=False)
    fin = d[d.ratio_state == "finite"]
    print(f"[fba-channel] {len(d):,} rows -> {a.out}")
    for st, n in d.ratio_state.value_counts().items():
        print(f"[fba-channel]   {st}: {n:,}")
    print(f"[fba-channel] host ratio {d.host_ratio.iloc[0]:.6g} "
          f"({d.host_num.iloc[0]:.6g} / {d.host_den.iloc[0]:.6g})")
    print(f"[fba-channel] responders {int((fin.delta_ratio_pct.abs() > 1e-12).sum()):,} | "
          f"|delta| >= 1% {int((fin.delta_ratio_pct.abs() >= 1).sum()):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
