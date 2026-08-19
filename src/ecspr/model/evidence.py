"""ECSPr evidence weights: belief conservation, then log-odds pooling.

Turns an annotation evidence table (one row per ORF/channel/nominated MNXR, as
`annotation::gpr_table` produces) into per-reaction conductances `E_r`, and into
per-unit (fosmid contig / metaG ORF) addition maps for the same allocation.

TWO STAGES, AND WHY THE SECOND ONE EXISTS
-----------------------------------------
`nomination_contributions` is the DILUTION: each ORF's total nomination is exactly 1.0,
spread across the reactions it nominates, so a promiscuous annotation cannot out-vote a
specific one and leave-one-out stays an exact subtraction.

That dilution is one-sided. A gene is diluted across the reactions it nominates, but
nothing dilutes a reaction across the genes nominating it, so summing the mass makes a
paralog family read as strength of evidence: on the K-12 de novo table MNXR172198 came
top-6 at E = 12.3 purely because 65 ORFs assert the same single EC number through the
same single channel, out-ranking reactions confirmed independently by three different
methods. `pool_logodds` is the fix -- independent assertions add in log-odds, repeated
ones saturate, and the result is a probability rather than an unbounded sum.

The unit is inside the assertion key deliberately. The GPR module's whole account of an
overexpression is that the host's row and the clone's row are both selected and their
conductances sum; collapsing the two units into one assertion would make every
overexpression arm a no-op. A physically separate copy is separate evidence; a paralog
within one copy is not.

`belief_mass` keeps the pre-pooling sum available, because that is the quantity per-ORF
conservation is a statement about and the one a gene-count ledger has to read.

Env: numpy + pandas (CPU).
"""
from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd

# Pooling constants -- see `research/fabfos/benchmarks/pooling/rank_eval.py --sweep`.
# `lam1` is what one fully-corroborating assertion is worth in log-odds, `tau` the belief
# mass at which an assertion is ~63% of the way to that ceiling, `lam0` the prior for a
# reaction that has any evidence at all. Overrides are keyword arguments, not CLI flags:
# the sweep calls these functions directly and nobody turns these per run.
#
# Only `tau` moves the RANKING -- `lam0` shifts and `lam1` scales the pooled log-odds
# uniformly, so both are monotone in it. They are a choice of dynamic range, and the
# range they buy is what the sweep cannot pick for you:
#
#   lam1 = 1.0   one assertion is one log-odds unit, a factor of e in the odds. Raising
#                it buys separation at the top and costs it at the bottom: sigma reaches
#                exactly 1.0 in float64 once the pooled log-odds pass ~37, so the most
#                corroborated reactions start tying. At 1.0 the K-12 lane tops out at
#                1 - 1.4e-13 and the four-channel ag1 lane ties 4 reactions of 14,180 --
#                40 independent assertions IS certainty, and the tie is honest.
#   lam0 = -3.0  prior odds of e^-3, so a nominated reaction with negligible belief mass
#                sits at 0.047 rather than at zero, and E spans ~21x across K-12.
#   tau  = 0.25  0.1 ranks a hair better (AUROC 0.773 vs 0.771) but is a hard count of
#                distinct assertions wearing a sigmoid -- at 0.25 an ORF split four ways
#                still carries most of an assertion and the dilution stage still matters.
POOL_LAM0 = -3.0
POOL_LAM1 = 1.0
POOL_TAU = 0.25

#: One independent assertion. `unit_id` is absent from composed/synthetic tables,
#: which then pool as a single unit -- correct, since they describe one genome.
ASSERTION_KEY = ("unit_id", "channel", "intermediate_id")


def nomination_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Per-row contribution = (w_n / F_n) / L_orf, where the nomination unit is
    (orf, channel, intermediate_id), w_n splits raw_score within (orf, channel),
    F_n = distinct-mnxr fanout, and L_orf = distinct channels for the ORF. Each
    ORF's contributions sum to 1.0 (belief conservation), making leave-one-out an
    exact subtraction downstream."""
    d = df.drop_duplicates(["orf", "channel", "intermediate_id", "mnxr"]).copy()
    nom = (d.groupby(["orf", "channel", "intermediate_id"], sort=False)
           .agg(s_n=("raw_score", "max"), F_n=("mnxr", "nunique"))
           .reset_index())
    grp = nom.groupby(["orf", "channel"], sort=False)
    nom["sum_s"] = grp["s_n"].transform("sum")
    nom["n_nom"] = grp["intermediate_id"].transform("count")
    nonzero = nom["sum_s"] > 0
    nom["w_n"] = np.where(nonzero, nom["s_n"] / nom["sum_s"], 1.0 / nom["n_nom"])
    nom["contrib"] = nom["w_n"] / nom["F_n"]
    out = d.merge(nom[["orf", "channel", "intermediate_id", "contrib"]],
                  on=["orf", "channel", "intermediate_id"], how="left")
    n_lanes = out.groupby("orf")["channel"].transform("nunique")
    out["contrib"] = out["contrib"] / n_lanes
    return out


def _assert_conservation(rows: pd.DataFrame, label: str) -> None:
    per_orf = rows.groupby("orf")["contrib"].sum()
    worst = float((per_orf - 1.0).abs().max()) if len(per_orf) else 0.0
    assert worst < 1e-9, f"{label}: per-orf conservation off by {worst:.2e}"


def pool_logodds(rows: pd.DataFrame, *, lam0: float = POOL_LAM0,
                 lam1: float = POOL_LAM1, tau: float = POOL_TAU) -> pd.Series:
    """`{mnxr: E}` -- a probability, pooling `nomination_contributions`' output over
    assertions. Bounded to (0, 1), with the float64 caveat noted at `POOL_LAM1`.

    For each assertion `i` in `ASSERTION_KEY`, `s[r,i]` is the belief mass it puts on
    `r`; it buys `lam1 * (1 - exp(-s/tau))` log-odds, so N genes repeating one assertion
    approach `lam1` instead of summing to N. Reactions add up over DISTINCT assertions,
    which is where cross-method agreement earns more than one method repeated.

    `lam0` is added only to reactions that have at least one row here -- the returned index
    is exactly the nominated set. Applied to all of MNXref it would give every unnominated
    reaction a positive conductance and pull the whole universe into the network. Within
    the nominated set every `E` is strictly positive, so a nomination that carries
    negligible belief mass sits at the prior rather than dropping out of the graph.
    """
    key = [c for c in ASSERTION_KEY if c in rows.columns] + ["mnxr"]
    s = rows.groupby(key, sort=False)["contrib"].sum()
    lam = pd.Series(lam1 * -np.expm1(-s.to_numpy() / tau),
                    index=s.index.get_level_values("mnxr"))
    return 1.0 / (1.0 + np.exp(-(lam.groupby(level=0).sum() + lam0)))


def pooled_E_of_mass(mass: float, *, lam0: float = POOL_LAM0, lam1: float = POOL_LAM1,
                     tau: float = POOL_TAU) -> float:
    """`E` for a reaction carried by exactly ONE assertion holding `mass` belief.

    `compose` needs this: every bridge reaction is one synthetic ORF spending its whole
    1.0 on one pseudo-reaction, so all bridges land on `pooled_E_of_mass(1.0)` and the
    conductance they were built to carry has to be divided back out of `pair_w`.
    """
    lam = lam1 * -np.expm1(-mass / tau)
    return float(1.0 / (1.0 + np.exp(-(lam0 + lam))))


def belief_mass(df_src: pd.DataFrame, label: str = "") -> pd.Series:
    """`{mnxr: SUM_g e_g(r)}` -- the PRE-pooling ledger, in units of ORFs.

    This, not `compute_E`, is what per-ORF conservation is a statement about, and what a
    diluted gene count has to read. It sums to the number of ORFs in `df_src`.
    """
    rows = nomination_contributions(df_src)
    _assert_conservation(rows, label or "E")
    return rows.groupby("mnxr")["contrib"].sum()


def compute_E(df_src: pd.DataFrame, label: str = "", **pool) -> pd.Series:
    """`{mnxr: E}` -- diluted, then pooled. The conductances `graph_from_pairs` reads."""
    rows = nomination_contributions(df_src)
    _assert_conservation(rows, label or "E")
    return pool_logodds(rows, **pool)


def compute_weights(ev: pd.DataFrame) -> pd.DataFrame:
    """evidence_weights.parquet: per (source, mnxr) E_full/E_dlec/belief_mass/n_orf.

    `E_full` is the pooled conductance and is bounded by 1; `belief_mass` is the raw
    per-ORF allocation behind it, which is what stays exactly additive under
    leave-one-out and what the conservation ledger asserts on.
    """
    parts = []
    for source, g in ev.groupby("source"):
        rows = nomination_contributions(g)
        _assert_conservation(rows, f"{source}/full")
        e_full = pool_logodds(rows)
        mass = rows.groupby("mnxr")["contrib"].sum()
        dlec = g[g["channel"] == "dl_ec"]
        e_dlec = compute_E(dlec, f"{source}/dlec") if len(dlec) else pd.Series(dtype=float)
        n_orf = g.groupby("mnxr")["orf"].nunique()
        part = (pd.DataFrame({"E_full": e_full})
                .join(mass.rename("belief_mass"), how="outer")
                .join(e_dlec.rename("E_dlec"), how="outer")
                .join(n_orf.rename("n_orf"), how="left"))
        for c in ("E_full", "E_dlec", "belief_mass"):
            part[c] = part[c].fillna(0.0)
        part["n_orf"] = part["n_orf"].fillna(0).astype(int)
        part.insert(0, "source", source)
        part = part.reset_index().rename(columns={"index": "mnxr"})
        parts.append(part)
    out = pd.concat(parts, ignore_index=True)
    return out[["source", "mnxr", "E_full", "E_dlec", "belief_mass", "n_orf"]]


def per_unit_weights(df_src: pd.DataFrame, unit_col: str) -> dict:
    """{unit: {mnxr: E}} with per-ORF-normalized belief, re-aggregated by
    'contig' (per fosmid) or 'orf' (per metaG ORF).

    This is the PRE-pooling ledger (`belief_mass` at unit grain), not a conductance:
    the unit is part of the assertion key, so summing these maps over units is not
    `compute_E`. Null-draw research wants exactly this additive form.
    """
    rows = nomination_contributions(df_src)
    if unit_col == "contig":
        rows = rows.copy()
        rows["contig"] = rows["orf"].str.rsplit("_", n=1).str[0]
    agg = rows.groupby([unit_col, "mnxr"], sort=False)["contrib"].sum()
    out: dict = defaultdict(dict)
    for (unit, mnxr), c in agg.items():
        if c > 0:
            out[unit][mnxr] = float(c)
    return dict(out)
