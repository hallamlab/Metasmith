# ECSPr evidence weights: belief conservation, then log-odds pooling.
#
# Turns an annotation evidence table (one row per ORF/channel/nominated MNXR, as
# `annotation::gpr_table` produces) into per-reaction conductances `E_r`, and into
# per-unit (fosmid contig / metaG ORF) addition maps for the same allocation.
#
# TWO STAGES, AND WHY THE SECOND ONE EXISTS
# -----------------------------------------
# `nomination_contributions` is the DILUTION: each ORF's total nomination is exactly 1.0,
# spread across the reactions it nominates, so a promiscuous annotation cannot out-vote a
# specific one and leave-one-out stays an exact subtraction.
#
# That dilution is one-sided. A gene is diluted across the reactions it nominates, but
# nothing dilutes a reaction across the genes nominating it, so summing the mass makes a
# paralog family read as strength of evidence: on the K-12 de novo table MNXR172198 came
# top-6 at E = 12.3 purely because 65 ORFs assert the same single EC number through the
# same single channel, out-ranking reactions confirmed independently by three different
# methods. `pool_logodds` is the fix -- independent assertions add in log-odds, repeated
# ones saturate, and the result is a probability rather than an unbounded sum.
#
# The unit is inside the assertion key deliberately. The GPR module's whole account of an
# overexpression is that the host's row and the clone's row are both selected and their
# conductances sum; collapsing the two units into one assertion would make every
# overexpression arm a no-op. A physically separate copy is separate evidence; a paralog
# within one copy is not.
#
# `belief_mass` keeps the pre-pooling sum available, because that is the quantity per-ORF
# conservation is a statement about and the one a gene-count ledger has to read.
#
# Env: numpy + pandas (CPU).
from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd

# Pooling constants -- see `research/fabfos/benchmarks/pooling/rank_eval.py --sweep`.
# `tau` is the belief mass at which an assertion is ~63% of the way to its ceiling, and
# `lam0` the prior for a reaction that has any evidence at all. Overrides are keyword
# arguments, not CLI flags: the sweep calls these functions directly and nobody turns
# these per run.
#
# WHAT ONE ASSERTION IS WORTH IS NO LONGER A CONSTANT. It was `POOL_LAM1 = 1.0`, one
# log-odds unit for every assertion from every lane, and the comment here admitted the
# sweep could not choose it -- because a sweep over a number that scales the pooled
# log-odds uniformly can only ever report a monotone transform of one ranking. The
# number is now the row's own `llr`, measured per lane against the DH10B Swiss-Prot
# cohort. `ref::lane_calibration` holds the fit; the mapper writes the column.
#
#   lam0 = -3.0  prior odds of e^-3, so a nominated reaction with negligible belief mass
#                sits at 0.047 rather than at zero.
#
#                **CAUTION** THIS NUMBER IS SHARED, and the sharing is load-bearing.
#                `fabfos_evidence.POOL_LAM0` carries the same value, and `llr` is fitted
#                as `logit(P(correct | score)) - POOL_LAM0`. That makes
#                `sigmoid(llr + lam0)` come out at exactly the calibrated accuracy of a
#                reaction carrying ONE assertion. Change it here alone and every
#                single-assertion reaction lands at a number that is nobody's estimate
#                of anything. `fabfos_evidence.load_calibration` refuses a table whose
#                fit used a different reference; this side has no such check because
#                ecspr does not stage the artifact, so the two are kept in step by this
#                comment and by that refusal.
#   tau  = 0.5   RE-SWEPT after calibration, and it moved from 0.25. Calibrated
#                assertions are worth 3 to 9 nats where the retired constant was worth
#                1, so the pooled log-odds inflate ~7x and the float64 tie point at ~37
#                arrives after five corroborating assertions rather than forty. On K-12
#                that took the reactions tied at exactly E = 1.0 from 2 of 11,203 to 51.
#                A tie at the ceiling is a real degeneracy -- two reactions with the
#                same conductance -- and it lands on the most-corroborated reactions,
#                which is where a ranking is read.
#
#                The sweep, on K-12 de novo against iML1515:
#
#                  tau    AUROC  P@100  >1-chan@100  1-assert@100  tied
#                  0.1    0.759   0.63      98%           0%        137
#                  0.25   0.753   0.62      96%           0%         51
#                  0.5    0.750   0.61      94%           0%         18
#                  1.0    0.747   0.58      92%           0%          8
#                  2.0    0.746   0.52      86%           3%          5
#                  legacy 0.741   0.59      94%           0%          2
#
#                AUROC is confounded here and the module header says why. The
#                unconfounded statistic is `1-assert@100`, and it is 0% everywhere below
#                tau = 2, so it does not choose. What is left is the tie count against
#                the corroboration statistics, and 0.5 is where they meet: it matches
#                the legacy's 94% multichannel top-100, still beats it on AUROC and
#                P@500, and cuts ties by two thirds against holding tau at 0.25.
#
#                **CAUTION** The tie count going up 25x is not a tau problem, it is an
#                INDEPENDENCE problem that calibration made visible. The pool adds
#                assertions as if they were independent, and the retired lam1 = 1.0 was
#                silently discounting for the fact that they are not. Five calibrated
#                calls now reach certainty because the model says five independent
#                99%-accurate calls are certainty -- and lanes trained on overlapping
#                databases are not independent. Raising tau hides this; it does not fix
#                it. The fix is a correlation term the assertion key cannot express.
POOL_LAM0 = -3.0
POOL_TAU = 0.5

#: One independent assertion. `unit_id` is absent from composed/synthetic tables,
#: which then pool as a single unit -- correct, since they describe one genome.
ASSERTION_KEY = ("unit_id", "channel", "intermediate_id")


def nomination_contributions(df: pd.DataFrame) -> pd.DataFrame:
    # Per-row contribution = (1/n_nom / F_n) / L_orf, where the nomination unit is
    # (orf, channel, intermediate_id), n_nom = nominations the lane made for the ORF,
    # F_n = distinct-mnxr fanout, and L_orf = distinct channels for the ORF. Each
    # ORF's contributions sum to 1.0 (belief conservation), making leave-one-out an
    # exact subtraction downstream.
    #
    # THE SPLIT IS UNIFORM, and used to be share-of-score. The score's discriminative
    # job moved to `llr`, which `pool_logodds` reads, and leaving it here as well would
    # count the same evidence twice -- once to decide how much of the ORF's belief this
    # nomination gets, and again to decide what the resulting assertion is worth.
    #
    # It also makes the conservation guarantee STRUCTURAL rather than score-dependent.
    # The old form summed to 1.0 only because `w_n` was a normalised simplex, which
    # held for any non-negative score and silently fell back to uniform when a lane's
    # scores summed to zero. A uniform split sums to one by construction, so
    # `_assert_conservation` now checks arithmetic rather than an input contract.
    d = df.drop_duplicates(["orf", "channel", "intermediate_id", "mnxr"]).copy()
    nom = (d.groupby(["orf", "channel", "intermediate_id"], sort=False)
           .agg(F_n=("mnxr", "nunique"))
           .reset_index())
    grp = nom.groupby(["orf", "channel"], sort=False)
    nom["n_nom"] = grp["intermediate_id"].transform("count")
    nom["contrib"] = 1.0 / (nom["n_nom"] * nom["F_n"])
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
                 tau: float = POOL_TAU) -> pd.Series:
    # `{mnxr: E}` -- a probability, pooling `nomination_contributions`' output over
    # assertions. Bounded to (0, 1); float64 saturates the sigmoid at exactly 1.0 once
    # the pooled log-odds pass ~37, so the most corroborated reactions can tie.
    #
    # For each assertion `i` in `ASSERTION_KEY`, `s[r,i]` is the belief mass it puts on
    # `r`; it buys `llr_i * (1 - exp(-s/tau))` log-odds, so N genes repeating one
    # assertion approach `llr_i` instead of summing to N. Reactions add up over DISTINCT
    # assertions, which is where cross-method agreement earns more than one method
    # repeated.
    #
    # `llr_i` IS THE MAXIMUM over the rows in the assertion, not their mean. The rows of
    # one assertion are the same claim made about different ORFs -- a paralogue family
    # asserting one KO -- and the claim is corroborated at least as well as its best
    # member states it. Averaging would let a weak paralogue dilute a strong call, which
    # is the dilution the saturating form already handles by mass.
    #
    # `lam0` is added only to reactions that have at least one row here -- the returned index
    # is exactly the nominated set. Applied to all of MNXref it would give every unnominated
    # reaction a positive conductance and pull the whole universe into the network. Within
    # the nominated set every `E` is strictly positive, so a nomination that carries
    # negligible belief mass sits at the prior rather than dropping out of the graph.
    key = [c for c in ASSERTION_KEY if c in rows.columns] + ["mnxr"]
    g = rows.groupby(key, sort=False)
    s = g["contrib"].sum()
    llr = g["llr"].max().to_numpy()
    lam = pd.Series(llr * -np.expm1(-s.to_numpy() / tau),
                    index=s.index.get_level_values("mnxr"))
    return 1.0 / (1.0 + np.exp(-(lam.groupby(level=0).sum() + lam0)))


def pooled_E_of_mass(mass: float, llr: float, *, lam0: float = POOL_LAM0,
                     tau: float = POOL_TAU) -> float:
    # `E` for a reaction carried by exactly ONE assertion of worth `llr` holding `mass`
    # belief.
    #
    # `compose` needs this: every bridge reaction is one synthetic ORF spending its whole
    # 1.0 on one pseudo-reaction, so all bridges land on `pooled_E_of_mass(1.0, BRIDGE_LLR)`
    # and the conductance they were built to carry has to be divided back out of `pair_w`.
    # `llr` is required rather than defaulted: the caller knows what its rows carry, and a
    # default here would silently disagree with them.
    lam = llr * -np.expm1(-mass / tau)
    return float(1.0 / (1.0 + np.exp(-(lam0 + lam))))


def belief_mass(df_src: pd.DataFrame, label: str = "") -> pd.Series:
    # `{mnxr: SUM_g e_g(r)}` -- the PRE-pooling ledger, in units of ORFs.
    #
    # This, not `compute_E`, is what per-ORF conservation is a statement about, and what a
    # diluted gene count has to read. It sums to the number of ORFs in `df_src`.
    rows = nomination_contributions(df_src)
    _assert_conservation(rows, label or "E")
    return rows.groupby("mnxr")["contrib"].sum()


def compute_E(df_src: pd.DataFrame, label: str = "", **pool) -> pd.Series:
    # `{mnxr: E}` -- diluted, then pooled. The conductances `graph_from_pairs` reads.
    rows = nomination_contributions(df_src)
    _assert_conservation(rows, label or "E")
    return pool_logodds(rows, **pool)


def compute_weights(ev: pd.DataFrame) -> pd.DataFrame:
    # evidence_weights.parquet: per (source, mnxr) E_full/E_dlec/belief_mass/n_orf.
    #
    # `E_full` is the pooled conductance and is bounded by 1; `belief_mass` is the raw
    # per-ORF allocation behind it, which is what stays exactly additive under
    # leave-one-out and what the conservation ledger asserts on.
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
    # {unit: {mnxr: E}} with per-ORF-normalized belief, re-aggregated by
    # 'contig' (per fosmid) or 'orf' (per metaG ORF).
    #
    # This is the PRE-pooling ledger (`belief_mass` at unit grain), not a conductance:
    # the unit is part of the assertion key, so summing these maps over units is not
    # `compute_E`. Null-draw research wants exactly this additive form.
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
