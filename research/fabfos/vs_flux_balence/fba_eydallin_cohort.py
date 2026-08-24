#!/usr/bin/env python3
"""Genome-scale FBA as an independent method on the Eydallin glycogen cohort.

Generalises fba_glycogen.py (5 hardcoded genes) to every one of Eydallin 2010's 86
screened genes that iML1515 carries, and scores the sign of each gene's predicted
glycogen change against Fig. 1.

Host.  iML1515 (K-12 MG1655).  The screen ran in AG1 and ECSPr reads DH1; no
DH1/AG1 GEM is on disk in a cobra-loadable form, so the FBA arm uses the K-12
model fba_glycogen.py already used.  Gene identity is carried by b-number, which
is shared, so the join is exact even though the model is not the screen's strain.

Mapping.  measured_glycogen.tsv -> clone_gem_census.tsv (b_number, current_symbol)
-> iML1515 gene id.  iML1515 gene ids ARE b-numbers, so the primary join needs no
cross-reference table; current_symbol and the raw name are fallbacks that only pick
up ppk (blank b_number in the census, and the `ppK`/`ppk` case trap).

Reference state.  Growth pinned at GROWTH_FRAC x max, DM_glycogen_c maximised, pFBA.
Every capacity below is read off this state, so "WT" means "the flux distribution a
glycogen-maximising cell at 90% growth would run".

Two overexpression conventions, because stock iML1515 bounds are all +/-1000 and
therefore non-binding -- relaxing an unbound capacity is a bit-exact no-op:

  relax  Cap EVERY reaction at max(|v_ref|, VMIN); overexpression multiplies the
         caps of the gene's reactions by FOLD.  This is the direct analogue of
         ECSPr's conductance fold, and it inherits ECSPr's Rayleigh problem: an LP
         maximum is non-decreasing in every bound, so this convention can only ever
         predict "up or flat".  Reported as the monotone control.

  force  The gene's reactions must CARRY FOLD x max(|v_ref|, VMIN), in whichever
         direction the reference ran them (lb raised for forward, ub lowered for
         reverse).  "Twice the enzyme, and it is running."  This is signed -- forcing
         a catabolic or diverting step up lowers glycogen -- and it can be infeasible,
         which is reported as an unanswered condition rather than as a prediction.

Both arms turn out to be ONE-SIDED on this cohort, which is the result rather than a
bug in the conventions.  `relax` never moves the optimum at all: growth-pinned maximum
glycogen is carbon-limited by glucose uptake, not by any internal capacity, so no single
gene is rate-limiting and all 39 deltas are solver noise.  `force` moves every gene DOWN:
WT already sits at the glycogen ceiling, so any obligate flux -- on-path or off -- is
carbon spent elsewhere.  Its sign accuracy therefore equals an always-down constant
predictor's, which is what the printout compares it against.

Taking capacities from a GROWTH-maximising reference instead (glycogen off the objective,
so the glg reactions cap at VMIN) does not rescue `relax`: the whole synthesis path then
shares the same floor, so doubling any one step leaves the bottleneck where it was.
Measured, all deltas < 1e-16.

VMIN is the floor capacity given to a reaction the reference state leaves at zero;
without it such a reaction is locked off (relax) or unperturbable (force).  It sets
the size of the perturbation for every off-in-WT gene, so --sweep re-runs the whole
cohort over a VMIN x FOLD grid and reports how many signs move.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import cobra
import numpy as np
import pandas as pd
from cobra.flux_analysis import pfba
from cobra.util.solver import linear_reaction_coefficients

ROOT = Path(__file__).resolve().parents[3]
MODEL = ROOT / "data/fabfos/originals/genomes/e_coli_k12/GEM/iML1515.json"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
CENSUS = ROOT / "data/fabfos/runs/eydallin_clones/gpr/clone_gem_census.tsv"
ECSPR = ROOT / "research/fabfos/benchmarks/eydallin/cache/m1_r10_comparison.parquet"
OUT = Path(__file__).resolve().parent / "out"

GROWTH_FRAC = 0.9
FOLD = 2.0
VMIN = 0.1
DEGENERATE_ABS = 1e-6


def load_model():
    m = cobra.io.load_json_model(str(MODEL))
    m.add_boundary(m.metabolites.glycogen_c, type="demand", reaction_id="DM_glycogen_c")
    return m


def reference_state(m):
    bio = list(linear_reaction_coefficients(m))[0]
    gmax = m.slim_optimize()
    with m:
        bio.lower_bound = GROWTH_FRAC * gmax
        m.objective = m.reactions.DM_glycogen_c
        sol = pfba(m)
    return bio, gmax, sol.fluxes


def gene_map(m):
    meas = pd.read_csv(MEASURED, sep="\t")
    cen = pd.read_csv(CENSUS, sep="\t")
    # measured_glycogen.tsv writes ppk, the census writes ppK: key on lowercase.
    meas["_k"] = meas.gene.str.lower()
    cen["_k"] = cen.gene.str.lower()
    df = meas.merge(cen[["_k", "b_number", "current_symbol"]], on="_k", how="left")
    assert len(df) == len(meas), "census join duplicated a measured gene"
    by_id = {g.id: g for g in m.genes}
    by_name = {}
    for g in m.genes:
        by_name.setdefault(g.name.lower(), g)

    rows = []
    for r in df.itertuples():
        g = by_id.get(str(r.b_number))
        route = "b_number"
        if g is None:
            g = by_name.get(str(r.current_symbol).lower())
            route = "current_symbol"
        if g is None:
            g = by_name.get(str(r.gene).lower())
            route = "gene_name"
        rows.append(dict(
            gene=r.gene, measured_pct_wt=r.pct_wt,
            b_number=r.b_number, current_symbol=r.current_symbol,
            iml_gene=None if g is None else g.id,
            map_route=None if g is None else route,
            rxns=[] if g is None else sorted(x.id for x in g.reactions),
        ))
    out = pd.DataFrame(rows)
    out["n_rxn"] = out.rxns.apply(len)
    return out


def _capped(m, vref, scale=None):
    """Set every reaction's bounds to +/- max(|v_ref|, VMIN), scaled per reaction."""
    scale = scale or {}
    for r in m.reactions:
        if r.id == "DM_glycogen_c" or r.boundary:
            continue
        cap = max(abs(vref.get(r.id, 0.0)), VMIN) * scale.get(r.id, 1.0)
        r.lower_bound = max(r.lower_bound, -cap)
        r.upper_bound = min(r.upper_bound, cap)


def _forced(m, vref, rxn_ids):
    for rid in rxn_ids:
        r = m.reactions.get_by_id(rid)
        t = FOLD * max(abs(vref.get(rid, 0.0)), VMIN)
        if vref.get(rid, 0.0) >= 0:
            r.lower_bound = min(t, r.upper_bound)
        else:
            r.upper_bound = max(-t, r.lower_bound)


def _solve(m, bio, gmax):
    bio.lower_bound = GROWTH_FRAC * gmax
    m.objective = m.reactions.DM_glycogen_c
    v = m.slim_optimize()
    return None if v is None or v != v else float(v)


def run(m, bio, gmax, vref, mapping):
    with m:
        _capped(m, vref)
        wt_relax = _solve(m, bio, gmax)
    with m:
        wt_force = _solve(m, bio, gmax)

    recs = []
    for r in mapping[mapping.iml_gene.notna()].itertuples():
        with m:
            _capped(m, vref, scale={rid: FOLD for rid in r.rxns})
            v_relax = _solve(m, bio, gmax)
        with m:
            _forced(m, vref, r.rxns)
            v_force = _solve(m, bio, gmax)
        recs.append(dict(
            gene=r.gene, iml_gene=r.iml_gene, n_rxn=r.n_rxn,
            rxns=",".join(r.rxns),
            ref_flux_max=max((abs(vref.get(x, 0.0)) for x in r.rxns), default=0.0),
            wt_relax=wt_relax, fba_relax=v_relax,
            wt_force=wt_force, fba_force=v_force,
        ))
    d = pd.DataFrame(recs)
    for arm in ("relax", "force"):
        wt, v = d[f"wt_{arm}"], d[f"fba_{arm}"]
        d[f"delta_{arm}"] = v - wt
        d[f"delta_{arm}_pct"] = 100.0 * (v - wt) / wt
        d[f"answered_{arm}"] = v.notna() & (d[f"delta_{arm}"].abs() > DEGENERATE_ABS)
    return d


def score(d, mapping):
    m = mapping[["gene", "measured_pct_wt", "iml_gene", "map_route", "n_rxn"]]
    d = d.merge(m.drop(columns=["n_rxn"]), on=["gene", "iml_gene"], how="left")
    d["measured_pct_change"] = d.measured_pct_wt - 100.0
    d["measured_sign"] = np.sign(d.measured_pct_change).astype(int)
    for arm in ("relax", "force"):
        d[f"sign_{arm}"] = np.sign(d[f"delta_{arm}"].fillna(0.0)).astype(int)
        d[f"match_{arm}"] = np.where(
            d[f"answered_{arm}"], d[f"sign_{arm}"] == d.measured_sign, None)
    return d


def join_ecspr(d, mapping):
    e = pd.read_parquet(ECSPR)[[
        "gene", "channel", "delta_gg_pct", "delta_ratio_pct",
        "new_gg_sign_match", "new_ratio_sign_match"]]
    e = e.rename(columns={"channel": "ecspr_channel",
                          "new_gg_sign_match": "ecspr_gg_match",
                          "new_ratio_sign_match": "ecspr_ratio_match",
                          "delta_gg_pct": "ecspr_delta_gg_pct",
                          "delta_ratio_pct": "ecspr_delta_ratio_pct"})
    e["_k"] = e.gene.str.lower()
    j = mapping[["gene", "measured_pct_wt", "iml_gene", "map_route", "n_rxn"]].copy()
    j["_k"] = j.gene.str.lower()
    j = j.merge(d.drop(columns=["measured_pct_wt", "iml_gene"]), on="gene", how="left")
    j = j.merge(e.drop(columns=["gene"]), on="_k", how="left").drop(columns=["_k"])
    j["in_ecspr"] = j.ecspr_channel.notna()
    j["in_fba"] = j.iml_gene.notna()
    return j


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sweep", action="store_true",
                    help="re-run the force arm over a VMIN x FOLD grid")
    a = ap.parse_args()

    OUT.mkdir(exist_ok=True)
    m = load_model()
    bio, gmax, vref = reference_state(m)
    mapping = gene_map(m)
    print(f"iML1515: {len(m.genes)} genes / {len(m.reactions)} reactions")
    print(f"max growth {gmax:.4f}/h; pinned {GROWTH_FRAC:g}x; "
          f"reference glycogen demand {vref['DM_glycogen_c']:.4f} mmol/gDW/h")
    print(f"mapped {int(mapping.iml_gene.notna().sum())}/{len(mapping)} Eydallin genes "
          f"({mapping.map_route.value_counts().to_dict()})")
    print(f"  unmapped (no iML1515 gene at all): "
          f"{int(mapping.iml_gene.isna().sum())}")

    d = run(m, bio, gmax, vref, mapping)
    d = score(d, mapping)
    mapping.assign(rxns=mapping.rxns.apply(",".join)).to_csv(
        OUT / "eydallin_gene_map.tsv", sep="\t", index=False)
    d.to_csv(OUT / "eydallin_fba.tsv", sep="\t", index=False)

    for arm in ("relax", "force"):
        a_ = d[d[f"answered_{arm}"]]
        n, k = len(a_), int((a_[f"match_{arm}"] == True).sum())  # noqa: E712
        inf = int(d[f"fba_{arm}"].isna().sum())
        up = int((a_[f"sign_{arm}"] > 0).sum())
        base = int((a_.measured_sign < 0).sum())
        rho = (a_[f"delta_{arm}_pct"].corr(a_.measured_pct_change, method="spearman")
               if n > 2 else float("nan"))
        print(f"[{arm}] answered {n}/{len(d)} mapped "
              f"({inf} infeasible, {len(d)-n-inf} degenerate); sign {k}/{n}")
        print(f"       predicted up {up}/{n}, down {n-up}/{n}; "
              f"always-down baseline would score {base}/{n}; "
              f"spearman(delta, measured) = {rho:.3f}")

    j = join_ecspr(d, mapping)
    assert len(j) == len(mapping) and int(j.in_ecspr.sum()) == len(pd.read_parquet(ECSPR))
    j.to_csv(OUT / "eydallin_fba_vs_ecspr.tsv", sep="\t", index=False)
    ec = j[j.in_ecspr]
    both = ec[ec.answered_force == True]  # noqa: E712
    print(f"\nECSPr cohort n={len(ec)} (FBA maps {int(ec.in_fba.sum())}, "
          f"answers {len(both)})")
    print(f"  on all {len(ec)}: ECSPr two-point {int(ec.ecspr_gg_match.sum())}"
          f" | ECSPr ratio {int(ec.ecspr_ratio_match.sum())}"
          f" | FBA {int((ec.match_force == True).sum())}")  # noqa: E712
    if len(both):
        print(f"  FBA sign  {int((both.match_force == True).sum())}/{len(both)}"  # noqa: E712
              f" | ECSPr two-point {int(both.ecspr_gg_match.sum())}/{len(both)}"
              f" | ECSPr ratio {int(both.ecspr_ratio_match.sum())}/{len(both)}")
    only_fba = j[j.in_fba & ~j.in_ecspr & (j.answered_force == True)]  # noqa: E712
    print(f"  answered by FBA but outside ECSPr's cohort: "
          f"{sorted(only_fba.gene)}")

    if a.sweep:
        global VMIN, FOLD
        rows = []
        base = dict(VMIN=VMIN, FOLD=FOLD)
        for vmin in (0.05, 0.1, 0.5):
            for fold in (2.0, 10.0):
                VMIN, FOLD = vmin, fold
                s = score(run(m, bio, gmax, vref, mapping), mapping)
                n = int(s.answered_force.sum())
                k = int((s.match_force == True).sum())  # noqa: E712
                rows.append(dict(vmin=vmin, fold=fold, answered=n, correct=k,
                                 acc=k / n if n else np.nan))
                print(f"  sweep vmin={vmin} fold={fold}: {k}/{n}")
        VMIN, FOLD = base["VMIN"], base["FOLD"]
        pd.DataFrame(rows).to_csv(OUT / "eydallin_fba_sweep.tsv", sep="\t", index=False)

    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
