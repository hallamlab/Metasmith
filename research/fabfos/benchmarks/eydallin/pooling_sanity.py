#!/usr/bin/env python3
# End-to-end sanity check for the belief path: ag1 alone vs ag1 + one clone.
#
# Every other script in this directory hands `graph_from_pairs` a flat `{mnxr: 1.0}`
# over the curated k12 GEM, so none of them ever reaches
# `ecspr.model.gpr.weights_from_rows(..., "belief")`. This one does: weights come from
# the de novo `e_coli_ag1` proteome through `resolve_weights`, and the perturbation is a
# clone's rows unioned onto the host's -- the set algebra, two units, the belief
# allocation, the bake and the solve, in one run.
#
# Run it before and after any change to the evidence allocation and diff the output.
# Node/edge counts must be IDENTICAL across such a change: an allocation change moves
# the value attached to a reaction, never which reactions are in the graph.
#
#   --cohort     measure every eydallin condition, not just glgC, and rank them
#   --host-gpr   / --clone-gpr   read tables from somewhere other than the run dirs, so
#                a rebuilt pair can be scored without publishing it over a DVC pin
#   --legacy     weight with the frozen pre-calibration allocation instead. It reads
#                `raw_score`, so it only runs against a table built before that change,
#                and it is the "before" half of a before/after.
#   --measured   join the digitised Fig. 1 phenotype and correlate.
#
# **CAUTION** THE CORRELATION IS ON MAGNITUDE, not sign, and that is forced rather than
# chosen. Effective conductance is non-decreasing in every edge conductance (Rayleigh),
# so selecting a clone's rows alongside the host's can only RAISE this readout. A signed
# correlation against a phenotype that goes both ways is unanswerable by construction --
# `twopoint_cohort.py` carries the same argument and the verification behind it. The
# signed number is printed too, because a reader will look for it, and it means nothing.
#
# **CAUTION** This is the ONLY eydallin scorer that can see an evidence-weighting change
# at all. `benchmarks/eydallin_fba/` carves by `channel.nunique() >= min_lanes` and never
# reads the weight column; `cohort_delta_panel.py` and `twopoint_cohort.py` run on the
# curated `gem_gpr` channel, which is a uniform assertion. A null from any of those is a
# statement about what they consume, not about the weighting.
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import bake_pairs  # noqa: E402
from ecspr.model import conditions as cond_mod  # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.gpr import load_gpr, resolve_weights  # noqa: E402
from ecspr.model.graph import Terminal, solve  # noqa: E402

HOST_GPR = ROOT / "data/fabfos/runs/e_coli_ag1/gpr/gpr_denovo.parquet"
CLONE_GPR = ROOT / "data/fabfos/runs/eydallin_clones/gpr/gpr_denovo.parquet"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
WT = 100.0

# The corpus keys. A condition names its table by BASENAME and both files are called
# `gpr_denovo.parquet`, so the corpus is built here with names that do not collide rather
# than from the paths.
HOST_TABLE = "host.parquet"
CLONE_TABLE = "clones.parquet"

# READ FROM THE HOST TABLE, never restated. This was the literal "proteome", which
# `genome_unit` has not carried since it became the ORF set's accession -- so the background
# mask selected zero rows, the weight dict came back empty, and the run died at
# "terminal absent" as though the bake were wrong. A background selector that names a
# value no table holds fails far from its cause.
SOURCE = "MNXM1364061"    # D-glucose
TARGET = "MNXM738130"     # glycogen, the BiGG species iML1515 carries


def _legacy_weights(condition, corpus):
    # The shipped construction with the frozen pre-calibration allocation swapped in. The
    # CONDITION is the shipped one -- only the weighting differs, which is the whole point.
    sys.path.insert(0, str(ROOT / "research/fabfos/annotation_lanes/calibration"))
    import _legacy
    rows = cond_mod.resolve(condition, corpus)
    E = _legacy.pool(_legacy.contributions(_legacy.normalise(rows)))
    w = {str(r): float(e) for r, e in E.items()}
    cov = dict(n_rows=int(len(rows)), n_units=int(rows["genome_unit"].nunique()),
               n_reactions=len(w))
    return w, cov


def host_unit(df) -> str:
    u = sorted(set(df.loc[df["condition_id"].isna(), "genome_unit"].astype(str))
               if "condition_id" in df.columns else set(df["genome_unit"].astype(str)))
    if len(u) != 1:
        raise SystemExit(f"expected one background genome_unit, found {u}")
    return u[0]


def condition_for(cond: str | None) -> cond_mod.Condition:
    """The host alone, or the host plus one clone's rows.

    THE HOST IS THE WHOLE HOST TABLE, stated as a step over that table rather than as a
    `genome_unit` selector. The two are the same rows here and the step says what it means
    -- a background selector naming a value no table holds fails far from its cause, which
    is exactly how this script broke once.
    """
    steps = (cond_mod.Step(table=HOST_TABLE, type="", set=(), operation="union"),)
    if cond is not None:
        steps += (cond_mod.Step(table=CLONE_TABLE, type="condition_id", set=(cond,),
                                operation="union"),)
    return cond_mod.Condition(condition_id=cond or "base", steps=steps)


def measure(pairs, ratios, corpus, label, legacy=False, cond=None):
    condition = condition_for(cond)
    w, cov = (_legacy_weights(condition, corpus) if legacy
              else resolve_weights(condition, corpus, weighting="belief"))
    g = graph_from_pairs(pairs, "C", w, ratios)
    src = Terminal.metabolite(g, SOURCE, label="glucose")
    snk = Terminal.metabolite(g, TARGET, label="glycogen")
    if src.missing or snk.missing:
        raise SystemExit(f"{label}: terminal absent (src={src.missing} snk={snk.missing})")
    s = solve(g, src, snk)
    return dict(label=label, n_rows=cov["n_rows"], n_units=cov["n_units"],
                n_reactions=cov["n_reactions"], n_used=int(g.meta["n_reactions_used"]),
                nodes=int(g.n), edges=int(g.m),
                E_sum=float(sum(w.values())), E_max=float(max(w.values())),
                conductance=float(s.total))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", action="store_true")
    ap.add_argument("--host-gpr", type=Path, default=HOST_GPR)
    ap.add_argument("--clone-gpr", type=Path, default=CLONE_GPR)
    ap.add_argument("--legacy", action="store_true")
    ap.add_argument("--measured", action="store_true")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    if args.legacy:
        # `load_gpr` requires `llr`, which a pre-calibration table does not have.
        corpus = {HOST_TABLE: pd.read_parquet(args.host_gpr),
                  CLONE_TABLE: pd.read_parquet(args.clone_gpr)}
    else:
        corpus = {HOST_TABLE: load_gpr(args.host_gpr),
                  CLONE_TABLE: load_gpr(args.clone_gpr)}
    df = pd.concat(corpus.values(), ignore_index=True, sort=False)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    pairs = load_pairs(bake_pairs.atom_pairs(), element="C")

    print(f"background genome_unit = {host_unit(df)!r}")
    base = measure(pairs, ratios, corpus, "base (ag1 alone)", legacy=args.legacy)
    print(f"base   rows={base['n_rows']:6d} units={base['n_units']} "
          f"reactions={base['n_reactions']:5d} used={base['n_used']:5d} "
          f"nodes={base['nodes']:,} edges={base['edges']:,}")
    print(f"       sum(E)={base['E_sum']:.4f}  max(E)={base['E_max']:.6g}  "
          f"conductance={base['conductance']:.6g}")

    conds = ["eydallin:glgC"]
    if args.cohort:
        conds = sorted(df.condition_id.dropna().unique())

    rows = []
    for cond in conds:
        p = measure(pairs, ratios, corpus, cond, legacy=args.legacy, cond=cond)
        p["d_abs"] = p["conductance"] - base["conductance"]
        p["d_rel"] = p["d_abs"] / base["conductance"]
        rows.append(p)
        if not args.cohort:
            print(f"\n{cond}")
            print(f"       rows={p['n_rows']:6d} units={p['n_units']} "
                  f"reactions={p['n_reactions']:5d} used={p['n_used']:5d} "
                  f"nodes={p['nodes']:,} edges={p['edges']:,}")
            print(f"       sum(E)={p['E_sum']:.4f}  max(E)={p['E_max']:.6g}  "
                  f"conductance={p['conductance']:.6g}")
            print(f"       delta={p['d_abs']:+.6g}  relative={p['d_rel']:+.6%}")

    if args.cohort:
        t = pd.DataFrame(rows).sort_values("d_rel", ascending=False)
        t["rank"] = range(1, len(t) + 1)
        print(f"\n== {len(t)} conditions, ranked by relative delta ==")
        print(t[["rank", "label", "n_reactions", "conductance", "d_abs",
                 "d_rel"]].to_string(index=False))
        g = t[t.label == "eydallin:glgC"]
        if len(g):
            print(f"\nglgC rank {int(g['rank'].iloc[0])} of {len(t)}")
        if args.measured:
            from scipy.stats import spearmanr
            m = pd.read_csv(MEASURED, sep="\t")
            m["label"] = "eydallin:" + m["gene"].astype(str)
            j = t.merge(m[["label", "pct_wt"]], on="label", how="inner")
            j = j[j["d_rel"].notna() & j["pct_wt"].notna()]
            j["mag_meas"] = (j["pct_wt"] / WT).apply(lambda v: abs(np.log2(v)))
            print(f"\n== against the measured phenotype, {len(j)} of {len(t)} "
                  f"conditions joined ==")
            if len(j) >= 3:
                rho_m, p_m = spearmanr(j["d_rel"], j["mag_meas"])
                rho_s, p_s = spearmanr(j["d_rel"], j["pct_wt"])
                print(f"  |log2 FC| (the answerable one)  rho {rho_m:+.4f}  p {p_m:.4f}")
                print(f"  signed pct_wt (means nothing)   rho {rho_s:+.4f}  p {p_s:.4f}")
                rows_out = dict(n_joined=len(j), rho_magnitude=float(rho_m),
                                p_magnitude=float(p_m), rho_signed=float(rho_s),
                                p_signed=float(p_s))
            else:
                rows_out = dict(n_joined=len(j))
            base["phenotype"] = rows_out

    if args.out:
        args.out.write_text(json.dumps({"base": base, "conditions": rows}, indent=2))
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
