#!/usr/bin/env python3
"""The signed probe: what FRACTION of glucose carbon ends up as glycogen.

`monotonicity_ladder.py` settles the mechanism. A two-point conductance can only rise when
an edge widens -- Rayleigh -- so no fold on it can express a glycogen-DEFICIENT phenotype.
A share of the injected current can fall, because a share is homogeneous of degree zero and
its elasticities therefore sum to zero rather than to one: positive and negative levers must
both exist. What buys the sign is not a better network. It is grounding somewhere other
than the target, so that current diverted away from glycogen has somewhere to go.

That is also the phenotype Eydallin measured. Fig. 1 reports glycogen as nmol glucose per
mg protein -- a fraction of the cell's carbon, not a flux capacity.

Three ways to ground, in increasing physiological honesty, all through `attach_leak`:

  leak      every metabolite leaks equally; draw[glycogen] is its share of the escape
  glycogen  glycogen is a real port, everything else leaks -- glycogen as the only product
  biomass   glycogen AND the host's biomass precursors are ports -- the real competition

The leak magnitude is the knob that decides whether a competitor can drain at all. Rung G
of the ladder shows the shunt elasticity is zero to five decimals at 1e-6, which is what
`cohort_delta_panel.py` ran at, so sweep it rather than assume it.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/glycogen_share.py --diagnose
    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/glycogen_share.py --cohort
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import (load_pairs, load_direction_ratios,  # noqa: E402
                               graph_from_pairs)
from ecspr.model.graph import Terminal, measure_leak                # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
AG1_GEM = ROOT / "data/fabfos/originals/genomes/e_coli_dh1/GEM/iECDH1ME8569_1439.json"
CHEM_XREF = ROOT / "data/fabfos/originals/metanetx/4.5/chem_xref.tsv"
CLONE_GPR = ROOT / "data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
CACHE = Path(__file__).resolve().parents[1] / "cache"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"
PHOSPHORYLASE = ("MNXR145036", "MNXR145038")
_COMP = re.compile(r"_([a-z]{1,2})$")


def biomass_precursors(universe) -> list:
    cache = CACHE / "biggM_bridge.parquet"
    if cache.exists():
        br = pd.read_parquet(cache)
    else:
        x = pd.read_csv(CHEM_XREF, sep="\t", comment="#", header=None,
                        names=["source", "mnxm", "description"], dtype=str,
                        low_memory=False)
        x = x[x.source.str.startswith("biggM:", na=False)]
        br = pd.DataFrame(dict(bigg=x.source.str[len("biggM:"):], mnxm=x.mnxm)) \
            .drop_duplicates()
        CACHE.mkdir(exist_ok=True)
        br.to_parquet(cache, index=False)
    by_bigg = br.groupby("bigg").mnxm.apply(list).to_dict()

    model = json.loads(AG1_GEM.read_text())
    cands = [r for r in model["reactions"]
             if r.get("objective_coefficient", 0) or "BIOMASS" in r["id"].upper()]
    obj = [r for r in cands if r.get("objective_coefficient", 0)]
    rxn = (obj or [r for r in cands if "core" in r["id"].lower()] or cands)[0]
    subs = [m for m, c in rxn["metabolites"].items() if c < 0]
    out = set()
    for m in subs:
        out.update(c for c in by_bigg.get(_COMP.sub("", m), []) if c in universe)
    return sorted(out)


def make_probe(graph, mode, precursors):
    prec = {"leak": None, "glycogen": [GLYCOGEN_MNXM],
            "biomass": sorted(set(precursors) | {GLYCOGEN_MNXM})}[mode]
    return prec


def draw(pairs, element, weights, ratios, prec, *, leak, port=1.0) -> float:
    g = graph_from_pairs(pairs, element, weights, ratios)
    src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
    if src.missing:
        raise SystemExit("[share] source missing")
    r = measure_leak(g, src, prec, leak=leak, port=port)
    return float(r["draw"].get(GLYCOGEN_MNXM, 0.0))


_S: dict = {}


def _spectrum_one(mnxr) -> dict:
    w = dict(_S["base_w"], **{mnxr: _S["base_w"][mnxr] * _S["fold"]})
    v = draw(_S["pairs"], _S["element"], w, _S["ratios"], _S["prec"], leak=_S["leak"])
    b = _S["base"]
    return dict(mnxr=mnxr,
                elasticity=float(np.log(v / b) / np.log(_S["fold"])) if v > 0 else np.nan,
                share_pert=v)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--diagnose", action="store_true",
                   help="a few informative genes over the grounding x leak grid")
    p.add_argument("--cohort", action="store_true",
                   help="the 25 cohort genes at one setting, scored against the phenotype")
    p.add_argument("--spectrum", action="store_true",
                   help="signed elasticity of every reaction: where the negative mass is")
    p.add_argument("--scan", action="store_true",
                   help="the cohort's signed correlation over the whole grounding x leak x "
                        "override-strength grid, with a permutation p. A number that only "
                        "appears at one setting is a knob, not a result.")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--mode", default="biomass", choices=("leak", "glycogen", "biomass"))
    p.add_argument("--leak", type=float, default=1.0)
    p.add_argument("--ratio-override", default="",
                   help="`MNXR:ratio,...` on top of the baked ensemble, as in sweep_aska")
    p.add_argument("--ratio-cap", type=float, default=None,
                   help="bound |log10 direction ratio| at this many decades before the "
                        "graph is built (ecspr.model.build.cap_direction_ratios). The "
                        "shipped table spans 28.7 decades over a host's reactions and "
                        "saturates by 6; the arm is tagged so a capped run cannot be "
                        "mistaken for an uncapped one")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = p.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios(), cap=a.ratio_cap)
    override = {}
    for item in filter(None, a.ratio_override.split(",")):
        k, v = item.split(":")
        override[k.strip()] = float(v)
    if override:
        ratios = dict(ratios, **override)
        print(f"[share] direction override {override}", file=sys.stderr)

    host = pd.read_parquet(RUNS / a.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    g0 = graph_from_pairs(pairs, a.element, base_w, ratios)
    universe = set(g0.metabolites())
    prec_biomass = biomass_precursors(universe)
    print(f"[share] {g0.meta['n_reactions_used']} reactions, {g0.n} nodes; "
          f"{len(prec_biomass)} biomass precursors in the carbon universe", file=sys.stderr)

    gene_rxns = (host[host.in_atom_universe.fillna(False)]
                 .groupby("feature_name").mnxr.apply(lambda s: sorted(set(s.astype(str))))
                 .to_dict())

    def response(gene, prec, leak):
        rs = gene_rxns.get(gene, [])
        if not rs:
            return np.nan, np.nan
        b = draw(pairs, a.element, base_w, ratios, prec, leak=leak)
        v = draw(pairs, a.element,
                 dict(base_w, **{r: base_w[r] * a.fold for r in rs}), ratios,
                 prec, leak=leak)
        return b, v

    if a.diagnose:
        probes = ["glgC", "glgA", "glgP", "malP", "glgB", "agp", "pfkA"]
        rows = []
        for mode in ("leak", "glycogen", "biomass"):
            prec = make_probe(g0, mode, prec_biomass)
            for leak in (1e-6, 1e-3, 1e-1, 1.0, 10.0):
                line = []
                for gene in probes:
                    b, v = response(gene, prec, leak)
                    lf = np.log2(v / b) if (b and b > 0 and v > 0) else np.nan
                    rows.append(dict(mode=mode, leak=leak, gene=gene, base=b, pert=v,
                                     log2fc=lf))
                    line.append(f"{gene}={lf:+.4f}")
                base = rows[-1]["base"]
                print(f"  {mode:9} leak={leak:<8g} share={base:.5f}  " + "  ".join(line),
                      file=sys.stderr)
        df = pd.DataFrame(rows)
        out = a.out_dir / f"glycogen_share_diagnose_{a.host}_{a.element}.tsv"
        df.to_csv(out, sep="\t", index=False)
        print(f"\n[share] wrote {out}", file=sys.stderr)
        neg = df.groupby(["mode", "leak"]).log2fc.apply(lambda s: int((s < -1e-12).sum()))
        print("\nnegative responses per setting (of 7 probes):", file=sys.stderr)
        print(neg.to_string(), file=sys.stderr)

    if a.scan:
        clone = pd.read_parquet(CLONE_GPR)
        clone = clone[clone.in_atom_universe & (clone.channel == "gem_gpr")]
        genes = (clone.groupby("condition_id")
                 .agg(gene=("feature_name", "first"),
                      rxns=("mnxr", lambda s: sorted(set(s.astype(str)))))
                 .reset_index())
        meas = pd.read_csv(MEASURED, sep="\t")
        pct = {str(c).split(":")[-1].lower(): v
               for c, v in zip(meas.condition_id, meas.pct_wt)}
        genes["pct_wt"] = genes.gene.str.lower().map(pct)
        genes = genes.dropna(subset=["pct_wt"])
        y = np.log2(genes.pct_wt.to_numpy() / 100.0)
        rng = np.random.default_rng(0)

        rows = []
        for tau in (1.0, 10.0, 1e2, 1e3, 1e6):
            rat = ratios if tau == 1.0 else dict(
                ratios, **{"MNXR145046": 1.0 / tau, "MNXR145036": tau, "MNXR145038": tau})
            for mode in ("leak", "glycogen", "biomass"):
                prec = make_probe(g0, mode, prec_biomass)
                for leak in (1e-3, 1e-1, 1.0):
                    b = draw(pairs, a.element, base_w, rat, prec, leak=leak)
                    if not b > 0:
                        continue
                    x = np.array([np.log2(max(draw(pairs, a.element,
                                                   dict(base_w, **{r: a.fold
                                                                   for r in g.rxns}),
                                                   rat, prec, leak=leak), 1e-300) / b)
                                  for g in genes.itertuples()])
                    rs = spearmanr(y, x).statistic
                    null = np.array([spearmanr(rng.permutation(y), x).statistic
                                     for _ in range(2000)])
                    pperm = float((np.abs(null) >= abs(rs)).mean())
                    n_neg = int((x < -1e-12).sum())
                    rows.append(dict(tau=tau, mode=mode, leak=leak, base=b,
                                     rho_signed=rs, p_perm=pperm, n_negative=n_neg,
                                     n=len(x)))
                    print(f"  tau={tau:<8g} {mode:9} leak={leak:<6g} base={b:.5f}  "
                          f"rho={rs:+.4f}  p_perm={pperm:.3f}  neg={n_neg}/{len(x)}",
                          file=sys.stderr)
        df = pd.DataFrame(rows)
        out = a.out_dir / f"glycogen_share_scan_{a.host}_{a.element}.tsv"
        df.to_csv(out, sep="\t", index=False)
        print(f"\n[share] wrote {out}", file=sys.stderr)
        pd.set_option("display.width", 200)
        print("\nsigned rho by tau x grounding (median over the leak grid):",
              file=sys.stderr)
        print(df.pivot_table(index="tau", columns="mode", values="rho_signed",
                             aggfunc="median").to_string(float_format="%+.4f"),
              file=sys.stderr)

    if a.spectrum:
        prec = make_probe(g0, a.mode, prec_biomass)
        gp = graph_from_pairs(pairs, a.element, base_w, ratios, with_provenance=True)
        used = sorted(set(gp.meta["edge_reactions"].mnxr))
        _S.update(pairs=pairs, element=a.element, base_w=base_w, ratios=ratios,
                  prec=prec, leak=a.leak, fold=1.01,
                  base=draw(pairs, a.element, base_w, ratios, prec, leak=a.leak))
        print(f"[share] spectrum: mode={a.mode} leak={a.leak} base={_S['base']:.6f}; "
              f"{len(used)} reactions", file=sys.stderr)
        import multiprocessing as mp
        import time
        t0 = time.time()
        with mp.get_context("fork").Pool(a.workers) as pool:
            rows = []
            for i, r in enumerate(pool.imap_unordered(_spectrum_one, used, chunksize=8), 1):
                rows.append(r)
                if i % 300 == 0:
                    el = time.time() - t0
                    print(f"  {i}/{len(used)}  {el:.0f}s  eta {el/i*(len(used)-i):.0f}s",
                          file=sys.stderr)
        df = pd.DataFrame(rows)
        nm = host.groupby(host.mnxr.astype(str)).agg(
            genes=("feature_name", lambda s: ",".join(sorted({x for x in s
                                                              if isinstance(x, str) and x}))),
            rxn_name=("evidence_name", "first"))
        df = df.join(nm, on="mnxr").sort_values("elasticity", ascending=False)
        tag = (f"{a.mode}_leak{a.leak:g}" + (f"_dir{len(override)}" if override else "")
               + (f"_cap{a.ratio_cap:g}" if a.ratio_cap is not None else ""))
        out = a.out_dir / f"glycogen_share_spectrum_{tag}_{a.host}_{a.element}.tsv"
        df.to_csv(out, sep="\t", index=False)
        e = df.elasticity.dropna()
        print(f"\n[share] wrote {out}\n"
              f"  sum of elasticities  = {e.sum():+.6f}  (a share, so 0 over ALL "
              f"conductances; the leak and port edges carry the balance)\n"
              f"  positive mass        = {e[e > 0].sum():+.6f} over {int((e > 1e-9).sum())} "
              f"reactions\n"
              f"  NEGATIVE mass        = {e[e < 0].sum():+.6f} over {int((e < -1e-9).sum())} "
              f"reactions\n"
              f"  most negative        = {e.min():+.6f}", file=sys.stderr)
        pd.set_option("display.width", 220)
        print("\n=== the 12 most NEGATIVE levers (carbon diverted away from glycogen) ===",
              file=sys.stderr)
        print(df.nsmallest(12, "elasticity")[["mnxr", "genes", "rxn_name", "elasticity"]]
              .to_string(index=False, formatters={"elasticity": "{:+.6f}".format}),
              file=sys.stderr)
        print("\n=== the 12 most positive ===", file=sys.stderr)
        print(df.nlargest(12, "elasticity")[["mnxr", "genes", "rxn_name", "elasticity"]]
              .to_string(index=False, formatters={"elasticity": "{:+.6f}".format}),
              file=sys.stderr)

    if a.cohort:
        prec = make_probe(g0, a.mode, prec_biomass)
        clone = pd.read_parquet(CLONE_GPR)
        clone = clone[clone.in_atom_universe & (clone.channel == "gem_gpr")]
        genes = (clone.groupby("condition_id")
                 .agg(gene=("feature_name", "first"),
                      rxns=("mnxr", lambda s: sorted(set(s.astype(str)))))
                 .reset_index())
        base = draw(pairs, a.element, base_w, ratios, prec, leak=a.leak)
        print(f"[share] mode={a.mode} leak={a.leak} base share = {base:.6f}", file=sys.stderr)
        rows = []
        for r in genes.itertuples():
            v = draw(pairs, a.element,
                     dict(base_w, **{x: base_w[x] * a.fold for x in r.rxns}), ratios,
                     prec, leak=a.leak)
            rows.append(dict(condition_id=r.condition_id, gene=r.gene, n_rxn=len(r.rxns),
                             share_base=base, share_pert=v, delta=v - base,
                             log2fc=np.log2(v / base) if v > 0 else np.nan,
                             rxns=",".join(r.rxns)))
            print(f"  {r.gene:8} {v:.6f}  {v - base:+.3e}", file=sys.stderr)
        df = pd.DataFrame(rows)
        meas = pd.read_csv(MEASURED, sep="\t")
        pct = {str(c).split(":")[-1].lower(): v
               for c, v in zip(meas.condition_id, meas.pct_wt)}
        df["pct_wt"] = df.gene.str.lower().map(pct)
        df = df.dropna(subset=["pct_wt"]).copy()
        df["log2fc_meas"] = np.log2(df.pct_wt / 100.0)
        tag = (f"{a.mode}_leak{a.leak:g}" + (f"_dir{len(override)}" if override else "")
               + (f"_cap{a.ratio_cap:g}" if a.ratio_cap is not None else ""))
        out = a.out_dir / f"glycogen_share_cohort_{tag}_{a.host}_{a.element}.tsv"
        df.to_csv(out, sep="\t", index=False)

        pd.set_option("display.width", 200)
        print("\n" + df.sort_values("log2fc", ascending=False)
              [["gene", "pct_wt", "log2fc_meas", "log2fc", "n_rxn"]]
              .to_string(index=False, formatters={"log2fc": "{:+.6f}".format,
                                                  "log2fc_meas": "{:+.3f}".format}),
              file=sys.stderr)
        n_neg = int((df.log2fc < -1e-12).sum())
        rs, ps = spearmanr(df.log2fc_meas, df.log2fc)
        ra, pa = spearmanr(df.log2fc_meas.abs(), df.log2fc.abs())
        print(f"\n[share] {n_neg}/{len(df)} clones move glycogen DOWN "
              f"(the two-point probe gives 0 by Rayleigh)\n"
              f"        SIGNED   Spearman rho = {rs:+.4f}  p = {ps:.3g}\n"
              f"        |magnitude| Spearman rho = {ra:+.4f}  p = {pa:.3g}\n"
              f"[share] wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
