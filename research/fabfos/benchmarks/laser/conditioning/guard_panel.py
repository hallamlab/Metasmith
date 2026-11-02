#!/usr/bin/env python3
"""Does bounding the direction ratio cost anything the benchmark can measure?

`decade_budget.py` showed the bound is nearly free on the numbers a conditioning argument
cares about. This asks the question that decides whether to adopt it: run the study's own
classifier over the whole ASKA library at each bound and see whether the answer moves.

TWO ARMS, BOTH ON r9, BECAUSE THE COMMITTED NUMBERS ARE NOT A BASELINE. The committed
eydallin sweeps report a base glucose -> glycogen conductance of 5.689489; the same code path
on the pinned bake returns 1.962979. The difference is the bake, not the code -- r9's
direction table moved it -- so comparing a capped r9 arm against those numbers would
attribute a bake change to the cap. Every comparison here is r9 against r9.

THE SCORE IS FIRST ORDER, AND THAT IS NOT A COMPROMISE HERE. A fold `f` on a clone's
reactions moves the readout by `(sum eps_r) * log f`, so the whole library falls out of the
ONE solve that produced the elasticities. Two reasons it is the right instrument rather than a
cheap one:

  * `sweep_aska.py` CANNOT RUN on this tree. Four reactions the ASKA clone GPR carries
    (MNXR145836, MNXR146084, MNXR152618, MNXR152661 -- fabA, fabG, fabZ, bioH) are absent
    from the rebuilt ag1 host GEM, and the sweep refuses rather than let a clone create a
    reaction its background never had. The committed sweep scored all four, so the two tables
    have drifted apart since. That guard is right and the drift is real; an elasticity is
    simply immune to it, because a reaction outside the solved graph has no share to
    contribute and contributes zero -- which is the answer the guard is protecting.
  * It is validated here rather than assumed: `--validate` re-solves a sample of clones
    exactly and reports the agreement.

Curated arm only; the de-novo tables carry a pbert channel under repair.

    mamba run -n ecspr python research/fabfos/benchmarks/laser/conditioning/guard_panel.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))

import ecspr.model.build as EB                                        # noqa: E402
from ecspr.model.build import cap_direction_ratios                    # noqa: E402
from ecspr.model.graph import Terminal, solve                         # noqa: E402
from ecspr.model.scoring import responders                            # noqa: E402
from analyse_aska_sweep import GLYCOGEN_MODULE, analyse               # noqa: E402
import bake_pairs                                                     # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
ASKA_GPR = ROOT / "data/fabfos/runs/aska/gpr"
OUT_DIR = Path(__file__).resolve().parent / "cache"

SOURCE = "MNXM1364061"
GLYCOGEN = "MNXM738130"
WIDTHS = (0.0, 1.0, 2.0, 3.0, 4.0, 6.0, None)


def library(base_w: dict) -> pd.DataFrame:
    # One row per ASKA clone gene: its reactions, and the Eydallin label.
    #
    # Reactions absent from the background are KEPT and counted. Dropping them would silently
    # change `n_rxn`, which is the size control every AUC below is scored against.
    clone = pd.read_parquet(ASKA_GPR / "gpr_gem.parquet")
    clone = clone[clone.in_atom_universe.fillna(False)]
    by_gene = (clone.assign(g=clone.condition_id.str.replace("aska:", "", regex=False))
               .groupby("g").mnxr.apply(lambda s: sorted(set(s.astype(str)))).to_dict())
    census = pd.read_csv(ASKA_GPR / "clone_census.tsv", sep="\t").drop_duplicates("gene")
    census["rxn_list"] = census.gene.map(by_gene)
    census["rxn_list"] = census.rxn_list.apply(lambda v: v if isinstance(v, list) else [])
    census["n_rxn"] = census.rxn_list.apply(len)
    census["n_absent"] = census.rxn_list.apply(
        lambda rs: sum(1 for r in rs if r not in base_w))
    census["is_positive"] = census.eydallin_phenotype.notna()
    return census.reset_index(drop=True)


def scored(lib: pd.DataFrame, eps: pd.Series) -> pd.DataFrame:
    lut = eps.to_dict()
    df = lib.copy()
    df["score"] = [abs(sum(lut.get(r, 0.0) for r in rs)) for rs in df.rxn_list]
    return df


def validate(pairs, base_w, ratios, lib, n=40, fold=2.0, seed=0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    live = lib[lib.n_rxn > 0].sort_values("score", ascending=False)
    pick = pd.concat([live.head(n // 2),
                      live.iloc[n // 2:].sample(min(n // 2, max(len(live) - n // 2, 0)),
                                                random_state=seed)])
    g0 = EB.graph_from_pairs(pairs, "C", base_w, ratios)
    base = float(solve(g0, Terminal.metabolite(g0, SOURCE),
                       Terminal.metabolite(g0, GLYCOGEN)).total)
    rows = []
    for _, r in pick.iterrows():
        w = dict(base_w)
        for x in r.rxn_list:
            if x in w:
                w[x] = w[x] * fold
        g = EB.graph_from_pairs(pairs, "C", w, ratios)
        v = float(solve(g, Terminal.metabolite(g, SOURCE),
                        Terminal.metabolite(g, GLYCOGEN)).total)
        rows.append(dict(gene=r.gene, n_rxn=r.n_rxn, first_order=r.score,
                         exact=abs(np.log(v / base)) if v > 0 and base > 0 else np.nan))
    out = pd.DataFrame(rows)
    out["exact"] = out.exact / np.log(fold)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--validate", type=int, default=40,
                   help="clones to re-solve exactly; 0 skips the check")
    p.add_argument("--out", default=str(OUT_DIR / "guard_panel.tsv"))
    a = p.parse_args()

    pairs = EB.load_pairs(bake_pairs.atom_pairs(), element="C")
    raw = EB.load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(RUNS / a.host / "gpr" / "gpr_gem.parquet")
    assert set(host.lane_set.unique()) == {"curated"}, "not the curated arm"
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    lib = library(base_w)
    print(f"[guard] {len(lib):,} clone genes, {int((lib.n_rxn > 0).sum()):,} atom-mapped, "
          f"{int(lib.is_positive.sum())} Eydallin positives, "
          f"{int((lib.n_absent > 0).sum())} clones carrying a reaction the background lacks",
          file=sys.stderr)

    rows, first = [], None
    for width in WIDTHS:
        ratios = cap_direction_ratios(raw, width)
        g = EB.graph_from_pairs(pairs, "C", base_w, ratios, with_provenance=True)
        sol = solve(g, Terminal.metabolite(g, SOURCE), Terminal.metabolite(g, GLYCOGEN))
        eps = EB.reaction_elasticities(g, sol)
        df = scored(lib, eps)
        keep = responders(df.score.to_numpy())
        tag = "uncapped" if width is None else f"cap{width:g}"

        quiet = lambda *_a, **_k: None
        mod = df.gene.isin(GLYCOGEN_MODULE) | df.get(
            "eydallin_gene", pd.Series("", index=df.index)).astype(str).isin(GLYCOGEN_MODULE)
        full = analyse(df.reset_index(drop=True), tag, quiet)["auc_library"]
        struck = analyse(df[~mod].reset_index(drop=True), tag, quiet)["auc_library"]
        rows.append(dict(
            arm=tag, width=(np.inf if width is None else width), ceff=float(sol.total),
            n_eff=float(1.0 / np.sum(eps.to_numpy() ** 2)),
            n_responders=int(keep.sum()),
            responder_decades=float(np.ptp(np.log10(df.score.to_numpy()[keep]))),
            auc=full["ecspr"], auc_p=full["ecspr_p"], auc_size=full["size"],
            auc_no_module=struck["ecspr"], auc_no_module_p=struck["ecspr_p"],
            auc_no_module_size=struck["size"]))
        print(f"  {tag:>9}: C_eff={sol.total:.6g} AUC={rows[-1]['auc']:.4f} "
              f"(size {rows[-1]['auc_size']:.4f}) no-module={rows[-1]['auc_no_module']:.4f} "
              f"(size {rows[-1]['auc_no_module_size']:.4f}) responders="
              f"{rows[-1]['n_responders']} spanning "
              f"{rows[-1]['responder_decades']:.2f} decades", file=sys.stderr)
        if width is None:
            first = (ratios, df)

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, sep="\t", index=False)
    print(f"\nwrote {out}", file=sys.stderr)

    if a.validate and first is not None:
        v = validate(pairs, base_w, first[0], first[1], n=a.validate)
        ok = v.dropna()
        r = float(np.corrcoef(ok.first_order, ok.exact)[0, 1]) if len(ok) > 2 else np.nan
        rho = float(ok.first_order.corr(ok.exact, method="spearman")) if len(ok) > 2 else np.nan
        vout = out.with_name("guard_panel_validation.tsv")
        v.to_csv(vout, sep="\t", index=False)
        print(f"[guard] first order vs exact on {len(ok)} clones: pearson {r:.6f}, "
              f"spearman {rho:.6f}\nwrote {vout}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
