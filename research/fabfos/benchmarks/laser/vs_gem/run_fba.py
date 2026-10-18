#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bridge  # noqa: E402
import common as C  # noqa: E402
import fba_scaffold as FS  # noqa: E402
import resolve_names as RN  # noqa: E402
import run_arms as RA  # noqa: E402

LOG = logging.getLogger("run_fba")
CARBON_BUDGET = 60.0

FAILURES = ("medium_unresolved", "target_unresolved", "infeasible_base",
            "no_growth_pert", "edits_noop", "adds_all_dead",
            "target_structurally_absent", "target_zero_both", "target_unbounded",
            "hit_bound", "ok")


def native_map(host_dir: str) -> dict:
    g = C.read_gpr(C.RUNS / host_dir / "gpr" / "gpr_gem.parquet")
    out = {}
    for mnxr, ev in zip(g.mnxr.astype(str), g.intermediate_id.astype(str)):
        out.setdefault(mnxr, set()).add(ev)
    return {k: sorted(v) for k, v in out.items()}


def build_scaffold(host_dir: str, panel: list, add_universe: list, prop: pd.DataFrame):
    import cobra
    model = cobra.io.load_json_model(str(bridge.MODELS[host_dir]))
    rxn_ids = {r.id for r in model.reactions}
    mb = FS.MetBridge(model, prop)
    nat = native_map(host_dir)

    diag = dict(host=host_dir, n_panel=len(panel), n_add_universe=len(add_universe))

    new = []

    need = [r for r in add_universe if r not in nat]
    rp = FS.load_reac_prop(set(need))
    het, het_state, orphan = {}, {}, {}
    for mnxr in need:
        info = rp.get(mnxr)
        stoich = FS.parse_mnx_equation(info["equation"]) if info else None
        if not stoich:
            het_state[mnxr] = "unmappable"
            continue
        mets, created = {}, []
        for mnxm, coef in stoich.items():
            met, tier = mb.get(mnxm, create=True)
            if tier == "created":
                created.append(mnxm)
            mets[met] = mets.get(met, 0.0) + coef
        mets = {k: v for k, v in mets.items() if v != 0.0}
        if not mets:
            het_state[mnxr] = "degenerate"
            continue
        rid = FS.HET_PREFIX + mnxr
        r = cobra.Reaction(rid, name=f"heterologous {mnxr}",
                           lower_bound=0.0, upper_bound=0.0)
        r.add_metabolites(mets)
        new.append(r)
        het[mnxr] = rid
        het_state[mnxr] = "inserted"
        if created:
            orphan[mnxr] = created

    demand, target_state = {}, {}
    for m in panel:
        met, tier = mb.get(m, create=False)
        if met is None:
            target_state[m] = "target_structurally_absent"
            continue
        rid = FS.DEMAND_PREFIX + m
        if rid not in rxn_ids:
            r = cobra.Reaction(rid, name=f"benchmark demand {m}",
                               lower_bound=0.0, upper_bound=0.0)
            r.add_metabolites({met: -1.0})
            new.append(r)
        demand[m] = rid
        target_state[m] = "ok"

    model.add_reactions(new)

    diag.update(
        n_native_add=len(add_universe) - len(need),
        n_novel_add=len(need),
        n_inserted=len(het),
        n_unmappable=sum(1 for v in het_state.values() if v != "inserted"),
        n_reactions_with_created_metabolites=len(orphan),
        n_created_metabolites=len({m for v in orphan.values() for m in v}),
        n_demand=len(demand),
        n_target_absent=sum(1 for v in target_state.values()
                            if v == "target_structurally_absent"),
        bridge_tiers=dict(mb.tiers),
    )
    return model, mb, nat, demand, het, target_state, het_state, diag


AA_BIGG = ["ala__L", "arg__L", "asn__L", "asp__L", "cys__L", "glu__L", "gln__L",
           "gly", "his__L", "ile__L", "leu__L", "lys__L", "met__L", "phe__L",
           "pro__L", "ser__L", "thr__L", "trp__L", "tyr__L", "val__L"]
BASE_BIGG = ["ade", "adn", "cytd", "csn", "gua", "gsn", "hxan", "ins", "thym",
             "thymd", "ura", "uri", "xan", "xtsn", "din", "dad_2", "dcyt"]


def media_sets(model) -> tuple[dict, dict]:
    ex = {r.id for r in model.reactions}
    m9 = {k: v for k, v in model.medium.items() if not k.startswith("EX_glc")}
    lb = dict(m9)
    absent = []
    for b in AA_BIGG + BASE_BIGG:
        rid = f"EX_{b}_e"
        if rid in ex:
            lb[rid] = 10.0
        else:
            absent.append(rid)
    return m9, lb, absent


def carbon_exchanges(model, mnxms: list, prop_formula: dict) -> tuple[dict, list]:
    br = bridge.bigg_bridge()
    bigg_by_mnxm = br.groupby("mnxm").bigg.apply(list).to_dict()
    ex = {r.id for r in model.reactions}
    found, missing = [], []
    for m in mnxms:
        rid = None
        for b in bigg_by_mnxm.get(m, []):
            if f"EX_{b}_e" in ex:
                rid = f"EX_{b}_e"
                break
        if rid is None:
            missing.append(m)
            continue
        f = prop_formula.get(m) or ""
        nc = 0
        import re as _re
        mm = _re.search(r"C(\d*)", str(f))
        if mm:
            nc = int(mm.group(1)) if mm.group(1) else 1
        found.append((rid, max(1, nc)))
    if not found:
        return {}, missing
    per = CARBON_BUDGET / len(found)
    return {rid: per / nc for rid, nc in found}, missing


def run_host(host_dir: str, alpha: float, n_cf: int, run_id: str, flush: int,
             dry_run: bool):
    import cobra  # noqa: F401
    prop = pd.read_parquet(RN.PROP_CACHE)
    prop_formula = {k: ("" if pd.isna(v) else str(v))
                    for k, v in prop.set_index("mnxm").formula.to_dict().items()}

    idx = pd.read_csv(C.REFS / "design_index.tsv", sep="\t")
    panel = sorted({m for s in idx.target_mnxms.fillna("")
                    for m in str(s).split(";") if m})
    eu = pd.read_csv(C.REFS / "edit_universe.tsv", sep="\t")
    add_universe = sorted(eu[eu.kind == "add"].mnxr)

    t0 = time.time()
    model, mb, nat, demand, het, tstate, hstate, diag = build_scaffold(
        host_dir, panel, add_universe, prop)
    LOG.info("scaffold built in %.1fs", time.time() - t0)

    m9, lb, absent_ex = media_sets(model)
    diag["media_exchanges_absent"] = absent_ex
    mm = pd.read_csv(C.REFS / "media_map.tsv", sep="\t", comment="#")
    media_base = dict(zip(mm.medium, mm.base))
    media_status = dict(zip(mm.medium, mm.status))

    designs = RA.load_designs(host_dir, n_cf)
    tok = RA.carbon_terminals()

    real = idx[idx.host_dir == host_dir]
    diag["n_designs"] = len(designs)
    diag["n_real"] = len(real)
    diag["n_topology_unchanged"] = int(real.topology_unchanged.sum())
    diag["n_all_native_add"] = int(real.all_native_add.sum())
    diag["n_medium_unresolved"] = int(
        (~real.medium.fillna("").isin(media_base)).sum())
    (C.OUT / f"fba_scaffold_{host_dir}.json").write_text(json.dumps(diag, indent=1))
    LOG.info("scaffold diagnostics:\n%s", json.dumps(diag, indent=1))
    if dry_run:
        return

    with model:
        model.medium = dict(m9, EX_glc__D_e=10.0)
        mu_gate = model.slim_optimize()
    LOG.info("scaffold base growth on M9+glucose: %.6f", mu_gate)

    bio = bridge.biomass_reaction(bridge.load_model_json(host_dir))["id"]
    biomass = model.reactions.get_by_id(bio)

    shard = C.CACHE / f"pred_{host_dir}__fba_a{alpha}__C__dir.parquet"
    scal = C.CACHE / f"scalars_{host_dir}__fba_a{alpha}__C__dir.parquet"
    done, prev, prev_s = set(), None, None
    if shard.exists():
        prev = pd.read_parquet(shard)
        done = set(prev.design_id.unique())
        prev_s = pd.read_parquet(scal) if scal.exists() else None
        LOG.info("resuming: %d designs on disk", len(done))

    base_cache: dict = {}

    def env_for(row):
        med = row.medium if isinstance(row.medium, str) else ""
        base = media_base.get(med, "M9")
        status = media_status.get(med, "fallback")
        exs = dict(lb if base == "LB" else m9)
        _, mnxms = RA.source_for(row.carbon, tok)
        cex, missing = carbon_exchanges(model, mnxms, prop_formula)
        exs.update(cex)
        return exs, base, status, bool(cex), missing

    def base_profile(key, exs):
        if key in base_cache:
            return base_cache[key]
        prof = {}
        with model:
            model.medium = exs
            mu = model.slim_optimize()
            if not np.isfinite(mu):
                base_cache[key] = (np.nan, {})
                return base_cache[key]
            biomass.lower_bound = alpha * mu
            for m, rid in demand.items():
                r = model.reactions.get_by_id(rid)
                r.upper_bound = FS.BIG
                model.objective = r
                v = model.slim_optimize()
                prof[m] = float(v) if np.isfinite(v) else np.nan
                r.upper_bound = 0.0
        base_cache[key] = (float(mu), prof)
        return base_cache[key]

    rows, srows = ([prev] if prev is not None else []), \
                  ([prev_s] if prev_s is not None else [])
    buf, sbuf = [], []

    def flush_now():
        for frames, extra, path in ((rows, buf, shard), (srows, sbuf, scal)):
            allf = frames + ([pd.DataFrame(extra)] if extra else [])
            if not allf:
                continue
            tmp = path.with_suffix(".tmp.parquet")
            pd.concat(allf, ignore_index=True).to_parquet(tmp, index=False)
            os.replace(tmp, path)

    t_start, n_run = time.time(), 0
    for _, d in designs.iterrows():
        if d.design_id in done:
            continue
        exs, mbase, mstatus, has_carbon, miss_c = env_for(d)
        key = (mbase, tuple(sorted(exs.items())))
        if not has_carbon:
            sbuf.append(dict(run_id=run_id, arm=f"fba_a{alpha}", host=host_dir,
                             design_id=d.design_id, status="medium_unresolved",
                             medium=d.medium, carbon=d.carbon))
            continue
        mu_base, prof = base_profile(key, exs)
        if not np.isfinite(mu_base):
            sbuf.append(dict(run_id=run_id, arm=f"fba_a{alpha}", host=host_dir,
                             design_id=d.design_id, status="infeasible_base",
                             medium=d.medium, carbon=d.carbon))
            continue

        t1 = time.time()
        census = {k: 0 for k in ("native_add", "inserted_add", "unmappable_add",
                                 "knockout", "del_noop")}
        with model:
            model.medium = exs
            for r in d.add_list:
                if r in nat:
                    census["native_add"] += 1
                elif r in het:
                    rr = model.reactions.get_by_id(het[r])
                    rr.lower_bound, rr.upper_bound = -FS.BIG, FS.BIG
                    census["inserted_add"] += 1
                else:
                    census["unmappable_add"] += 1
            for r in d.del_list:
                ids = nat.get(r, [])
                hit = 0
                for rid in ids:
                    if rid in model.reactions:
                        rr = model.reactions.get_by_id(rid)
                        rr.lower_bound = rr.upper_bound = 0.0
                        hit += 1
                census["knockout" if hit else "del_noop"] += 1

            mu_pert = model.slim_optimize()
            if not np.isfinite(mu_pert) or mu_pert < alpha * mu_base:
                sbuf.append(dict(run_id=run_id, arm=f"fba_a{alpha}", host=host_dir,
                                 design_id=d.design_id, status="no_growth_pert",
                                 mu_base=mu_base, mu_pert=float(mu_pert)
                                 if np.isfinite(mu_pert) else np.nan,
                                 medium=d.medium, carbon=d.carbon, **census))
                n_run += 1
                continue
            biomass.lower_bound = alpha * mu_base
            for m, rid in demand.items():
                r = model.reactions.get_by_id(rid)
                r.upper_bound = FS.BIG
                model.objective = r
                v = model.slim_optimize()
                v = float(v) if np.isfinite(v) else np.nan
                r.upper_bound = 0.0
                b = prof.get(m, np.nan)
                st = tstate.get(m, "ok")
                if st != "ok":
                    val = np.nan
                elif not np.isfinite(v) or not np.isfinite(b):
                    st, val = "target_unbounded", np.nan
                elif v >= FS.BIG - 1e-6 or b >= FS.BIG - 1e-6:
                    st, val = "hit_bound", v - b
                elif v == 0.0 and b == 0.0:
                    st, val = "target_zero_both", 0.0
                else:
                    val = v - b
                buf.append(dict(
                    run_id=run_id, arm=f"fba_a{alpha}", host=host_dir,
                    element="C", directed=True, design_id=d.design_id,
                    condition_id=d.condition_id, obs_id=d.obs_id,
                    is_counterfactual=bool(d.is_counterfactual),
                    source_record=d.source_record, n_add=int(d.n_add),
                    n_del=int(d.n_del), size_bin=d.size_bin,
                    composition=d.composition, mnxm=m, prediction=val,
                    prediction_kind="delta_flux", status=st, noise_floor=0.0,
                    base_value=b, pert_value=v, pct_rank=np.nan))

        sbuf.append(dict(run_id=run_id, arm=f"fba_a{alpha}", host=host_dir,
                         design_id=d.design_id, condition_id=d.condition_id,
                         is_counterfactual=bool(d.is_counterfactual), status="ok",
                         medium=d.medium, medium_base=mbase,
                         medium_status=mstatus, carbon=d.carbon,
                         mu_base=mu_base, mu_pert=float(mu_pert),
                         seconds=time.time() - t1, **census))
        n_run += 1
        if n_run % flush == 0:
            flush_now()
            el = time.time() - t_start
            LOG.info("%s %d designs (%.2fs/design, %.1f min)", host_dir, n_run,
                     el / n_run, el / 60)
    flush_now()
    LOG.info("FBA %s DONE: %d designs in %.1f min", host_dir, n_run,
             (time.time() - t_start) / 60)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="e_coli_k12")
    p.add_argument("--alpha", type=float, default=0.1)
    p.add_argument("--n-cf", type=int, default=C.POOL_N)
    p.add_argument("--flush", type=int, default=10)
    p.add_argument("--run-id", default="r1")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stderr),
                  logging.FileHandler(C.OUT / f"run_fba_{a.host}_a{a.alpha}.log")])
    run_host(a.host, a.alpha, a.n_cf, a.run_id, a.flush, a.dry_run)


if __name__ == "__main__":
    main()
