#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "src"))
import common as C  # noqa: E402
import bridge  # noqa: E402
import ecspr.model.build as EB  # noqa: E402
import ecspr.model.evidence as EV  # noqa: E402
from ecspr.model.graph import Terminal, measure_leak  # noqa: E402

LOG = logging.getLogger("run_arms")
ARMS = ("gem", "denovo_ev", "denovo_uni")


def base_weights(arm: str, host_dir: str) -> tuple[dict, float]:
    if arm == "gem":
        g = C.read_gpr(C.RUNS / host_dir / "gpr" / "gpr_gem.parquet")
        w = {r: 1.0 for r in g.mnxr.astype(str).unique()}
        return w, 1.0
    src = C.RUNS / host_dir / "gpr" / "gpr_denovo.parquet"
    d = C.read_gpr(src)
    if arm == "denovo_uni":
        w = {r: 1.0 for r in d.mnxr.astype(str).unique()}
        return w, 1.0
    d["raw_score"] = d["raw_score"].astype("float64")
    e = EV.compute_E(d[["orf", "channel", "intermediate_id", "mnxr", "raw_score"]],
                     label=f"{host_dir}/denovo")
    w = {str(k): float(v) for k, v in e.items()}
    return w, float(np.median(list(w.values())))


EDIT_KINDS = ("heterologous_add", "overexpression", "knockout", "del_noop")


def apply_edits(weights: dict, add: list, dele: list, unit: float) -> tuple[dict, dict]:
    w = dict(weights)
    census = {k: 0 for k in EDIT_KINDS}
    for r in add:
        if r in w:
            w[r] = w[r] + unit
            census["overexpression"] += 1
        else:
            w[r] = unit
            census["heterologous_add"] += 1
    for r in dele:
        if r in w:
            del w[r]
            census["knockout"] += 1
        else:
            census["del_noop"] += 1
    return w, census


def load_designs(host_dir: str, n_cf: int) -> pd.DataFrame:
    idx = pd.read_csv(C.REFS / "design_index.tsv", sep="\t")
    real = idx[idx.host_dir == host_dir].copy()
    real["add_list"] = real.add_mnxr.fillna("").apply(C.split_mnxr)
    real["del_list"] = real.del_mnxr.fillna("").apply(C.split_mnxr)

    pool = pd.read_parquet(C.REFS / "counterfactual_pool.parquet").head(n_cf).copy()
    donor = real.set_index("design_id")
    fallback = real.iloc[0] if len(real) else None
    rows = []
    for _, p in pool.iterrows():
        d = donor.loc[p.donor_condition] if p.donor_condition in donor.index else fallback
        if d is None:
            continue
        rows.append(dict(
            design_id=p.design_id, condition_id=p.design_id, obs_id="",
            is_counterfactual=True, source_record="", host_dir=host_dir,
            host_gem=d.host_gem, carbon=d.carbon, medium=d.medium,
            add_list=C.split_mnxr(p.add_mnxr), del_list=C.split_mnxr(p.del_mnxr),
            n_add=int(p.n_add), n_del=int(p.n_del), total_edits=int(p.total_edits),
            size_bin=p.size_bin, composition=p.composition,
            target_mnxms="", donor_condition=p.donor_condition))
    cf = pd.DataFrame(rows)
    keep = ["design_id", "condition_id", "obs_id", "is_counterfactual",
            "source_record", "host_dir", "host_gem", "carbon", "medium",
            "add_list", "del_list", "n_add", "n_del", "total_edits", "size_bin",
            "composition", "target_mnxms"]
    real["donor_condition"] = ""
    return pd.concat([real[keep + ["donor_condition"]], cf], ignore_index=True)


def carbon_terminals() -> dict:
    car = pd.read_csv(C.REFS / "carbon_resolved.tsv", sep="\t").fillna("")
    tok = {r.token: [m for m in str(r.mnxms).split(";") if m] for _, r in car.iterrows()}
    return tok


def source_for(carbon: str, tok: dict) -> tuple[str, list]:
    if not isinstance(carbon, str) or not carbon.strip():
        return "", []
    parts = [t.strip() for t in carbon.split(",") if t.strip()]
    mnxms, labels = [], []
    for p in parts:
        m = tok.get(p, [])
        if m:
            mnxms.extend(m)
            labels.append(p)
    return "|".join(sorted(labels)), sorted(set(mnxms))


def run_unit(arm: str, host_dir: str, element: str, directed: bool, n_cf: int,
             leak: float, flush: int, run_id: str, do_biomass: bool):
    tag = f"{host_dir}__{arm}__{element}__{'dir' if directed else 'und'}"
    shard = C.CACHE / f"pred_{tag}.parquet"
    scal = C.CACHE / f"scalars_{tag}.parquet"

    done = set()
    prev = prev_s = None
    if shard.exists():
        prev = pd.read_parquet(shard)
        done = set(prev.design_id.unique())
        prev_s = pd.read_parquet(scal) if scal.exists() else None
        LOG.info("resuming %s: %d designs already on disk", tag, len(done))

    LOG.info("loading pairs")
    pairs = pd.read_parquet(C.ATOM_PAIRS)
    pairs = pairs[pairs.element == element]
    ratios = {}
    if directed:
        import verify_bake_join as VB
        ratios = VB.direction_ratios()
    LOG.info("pairs %d rows, %d direction ratios", len(pairs), len(ratios))

    w0, unit = base_weights(arm, host_dir)
    LOG.info("arm %s: %d base reactions, conductance unit %.6g", arm, len(w0), unit)

    designs = load_designs(host_dir, n_cf)
    tok = carbon_terminals()
    panel = sorted({m for s in
                    pd.read_csv(C.REFS / "design_index.tsv", sep="\t")
                    .target_mnxms.fillna("") for m in str(s).split(";") if m})
    prec = bridge.biomass_precursors(host_dir)[0] if do_biomass else []
    LOG.info("%d designs, %d target columns, %d biomass precursors",
             len(designs), len(panel), len(prec))

    base_cache: dict = {}

    def baseline(src_key, src_mnxms):
        if src_key in base_cache:
            return base_cache[src_key]
        g = EB.graph_from_pairs(pairs, element, w0, ratios)
        src = Terminal.merge(g, src_mnxms, label=src_key)
        t0 = time.time()
        res = measure_leak(g, src, None, leak=leak) if src else None
        floor = float("nan")
        if res is not None:
            rng = np.random.default_rng(7)
            wj = {k: v * (1.0 + 1e-10 * rng.standard_normal()) for k, v in w0.items()}
            gj = EB.graph_from_pairs(pairs, element, wj, ratios)
            sj = Terminal.merge(gj, src_mnxms, label=src_key)
            rj = measure_leak(gj, sj, None, leak=leak)
            d = np.array([abs(rj["draw"].get(m, 0.0) - res["draw"].get(m, 0.0))
                          for m in res["draw"]])
            floor = float(np.nanmax(d)) if d.size else float("nan")
        bio = (measure_leak(g, src, prec, leak=leak) if (res is not None and prec)
               else None)
        LOG.info("baseline[%s] built+solved in %.1fs; noise floor %.3g",
                 src_key, time.time() - t0, floor)
        base_cache[src_key] = (g, src, res, floor, bio)
        return base_cache[src_key]

    rows, srows = [], []
    if prev is not None:
        rows.append(prev)
    if prev_s is not None:
        srows.append(prev_s)
    buf, sbuf = [], []

    def flush_now():
        if not buf and not sbuf:
            return
        allrows = rows + ([pd.DataFrame(buf)] if buf else [])
        alls = srows + ([pd.DataFrame(sbuf)] if sbuf else [])
        for frames, path in ((allrows, shard), (alls, scal)):
            if not frames:
                continue
            tmp = path.with_suffix(".tmp.parquet")
            pd.concat(frames, ignore_index=True).to_parquet(tmp, index=False)
            os.replace(tmp, path)

    t_start = time.time()
    n_run = 0
    for i, d in designs.iterrows():
        if d.design_id in done:
            continue
        src_key, src_mnxms = source_for(d.carbon, tok)
        if not src_mnxms:
            sbuf.append(dict(run_id=run_id, arm=arm, host=host_dir, element=element,
                             directed=directed, design_id=d.design_id,
                             status="no_source", carbon=d.carbon))
            continue
        g0, src0, base, floor, bio0 = baseline(src_key, src_mnxms)
        if base is None:
            sbuf.append(dict(run_id=run_id, arm=arm, host=host_dir, element=element,
                             directed=directed, design_id=d.design_id,
                             status="source_absent_from_graph", carbon=d.carbon,
                             source_key=src_key))
            continue
        w, census = apply_edits(w0, list(d.add_list), list(d.del_list), unit)
        t0 = time.time()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            g = EB.graph_from_pairs(pairs, element, w, ratios)
            gsrc = Terminal.merge(g, src_mnxms, label=src_key)
            pert = measure_leak(g, gsrc, None, leak=leak)
            biop = (measure_leak(g, gsrc, prec, leak=leak)
                    if (prec and not d.is_counterfactual) else None)
        n_chol = sum(1 for c in caught if "holmod" in str(c.category) + str(c.message))
        dt = time.time() - t0

        deltas = np.array([pert["draw"].get(m, np.nan) - base["draw"].get(m, np.nan)
                           for m in sorted(pert["draw"])], float)
        order = np.argsort(np.argsort(np.nan_to_num(deltas, nan=-np.inf)))
        pct = {m: float(order[j]) / max(1, len(deltas) - 1)
               for j, m in enumerate(sorted(pert["draw"]))}

        for m in panel:
            b = base["draw"].get(m)
            p = pert["draw"].get(m)
            if b is None and p is None:
                status, val = "not_in_base", np.nan
            elif b is None:
                status, val = "created", float(p)
            elif p is None:
                status, val = "destroyed", -float(b)
            else:
                val = float(p) - float(b)
                status = "below_floor" if (np.isfinite(floor) and
                                           abs(val) <= 10 * floor) else "ok"
            buf.append(dict(
                run_id=run_id, arm=arm, host=host_dir, element=element,
                directed=directed, design_id=d.design_id,
                condition_id=d.condition_id, obs_id=d.obs_id,
                is_counterfactual=bool(d.is_counterfactual),
                source_record=d.source_record, n_add=int(d.n_add),
                n_del=int(d.n_del), size_bin=d.size_bin,
                composition=d.composition, mnxm=m, prediction=val,
                prediction_kind="delta_draw", status=status,
                noise_floor=floor, base_value=b, pert_value=p,
                pct_rank=pct.get(m, np.nan)))

        sbuf.append(dict(
            run_id=run_id, arm=arm, host=host_dir, element=element,
            directed=directed, design_id=d.design_id, condition_id=d.condition_id,
            is_counterfactual=bool(d.is_counterfactual), status="ok",
            carbon=d.carbon, source_key=src_key, seconds=dt,
            i_eff_base=base["total"], i_eff_pert=pert["total"],
            converged=bool(pert["converged"]), n_metabolites=pert["n_metabolites"],
            n_cholmod_warnings=n_chol, noise_floor=floor,
            prec_share_base=(bio0 or {}).get("prec_share", np.nan),
            prec_share_pert=(biop or {}).get("prec_share", np.nan),
            **{f"n_{k}": v for k, v in census.items()}))

        n_run += 1
        if n_run % flush == 0:
            flush_now()
            el = time.time() - t_start
            LOG.info("%s %d/%d designs (%.1fs/design, %.1f min elapsed)",
                     tag, n_run, len(designs) - len(done), el / n_run, el / 60)
    flush_now()
    LOG.info("%s DONE: %d designs in %.1f min", tag, n_run, (time.time() - t_start) / 60)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--arm", choices=ARMS, required=True)
    p.add_argument("--host", default="e_coli_k12")
    p.add_argument("--element", default="C")
    p.add_argument("--directed", type=int, default=1)
    p.add_argument("--n-cf", type=int, default=C.POOL_N)
    p.add_argument("--leak", type=float, default=1e-6)
    p.add_argument("--flush", type=int, default=10)
    p.add_argument("--biomass", type=int, default=0)
    p.add_argument("--run-id", default="r1")
    a = p.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stderr),
                  logging.FileHandler(C.OUT / f"run_arms_{a.arm}_{a.host}_"
                                              f"{'dir' if a.directed else 'und'}.log")])
    run_unit(a.arm, a.host, a.element, bool(a.directed), a.n_cf, a.leak, a.flush,
             a.run_id, bool(a.biomass))


if __name__ == "__main__":
    main()
