#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402

RNG = np.random.default_rng(11)
N_BOOT = 2000
SCORED_STATES = ("ok", "below_floor", "created", "destroyed", "not_in_base",
                 "target_zero_both", "hit_bound")


def load_predictions() -> pd.DataFrame:
    frames = []
    for p in sorted(C.CACHE.glob("pred_*.parquet")):
        d = pd.read_parquet(p)
        d["unit"] = p.stem[len("pred_"):]
        frames.append(d)
    if not frames:
        raise SystemExit("no prediction shards in cache/")
    df = pd.concat(frames, ignore_index=True)
    df["value"] = df.prediction.where(df.status != "not_in_base", 0.0)
    df.loc[~df.status.isin(SCORED_STATES), "value"] = np.nan
    return df


def load_scalars() -> pd.DataFrame:
    frames = []
    for p in sorted(C.CACHE.glob("scalars_*.parquet")):
        d = pd.read_parquet(p)
        d["unit"] = p.stem[len("scalars_"):]
        frames.append(d)
    if not frames:
        return pd.DataFrame()
    d = pd.concat(frames, ignore_index=True)
    return d.drop_duplicates(["unit", "design_id"], keep="last")


def design_index() -> pd.DataFrame:
    return pd.read_csv(C.REFS / "design_index.tsv", sep="\t")


def trivial_arm(units: list) -> pd.DataFrame:
    pairs = pd.read_parquet(C.ATOM_PAIRS, columns=["mnxr", "element", "product"])
    pairs = pairs[pairs.element == "C"]
    prod = pairs.groupby("mnxr")["product"].apply(set).to_dict()

    idx = design_index()
    panel = sorted({m for s in idx.target_mnxms.fillna("")
                    for m in str(s).split(";") if m})
    pool = pd.read_parquet(C.REFS / "counterfactual_pool.parquet")

    rows = []
    for host_dir, sub in idx.groupby("host_dir"):
        designs = [(r.design_id, r.condition_id, r.obs_id, False, r.source_record,
                    r.n_add, r.n_del, r.size_bin, r.composition,
                    C.split_mnxr(str(r.add_mnxr)))
                   for r in sub.itertuples(index=False)]
        designs += [(p.design_id, p.design_id, "", True, "", p.n_add, p.n_del,
                     p.size_bin, p.composition, C.split_mnxr(str(p.add_mnxr)))
                    for p in pool.itertuples(index=False)]
        for (did, cid, oid, cf, rec, na, nd, sb, comp, adds) in designs:
            made = set()
            for a in adds:
                made |= prod.get(a, set())
            for m in panel:
                rows.append(dict(
                    run_id="trivial", arm="trivial", host=host_dir, element="C",
                    directed=True, design_id=did, condition_id=cid, obs_id=oid,
                    is_counterfactual=cf, source_record=rec, n_add=na, n_del=nd,
                    size_bin=sb, composition=comp, mnxm=m,
                    prediction=1.0 if m in made else 0.0,
                    prediction_kind="is_product_of_added_reaction", status="ok",
                    noise_floor=0.0, base_value=np.nan, pert_value=np.nan,
                    pct_rank=np.nan, unit=f"{host_dir}__trivial__C__dir"))
    df = pd.DataFrame(rows)
    df["value"] = df.prediction
    return df


def studentised(obs: float, null: np.ndarray, na, nd, null_na, null_nd,
                tail: str = "greater"):
    ok = np.isfinite(null)
    if ok.sum() < 20:
        return np.nan, np.nan
    X = np.column_stack([np.ones(ok.sum()), null_na[ok], null_nd[ok]])
    y = null[ok]
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    sd = resid.std(ddof=3)
    if not np.isfinite(sd) or sd == 0:
        return np.nan, np.nan
    obs_r = obs - np.array([1.0, na, nd]) @ beta
    return float(obs_r / sd), C.empirical_p(obs_r, resid, tail)


def counterfactual_null(pred: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    tmap = {r.design_id: [m for m in str(r.target_mnxms).split(";") if m]
            for r in idx.itertuples(index=False)}
    meta = idx.set_index("design_id")
    out, abstained = [], []
    for unit, u in pred.groupby("unit"):
        cf = u[u.is_counterfactual]
        real = u[~u.is_counterfactual]
        by_m = {m: g for m, g in cf.groupby("mnxm")}
        for did, g in real.groupby("design_id"):
            tgts = tmap.get(did, [])
            if not tgts:
                continue
            row_meta = meta.loc[did] if did in meta.index else None
            if row_meta is None:
                continue
            direction = str(row_meta.measured)
            tail = "less" if direction == "down" else "greater"
            gm = g.set_index("mnxm")
            for t in tgts:
                if t not in gm.index:
                    continue
                obs = gm.loc[t, "value"]
                if isinstance(obs, pd.Series):
                    obs = float(obs.iloc[0])
                st0 = gm.loc[t, "status"]
                if isinstance(st0, pd.Series):
                    st0 = st0.iloc[0]
                if not np.isfinite(obs):
                    continue
                if st0 == "not_in_base":
                    abstained.append(dict(unit=unit, arm=u.arm.iloc[0],
                                          design_id=did, mnxm=t,
                                          target=row_meta.target))
                    continue
                cg = by_m.get(t)
                if cg is None or cg.empty:
                    continue
                same = cg[(cg.size_bin == row_meta.size_bin) &
                          (cg.composition == row_meta.composition)]
                if len(same) < 15:
                    same = cg[cg.size_bin == row_meta.size_bin]
                if len(same) < 15:
                    same = cg
                nullv = same.value.to_numpy(float)
                p_bin = C.empirical_p(obs, nullv, tail)
                allv = cg.value.to_numpy(float)
                z, p_res = studentised(obs, allv, row_meta.n_add, row_meta.n_del,
                                       cg.n_add.to_numpy(float),
                                       cg.n_del.to_numpy(float), tail)
                st = gm.loc[t, "status"]
                if isinstance(st, pd.Series):
                    st = st.iloc[0]
                out.append(dict(
                    unit=unit, arm=u.arm.iloc[0], host=u.host.iloc[0],
                    element=u.element.iloc[0], directed=bool(u.directed.iloc[0]),
                    design_id=did, source_record=row_meta.source_record,
                    mnxm=t, target=row_meta.target, measured=direction,
                    measurement_type=row_meta.measurement_type,
                    magnitude=row_meta.magnitude,
                    observed=float(obs), status=st,
                    n_null_bin=len(same), n_null_all=len(cg),
                    null_median=float(np.nanmedian(nullv)) if len(nullv) else np.nan,
                    null_p95=float(np.nanpercentile(nullv, 95)) if len(nullv) else np.nan,
                    p_bin=p_bin, z_resid=z, p_resid=p_res,
                    size_bin=row_meta.size_bin, composition=row_meta.composition,
                    n_add=int(row_meta.n_add), n_del=int(row_meta.n_del),
                    no_heterologous_add=bool(row_meta.no_heterologous_add),
                    deletion_only=bool(row_meta.deletion_only),
                    topology_unchanged=bool(row_meta.topology_unchanged),
                    panel=("C" if st in ("created",) else "R"),
                    is_probe=bool(row_meta.is_probe)))
    pd.DataFrame(abstained).to_csv(C.OUT / "abstentions.tsv", sep="\t", index=False)
    return pd.DataFrame(out)


def paper_bootstrap(df: pd.DataFrame, stat, n=N_BOOT) -> tuple:
    papers = df.source_record.fillna("").unique()
    if len(papers) < 2:
        return (np.nan, np.nan)
    by = {p: g for p, g in df.groupby(df.source_record.fillna(""))}
    vals = []
    for _ in range(n):
        pick = RNG.choice(papers, size=len(papers), replace=True)
        s = stat(pd.concat([by[p] for p in pick], ignore_index=True))
        if np.isfinite(s):
            vals.append(s)
    if not vals:
        return (np.nan, np.nan)
    return (float(np.percentile(vals, 2.5)), float(np.percentile(vals, 97.5)))


def headline(nulls: pd.DataFrame, abst: pd.DataFrame) -> pd.DataFrame:
    rows = []
    nabs = (abst.groupby("unit").size().to_dict() if len(abst) else {})

    def frac_sig(g, col="p_bin"):
        q, sig = C.bh_fdr(g[col].to_numpy(float))
        return float(np.mean(sig)) if len(sig) else np.nan

    for (unit, arm, host, element, directed), g in nulls.groupby(
            ["unit", "arm", "host", "element", "directed"]):
        lo, hi = paper_bootstrap(g, lambda x: frac_sig(x, "p_resid"))
        lo_m, hi_m = paper_bootstrap(
            g, lambda x: float(np.nanmedian(x.p_resid)))
        _, sig = C.bh_fdr(g.p_bin.to_numpy(float))
        _, sigr = C.bh_fdr(g.p_resid.to_numpy(float))
        nb = float(np.nanmedian(g.n_null_bin))
        rows.append(dict(
            unit=unit, arm=arm, host=host, element=element, directed=directed,
            n_conditions=g.design_id.nunique(), n_cells=len(g),
            n_abstained_cells=int(nabs.get(unit, 0)),
            n_papers=g.source_record.nunique(),
            frac_sig=float(np.mean(sigr)), ci_lo=lo, ci_hi=hi,
            median_p=float(np.nanmedian(g.p_resid)),
            median_p_ci_lo=lo_m, median_p_ci_hi=hi_m,
            frac_p_lt_05=float(np.nanmean(g.p_resid < 0.05)),
            frac_sig_binned=float(np.mean(sig)),
            median_p_binned=float(np.nanmedian(g.p_bin)),
            frac_p_lt_05_binned=float(np.nanmean(g.p_bin < 0.05)),
            median_n_null_bin=nb,
            bh_resolution_floor=1.0 / (nb + 1) if np.isfinite(nb) else np.nan,
            bh_threshold_k1=0.05 / max(1, len(g)),
        ))
    return pd.DataFrame(rows).sort_values("frac_sig", ascending=False)


def matched_head_to_head(nulls: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for host, hg in nulls.groupby("host"):
        triv = hg[hg.arm == "trivial"]
        if triv.empty:
            continue
        tkey = triv.set_index(["design_id", "mnxm"]).p_resid
        for unit, g in hg[hg.arm != "trivial"].groupby("unit"):
            gk = g.set_index(["design_id", "mnxm"])
            common = gk.index.intersection(tkey.index)
            if len(common) < 10:
                continue
            a = gk.loc[common]
            b = tkey.loc[common]
            _, sa = C.bh_fdr(a.p_resid.to_numpy(float))
            _, sb = C.bh_fdr(b.to_numpy(float))
            rows.append(dict(
                unit=unit, arm=g.arm.iloc[0], host=host, n_matched=len(common),
                arm_frac_sig=float(np.mean(sa)),
                trivial_frac_sig=float(np.mean(sb)),
                margin=float(np.mean(sa) - np.mean(sb)),
                arm_median_p=float(np.nanmedian(a.p_resid)),
                trivial_median_p=float(np.nanmedian(b)),
                frac_arm_better=float(np.nanmean(
                    a.p_resid.to_numpy(float) < b.to_numpy(float))),
                frac_tied=float(np.nanmean(
                    a.p_resid.to_numpy(float) == b.to_numpy(float)))))
    out = pd.DataFrame(rows).sort_values("margin", ascending=False)
    out.to_csv(C.OUT / "T14_matched_vs_trivial.tsv", sep="\t", index=False)
    return out


def strata_table(nulls: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (unit, arm), g in nulls.groupby(["unit", "arm"]):
        for name, mask in (("all", np.ones(len(g), bool)),
                           ("no_heterologous_add", g.no_heterologous_add.to_numpy()),
                           ("has_heterologous_add", ~g.no_heterologous_add.to_numpy()),
                           ("deletion_only", g.deletion_only.to_numpy()),
                           ("topology_unchanged", g.topology_unchanged.to_numpy()),
                           ("panel_R", (g.panel == "R").to_numpy()),
                           ("panel_C", (g.panel == "C").to_numpy())):
            sub = g[mask]
            if sub.empty:
                continue
            _, sig = C.bh_fdr(sub.p_resid.to_numpy(float))
            rows.append(dict(unit=unit, arm=arm, stratum=name, n=len(sub),
                             frac_sig=float(np.mean(sig)),
                             median_p=float(np.nanmedian(sub.p_resid)),
                             frac_p_lt_05=float(np.nanmean(sub.p_resid < 0.05))))
    return pd.DataFrame(rows)


def auroc(pos: np.ndarray, neg: np.ndarray) -> float:
    pos, neg = pos[np.isfinite(pos)], neg[np.isfinite(neg)]
    if pos.size == 0 or neg.size == 0:
        return np.nan
    allv = np.concatenate([pos, neg])
    r = pd.Series(allv).rank().to_numpy()
    return float((r[:pos.size].sum() - pos.size * (pos.size + 1) / 2) /
                 (pos.size * neg.size))


def off_diagonal(pred: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    tmap = {r.design_id: set(m for m in str(r.target_mnxms).split(";") if m)
            for r in idx.itertuples(index=False)}
    rows = []
    for unit, u in pred[~pred.is_counterfactual].groupby("unit"):
        for m, g in u.groupby("mnxm"):
            is_pos = g.design_id.map(lambda d: m in tmap.get(d, set())).to_numpy()
            if is_pos.sum() == 0 or (~is_pos).sum() == 0:
                continue
            v = g.value.to_numpy(float)
            rows.append(dict(unit=unit, arm=u.arm.iloc[0], mnxm=m,
                             n_pos=int(is_pos.sum()), n_neg=int((~is_pos).sum()),
                             auroc=auroc(v[is_pos], v[~is_pos])))
    return pd.DataFrame(rows)


def spearman(x, y) -> tuple:
    x, y = np.asarray(x, float), np.asarray(y, float)
    ok = np.isfinite(x) & np.isfinite(y)
    if ok.sum() < 5:
        return np.nan, 0
    rx = pd.Series(x[ok]).rank().to_numpy()
    ry = pd.Series(y[ok]).rank().to_numpy()
    if rx.std() == 0 or ry.std() == 0:
        return np.nan, int(ok.sum())
    return float(np.corrcoef(rx, ry)[0, 1]), int(ok.sum())


def secondary(nulls: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (unit, arm), g in nulls.groupby(["unit", "arm"]):
        sgn = g[g.status.isin(("ok", "created", "destroyed"))]
        pred_up = sgn.observed > 0
        want_up = sgn.measured == "up"
        agree = float((pred_up == want_up).mean()) if len(sgn) else np.nan
        const = float(want_up.mean()) if len(sgn) else np.nan
        perm = [float((RNG.permutation(pred_up.to_numpy()) == want_up.to_numpy()).mean())
                for _ in range(1000)] if len(sgn) else [np.nan]
        fold = g[(g.measurement_type == "fold") & g.status.isin(("ok", "created"))]
        rho, n = spearman(fold.observed, np.log2(
            pd.to_numeric(fold.magnitude, errors="coerce").clip(lower=1e-6)))
        rows.append(dict(unit=unit, arm=arm, n_sign=len(sgn),
                         sign_agreement=agree, constant_baseline=const,
                         sign_perm_p=float(np.mean(np.array(perm) >= agree))
                         if len(sgn) else np.nan,
                         spearman_fold=rho, n_fold=n))
    return pd.DataFrame(rows)


def ecspr_vs_fba(pred: pd.DataFrame) -> pd.DataFrame:
    fba_units = [u for u in pred.unit.unique() if "__fba_a0.1__" in u]
    ec_units = [u for u in pred.unit.unique()
                if "__fba_" not in u and "__trivial__" not in u]
    rows = []
    for fu in fba_units:
        host = fu.split("__")[0]
        f = pred[(pred.unit == fu)][["design_id", "mnxm", "value"]].rename(
            columns={"value": "flux"})
        for eu in ec_units:
            if not eu.startswith(host):
                continue
            e = pred[pred.unit == eu][["design_id", "mnxm", "value"]].rename(
                columns={"value": "draw"})
            j = e.merge(f, on=["design_id", "mnxm"], how="inner").dropna()
            if len(j) < 50:
                continue
            colwise = [spearman(g.draw, g.flux)[0] for _, g in j.groupby("mnxm")
                       if len(g) >= 20]
            rowwise = [spearman(g.draw, g.flux)[0] for _, g in j.groupby("design_id")
                       if len(g) >= 10]
            rho, n = spearman(j.draw, j.flux)
            rows.append(dict(
                ecspr_unit=eu, fba_unit=fu, n_cells=n, pooled_spearman=rho,
                colwise_median=float(np.nanmedian(colwise)) if colwise else np.nan,
                colwise_n=len(colwise),
                rowwise_median=float(np.nanmedian(rowwise)) if rowwise else np.nan,
                rowwise_n=len(rowwise)))
    return pd.DataFrame(rows)


def biomass_panel(scal: pd.DataFrame) -> pd.DataFrame:
    fba = scal[scal.arm.astype(str).str.startswith("fba_a0.1") &
               (scal.status == "ok")][["design_id", "host", "mu_base", "mu_pert"]]
    fba = fba.assign(d_mu=fba.mu_pert - fba.mu_base)
    rows = []
    ec = scal[scal.get("prec_share_pert", pd.Series(dtype=float)).notna()] \
        if "prec_share_pert" in scal else pd.DataFrame()
    for unit, g in (ec.groupby("unit") if len(ec) else []):
        g = g.assign(d_prec=g.prec_share_pert - g.prec_share_base)
        j = g[["design_id", "host", "d_prec"]].merge(fba, on=["design_id", "host"])
        if len(j) < 5:
            continue
        rho, n = spearman(j.d_prec, j.d_mu)
        rows.append(dict(unit=unit, n=n, spearman_delta=rho,
                         spearman_level=spearman(
                             g.set_index("design_id").prec_share_pert.reindex(
                                 j.design_id), j.mu_pert)[0]))
        j.to_csv(C.OUT / f"biomass_scatter_{unit}.tsv", sep="\t", index=False)
    return pd.DataFrame(rows)


def main():
    pred = load_predictions()
    idx = design_index()
    triv = trivial_arm(sorted(pred.unit.unique()))
    pred = pd.concat([pred, triv], ignore_index=True)
    scal = load_scalars()

    print(f"predictions: {len(pred):,} rows over {pred.unit.nunique()} units")
    print(pred.groupby("unit").design_id.nunique().to_string())

    nulls = counterfactual_null(pred, idx)
    nulls.to_csv(C.OUT / "counterfactual_nulls.tsv", sep="\t", index=False)

    abst = (pd.read_csv(C.OUT / "abstentions.tsv", sep="\t")
            if (C.OUT / "abstentions.tsv").stat().st_size > 1 else pd.DataFrame())
    h = headline(nulls, abst)
    h.to_csv(C.OUT / "T2_headline.tsv", sep="\t", index=False)
    strata_table(nulls).to_csv(C.OUT / "T3_strata.tsv", sep="\t", index=False)
    mh = matched_head_to_head(nulls)
    od = off_diagonal(pred, idx)
    od.to_csv(C.OUT / "T4_off_diagonal_auroc.tsv", sep="\t", index=False)
    secondary(nulls).to_csv(C.OUT / "T5_secondary.tsv", sep="\t", index=False)
    ecspr_vs_fba(pred).to_csv(C.OUT / "T6_method_vs_method.tsv", sep="\t", index=False)
    if len(scal):
        scal.to_parquet(C.OUT / "scalars_all.parquet", index=False)
        biomass_panel(scal).to_csv(C.OUT / "T7_biomass.tsv", sep="\t", index=False)

    print("\n== T14 matched against the trivial baseline ==")
    print(mh.to_string(index=False))
    print("\n== HEADLINE: fraction of conditions significant at q<0.05 ==")
    print(h.to_string(index=False))
    print("\nwrote out/*.tsv")


if __name__ == "__main__":
    main()
