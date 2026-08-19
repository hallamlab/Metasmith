from __future__ import annotations

import numpy as np
import pandas as pd

DIAG_PREFIX = "_"
KEY = ("probe", "orientation", "element", "readout")


def _measurements(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df.readout.astype(str).str.startswith(DIAG_PREFIX)].copy()


def _bh(p: np.ndarray) -> np.ndarray:
    n = p.size
    if n == 0:
        return p
    order = np.argsort(p)
    ranked = p[order] * n / (np.arange(n) + 1)
    q = np.minimum.accumulate(ranked[::-1])[::-1]
    out = np.empty_like(q)
    out[order] = np.minimum(q, 1.0)
    return out


def _stats(obs: np.ndarray, null: np.ndarray) -> dict:
    m = null.size
    if m == 0:
        nan = np.full(obs.shape, np.nan)
        return dict(null_n=0, null_mean=np.nan, null_sd=np.nan,
                    z=nan, p_emp=nan, pct_rank=nan, p_floor=np.nan)
    mu, sd = float(np.mean(null)), float(np.std(null, ddof=1)) if m > 1 else 0.0
    z = (obs - mu) / sd if sd > 0 else np.where(obs == mu, 0.0, np.inf) * np.sign(obs - mu)
    # Two-sided, and by |deviation from the null's own centre| rather than by
    # |value|: fed and starved are both departures, and a one-tailed count would
    # call one of them uninteresting by construction.
    dev_null = np.abs(null - mu)
    ge = (dev_null[None, :] >= np.abs(obs - mu)[:, None]).sum(axis=1)
    p = (ge + 1.0) / (m + 1.0)
    pct = (null[None, :] < obs[:, None]).sum(axis=1) / m
    return dict(null_n=m, null_mean=mu, null_sd=sd, z=z, p_emp=p, pct_rank=pct,
                p_floor=1.0 / (m + 1.0))


def score(observed: pd.DataFrame, null: pd.DataFrame, *, baseline: str,
          conditions=None) -> pd.DataFrame:
    obs = _measurements(observed)
    nul = _measurements(null)
    if baseline not in set(obs.condition_id):
        raise ValueError(f"baseline condition {baseline!r} is not in the observed "
                         f"results; it must be measured like any other row")

    base = (obs[obs.condition_id == baseline]
            .set_index(list(KEY))["value"].rename("baseline"))
    meta = {}
    if conditions:
        meta = {c.condition_id: dict(
            is_control=bool(c.meta.get("is_control")),
            stratum=(int(c.meta["n_units"]) if c.meta.get("n_units") is not None
                     and c.meta.get("n_units") == c.meta.get("n_units") else None))
            for c in conditions}

    obs = obs[obs.condition_id != baseline].join(base, on=list(KEY))
    obs["delta"] = obs.value - obs.baseline
    obs["is_control"] = [bool(meta.get(c, {}).get("is_control")) for c in obs.condition_id]
    obs["stratum"] = [meta.get(c, {}).get("stratum") for c in obs.condition_id]

    nul = nul.join(base, on=list(KEY))
    nul["delta"] = nul.value - nul.baseline
    nul_stratum = {c.condition_id: c.meta.get("stratum") for c in (conditions or [])}
    nul["stratum"] = [nul_stratum.get(c) for c in nul.condition_id]
    if nul.stratum.isna().all():
        # A pool drawn by `ecspr draw` names its stratum in the condition id, which
        # is what keeps size matching working when the null's own conditions table
        # was not passed alongside the results.
        nul["stratum"] = nul.condition_id.astype(str).str.extract(r":s(\d+):")[0]
        nul["stratum"] = pd.to_numeric(nul["stratum"], errors="coerce")

    out = []
    for key, g in obs.groupby(list(KEY), sort=False):
        pool = nul[(nul.probe == key[0]) & (nul.orientation == key[1])
                   & (nul.element == key[2]) & (nul.readout == key[3])]
        g = g.copy()
        st = _stats(g.delta.to_numpy(float), pool.delta.dropna().to_numpy(float))
        for c in ("null_n", "null_mean", "null_sd", "z", "p_emp", "pct_rank", "p_floor"):
            g[c] = st[c]
        # ...and again inside the size-matched stratum, which is the comparison the
        # size confound actually requires.
        zs, ps, ns = [], [], []
        for s, d in zip(g.stratum, g.delta):
            sub = pool[pool.stratum == s].delta.dropna().to_numpy(float) \
                if s is not None and s == s else np.zeros(0)
            r = _stats(np.array([float(d)]), sub)
            zs.append(np.ravel(r["z"])[0]); ps.append(np.ravel(r["p_emp"])[0])
            ns.append(r["null_n"])
        g["z_stratum"], g["p_stratum"], g["null_n_stratum"] = zs, ps, ns
        real = ~g.is_control.to_numpy(bool)
        q = np.full(len(g), np.nan)
        q[real] = _bh(g.p_emp.to_numpy(float)[real])
        g["q_bh"] = q
        out.append(g)

    if not out:
        return obs
    cols = ["condition_id", *KEY, "value", "baseline", "delta", "is_control", "stratum",
            "null_n", "null_mean", "null_sd", "z", "p_emp", "q_bh", "p_floor",
            "pct_rank", "z_stratum", "p_stratum", "null_n_stratum"]
    return pd.concat(out, ignore_index=True)[cols]


def gate(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for key, g in scored.groupby(list(KEY), sort=False):
        ctl = g[g.is_control].delta.dropna().to_numpy(float)
        rows.append(dict(zip(KEY, key)) | dict(
            n_controls=ctl.size,
            control_sd=float(np.std(ctl, ddof=1)) if ctl.size > 1 else float("nan"),
            control_max_abs=float(np.max(np.abs(ctl))) if ctl.size else float("nan"),
            null_n=int(g.null_n.iloc[0]), null_sd=float(g.null_sd.iloc[0]),
        ))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["null_over_control"] = df.null_sd / df.control_sd.replace(0.0, np.nan)
    return df


# What fraction of a response a reported set must account for. Responses partition the
# measurement (Tellegen; `build.reaction_elasticities` sums to 1), so "the members carrying
# 90% of it" is a set the network chooses rather than a threshold somebody picked.
RESPONSE_COVERAGE = 0.90


def responders(values, coverage: float = RESPONSE_COVERAGE) -> np.ndarray:
    """Boolean mask of the largest-|value| members that together carry ``coverage`` of the
    total. The complement is the FLOOR, and it is not a set of small measurements.

    A partition with an effective size near ten does not have four thousand members worth
    reporting, and reporting them anyway is what makes an ECSPr readout look unusable: on the
    4,102-clone ASKA library the whole distribution spans 7.7 decades between its 5th and
    95th percentiles, while the eight genes carrying 90% of it span 1.0 and the twenty-five
    carrying 99% span 2.5. Nothing about the network changed between those numbers -- only
    which members were read as measurements.

    So this is a reporting decision with a measured basis, not a cleanup. The floor keeps its
    own count and mass, which is what stops "the tail is small" from becoming "the tail is
    absent"; a member that leaves the responder set has not been shown to be zero.
    """
    v = np.abs(np.asarray(values, float))
    v = np.where(np.isfinite(v), v, 0.0)
    total = v.sum()
    if not total > 0:
        return np.zeros(v.shape, bool)
    order = np.argsort(v)[::-1]
    keep = np.zeros(v.shape, bool)
    keep[order[:int(np.searchsorted(np.cumsum(v[order]) / total, coverage) + 1)]] = True
    return keep
