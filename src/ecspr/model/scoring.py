from __future__ import annotations

import numpy as np
import pandas as pd

from .tests import SEP

DIAG_PREFIX = "_"

# WHAT MAKES TWO ROWS THE SAME MEASUREMENT, and therefore what an observation is ranked
# against. `readout` alone is not it, and the gap is silent in both directions:
#
#   * every two-point test emits a readout called `total`, so N axes in one run collapse
#     into one pool and each observation is scored against the others' nulls;
#   * two axes can share a sink and differ only in their source -- eight of set4's 79 do --
#     so even naming the readout after the sink does not separate them.
#
# The terminals are the missing half, and they live on the tests table rather than in the
# results. `score` joins them on when a tests table is given, and refuses a run that needs
# them and has none.
AXIS = ("source_hub", "sink_hub")
KEY = ("probe", "orientation", "element") + AXIS + ("readout",)


def _measurements(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df.readout.astype(str).str.startswith(DIAG_PREFIX)].copy()


def _with_axis(df: pd.DataFrame, tests, side: str) -> pd.DataFrame:
    # The terminals a test declared, attached to every row it produced. Absent a tests
    # table there is exactly one safe reading -- that the whole run is one axis -- and it
    # is written in rather than assumed, so `KEY` has the same arity either way.
    if not tests:
        for c in AXIS:
            df[c] = ""
        return df
    axis = {t.test_id: (t.source_hub, SEP.join(t.sinks)) for t in tests}
    missing = sorted(set(df.test_id.astype(str)) - set(axis))
    if missing:
        raise ValueError(
            f"{len(missing)} {side} test(s) have results but no row in the tests "
            f"table(s), e.g. {missing[:3]}. Their terminals are unknown, so they cannot "
            f"be told apart from another axis measured in the same run")
    ids = df.test_id.astype(str)
    for i, c in enumerate(AXIS):
        df[c] = [axis[tid][i] for tid in ids]
    return df


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


def _baselines(obs: pd.DataFrame, tests, baseline) -> dict:
    """`{test_id: the test_id its deltas are taken against}`.

    THE COLUMN WINS, AND A DISAGREEMENT IS REFUSED. `read_against` is per test, so it
    can say two different things in one run -- two probes on one network have two
    baselines -- and a single `--baseline` flag cannot express that. The flag survives
    only as the fallback for a test whose column is blank; honouring both silently
    would make the reported deltas depend on which one a reader happened to look at.
    """
    declared = {t.test_id: t.read_against for t in (tests or []) if t.read_against}
    if baseline and declared:
        disagree = sorted(tid for tid, b in declared.items() if b != baseline)
        if disagree:
            raise ValueError(
                f"--baseline {baseline!r} disagrees with the read_against column on "
                f"{len(disagree)} test(s), e.g. {disagree[:3]}. Drop the flag: the "
                f"column is per test and the flag cannot be")
    out = dict(declared)
    if baseline:
        for tid in obs.test_id.astype(str).unique():
            out.setdefault(tid, baseline)
    if not out:
        raise ValueError("no test names a baseline: the tests table's read_against "
                         "column is empty and no --baseline was given")
    missing = sorted(set(out.values()) - set(obs.test_id.astype(str)))
    if missing:
        raise ValueError(f"baseline test(s) {missing[:5]} are not in the observed "
                         f"results; a baseline is measured like any other test")
    return out


def score(observed: pd.DataFrame, null: pd.DataFrame, *, baseline=None,
          tests=None) -> pd.DataFrame:
    obs = _with_axis(_measurements(observed), tests, "observed")
    nul = _with_axis(_measurements(null), tests, "null")
    base_of = _baselines(obs, tests, baseline)

    # One baseline VECTOR per distinct baseline test, indexed by the readout key.
    base_tables = {b: obs[obs.test_id == b].set_index(list(KEY))["value"]
                   for b in set(base_of.values())}
    meta = {}
    if tests:
        meta = {t.test_id: dict(
            is_control=bool(t.meta.get("is_control")),
            stratum=(int(t.meta["n_units"]) if t.meta.get("n_units") is not None
                     and t.meta.get("n_units") == t.meta.get("n_units") else None))
            for t in tests}

    obs = obs[~obs.test_id.isin(set(base_of.values()))].copy()
    idx = pd.MultiIndex.from_frame(obs[list(KEY)])
    obs["baseline"] = [
        base_tables[base_of[tid]].get(k, np.nan)
        for tid, k in zip(obs.test_id.astype(str), idx)]
    # A measurement whose baseline did not resolve is not a small effect, it is no
    # measurement -- and unrefused it becomes a NaN delta that the pool quietly drops.
    # It is also how a many-axis run scored WITHOUT a tests table announces itself: every
    # axis but the baseline test's own finds nothing to subtract.
    orphan = obs[obs.baseline.isna()]
    if len(orphan):
        hint = ("" if tests else
                "\n  no tests table was given, so every row was keyed on an empty axis; "
                "pass --tests (and --null-tests) if this run measures more than one "
                "source -> sink pair")
        raise ValueError(
            f"{len(orphan)} of {len(obs)} observed measurement(s) have no baseline at "
            f"their readout key, e.g. "
            f"{orphan.iloc[0][['test_id', *KEY]].to_dict()}{hint}")
    obs["delta"] = obs.value - obs.baseline
    obs["is_control"] = [bool(meta.get(c, {}).get("is_control")) for c in obs.test_id]
    obs["stratum"] = [meta.get(c, {}).get("stratum") for c in obs.test_id]

    # THE NULL IS READ AGAINST THE BASELINE OF ITS OWN AXIS. A null draw carries no
    # `read_against` -- it is a construction, not an arm -- so the axis in `KEY` is what
    # says which baseline is its. This used to take whichever baseline sorted first,
    # which was right while a run had one and wrong the moment it had 83: every axis
    # would have been scored against one axis's host measurement.
    null_base = pd.concat(base_tables.values())
    clash = null_base.index[null_base.index.duplicated()]
    if len(clash):
        raise ValueError(
            f"{len(clash)} readout key(s) are claimed by more than one baseline test, "
            f"e.g. {list(clash[:2])}. Two baselines for one measurement is two answers "
            f"to 'what is this a delta from'")
    nul = nul.join(null_base.rename("baseline"), on=list(KEY))
    nul["delta"] = nul.value - nul.baseline
    nul_stratum = {t.test_id: t.meta.get("stratum") for t in (tests or [])}
    nul["stratum"] = [nul_stratum.get(c) for c in nul.test_id]
    if nul.stratum.isna().all():
        # A pool drawn by `ecspr draw` names its stratum in the test id, which is what
        # keeps size matching working when the null's own tests table was not passed
        # alongside the results.
        nul["stratum"] = nul.test_id.astype(str).str.extract(r":s(\d+):")[0]
        nul["stratum"] = pd.to_numeric(nul["stratum"], errors="coerce")

    out = []
    nul_by_key = {k: v for k, v in nul.groupby(list(KEY), sort=False)}
    empty_pool = nul.iloc[0:0]
    for key, g in obs.groupby(list(KEY), sort=False):
        # Matched on the WHOLE key, by name rather than by position: the null for one axis
        # is the draws measured on that axis, and nothing else in the table.
        pool = nul_by_key.get(key, empty_pool)
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
    cols = ["test_id", *KEY, "value", "baseline", "delta", "is_control", "stratum",
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
    # Boolean mask of the largest-|value| members that together carry ``coverage`` of the
    # total. The complement is the FLOOR, and it is not a set of small measurements.
    #
    # A partition with an effective size near ten does not have four thousand members worth
    # reporting, and reporting them anyway is what makes an ECSPr readout look unusable: on the
    # 4,102-clone ASKA library the whole distribution spans 7.7 decades between its 5th and
    # 95th percentiles, while the eight genes carrying 90% of it span 1.0 and the twenty-five
    # carrying 99% span 2.5. Nothing about the network changed between those numbers -- only
    # which members were read as measurements.
    #
    # So this is a reporting decision with a measured basis, not a cleanup. The floor keeps its
    # own count and mass, which is what stops "the tail is small" from becoming "the tail is
    # absent"; a member that leaves the responder set has not been shown to be zero.
    v = np.abs(np.asarray(values, float))
    v = np.where(np.isfinite(v), v, 0.0)
    total = v.sum()
    if not total > 0:
        return np.zeros(v.shape, bool)
    order = np.argsort(v)[::-1]
    keep = np.zeros(v.shape, bool)
    keep[order[:int(np.searchsorted(np.cumsum(v[order]) / total, coverage) + 1)]] = True
    return keep
