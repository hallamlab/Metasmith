from __future__ import annotations

import numpy as np
import pandas as pd

DIAG_PREFIX = "_"

#: WHAT MAKES TWO ROWS THE SAME MEASUREMENT, per arm. Everything below this line -- the
#: empirical p, the z, the BH pass, the stratum-matched pass and the control gate -- is
#: arm-neutral and must stay that way; the arm enters here and nowhere else.
#:
#: A conductance row is keyed on the probe, the orientation, the element and the readout.
#: A flux balance row carries none of those: it names the reaction that was maximised and
#: the readout taken. Keying a gem result on the conductance columns does not fail, it
#: matches NOTHING -- the null pool comes back empty and every z is nan.
KEYS = {"ecspr": ("probe", "orientation", "element", "readout"),
        "gem": ("objective", "readout")}

#: The conductance key, kept as a name because it is what this module meant by `KEY`
#: before there were two arms.
KEY = KEYS["ecspr"]


def key_for(arm: str) -> tuple:
    if arm not in KEYS:
        raise ValueError(f"arm must be one of {sorted(KEYS)}, got {arm!r}")
    return KEYS[arm]


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


def _baselines(obs: pd.DataFrame, tests, baseline, also=None) -> dict:
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
    # A NULL POOL CARRIES ITS OWN BASELINE, so the search is over both result tables.
    # `ecspr draw` emits the background alone as `null:*:baseline` and points every drawn
    # test at it: the null's constructions are the background plus a draw, so the
    # background is what their deltas are against, and it is measured in the null run.
    have = set(obs.test_id.astype(str))
    if also is not None and len(also):
        have |= set(also.test_id.astype(str))
    missing = sorted(set(out.values()) - have)
    if missing:
        raise ValueError(f"baseline test(s) {missing[:5]} are not in the observed or "
                         f"null results; a baseline is measured like any other test")
    return out


def score(observed: pd.DataFrame, null: pd.DataFrame, *, baseline=None,
          tests=None, arm: str = "ecspr") -> pd.DataFrame:
    """Observed against a drawn null, on one arm's own measurement key.

    `arm` decides only which columns identify a measurement (`KEYS`). The statistics are
    the same for both, which is the point: a claim that one arm sees what the other does
    not has to come out of one comparison, not two that resemble each other.
    """
    key = list(key_for(arm))
    obs = _measurements(observed)
    nul = _measurements(null)
    for c in key:
        for name, d in (("observed", obs), ("null", nul)):
            if c not in d.columns:
                raise ValueError(
                    f"the {name} results have no {c!r} column, which the {arm!r} arm "
                    f"keys on (has {sorted(d.columns)}). Scoring on the wrong arm's key "
                    f"matches nothing rather than failing, and every z comes back nan")
    base_of = _baselines(obs, tests, baseline, also=nul)

    # One baseline VECTOR per distinct baseline test, indexed by the readout key. It is
    # looked up in whichever table measured it: the observed arm's baselines are in the
    # observed results and a drawn pool's baseline is in the null results.
    both = pd.concat([obs, nul], ignore_index=True)
    base_tables = {b: both[both.test_id == b].set_index(key)["value"]
                   for b in set(base_of.values())}
    meta = {}
    if tests:
        meta = {t.test_id: dict(
            is_control=bool(t.meta.get("is_control")),
            stratum=(int(t.meta["n_units"]) if t.meta.get("n_units") is not None
                     and t.meta.get("n_units") == t.meta.get("n_units") else None))
            for t in tests}

    baseline_ids = set(base_of.values())
    obs = obs[~obs.test_id.isin(baseline_ids)].copy()
    idx = pd.MultiIndex.from_frame(obs[key])
    obs["baseline"] = [
        base_tables[base_of[tid]].get(k, np.nan)
        for tid, k in zip(obs.test_id.astype(str), idx)]
    obs["delta"] = obs.value - obs.baseline
    obs["is_control"] = [bool(meta.get(c, {}).get("is_control")) for c in obs.test_id]
    obs["stratum"] = [meta.get(c, {}).get("stratum") for c in obs.test_id]

    # THE NULL IS READ AGAINST ITS OWN BASELINE WHERE IT DECLARES ONE, and against the
    # observed arm's otherwise. A pool from `ecspr draw` declares one -- the background
    # with nothing drawn onto it -- which is the construction its draws are a
    # perturbation of. A hand-built pool that declares none falls back to the observed
    # baseline, which is what this always did when there was only one.
    fallback = sorted(base_of.values())[0]
    # A DRAWN POOL'S OWN BASELINE IS NOT ONE OF ITS DRAWS. It is excluded already when the
    # null tests table is passed, because some test names it. Without that table it is
    # recognised by the id `ecspr draw` gives it -- the same id-shape fallback the
    # stratum uses below, and for the same reason: a results file has to be readable on
    # its own. Left in, it enters the pool as a spurious zero-delta draw.
    drawn_baseline = nul.test_id.astype(str).str.fullmatch(r"null:t\d+:baseline")
    nul = nul[~nul.test_id.isin(baseline_ids) & ~drawn_baseline].copy()
    nidx = pd.MultiIndex.from_frame(nul[key])
    nul["baseline"] = [
        base_tables[base_of.get(tid, fallback)].get(k, np.nan)
        for tid, k in zip(nul.test_id.astype(str), nidx)]
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
    for k, g in obs.groupby(key, sort=False):
        k = k if isinstance(k, tuple) else (k,)
        mask = np.ones(len(nul), bool)
        for c, v in zip(key, k):
            mask &= (nul[c] == v).to_numpy(bool)
        pool = nul[mask]
        g = g.copy()
        st = _stats(g.delta.to_numpy(float), pool.delta.dropna().to_numpy(float))
        for c in ("null_n", "null_mean", "null_sd", "z", "p_emp", "pct_rank", "p_floor"):
            g[c] = st[c]
        # ...and again inside the size-matched stratum, which is the comparison the
        # size confound actually requires.
        zs, ps, ns = [], [], []
        for sv, d in zip(g.stratum, g.delta):
            sub = pool[pool.stratum == sv].delta.dropna().to_numpy(float) \
                if sv is not None and sv == sv else np.zeros(0)
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
    cols = ["test_id", *key, "value", "baseline", "delta", "is_control", "stratum",
            "null_n", "null_mean", "null_sd", "z", "p_emp", "q_bh", "p_floor",
            "pct_rank", "z_stratum", "p_stratum", "null_n_stratum"]
    return pd.concat(out, ignore_index=True)[cols]


def gate(scored: pd.DataFrame, arm: str = "ecspr") -> pd.DataFrame:
    key = list(key_for(arm))
    rows = []
    for k, g in scored.groupby(key, sort=False):
        k = k if isinstance(k, tuple) else (k,)
        ctl = g[g.is_control].delta.dropna().to_numpy(float)
        rows.append(dict(zip(key, k)) | dict(
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
