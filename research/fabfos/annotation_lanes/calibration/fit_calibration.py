# THE QUESTION -- what is one call from each lane worth, in nats, as a function of that
# lane's own score?
#
# SCOPE   The four chosen lanes on the DH10B labelled join. Writes the calibration the
#         mapper will apply; the reference artifact is `emit_reference.py`.
# INPUT   out/labeled_<lane>.tsv  (build_labeled.py)
# METHOD  Two-parameter logistic on a monotone transform of the score, fitted by
#         L-BFGS on the penalised log-loss, scored on ORF-grouped folds.
# ENV     mamba run -n msm python research/fabfos/annotation_lanes/calibration/fit_calibration.py
# OUT     out/calibration.tsv   the fitted parameters, one row per lane
#         out/reliability.tsv   observed vs fitted error rate by score decile
#         out/folds.tsv         held-out scores, by lane and split rule
#
# WHY A LOG-LIKELIHOOD RATIO AND NOT A PRECISION. A precision is a posterior, so it
# carries the benchmark's own base rate inside it and cannot be moved to a corpus with
# a different one. Curated K-12 is not dark matter. Subtracting a prior in log-odds
# leaves the part that is a property of the evidence:
#
#     LLR(s) = logit(P(correct | s, lane)) - POOL_LAM0
#
# and `pool_logodds` reads `sigmoid(sum(LLR) + POOL_LAM0)`, so a reaction carrying ONE
# assertion comes out at exactly that assertion's own calibrated accuracy and further
# assertions move it from there. The prior is shared across all four lanes rather than
# fitted per lane: a per-lane prior would subtract exactly the between-lane difference
# this change exists to keep.
#
# TWO PRIORS, AND THEY ARE NOT INTERCHANGEABLE. This cost a wrong first cut.
#
#   pi_fit      the share of correct calls among calls the lanes MADE -- 0.947 here.
#               It is what the fitted intercept B is relative to, and it is reported
#               so `sigmoid(B)` can be read as a lane's precision. It is a property of
#               the benchmark's difficulty and does not transfer.
#   POOL_LAM0   the declared prior that a reaction is present before any lane speaks,
#               inherited unchanged from the pre-calibration pooling.
#
# Subtracting pi_fit and then flooring at zero fires on every below-average call --
# 58% of the CLEAN and ProteinBERT lanes on DH10B -- because it reads "worse than the
# average call" as "argues the reaction is absent". Those are different claims. It
# also pins the pooled belief into [0.947, 1.0], which is no range at all. The
# reference has to be the prior the pooling combines against, and it is.
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _cal as C  # noqa: E402

# The consumer's declared prior, read from the library rather than restated -- the two
# must be the same number, and `fabfos_evidence.load_calibration` refuses a mismatch.
POOL_LAM0 = C.fe.POOL_LAM0

EPS = 1e-4
N_FOLDS = 5
# A weakly-informative Gaussian prior on (A, B), sd = 10 in log-odds. Flat over any
# plausible fit and finite where the data separate -- which they nearly do for
# uniref50, at three errors in 1,086 calls. `converged_at_prior` reports whether it bound.
PRIOR_SD = 10.0

# The score transform menu. A two-parameter logistic is linear in log-odds, so the
# transform is where the lane's own scale is declared. All three are monotone, so none
# can reorder a lane's calls, and the choice between them is made on held-out folds
# rather than by inspection.
#
# **CAUTION** `logit` needs a clip, and the clip is a free parameter wherever a lane
# piles mass on an endpoint. ProteinBERT's vote is exactly 1.0 for over half its calls,
# so under `logit` its ceiling LLR is a function of EPS and not of the data -- 0.28 at
# 1e-4, 0.60 at 1e-6. `identity` is in the menu because it has no such constant, and a
# lane that saturates should be choosing it. EPS_SENSITIVITY re-runs the fit at a
# second EPS so a surviving dependence is visible rather than assumed away.
TRANSFORMS = ("identity", "logit", "log")
EPS_SENSITIVITY = 1e-6


def transform(kind: str, s: np.ndarray, eps: float = EPS) -> np.ndarray:
    s = np.asarray(s, dtype=float)
    if kind == "identity":
        return s
    if kind == "log":
        return np.log(np.clip(s, eps, None))
    p = np.clip(s, eps, 1.0 - eps)
    return np.log(p / (1.0 - p))


def usable(kind: str, s: np.ndarray) -> bool:
    """`log` needs positives; `logit` needs a score that is a share to begin with."""
    if kind == "log":
        return bool((s > 0).all())
    if kind == "logit":
        return bool((s >= 0).all() and s.max() <= 1.05)
    return True


def fit(x: np.ndarray, y: np.ndarray, sloped: bool = True) -> tuple[float, float]:
    """(A, B) with P(correct | x) = sigmoid(A x + B). `sloped=False` pins A at zero,
    which is the constant model -- that lane's precision, with no score in it."""
    xx = x if sloped else np.zeros_like(x)

    def nll(th):
        z = th[0] * xx + th[1]
        # log(1 + exp(-z)) for y=1, log(1 + exp(z)) for y=0, in the stable form.
        loss = np.logaddexp(0.0, np.where(y > 0, -z, z)).sum()
        return loss + (th[0] ** 2 + th[1] ** 2) / (2 * PRIOR_SD ** 2)

    def grad(th):
        z = th[0] * xx + th[1]
        r = 1.0 / (1.0 + np.exp(-z)) - y
        return np.array([float((r * xx).sum() + th[0] / PRIOR_SD ** 2),
                         float(r.sum() + th[1] / PRIOR_SD ** 2)])

    res = minimize(nll, np.array([1.0 if sloped else 0.0, 0.0]), jac=grad,
                   method="L-BFGS-B")
    return (float(res.x[0]) if sloped else 0.0), float(res.x[1])


def _sig(z):
    return 1.0 / (1.0 + np.exp(-z))


def folds_by(keys: np.ndarray, n: int) -> np.ndarray:
    """Assign each row a fold by hashing its group key, so a group never splits."""
    uniq = pd.unique(keys)
    assign = {k: i % n for i, k in enumerate(sorted(uniq, key=str))}
    return np.array([assign[k] for k in keys])


def cross_val(x, y, groups, sloped: bool, n=N_FOLDS) -> dict:
    """Held-out log-loss and Brier for one model form.

    Both forms are refitted per fold and scored the same way, so the comparison is
    between two out-of-fold predictors rather than between a fit and a number the
    whole dataset already knows."""
    f = folds_by(groups, n)
    ll, br, held = [], [], []
    per_row = np.full(len(y), np.nan)
    for k in range(n):
        tr, te = f != k, f == k
        if not te.any() or len(np.unique(y[tr])) < 2:
            continue
        A, B = fit(x[tr], y[tr], sloped)
        p = np.clip(_sig(A * x[te] + B), 1e-9, 1 - 1e-9)
        row = -(y[te] * np.log(p) + (1 - y[te]) * np.log(1 - p))
        per_row[te] = row
        ll.append(float(np.mean(row)))
        br.append(float(np.mean((p - y[te]) ** 2)))
        held.append((k, int(te.sum()), int((y[te] == 0).sum()), ll[-1], br[-1]))
    return dict(logloss=float(np.mean(ll)) if ll else float("nan"),
                brier=float(np.mean(br)) if br else float("nan"),
                rows=held, per_row=per_row)


def paired_gain(const_cv, sloped_cv, groups) -> tuple[float, float]:
    """(mean per-call log-loss the slope saves, its cluster-robust standard error).

    Clustered on the grouping key, because two calls on one ORF are not two
    independent observations -- a per-row standard error would call a five-paralogue
    family five pieces of evidence and shrink itself by sqrt(5) for free."""
    d = const_cv["per_row"] - sloped_cv["per_row"]
    ok = ~np.isnan(d)
    per_group = pd.Series(d[ok]).groupby(pd.Series(np.asarray(groups)[ok])).mean()
    if len(per_group) < 2:
        return float("nan"), float("nan")
    return float(per_group.mean()), float(per_group.std(ddof=1) / np.sqrt(len(per_group)))


def main() -> int:
    lab = {l: pd.read_csv(C.OUT / f"labeled_{l}.tsv", sep="\t") for l in C.LANE_ORDER}

    # THE SHARED PRIOR, pooled over every adjudicable call the four lanes made. Pooled by
    # call rather than macro-averaged over lanes, because the quantity wanted is "a call
    # drawn at random from this corpus", and the lanes do not make equal numbers of calls.
    n_all = sum(len(d) for d in lab.values())
    n_ok = sum(int(d["correct"].sum()) for d in lab.values())
    pi = n_ok / n_all
    lam0 = POOL_LAM0
    print(f"benchmark call prevalence  pi_fit = {n_ok}/{n_all} = {pi:.4f} "
          f"(logit {np.log(pi / (1 - pi)):.4f}) -- REPORTED, not subtracted")
    print(f"reference prior            POOL_LAM0 = {lam0} -- what llr is measured "
          f"against and what pooling adds back\n")

    rows, rel, foldrows, tsel = [], [], [], []
    for lane in C.LANE_ORDER:
        d = lab[lane]
        s = d["score"].to_numpy()
        y = d["correct"].to_numpy(dtype=float)
        orf = d["orf"].to_numpy()

        # THE TRANSFORM IS CHOSEN ON HELD-OUT FOLDS, not by inspection. Every candidate
        # is monotone, so this cannot reorder a lane's calls -- it only decides how the
        # lane's own scale maps onto log-odds, which is the one thing a two-parameter
        # logistic cannot learn for itself.
        cand = [t for t in TRANSFORMS if usable(t, s)]
        by_t = {}
        for t in cand:
            cv = cross_val(transform(t, s), y, orf, True)
            by_t[t] = cv["logloss"]
            tsel.append(dict(lane=lane, transform=t, cv_logloss=round(cv["logloss"], 5)))
        tkind = min(cand, key=lambda t: by_t[t])
        x = transform(tkind, s)

        # MODEL SELECTION ON HELD-OUT FOLDS, per lane. The sloped model is kept only
        # where it beats the constant one out of fold. A score that does not predict
        # correctness is not made to by fitting it: shipping a curve that loses to its
        # own intercept would put the benchmark's noise into every reaction's weight.
        splits = (("orf", orf),
                  ("ec_family", d["ec_family"].astype(str).to_numpy()),
                  ("intermediate", d["intermediate_id"].astype(str).to_numpy()))
        cvs = {}
        for rule, key in splits:
            for sloped in (True, False):
                cv = cross_val(x, y, key, sloped)
                cvs[(rule, sloped)] = cv
                for k, n, nerr, l_, b_ in cv["rows"]:
                    foldrows.append(dict(lane=lane, split=rule,
                                         model="sloped" if sloped else "constant",
                                         fold=k, n=n, errors=nerr,
                                         logloss=round(l_, 5), brier=round(b_, 5)))
        # ORF-grouped is the decision split. The other two are reported as robustness
        # and never overrule it -- picking whichever split favours the answer wanted is
        # how a holdout stops being one.
        #
        # THE SLOPE MUST CLEAR ITS OWN NOISE, not merely win the mean. Comparing two
        # mean log-losses made kofam sloped on a 0.001 margin carried by three folds of
        # five, which is a coin flip dressed as a decision. The test is the paired
        # per-call gain against its cluster-robust standard error.
        sloped_ll = cvs[("orf", True)]["logloss"]
        const_ll = cvs[("orf", False)]["logloss"]
        gain, se = paired_gain(cvs[("orf", False)], cvs[("orf", True)], orf)
        use_slope = bool(gain > 2 * se)
        A, B = fit(x, y, use_slope)
        cv_orf = cvs[("orf", use_slope)]

        lo, hi = float(s.min()), float(s.max())
        xlo, xhi = transform(tkind, np.array([lo, hi]))
        # LLR at the fitted extremes. Below `score_min` the mapper clamps to `llr_lo`
        # rather than continuing the trend -- the curve has no support down there.
        llr_lo, llr_hi = A * xlo + B - lam0, A * xhi + B - lam0
        med = float(np.median(s))
        llr_med = float(A * transform(tkind, np.array([med]))[0] + B - lam0)
        scalar_llr = float(np.log(y.mean() / (1 - y.mean())) - lam0)
        # sigmoid(B) is this lane's fitted precision at x = 0, which is only
        # interpretable for the constant model -- there it IS the lane's precision.
        p_at_median = float(_sig(A * transform(tkind, np.array([med]))[0] + B))
        # Does the answer move when the logit clip moves? It cannot under `identity`.
        A2, B2 = fit(transform(tkind, s, EPS_SENSITIVITY), y, use_slope)
        llr_hi2 = A2 * transform(tkind, np.array([hi]), EPS_SENSITIVITY)[0] + B2 - lam0

        rows.append(dict(
            lane=lane, model="sloped" if use_slope else "constant",
            transform=tkind, A=round(A, 5), B=round(B, 5),
            lam0=round(lam0, 5), pi_fit=round(pi, 5),
            p_correct_at_median=round(p_at_median, 5),
            n=len(d), errors=int((y == 0).sum()),
            score_min=round(lo, 5), score_max=round(hi, 5),
            llr_at_min=round(float(llr_lo), 4), llr_at_max=round(float(llr_hi), 4),
            llr_at_median=round(llr_med, 4), llr_scalar=round(scalar_llr, 4),
            llr_at_max_eps1e6=round(float(llr_hi2), 4),
            cv_logloss_sloped=round(sloped_ll, 5),
            cv_logloss_constant=round(const_ll, 5),
            slope_gain=round(gain, 5), slope_gain_se=round(se, 5),
            slope_gain_z=round(gain / se, 2) if se else None,
            cv_logloss=round(cv_orf["logloss"], 5),
            cv_brier=round(cv_orf["brier"], 5),
            cv_logloss_sloped_ecfam=round(cvs[("ec_family", True)]["logloss"], 5),
            cv_logloss_constant_ecfam=round(cvs[("ec_family", False)]["logloss"], 5),
            converged_at_prior=bool(abs(A) > 3 * PRIOR_SD or abs(B) > 3 * PRIOR_SD),
        ))

        # Reliability: observed vs fitted, by score decile. This is where a monotone
        # assumption either holds or is visibly wrong.
        q = pd.qcut(pd.Series(s).rank(method="first"), 10, labels=False)
        for b, g in d.assign(_q=q.to_numpy()).groupby("_q"):
            gx = transform(tkind, g["score"].to_numpy())
            rel.append(dict(lane=lane, decile=int(b), n=len(g),
                            score_lo=round(float(g["score"].min()), 4),
                            score_hi=round(float(g["score"].max()), 4),
                            obs_err=round(float(1 - g["correct"].mean()), 4),
                            fit_err=round(float(np.mean(1 - _sig(A * gx + B))), 4),
                            llr=round(float(np.mean(A * gx + B - lam0)), 3)))

    cal = pd.DataFrame(rows)
    cal.to_csv(C.OUT / "calibration.tsv", sep="\t", index=False)
    pd.DataFrame(rel).to_csv(C.OUT / "reliability.tsv", sep="\t", index=False)
    pd.DataFrame(foldrows).to_csv(C.OUT / "folds.tsv", sep="\t", index=False)

    ts = pd.DataFrame(tsel).pivot(index="lane", columns="transform",
                                  values="cv_logloss").reindex(C.LANE_ORDER)
    print("transform choice, by held-out log-loss of the sloped fit:")
    print(ts.to_string(), "\n")

    show = ["lane", "model", "transform", "n", "errors", "A", "B",
            "score_min", "score_max", "llr_at_min", "llr_at_median", "llr_at_max",
            "p_correct_at_median", "cv_logloss_sloped", "cv_logloss_constant",
            "slope_gain_z"]
    print(cal[show].to_string(index=False))
    print("\n`p_correct_at_median` is what a reaction carrying ONE assertion from that "
          "lane\nat its median score comes out at, by construction. It is the "
          "interpretable form.")
    print("\nsame comparison under the EC-family split (robustness, does not decide):")
    print(cal[["lane", "cv_logloss_sloped_ecfam",
               "cv_logloss_constant_ecfam"]].to_string(index=False))
    print("\nreliability (observed vs fitted error rate by decile):")
    print(pd.DataFrame(rel)[["lane", "decile", "n", "score_lo", "score_hi",
                             "obs_err", "fit_err", "llr"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
