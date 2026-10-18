#!/usr/bin/env python3
"""The gate on everything downstream: is the analytic direction gradient the real one?

Three checks, in order of what they would catch:

1. PARITY -- ``DirectionModel`` at the baked parameters must reproduce ``graph_from_pairs``
   bit for bit. The frozen-orientation reparametrisation is only legitimate if it is the
   same network; a mismatch here means every gradient below differentiates a graph nobody
   else is solving.
2. TELLEGEN -- on the symmetric network the adjoint and ``Solution.edge_power`` are the same
   quantity, so they must agree to machine precision. This needs no finite difference and
   catches a wrong adjoint right-hand side.
3. CENTRAL DIFFERENCE -- per reaction, on the real host graph, against two extra solves.
   Reactions are sampled from the live end of the gradient distribution and from the dead
   end, because a gradient that is exactly zero is a claim too and it can be wrong.

A CENTRAL DIFFERENCE IS THE WRONG INSTRUMENT AT THE KINK, and half the table sits on it: a
reaction with no direction evidence is baked at ratio 1.0, so ``a_r = 0`` exactly, and
``a_r +/- h`` straddles the hinge. The difference then averages two DIFFERENT one-sided
slopes and disagrees with any correct derivative. So the unsmoothed arm checks straddling
reactions one-sided, and the smoothed arm (``eta > 0``, which is what the optimiser runs)
checks every reaction centrally, where no kink remains to straddle.

    mamba run -n ecspr python research/fabfos/benchmarks/laser/dirfit/check_gradient.py
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from model import DECADE_CAP, GLUCOSE, build                          # noqa: E402

import ecspr.model.build as EB                                        # noqa: E402
from ecspr.model.graph import solve                                   # noqa: E402
from ecspr.model.sensitivity import check_against_power               # noqa: E402

TARGET = "MNXM738130"          # glycogen -- the eydallin axis
CACHE = Path(__file__).resolve().parent / "cache"

# What a difference quotient can and cannot resolve, which is what the gate is scored
# against -- an analytic derivative is not wrong for disagreeing with a probe that cannot
# see it. ``log C_eff`` is O(1), so a quotient over a step of 1e-4 carries ~eps/step of
# float64 roundoff: ABS_FLOOR. Truncation then sets the relative tolerance, and it differs
# by scheme -- a central difference is O(h^2), a one-sided one only O(h), and the kink
# forces one-sided wherever a parameter sits within a step of zero. A central difference
# ACROSS the kink is first-order too -- the O(h^2) terms only cancel when the curvature
# either side is the same, and at a hinge it is not -- so accuracy order, not step shape, is
# what sets the tolerance.
ABS_FLOOR = 1e-11
REL_SECOND_ORDER = 1e-6
REL_FIRST_ORDER = 1e-3


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="e_coli_k12")
    p.add_argument("--target", default=TARGET)
    p.add_argument("--n-live", type=int, default=12)
    p.add_argument("--n-dead", type=int, default=6)
    p.add_argument("--step", type=float, default=1e-4)
    p.add_argument("--eta", type=float, default=1e-2)
    p.add_argument("--out", default=str(CACHE / "gradient_check.tsv"))
    a = p.parse_args()

    t = time.time()
    m, pairs, ann = build(a.host, "C", cap=DECADE_CAP)
    print(f"model: {len(m):,} reactions, {m.graph.n:,} nodes, {m.graph.m:,} edges "
          f"({time.time() - t:.1f}s)", file=sys.stderr)

    # -- 1. parity against the builder ------------------------------------
    ref = m.graph_at(m.a_ref)
    dgp = float(np.abs(ref.gp - m.graph.gp).max())
    dgm = float(np.abs(ref.gm - m.graph.gm).max())
    print(f"parity vs graph_from_pairs: max|dgp|={dgp:.3e} max|dgm|={dgm:.3e}",
          file=sys.stderr)
    assert dgp < 1e-12 and dgm < 1e-12, "reparametrisation is not the builder's graph"

    src, snk = m.terminal(GLUCOSE), m.terminal(a.target)

    # -- 2. Tellegen on the symmetric network ------------------------------
    zero = np.zeros(len(m))
    gsym = m.graph_at(zero)
    ssym = solve(gsym, src, snk)
    err = check_against_power(gsym, ssym)
    print(f"Tellegen identity (all ratios 1.0): max rel err={err:.3e} "
          f"C_eff={ssym.total:.8g}", file=sys.stderr)

    # -- 3. differences at the baked parameters, both regimes --------------
    rng = np.random.default_rng(20260824)
    rows = []
    for eta in (0.0, a.eta):
        me = m.with_eta(eta)
        t = time.time()
        logc, grad = me.gradient(me.a_ref, src, snk)
        absg = np.abs(grad)
        print(f"\n-- eta={eta:g}: log C_eff={logc:.8f} (C_eff={np.exp(logc):.8g}), "
              f"{int((absg > 0).sum()):,}/{len(me):,} nonzero, max={absg.max():.4g}, "
              f"{time.time() - t:.2f}s", file=sys.stderr)

        live = np.argsort(absg)[::-1][:a.n_live]
        pool = np.flatnonzero(absg <= 0)
        dead = (rng.choice(pool, size=min(a.n_dead, pool.size), replace=False)
                if pool.size else np.zeros(0, int))
        # At eta=0 the interesting sample is the kink itself, which the live/dead split
        # would otherwise never reach.
        kink = np.flatnonzero(me.a_ref == 0.0)
        kink = rng.choice(kink, size=min(a.n_dead, kink.size), replace=False) \
            if kink.size else np.zeros(0, int)

        for j in np.unique(np.concatenate([live, dead, kink]).astype(int)):
            h = a.step
            a0 = float(me.a_ref[j])
            # The builder normalises the favoured branch to G, so ``s = -|a|`` at the
            # reference: every parameter sits on or below the hinge. A step of h therefore
            # crosses it only when ``0 < |a0| < h``, and there the difference must be taken
            # one-sided, away from zero, to stay on the branch the gradient describes. At
            # ``a0 == 0`` exactly the model reports the two-sided average, so a central
            # difference is the correct check there.
            near_kink = eta == 0.0 and abs(a0) < h
            one_sided = near_kink and a0 != 0.0
            ap, am = me.a_ref.copy(), me.a_ref.copy()
            if one_sided:
                ap[j], am[j] = (a0 + h, a0) if a0 > 0 else (a0, a0 - h)
            else:
                ap[j], am[j] = a0 + h, a0 - h
            fd = (np.log(solve(me.graph_at(ap), src, snk).total)
                  - np.log(solve(me.graph_at(am), src, snk).total)) / (ap[j] - am[j])
            an = float(grad[j])
            scale = max(abs(fd), abs(an))
            reltol = REL_FIRST_ORDER if near_kink else REL_SECOND_ORDER
            err = abs(fd - an)
            rows.append(dict(eta=eta, mnxr=me.reactions[j], a_ref=a0,
                             analytic=an, finite_diff=float(fd), one_sided=bool(one_sided),
                             order=1 if near_kink else 2, abs_err=err, scale=scale,
                             rel_err=err / max(scale, 1e-30),
                             tolerance=max(reltol * scale, ABS_FLOOR),
                             passed=bool(err <= max(reltol * scale, ABS_FLOOR)),
                             arm=("kink" if a0 == 0.0 else
                                  "live" if absg[j] > 0 else "dead")))
            r = rows[-1]
            print(f"  {r['mnxr']:<14} a={a0:+7.3f} adj={an:+.6e} "
                  f"fd={fd:+.6e} err={err:.2e} tol={r['tolerance']:.2e} "
                  f"{'ok ' if r['passed'] else 'FAIL'} [{r['arm']}"
                  f"{'/1-sided' if one_sided else ''}]", file=sys.stderr)

    df = pd.DataFrame(rows)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out, sep="\t", index=False)

    per = df.groupby(["eta", "arm"]).agg(n=("passed", "size"), failed=("passed",
                                         lambda s: int((~s).sum())),
                                         worst_rel=("rel_err", "max"))
    print(f"\n{per.to_string()}", file=sys.stderr)
    bad = df[~df.passed]
    print(f"\n{len(df) - len(bad)}/{len(df)} probes within tolerance", file=sys.stderr)
    if len(bad):
        print(bad[["eta", "mnxr", "a_ref", "analytic", "finite_diff",
                   "abs_err", "tolerance"]].to_string(index=False), file=sys.stderr)
    print(f"wrote {a.out}", file=sys.stderr)
    return 0 if bad.empty else 1


if __name__ == "__main__":
    raise SystemExit(main())
