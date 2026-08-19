from __future__ import annotations

import numpy as np
import scipy.sparse as sp
from scipy.sparse.linalg import splu
from scipy.special import spence

# CHOLMOD (scikit-sparse) gives a stable SPD factorisation with symbolic reuse; it is
# optional -- absent it, the solver falls back to ``splu`` and every answer is identical
# to numerical tolerance. Import-guarded so this module loads anywhere (the ``splu`` path
# is what the self-tests and the symmetric-limit parity gate exercise when CHOLMOD is gone).
try:
    from sksparse.cholmod import cho_factor as _cho_factor
    _HAVE_CHOLMOD = True
except Exception:                              # pragma: no cover - environment dependent
    _cho_factor = None
    _HAVE_CHOLMOD = False

# Newton convergence tolerance on the reduced-gradient inf-norm, and iteration cap.
# The cap is generous: backflow axes are ill-conditioned (the s->t mode can run through a
# ~1e-9 cut, so kappa(H) ~ 1e18) and the reduced gradient floors near ~1e-8 in float64 --
# convergence is therefore declared on EITHER reaching ``tol`` OR the energy line search
# stagnating (no descent step remains => at the minimiser within numerical precision).
DIRECTED_TOL = 1e-9
DIRECTED_MAXIT = 200

# The grounded potential is unique only when ``g-`` is strictly positive everywhere:
# a *perfect* diode (g- = 0) leaves the throttled side's potential free and the reduced
# Hessian singular. The primitive clips ``g-`` up to this fraction of ``g+`` as a safety
# net -- documented rather than silently accepting a singular system. Callers that want
# a sharper diode should lower this knowingly, not rely on zero.
DIODE_BACKWARD_FLOOR = 1e-9

# Width of the softplus that smooths the diode kink (see the module docstring). Small
# enough that the smoothed C_eff sits within ~1e-4 of the delta->0 hard-diode limit on the
# real graph, large enough to stop the active-set chattering that made backflow axes
# non-reproducible. At ``g- = g+`` it has NO effect (exact undirected parity), so it only
# ever biases genuinely directed edges, by a documented O(delta). Deterministic => the
# observed solve and the null run the identical model.
DIODE_SMOOTH_DELTA = 1e-6


def build_incidence(edges, n):
    m = len(edges)
    if m == 0:
        return sp.csr_matrix((0, n))
    rows = np.repeat(np.arange(m), 2)
    cols = np.empty(2 * m, dtype=np.int64)
    vals = np.empty(2 * m, dtype=float)
    for e, (a, b) in enumerate(edges):
        cols[2 * e], cols[2 * e + 1] = a, b
        vals[2 * e], vals[2 * e + 1] = 1.0, -1.0
    return sp.csr_matrix((vals, (rows, cols)), shape=(m, n))


def _reg_spsolve(H, rhs):
    try:
        return splu(H, permc_spec="MMD_AT_PLUS_A").solve(rhs)
    except RuntimeError:
        scale = float(np.abs(H.diagonal()).max()) or 1.0
        lam = 1e-12 * scale
        while lam <= scale:
            try:
                return splu((H + lam * sp.eye(H.shape[0], format="csc")).tocsc(),
                            permc_spec="MMD_AT_PLUS_A").solve(rhs)
            except RuntimeError:
                lam *= 10.0
        raise


def _sigmoid(z):
    out = np.empty_like(z, dtype=float)
    pos = z >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    ez = np.exp(z[~pos])
    out[~pos] = ez / (1.0 + ez)
    return out


def _softmax0(x, delta):
    z = x / delta
    return delta * np.where(z > 30.0, z, np.log1p(np.exp(np.minimum(z, 30.0))))


def _smooth_Hint(x, delta):
    z = x / delta
    out = np.empty_like(x, dtype=float)
    big = z > 30.0
    small = z < -30.0
    mid = ~(big | small)
    d2 = delta * delta
    c = np.pi * np.pi / 12.0
    out[mid] = d2 * (-spence(1.0 + np.exp(z[mid])) - c)
    out[big] = 0.5 * x[big] * x[big] + d2 * c
    out[small] = d2 * (np.exp(z[small]) - c)
    return out


class _SPDReuse:
    __slots__ = ("Bk", "_fac", "used_cholmod")

    def __init__(self, B, keep):
        self.Bk = (B.tocsc() if sp.issparse(B) else sp.csr_matrix(B).tocsc())[:, keep]
        self._fac = None
        self.used_cholmod = False

    def solve(self, d, rhs):
        H = (self.Bk.T @ sp.diags(d) @ self.Bk).tocsc()
        if _HAVE_CHOLMOD:
            try:
                if self._fac is None:
                    self._fac = _cho_factor(H)
                else:
                    self._fac.factorize(H)
                x = self._fac.solve(np.asarray(rhs, float).reshape(-1, 1)).ravel()
                # Guard the rare near-singular iterate CHOLMOD only WARNS about: verify the
                # residual and drop to the ridged splu path if the factorisation was inaccurate.
                if np.all(np.isfinite(x)) and \
                        np.abs(H @ x - rhs).max() <= 1e-6 * (np.abs(rhs).max() + 1.0):
                    self.used_cholmod = True
                    return x
            except Exception:
                self._fac = None
        return _reg_spsolve(H, rhs)


def directed_ceff(B, gp, gm, s, t, g=0, tol=DIRECTED_TOL, maxit=DIRECTED_MAXIT,
                  floor=DIODE_BACKWARD_FLOOR, delta=DIODE_SMOOTH_DELTA, reuse=None,
                  etol=1e-13, return_iters=False, phi0=None, return_phi=False,
                  return_converged=False):
    B = B.tocsr() if sp.issparse(B) else sp.csr_matrix(B)
    gp = np.asarray(gp, float)
    gm = np.maximum(np.asarray(gm, float), floor * gp)
    n = B.shape[1]
    I = np.zeros(n); I[s] = 1.0; I[t] = -1.0
    keep = np.arange(n) != g
    phi = np.zeros(n) if phi0 is None else np.array(phi0, float)
    if phi0 is not None:
        phi[g] = 0.0
    if reuse is None:
        reuse = _SPDReuse(B, keep)

    def cur(x):
        hx = _softmax0(x, delta)
        return gp * hx + gm * (x - hx)

    def energy(p):
        x = B @ p
        return float(np.sum(0.5 * gm * x * x + (gp - gm) * _smooth_Hint(x, delta)) - I @ p)

    converged = False
    nit = 0
    for nit in range(1, maxit + 1):
        x = B @ phi
        grad = B.T @ cur(x) - I
        if np.abs(grad[keep]).max() < tol:
            converged = True
            break
        sig = _sigmoid(x / delta)
        d = gp * sig + gm * (1.0 - sig)
        dphi = np.zeros(n)
        dphi[keep] = reuse.solve(d, -grad[keep])
        slope = grad @ dphi
        if slope > 0.0:
            dphi = np.zeros(n); dphi[keep] = -grad[keep]; slope = grad @ dphi
        E0 = energy(phi)
        step = 1.0
        ok = False
        E1 = E0
        for _ in range(60):
            E1 = energy(phi + step * dphi)
            if E1 <= E0 + 1e-4 * step * slope:
                ok = True
                break
            step *= 0.5
        if not ok:
            converged = True
            break
        phi = phi + step * dphi
        if E0 - E1 <= etol * (abs(E0) + 1.0):
            converged = True
            break

    ceff = 1.0 / (phi[s] - phi[t])
    out = (ceff,)
    if return_iters:
        out += (nit,)
    if return_phi:
        out += (phi,)
    if return_converged:
        out += (converged,)
    return out[0] if len(out) == 1 else out


def _directed_ceff_dense(B, gp, gm, s, t, g=0, floor=DIODE_BACKWARD_FLOOR,
                         delta=DIODE_SMOOTH_DELTA):
    from scipy.optimize import minimize
    B = B.toarray() if sp.issparse(B) else np.asarray(B, float)
    gp = np.asarray(gp, float)
    gm = np.maximum(np.asarray(gm, float), floor * gp)
    n = B.shape[1]
    I = np.zeros(n); I[s] = 1.0; I[t] = -1.0
    free = np.arange(n) != g

    def fg(p_free):
        p = np.zeros(n); p[free] = p_free
        x = B @ p
        hx = _softmax0(x, delta)
        E = float(np.sum(0.5 * gm * x * x + (gp - gm) * _smooth_Hint(x, delta)) - I @ p)
        grad = B.T @ (gp * hx + gm * (x - hx)) - I
        return E, grad[free]

    res = minimize(fg, np.zeros(int(free.sum())), jac=True, method="L-BFGS-B",
                   options=dict(maxiter=5000, ftol=1e-15, gtol=1e-12))
    p = np.zeros(n); p[free] = res.x
    return 1.0 / (p[s] - p[t])
