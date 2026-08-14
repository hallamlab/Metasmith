"""ECSPr directed (rectified "diode" network) solver -- the directional companion to
the undirected effective-conductance measurement in ``ecspr.graph.py``.

Each reaction edge carries a forward conductance ``g+`` and a backward conductance
``g-``. A metabolite potential field ``phi`` induces a per-edge signed potential drop
``x_e = (B phi)_e`` where ``B`` is the signed incidence (+1 at the tail, -1 at the head).
Current through an edge is *rectified*:

    i_e(x) = g+ * max(x, 0) - g- * max(-x, 0)  =  g+ * max(x, 0) + g- * min(x, 0)

so the edge conducts freely in the forward direction (``g+``) and is throttled in
reverse (``g- << g+`` for an irreversible reaction). Injecting a unit current s->t and
imposing the KKT stationarity conditions is equivalent to minimising the convex, C^1,
piecewise-quadratic energy

    E(phi) = 0.5 * sum_e [ g+ (x_e^+)^2 + g- (x_e^-)^2 ]  -  I^T phi,   I = e_s - e_t

whose unique (grounded) minimiser gives the two-terminal effective conductance

    C_eff = 1 / (phi[s] - phi[t]).

**Symmetric-limit parity.** At ``g- = g+`` the two rectified branches sum to the plain
quadratic ``0.5 x^T diag(g) x - I^T phi`` (because ``max(x,0)^2 + min(x,0)^2 == x^2``),
so the directed solve reproduces the ordinary undirected ``R_eff`` exactly. A directed
model therefore *degrades to the undirected one* precisely where direction is unknown
(ratio 1.0), and any divergence from the undirected answer is attributable to a real
forward/backward asymmetry -- never to the machinery.

**Solver.** Newton on a *smoothed* diode with an energy-Armijo line search as the
globalisation. The hard rectifier's kink at ``x_e = 0`` makes the active-set diagonal
``d`` discontinuous (``g+`` vs ``g-`` differ by up to 9 orders across the diode floor),
so on backflow-requiring axes -- where a route must push current the "wrong" way through
a near-irreversible cut -- edges that sit at ``x_e ≈ 0`` CHATTER between branches from one
iterate to the next, the Newton step cycles, and the solve stalls at a start- and
machine-dependent point (two "stable" KKT points, e.g. 0.092 vs 0.070 on one real S axis).
The cure is to replace the kink by a softplus transition of width ``δ`` (:data:`DIODE_SMOOTH_DELTA`):

    h(x) = δ·log(1 + e^{x/δ})   (smoothed max(x,0)),   σ = h'(x) = 1/(1+e^{-x/δ})
    i_e(x) = g+·h(x) + g-·(x - h(x)),   d_e(x) = i_e'(x) = g+·σ + g-·(1-σ) ∈ [g-, g+]

so the edge conductance ``d_e`` now varies SMOOTHLY through the transition (no ×1e9 jump),
the energy is strictly convex and C², and Newton converges to the UNIQUE grounded minimiser
start-independently and machine-independently. At ``g- = g+`` the smoothing is an exact
no-op (``d_e = g+`` for every ``x`` regardless of ``δ``), so the symmetric-limit parity with
the undirected ``R_eff`` still holds to machine precision -- the smoothing only ever acts on
genuinely directed edges, and its ``O(δ)`` bias there is a documented, reproducible model
choice, not solver noise. The reduced Hessian ``B^T diag(d) B`` is SPD on any connected
grounded graph; it is factored by CHOLMOD (analyse-once, refactor-per-iterate; the sparsity
pattern is topology-invariant so one symbolic factorisation is reused across a network's
Newton iterates AND its set4 axes) with a transparent ``splu`` fallback. An ADMM variant was
measured 10-100x slower and dropped.

This is a **pure primitive**: signed incidence + per-edge conductances + terminals in,
``C_eff`` out. It holds no SCADC path, reads no GEM, and does not import ``canon``.
Edge orientation and the ``g-/g+`` ratio are the caller's responsibility -- for ECSPr
they come from reaction roles (substrate -> rxn -> product) and the directionality
ensemble's per-reaction ``g_rev/g_fwd = exp(dG'/RT)`` ratio, both supplied by the SCADC
driver, never by this module.
"""
from __future__ import annotations

import numpy as np
import scipy.sparse as sp
from scipy.sparse.linalg import splu
from scipy.special import spence               # dilogarithm, for the smoothed energy

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
    """Signed incidence ``B`` (m x n, CSR): row ``e`` has +1 at the tail and -1 at the
    head. ``edges`` is a sequence of ``(tail_idx, head_idx)`` integer pairs; ``n`` is the
    node count. Orientation is exactly as given -- the caller decides which end is the
    tail (for ECSPr: substrate-side is the tail, product-side is the head)."""
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
    """Solve ``H x = rhs`` for the reduced Newton system, regularizing a singular pivot.

    With all-positive edge weights the grounded Laplacian ``H`` is nonsingular for any
    CONNECTED augmented graph, so a raw ``splu`` succeeds on every well-conditioned iterate
    (the self-tests and the symmetric-limit parity gate never reach the except branch). A rare
    Newton iterate can still drive ``splu`` to an exactly-zero pivot when the active-set
    weighting spreads ``g-``/``g+`` across the ~9 orders the 1e-9 diode floor allows. A
    vanishing Tikhonov ridge -- the linear-algebra analog of that floor -- restores a unique
    descent direction without perturbing the well-conditioned bulk. Deterministic, escalating,
    and only ever reached when the raw factorization is otherwise fatal, so both the observed
    solve and the null run the identical primitive (they diverge only where the raw solve would
    have crashed).

    ``H`` is structurally symmetric (a grounded Laplacian ``Bk^T diag(d) Bk``), so it is
    factored with the ``MMD_AT_PLUS_A`` minimum-degree ordering on ``A + A^T`` rather than
    SuperLU's default unsymmetric COLAMD: ~3.3x fewer fill-ins / faster factorization on these
    graphs, and the sole hot path once CHOLMOD punts (50-80% of directed solves). The ordering
    only permutes the elimination -- it solves the SAME system -- so the answer changes only by
    the O(1e-5) conditioning noise these kappa~1e9 diode Hessians already carry; the
    well-conditioned symmetric-limit systems (which reach here only if CHOLMOD is unavailable)
    are re-ordered to within rounding, so the 1e-9 parity gate is unaffected."""
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
    """Numerically stable logistic sigmoid, elementwise."""
    out = np.empty_like(z, dtype=float)
    pos = z >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    ez = np.exp(z[~pos])
    out[~pos] = ez / (1.0 + ez)
    return out


def _softmax0(x, delta):
    """Smoothed ``max(x, 0)`` = ``delta * log(1 + exp(x/delta))``, overflow-safe."""
    z = x / delta
    return delta * np.where(z > 30.0, z, np.log1p(np.exp(np.minimum(z, 30.0))))


def _smooth_Hint(x, delta):
    """``int_0^x softmax0(u) du`` in closed form, stable across the whole range.

    Equals ``delta^2 * (-Li2(-e^{x/delta}) - pi^2/12)`` with ``Li2(-e^z) = spence(1 + e^z)``.
    Asymptotes: ``x >> delta -> x^2/2``; ``x << -delta -> ~0``. Only used to evaluate the
    smoothed energy for the Armijo line search (the O(delta^2) offsets cancel in comparisons)."""
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
    """Reusable SPD solver for the reduced Newton system ``H = Bk^T diag(d) Bk`` where
    ``Bk`` is the grounded incidence (ground column dropped). The sparsity pattern of ``H``
    is fixed by ``Bk`` -- identical across every Newton iterate AND every (s, t) axis solved
    on the same network -- so CHOLMOD analyses it ONCE (``cho_factor``) and only refactors
    the numeric values thereafter (``factorize`` in place). One instance is shared across a
    network's set4 axes; a fresh instance is built per augmented network (the node set, hence
    the pattern, changes when a fosmid is added). Falls back to :func:`_reg_spsolve` (``splu``
    + a vanishing Tikhonov ridge) when CHOLMOD is absent, reports the iterate non-PD, or
    returns an inaccurate solve -- the fallback is answer-identical to numerical tolerance, so
    absence never changes a result."""

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
                    self._fac = _cho_factor(H)          # analyse + factor (symbolic cached)
                else:
                    self._fac.factorize(H)              # reuse symbolic, refactor in place
                x = self._fac.solve(np.asarray(rhs, float).reshape(-1, 1)).ravel()
                # Guard the rare near-singular iterate CHOLMOD only WARNS about: verify the
                # residual and drop to the ridged splu path if the factorisation was inaccurate.
                if np.all(np.isfinite(x)) and \
                        np.abs(H @ x - rhs).max() <= 1e-6 * (np.abs(rhs).max() + 1.0):
                    self.used_cholmod = True
                    return x
            except Exception:
                self._fac = None                        # non-PD etc. -> fall through to splu
        return _reg_spsolve(H, rhs)


def directed_ceff(B, gp, gm, s, t, g=0, tol=DIRECTED_TOL, maxit=DIRECTED_MAXIT,
                  floor=DIODE_BACKWARD_FLOOR, delta=DIODE_SMOOTH_DELTA, reuse=None,
                  etol=1e-13, return_iters=False, phi0=None, return_phi=False,
                  return_converged=False):
    """Two-terminal effective conductance of a rectified network by smoothed-diode Newton.

    The diode kink is softened by a softplus of width ``delta`` (see the module docstring),
    making the energy strictly convex and C^2 so the grounded minimiser is unique and reached
    start-independently -- the fix for the active-set chattering that made backflow axes
    non-reproducible. Globalised by an Armijo line search on the (smoothed, monotonically
    decreasing) energy; convergence is declared on reaching ``tol`` OR on the line search
    stagnating (no descent step remains -> at the minimiser within numerical precision).

    Parameters
    ----------
    B : (m, n) signed incidence (CSR), from :func:`build_incidence`.
    gp, gm : (m,) forward / backward conductances per edge (``gm`` is clipped up to
        ``floor * gp`` to keep the grounded system nonsingular).
    s, t : source / sink node indices (unit current injected s -> t).
    g : grounded node index (its potential row/col is dropped; default 0).
    delta : softplus width smoothing the diode kink (:data:`DIODE_SMOOTH_DELTA`).
    reuse : optional :class:`_SPDReuse` for this network's incidence -- shares one symbolic
        CHOLMOD factorisation across the network's axes and Newton iterates. Built transiently
        when ``None`` (correct, just no cross-axis reuse).
    phi0 : optional (n,) warm-start potential. The energy is convex with a unique grounded
        minimiser, so ANY start reaches the same answer -- ``phi0`` only changes the iteration
        count, never the result (this is what the warm==cold self-test proves).

    Returns
    -------
    ``C_eff`` (float). With the optional flags, extra items are appended in the order
    ``iters``, ``phi``, ``converged`` (``return_iters``, ``return_phi``, ``return_converged``).
    """
    B = B.tocsr() if sp.issparse(B) else sp.csr_matrix(B)
    gp = np.asarray(gp, float)
    gm = np.maximum(np.asarray(gm, float), floor * gp)
    n = B.shape[1]
    I = np.zeros(n); I[s] = 1.0; I[t] = -1.0
    keep = np.arange(n) != g
    phi = np.zeros(n) if phi0 is None else np.array(phi0, float)
    if phi0 is not None:
        phi[g] = 0.0                              # keep the ground pinned
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
        d = gp * sig + gm * (1.0 - sig)               # smoothed conductance in [gm, gp]
        dphi = np.zeros(n)
        dphi[keep] = reuse.solve(d, -grad[keep])
        slope = grad @ dphi
        if slope > 0.0:                               # numerical guard -> steepest descent
            dphi = np.zeros(n); dphi[keep] = -grad[keep]; slope = grad @ dphi
        E0 = energy(phi)
        step = 1.0
        ok = False
        E1 = E0
        for _ in range(60):                           # Armijo on the convex smoothed energy
            E1 = energy(phi + step * dphi)
            if E1 <= E0 + 1e-4 * step * slope:
                ok = True
                break
            step *= 0.5
        if not ok:                                    # no descent step remains -> at minimiser
            converged = True
            break
        phi = phi + step * dphi
        if E0 - E1 <= etol * (abs(E0) + 1.0):         # negligible further descent -> minimiser
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
    """Independent correctness referent for the toy self-tests.

    Minimises the *same* smoothed convex C^2 energy (softplus width ``delta``) with a
    quasi-Newton method (L-BFGS-B) that shares no code path with the Newton solve above --
    different algorithm family, so agreement is a genuine cross-check, not a tautology. Dense;
    toy nets only.
    """
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
