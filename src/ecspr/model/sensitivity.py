# Exact derivatives of a solved measurement with respect to every edge conductance, from
# one extra linear solve.
#
# WHY THIS IS CHEAP
# -----------------
# The directed solve is the argmin of a strictly convex C2 energy, so the readout's
# derivative does NOT require differentiating the Newton iteration that found it. At the
# minimiser the stationarity condition ``B' i(B phi) = I`` holds identically in the
# parameters, and differentiating THAT gives ``dphi/dtheta = -H^-1 dg/dtheta`` with ``H``
# the same Hessian the last Newton step already factorised. One adjoint solve against it
# yields all ``2m`` edge derivatives at once, in O(m) afterwards.
#
# The adjoint right-hand side is the injection ``I`` itself, because the readout
# ``R_eff = phi_source - phi_sink`` at unit injection is ``I' phi``. That coincidence is
# what makes the symmetric limit collapse onto Tellegen: there ``lambda = phi``, the edge
# derivative becomes ``-x_e^2``, and ``dlog C_eff / dlog g_e`` is exactly the edge's share
# of dissipated power -- i.e. this module and :func:`ecspr.model.build.reaction_elasticities`
# must agree wherever both are defined. :func:`check_against_power` is that assertion, and
# it is the only self-test that does not need a finite difference.
#
# SIGN CONVENTION. Everything here differentiates the RESISTANCE ``R_eff``; conductance and
# log-conductance derivatives are chained from it, since ``C_eff = 1/R_eff``.
from __future__ import annotations

import numpy as np
import scipy.sparse as sp

from .directed import _SPDReuse, _sigmoid, _softmax0, DIODE_SMOOTH_DELTA

__all__ = ["EdgeSensitivity", "edge_sensitivity", "check_against_power"]


class EdgeSensitivity:
    # ``d readout / d gp_e`` and ``d readout / d gm_e`` for every solved edge, plus the
    # bookkeeping a caller needs to map them back onto the graph it built.
    #
    # ``oedge`` indexes ``graph.edges``; an edge shorted away by terminal contraction or
    # dropped with the disconnected component simply does not appear, and its derivative is
    # a true zero rather than a missing entry.
    __slots__ = ("oedge", "d_reff_dgp", "d_reff_dgm", "reff", "x", "adjoint_drop")

    def __init__(self, oedge, d_reff_dgp, d_reff_dgm, reff, x, adjoint_drop):
        self.oedge = oedge
        self.d_reff_dgp = d_reff_dgp
        self.d_reff_dgm = d_reff_dgm
        self.reff = float(reff)
        self.x = x
        self.adjoint_drop = adjoint_drop

    @property
    def d_ceff_dgp(self) -> np.ndarray:
        return -self.d_reff_dgp / (self.reff * self.reff)

    @property
    def d_ceff_dgm(self) -> np.ndarray:
        return -self.d_reff_dgm / (self.reff * self.reff)

    @property
    def d_logceff_dgp(self) -> np.ndarray:
        return -self.d_reff_dgp / self.reff

    @property
    def d_logceff_dgm(self) -> np.ndarray:
        return -self.d_reff_dgm / self.reff


def edge_sensitivity(solution, *, delta: float = DIODE_SMOOTH_DELTA) -> EdgeSensitivity:
    # Differentiate a converged :class:`ecspr.model.graph.Solution` against its conductances.
    #
    # ``delta`` must be the smoothing width the solve itself ran at; a mismatch silently
    # differentiates a different model than the one that was solved.
    sysd = solution.solved_system()
    B, gp, gm, phi = sysd["B"], sysd["gp"], sysd["gm"], sysd["phi"]
    n, si, ti = sysd["n"], sysd["source"], sysd["sink"]
    if B.shape[0] == 0 or not np.isfinite(solution.total) or solution.total == 0.0:
        z = np.zeros(B.shape[0])
        return EdgeSensitivity(sysd["oedge"], z, z.copy(), np.inf, z.copy(), z.copy())

    x = B @ phi
    hx = _softmax0(x, delta)
    sig = _sigmoid(x / delta)
    d = gp * sig + gm * (1.0 - sig)

    # Ground at the sink, exactly as ``solve`` did -- the Hessian is singular otherwise and
    # the adjoint would be solving a different system than the forward pass.
    keep = np.arange(n) != ti
    rhs = np.zeros(n)
    rhs[si], rhs[ti] = 1.0, -1.0
    lam = np.zeros(n)
    lam[keep] = _SPDReuse(B, keep).solve(d, rhs[keep])

    bl = B @ lam
    return EdgeSensitivity(sysd["oedge"], -bl * hx, -bl * (x - hx),
                           float(phi[si] - phi[ti]), x, bl)


def check_against_power(graph, solution, *, rtol: float = 1e-7) -> float:
    # Max relative disagreement between this module and ``Solution.edge_power`` on the
    # symmetric network, where the two are the same quantity by Tellegen.
    #
    # Raises when the network is not symmetric, because there the identity does not hold and
    # a passing number would mean nothing.
    gp = graph.gp[solution.original_edge_index()]
    gm = graph.gm[solution.original_edge_index()]
    if not np.allclose(gm, gp, rtol=1e-12, atol=0.0):
        raise ValueError("edge-power identity holds only on the symmetric network")
    s = edge_sensitivity(solution)
    mine = gp * (s.d_logceff_dgp + s.d_logceff_dgm)
    _, power = solution.edge_power()
    theirs = power / s.reff
    scale = max(float(np.abs(theirs).max()), np.finfo(float).tiny)
    err = float(np.abs(mine - theirs).max() / scale)
    if err > rtol:
        raise AssertionError(f"adjoint disagrees with edge power by {err:.3g}")
    return err
