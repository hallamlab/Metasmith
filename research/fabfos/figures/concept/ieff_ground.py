"""Verified rectified universal-ground solves, and the per-source flow cone that makes
them affordable at universe scale.

**What is measured.** With ``attach_leak`` every metabolite leaks into a virtual node
OMEGA, which is grounded. Injecting one ampere spread over reaction A's product atoms and
solving the rectified network reports, in a single solve, the current every *other*
reaction draws -- so an N x N pairwise table costs N solves rather than N^2. Entry (a, b)
is that attributed current; the layout consumes ``R = 1/max(I, I^T)``.

**Why this file exists rather than calling the library.** ``ecspr_directed._SPDReuse``
tries CHOLMOD, accepts the result when ``|Hx - rhs|_inf <= 1e-6 (|rhs|_inf + 1)``, and
otherwise drops to a Tikhonov-ridged ``splu``. Three things about that shape are wrong and
all three bite at 589k unknowns:

* the acceptance test is scale-blind. It ignores ``|H| |x|``, so on a diode Hessian with
  kappa ~ 1e9 a perfectly backward-stable factorization gets discarded over arithmetic no
  other solver improves on. :class:`VerifiedSPD` tests the *relative* residual.
* the fallback's residual is never checked at all, so the "safe" path is the unverified
  one. Here both paths are checked and a solve that passes neither raises.
* ``used_cholmod`` is a sticky boolean, so the library docstring's claim that ``splu`` is
  "the sole hot path once CHOLMOD punts (50-80% of directed solves)" is unmeasurable.
  Here they are integer counters.

The library lives in the ``nosco`` worktree and is not edited from this scope; these are
reported there as a note.

**Cost.** The rectified conductance depends on the sign of each edge's own potential drop,
so the matrix changes between Newton iterates and every iterate pays a fresh numeric
factorization: ~30 s at universe scale against a 0.11 s triangular solve. That ratio is the
whole problem. What helps, measured: proximity source ordering with warm starts,
:class:`LaplacianAssembler` (the assembly is a fixed sparse matvec, not a triple product),
and one thread per worker. What does not help, also measured, is listed in the README --
fill-reducing ordering, the flow cone below, stale-factor PCG, and a frozen consensus
active set.

:func:`flow_cone` and :func:`cone_solve` implement goal-directed search per source: rank
edges by the current *this* source drives through them (one cheap symmetric solve, constant
matrix, one factorization for the whole run) and hand the rectified Newton only the
subgraph carrying all but a negligible tail. They are kept because they are how that idea
gets re-measured, not because they pay: at universe scale 99.9% of a source's flow needs
92% of edges, so the cone is the network. Raising the leak does not localize it either --
the field is identical from leak 1e-6 to 1e-4.
"""
import sys
import time

import numpy as np
import scipy.sparse as sp

import os                                                            # noqa: E402
LIB = os.environ.get(
    "ECSPR_LIB",
    "/home/tony/agentic_workspace/projects/fabfos/nosco/src/metasmith_libraries/resources/lib")
if LIB not in sys.path:
    sys.path.insert(0, LIB)
import ecspr_directed as ed                                          # noqa: E402
from ecspr_graph import attach_leak, OMEGA                           # noqa: E402

from atom_graph import incidence                                     # noqa: E402

# Relative-residual acceptance for one reduced Newton solve. float64 backward stability on
# a Laplacian this size lands near 1e-12; 1e-8 leaves four orders of headroom for the
# conditioning the 1e-9 diode floor introduces while still refusing a genuinely bad solve.
SPD_RTOL = 1e-8


class SolveRefused(RuntimeError):
    """Neither CHOLMOD nor the ridged fallback met the relative residual bound."""


class LaplacianAssembler:
    """``d -> H.data`` as one fixed sparse matvec.

    ``Bk^T diag(d) Bk`` has a sparsity pattern fixed by the topology, so recomputing it as a
    sparse triple product on every Newton iterate re-derives structure that never changes.
    Each edge contributes ``+d`` to two diagonals and ``-d`` to two off-diagonals, so the map
    from the edge vector to ``H.data`` is a constant matrix, built once here.
    """

    def __init__(self, edges, keep):
        e = np.asarray(edges, dtype=np.int64)
        nk = int(keep.sum())
        loc = np.full(len(keep), -1, np.int64)
        loc[keep] = np.arange(nk)
        u, v = loc[e[:, 0]], loc[e[:, 1]]
        ei = np.arange(len(e), dtype=np.int64)
        iu, iv = u >= 0, v >= 0
        both = iu & iv
        ri = np.concatenate([u[iu], v[iv], u[both], v[both]])
        ci = np.concatenate([u[iu], v[iv], v[both], u[both]])
        co = np.concatenate([np.ones(iu.sum()), np.ones(iv.sum()),
                             -np.ones(both.sum()), -np.ones(both.sum())])
        ee = np.concatenate([ei[iu], ei[iv], ei[both], ei[both]])

        # CSR order is lexicographic in (row, col), so a lexicographic sort of the
        # contributions puts them in data order and the run boundaries are the entries.
        key = ri * nk + ci
        order = np.argsort(key, kind="stable")
        skey = key[order]
        new = np.empty(len(skey), bool)
        new[0] = True
        np.not_equal(skey[1:], skey[:-1], out=new[1:])
        pos = np.empty(len(key), np.int64)
        pos[order] = np.cumsum(new) - 1
        upair = skey[new]
        self.indices = (upair % nk).astype(np.int32)
        self.indptr = np.concatenate(
            [[0], np.cumsum(np.bincount(upair // nk, minlength=nk))]).astype(np.int32)
        self.P = sp.csr_matrix((co, (pos, ee)), shape=(len(upair), len(e)))
        self.shape = (nk, nk)
        self.nnz = len(upair)

    def __call__(self, d):
        return sp.csr_matrix((self.P @ d, self.indices, self.indptr), shape=self.shape)


class VerifiedSPD:
    """Reduced Newton system ``H = Bk^T diag(d) Bk`` with symbolic reuse and a verified
    answer. ``Bk`` is the incidence with the ground column already dropped.

    **Amortizing the factorization.** The rectified conductance depends on the sign of each
    edge's own potential drop, so ``H`` changes between Newton iterates and, at 589k
    unknowns, each numeric refactorization costs ~30 s against a 0.11 s triangular solve --
    that ratio, not the solver, is why a universe sweep looks like months. With
    ``stale_ok``, a factorization built at a nearby ``d`` is kept as a *preconditioner* and
    the true Newton system is solved by preconditioned conjugate gradients instead. The
    outer Newton iteration is still driven to a vanishing gradient on the true residual, so
    this changes the path taken, never the fixed point; and the same relative-residual test
    gates the PCG answer as gates a direct one. When PCG cannot reach the tolerance in
    ``pcg_maxit`` the factor is refreshed and the solve redone, so a stale factor costs time
    at worst, never accuracy.
    """

    def __init__(self, Bk, rtol=SPD_RTOL, order="default", assembler=None, pcg_maxit=40):
        self.Bk = Bk.tocsc() if sp.issparse(Bk) else sp.csr_matrix(Bk).tocsc()
        self.rtol = float(rtol)
        self.order = order
        self.asm = assembler
        self.pcg_maxit = int(pcg_maxit)
        self._fac = None
        self.n_cholmod = 0
        self.n_fallback = 0
        self.n_refused = 0
        self.n_pcg = 0
        self.n_pcg_fail = 0
        self.pcg_iters = 0
        self.worst_rel = 0.0
        self.t_factor = 0.0
        self.t_solve = 0.0

    def _assemble(self, d):
        if self.asm is not None:
            return self.asm(d)
        return (self.Bk.T @ sp.diags(d) @ self.Bk).tocsc()

    def _rel(self, H, x, rhs):
        if not np.all(np.isfinite(x)):
            return np.inf
        r = np.abs(H @ x - rhs).max()
        # Scale by the size of the terms that were cancelled to form the residual, not by
        # the right-hand side alone -- that is the difference between a backward-stability
        # test and a coincidence about how big ``rhs`` happens to be.
        scale = float(np.abs(H).sum(axis=1).max()) * float(np.abs(x).max()) \
            + float(np.abs(rhs).max())
        return float(r / (scale + np.finfo(float).tiny))

    def _pcg(self, H, rhs, hnorm):
        """PCG on ``H x = rhs`` preconditioned by the stale factor. Returns
        ``(x, iterations, relative_residual)``."""
        x = np.zeros_like(rhs)
        r = rhs.copy()
        rmax = float(np.abs(rhs).max())
        z = self._fac.solve(r.reshape(-1, 1)).ravel()
        p = z.copy()
        rz = float(r @ z)
        it = 0
        for it in range(1, self.pcg_maxit + 1):
            Hp = H @ p
            pHp = float(p @ Hp)
            if pHp <= 0 or not np.isfinite(pHp):
                break
            a = rz / pHp
            x += a * p
            r -= a * Hp
            if float(np.abs(r).max()) <= self.rtol * (hnorm * float(np.abs(x).max()) + rmax):
                break
            z = self._fac.solve(r.reshape(-1, 1)).ravel()
            rz2 = float(r @ z)
            if rz == 0.0:
                break
            p = z + (rz2 / rz) * p
            rz = rz2
        return x, it, self._rel(H, x, rhs)

    def solve(self, d, rhs, stale_ok=False):
        H = self._assemble(d)
        rhs = np.asarray(rhs, float)
        if stale_ok and self._fac is not None:
            hnorm = float(np.abs(H).sum(axis=1).max())
            t = time.perf_counter()
            try:
                x, it, rel = self._pcg(H, rhs, hnorm)
            except Exception:
                x, it, rel = None, 0, np.inf
            self.t_solve += time.perf_counter() - t
            self.pcg_iters += it
            if rel <= self.rtol:
                self.n_pcg += 1
                self.worst_rel = max(self.worst_rel, rel)
                return x
            # Stale beyond use. Fall through to a numeric refactorization, which reuses the
            # symbolic analysis -- the pattern is topology-fixed, only the values moved.
            self.n_pcg_fail += 1
        if ed._HAVE_CHOLMOD:
            try:
                t = time.perf_counter()
                if self._fac is None:
                    self._fac = ed._cho_factor(H, order=self.order)
                else:
                    self._fac.factorize(H)
                self.t_factor += time.perf_counter() - t
                t = time.perf_counter()
                x = self._fac.solve(rhs.reshape(-1, 1)).ravel()
                self.t_solve += time.perf_counter() - t
                rel = self._rel(H, x, rhs)
                if rel <= self.rtol:
                    self.n_cholmod += 1
                    self.worst_rel = max(self.worst_rel, rel)
                    return x
            except Exception:
                self._fac = None
        x = ed._reg_spsolve(H, rhs)
        rel = self._rel(H, x, rhs)
        if rel <= self.rtol:
            self.n_fallback += 1
            self.worst_rel = max(self.worst_rel, rel)
            return x
        self.n_refused += 1
        raise SolveRefused(f"relative residual {rel:.3e} > {self.rtol:.1e} "
                           f"(n={H.shape[0]}, nnz={H.nnz})")

    def stats(self):
        return dict(cholmod=self.n_cholmod, fallback=self.n_fallback,
                    refused=self.n_refused, pcg=self.n_pcg, pcg_fail=self.n_pcg_fail,
                    pcg_iters=self.pcg_iters, worst_rel=self.worst_rel,
                    t_factor=self.t_factor, t_solve=self.t_solve)


def newton_rhs(B, gp, gm, I, keep, reuse, phi0=None,
               tol=ed.DIRECTED_TOL, maxit=ed.DIRECTED_MAXIT,
               delta=ed.DIODE_SMOOTH_DELTA, etol=1e-13, stale_ok=False):
    """``ecspr_directed.directed_ceff``'s smoothed-diode Newton, generalized from
    ``I = e_s - e_t`` to an arbitrary injection vector.

    That generalization is what fixes the sparsity pattern across sources: contracting a
    source terminal into a supernode (which is what ``ecspr_graph.solve`` does) changes the
    topology, so CHOLMOD must re-analyse per source. Injecting distributed current instead
    leaves the pattern identical, and is the more physical reading for a probe that reads
    downstream current anyway.

    Convergence is declared on the reduced-gradient tolerance OR on line-search stagnation
    -- the second is not a failure. Backflow axes floor the reduced gradient near 1e-8 in
    float64 and never reach ``tol``; a wrapper that accepts only the first spuriously
    refuses them.
    """
    n = B.shape[1]
    phi = np.zeros(n) if phi0 is None else np.array(phi0, float)
    phi[~keep] = 0.0

    def cur(x):
        hx = ed._softmax0(x, delta)
        return gp * hx + gm * (x - hx)

    def energy(p):
        x = B @ p
        return float(np.sum(0.5 * gm * x * x + (gp - gm) * ed._smooth_Hint(x, delta)) - I @ p)

    why, nit = "maxit", 0
    for nit in range(1, maxit + 1):
        x = B @ phi
        grad = B.T @ cur(x) - I
        if np.abs(grad[keep]).max() < tol:
            why = "gradient"
            break
        sig = ed._sigmoid(x / delta)
        d = gp * sig + gm * (1.0 - sig)
        dphi = np.zeros(n)
        dphi[keep] = reuse.solve(d, -grad[keep], stale_ok=stale_ok)
        slope = grad @ dphi
        if slope > 0.0:
            dphi = np.zeros(n)
            dphi[keep] = -grad[keep]
            slope = grad @ dphi
        E0 = energy(phi)
        step, ok, E1 = 1.0, False, E0
        for _ in range(60):
            E1 = energy(phi + step * dphi)
            if E1 <= E0 + 1e-4 * step * slope:
                ok = True
                break
            step *= 0.5
        if not ok:
            why = "linesearch"
            break
        phi = phi + step * dphi
        if E0 - E1 <= etol * (abs(E0) + 1.0):
            why = "energy"
            break
    return phi, nit, why


def edge_current(B, gp, gm, phi, delta=ed.DIODE_SMOOTH_DELTA):
    x = B @ phi
    hx = ed._softmax0(x, delta)
    return gp * hx + gm * (x - hx)


class GroundSystem:
    """The leaky atom graph plus everything a sweep needs: incidence, conductances, the
    OMEGA ground, per-reaction terminals and the edge->reaction attribution map."""

    def __init__(self, g, terminals, leak=1e-6, port=1.0):
        self.m_rxn = g.m                       # reaction edges come first; leaks are appended
        gl, self.leak_edges = attach_leak(g, None, leak=leak, port=port)
        self.gl = gl
        self.n = gl.n
        self.m = gl.m
        self.E = np.asarray(gl.edges, dtype=np.int64)
        self.B = incidence(gl.edges, gl.n)
        self.gp = np.asarray(gl.gp, float)
        self.gm = np.maximum(np.asarray(gl.gm, float), ed.DIODE_BACKWARD_FLOOR * self.gp)
        self.ground = gl.idx[OMEGA]
        self.keep = np.arange(gl.n) != self.ground
        self.terminals = terminals
        self.leak = float(leak)

        # Attribution: one provenance row per (edge, contributing reaction). A shared edge
        # splits by conductance share. Integer codes + bincount, not a pandas groupby --
        # at 1.8M rows the groupby costs ~31 ms per source of pure overhead.
        prov = gl.meta["edge_reactions"]
        self.pe = prov.edge.to_numpy().astype(np.int64)
        names, self.pcode = np.unique(prov.mnxr.to_numpy().astype(str), return_inverse=True)
        self.rxn_names = names
        self.rxn_index = {r: i for i, r in enumerate(names)}
        denom = self.gp[self.pe]
        self.pfrac = prov.gp.to_numpy(float) / np.where(denom > 0, denom, 1.0)
        self.n_rxn = len(names)

    def injection_at(self, src):
        """Unit current spread evenly over ``src`` atom nodes, out at OMEGA."""
        src = sorted(src)
        I = np.zeros(self.n)
        I[src] = 1.0 / len(src)
        I[self.ground] -= 1.0
        return I, src

    def injection(self, mnxr):
        """Unit current in at the reaction's product atoms, out at OMEGA."""
        return self.injection_at(self.terminals[mnxr][1])

    def node_throughput(self, ie_abs):
        """Current passing *through* each atom node.

        KCL makes in and out equal at every non-terminal node, so half the absolute current
        on the incident edges is the throughput. Leak edges are included: a node that drains
        to OMEGA really did carry that current. The ground node's own entry is meaningless
        (every leak lands there) and is zeroed.
        """
        w = np.repeat(ie_abs, 2)
        thr = 0.5 * np.bincount(self.E.ravel(), w, minlength=self.n)
        thr[self.ground] = 0.0
        return thr

    def attribute(self, ie_abs):
        """Edge currents (absolute, full-length) -> current per reaction."""
        return np.bincount(self.pcode, ie_abs[self.pe] * self.pfrac, minlength=self.n_rxn)

    def full_reuse(self, rtol=SPD_RTOL, order="default", fast_assembly=True):
        asm = LaplacianAssembler(self.E, self.keep) if fast_assembly else None
        return VerifiedSPD(self.B[:, self.keep], rtol=rtol, order=order, assembler=asm)

    def solve_full(self, mnxr, reuse, phi0=None, **kw):
        """Exact rectified solve on the whole network. The reference."""
        I, _src = self.injection(mnxr)
        phi, nit, why = newton_rhs(self.B, self.gp, self.gm, I, self.keep, reuse,
                                   phi0=phi0, **kw)
        ie = np.abs(edge_current(self.B, self.gp, self.gm, phi))
        return self.attribute(ie), phi, nit, why

    def solve_full_channels(self, mnxr, reuse, phi0=None, check=False, **kw):
        """One solve, three per-reaction channels.

        ``attribute`` reduces *any* per-edge vector to per-reaction (splitting a shared edge
        by conductance share), so the two extra channels are the same reducer over different
        edge quantities and cost nothing beyond the solve that already happened:

        * ``current`` -- the incumbent metric, unchanged.
        * ``power`` -- ``|I_e| * |dV_e|`` dissipated in the reaction's own atom transfers.
          Conductance spans four decades here, so a promiscuous cofactor edge (g up to 2666)
          dissipates ~1/g of the power a dedicated pathway edge does at the same current.
          That is the cofactor-leakage protection stated as physics.
        * ``dv`` -- the potential the source injects at, minus this reaction's own. A
          reaction's potential is the current-weighted mean of its edges' mid-edge
          potentials, which is two calls to the same reducer. The reference is the network's
          peak potential, which is an injection node -- the only current source -- so the
          drop is non-negative by construction: every reaction potential is a convex
          combination of node potentials and cannot exceed the maximum. Referencing the
          source reaction's own attributed potential instead makes it the mean over its own
          substrate-side edges, which sat *below* a close downstream neighbour on the very
          first source tried and handed back a negative "distance".

          The *difference* is what is returned, not phi: phi sits on a 1/(N*leak) pedestal
          of order 1e2-1e3 while the signal is 1e-2-1e1, and a float32 cast of the raw
          potential would quantize the signal away.

        With ``check``, also returns Tellegen closure -- total dissipation over every edge
        against the power the source delivers, and reaction+leak power against that total.
        """
        I, src = self.injection(mnxr)
        phi, nit, why = newton_rhs(self.B, self.gp, self.gm, I, self.keep, reuse,
                                   phi0=phi0, **kw)
        x = self.B @ phi                                   # per-edge potential drop
        ie = np.abs(edge_current(self.B, self.gp, self.gm, phi))
        row = self.attribute(ie)

        pe_edge = ie * np.abs(x)
        power = self.attribute(pe_edge)

        phi_mid = 0.5 * (phi[self.E[:, 0]] + phi[self.E[:, 1]])
        wsum = self.attribute(ie)
        phi_rxn = np.divide(self.attribute(ie * phi_mid), wsum,
                            out=np.zeros(self.n_rxn), where=wsum > 0)
        dv = float(phi.max()) - phi_rxn
        dv[wsum <= 0] = np.nan                             # unreached: not "coincident"

        aux = dict(power=power, dv=dv)
        if check:
            tot = float(pe_edge.sum())
            delivered = float(I @ phi)
            leak_p = float(pe_edge[self.m_rxn:].sum())
            aux["check"] = dict(
                tellegen=abs(tot - delivered) / max(abs(delivered), 1e-300),
                reclose=abs((power.sum() + leak_p) - tot) / max(tot, 1e-300),
                dv_min=float(np.nanmin(dv)), dv_max=float(np.nanmax(dv)),
                n_unreached=int((wsum <= 0).sum()))
        return row, aux, phi, nit, why


class SymmetricField:
    """One constant-matrix factorization, reused by every source, giving the flow field the
    cone is cut from.

    Uses ``d = gp``: the rectifier can only throttle an edge below its forward conductance,
    so the symmetric field over-estimates where current can go and the cone it selects is a
    superset of where the rectified solution actually flows. That direction of error is the
    one that matters -- a cone that is too generous costs time, a cone that is too tight
    silently truncates the answer.
    """

    def __init__(self, sys_: GroundSystem, rtol=SPD_RTOL, order="default"):
        self.s = sys_
        self.reuse = VerifiedSPD(sys_.B[:, sys_.keep], rtol=rtol, order=order)
        self.d = sys_.gp

    def phi(self, I):
        x = np.zeros(self.s.n)
        x[self.s.keep] = self.reuse.solve(self.d, I[self.s.keep])
        return x

    def edge_abs(self, I):
        return np.abs(self.d * (self.s.B @ self.phi(I)))


def flow_cone(sys_: GroundSystem, i_abs, src, cover=0.999, min_edges=2000):
    """Edges carrying all but ``1 - cover`` of the symmetric flow, closed under leaks.

    Returns ``(cone_edges, cone_nodes, local_of_node)``. Every kept node keeps its leak to
    OMEGA, so the discarded remainder of the network is replaced by ground rather than by
    an open circuit, and the cone is connected through OMEGA whatever the selection does.
    """
    m_rxn = sys_.m_rxn
    w = i_abs[:m_rxn]
    order = np.argsort(w)[::-1]
    c = np.cumsum(w[order])
    total = c[-1] if len(c) else 0.0
    k = int(np.searchsorted(c, cover * total) + 1) if total > 0 else min_edges
    k = max(k, min_edges)
    k = min(k, m_rxn)
    keep_rxn = order[:k]

    innode = np.zeros(sys_.n, bool)
    innode[sys_.E[keep_rxn].ravel()] = True
    innode[src] = True
    innode[sys_.ground] = True

    # Leak edges are (atom -> OMEGA) and were appended after the reaction edges.
    leak_tail = sys_.E[m_rxn:, 0]
    keep_leak = m_rxn + np.flatnonzero(innode[leak_tail])

    ce = np.concatenate([np.sort(keep_rxn), keep_leak])
    cn = np.flatnonzero(innode)
    loc = np.full(sys_.n, -1, np.int64)
    loc[cn] = np.arange(len(cn))
    return ce, cn, loc


def cone_solve(sys_: GroundSystem, mnxr, field: SymmetricField, cover=0.999,
               rtol=SPD_RTOL, order="default", **kw):
    """Rectified solve restricted to this source's flow cone. Returns per-reaction current
    over the *full* reaction vocabulary (reactions outside the cone read zero, which is the
    approximation the cone makes explicit)."""
    I, src = sys_.injection(mnxr)
    i_abs = field.edge_abs(I)
    ce, cn, loc = flow_cone(sys_, i_abs, src, cover=cover)

    nl = len(cn)
    rows = np.repeat(np.arange(len(ce)), 2)
    cols = loc[sys_.E[ce]].ravel()
    vals = np.tile(np.array([1.0, -1.0]), len(ce))
    Bc = sp.csr_matrix((vals, (rows, cols)), shape=(len(ce), nl))
    gpc, gmc = sys_.gp[ce], sys_.gm[ce]
    Ic = np.zeros(nl)
    Ic[loc[src]] = 1.0 / len(src)
    gl = loc[sys_.ground]
    Ic[gl] -= 1.0
    keepc = np.arange(nl) != gl

    reuse = VerifiedSPD(Bc[:, keepc], rtol=rtol, order=order)
    phi, nit, why = newton_rhs(Bc, gpc, gmc, Ic, keepc, reuse, **kw)
    iec = np.abs(edge_current(Bc, gpc, gmc, phi))
    ie = np.zeros(sys_.m)
    ie[ce] = iec
    return sys_.attribute(ie), dict(nit=nit, why=why, n_cone_edges=len(ce),
                                    n_cone_nodes=nl, spd=reuse.stats())
