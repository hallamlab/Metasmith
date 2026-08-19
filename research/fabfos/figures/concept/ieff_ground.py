import sys
import time

import numpy as np
import scipy.sparse as sp

import os                                                            # noqa: E402
from pathlib import Path                                             # noqa: E402
SRC = os.environ.get("ECSPR_SRC", str(Path(__file__).resolve().parents[4] / "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)
import ecspr.model.directed as ed                                          # noqa: E402
from ecspr.model.graph import attach_leak, OMEGA                           # noqa: E402

from atom_graph import incidence                                     # noqa: E402

SPD_RTOL = 1e-8


class SolveRefused(RuntimeError):
    pass
class LaplacianAssembler:
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
        scale = float(np.abs(H).sum(axis=1).max()) * float(np.abs(x).max()) \
            + float(np.abs(rhs).max())
        return float(r / (scale + np.finfo(float).tiny))

    def _pcg(self, H, rhs, hnorm):
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
    def __init__(self, g, terminals, leak=1e-6, port=1.0):
        self.m_rxn = g.m
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

        prov = gl.meta["edge_reactions"]
        self.pe = prov.edge.to_numpy().astype(np.int64)
        names, self.pcode = np.unique(prov.mnxr.to_numpy().astype(str), return_inverse=True)
        self.rxn_names = names
        self.rxn_index = {r: i for i, r in enumerate(names)}
        denom = self.gp[self.pe]
        self.pfrac = prov.gp.to_numpy(float) / np.where(denom > 0, denom, 1.0)
        self.n_rxn = len(names)

    def injection_at(self, src):
        src = sorted(src)
        I = np.zeros(self.n)
        I[src] = 1.0 / len(src)
        I[self.ground] -= 1.0
        return I, src

    def injection(self, mnxr):
        return self.injection_at(self.terminals[mnxr][1])

    def node_throughput(self, ie_abs):
        w = np.repeat(ie_abs, 2)
        thr = 0.5 * np.bincount(self.E.ravel(), w, minlength=self.n)
        thr[self.ground] = 0.0
        return thr

    def attribute(self, ie_abs):
        return np.bincount(self.pcode, ie_abs[self.pe] * self.pfrac, minlength=self.n_rxn)

    def full_reuse(self, rtol=SPD_RTOL, order="default", fast_assembly=True):
        asm = LaplacianAssembler(self.E, self.keep) if fast_assembly else None
        return VerifiedSPD(self.B[:, self.keep], rtol=rtol, order=order, assembler=asm)

    def solve_full(self, mnxr, reuse, phi0=None, **kw):
        I, _src = self.injection(mnxr)
        phi, nit, why = newton_rhs(self.B, self.gp, self.gm, I, self.keep, reuse,
                                   phi0=phi0, **kw)
        ie = np.abs(edge_current(self.B, self.gp, self.gm, phi))
        return self.attribute(ie), phi, nit, why

    def solve_full_channels(self, mnxr, reuse, phi0=None, check=False, **kw):
        I, src = self.injection(mnxr)
        phi, nit, why = newton_rhs(self.B, self.gp, self.gm, I, self.keep, reuse,
                                   phi0=phi0, **kw)
        x = self.B @ phi
        ie = np.abs(edge_current(self.B, self.gp, self.gm, phi))
        row = self.attribute(ie)

        pe_edge = ie * np.abs(x)
        power = self.attribute(pe_edge)

        phi_mid = 0.5 * (phi[self.E[:, 0]] + phi[self.E[:, 1]])
        wsum = self.attribute(ie)
        phi_rxn = np.divide(self.attribute(ie * phi_mid), wsum,
                            out=np.zeros(self.n_rxn), where=wsum > 0)
        dv = float(phi.max()) - phi_rxn
        dv[wsum <= 0] = np.nan

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

    leak_tail = sys_.E[m_rxn:, 0]
    keep_leak = m_rxn + np.flatnonzero(innode[leak_tail])

    ce = np.concatenate([np.sort(keep_rxn), keep_leak])
    cn = np.flatnonzero(innode)
    loc = np.full(sys_.n, -1, np.int64)
    loc[cn] = np.arange(len(cn))
    return ce, cn, loc


def cone_solve(sys_: GroundSystem, mnxr, field: SymmetricField, cover=0.999,
               rtol=SPD_RTOL, order="default", **kw):
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
