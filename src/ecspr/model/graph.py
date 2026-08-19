from __future__ import annotations

from dataclasses import dataclass, field
from collections import defaultdict
from pathlib import Path

import numpy as np
import scipy.sparse as sp
from scipy.sparse.linalg import splu

from .directed import (build_incidence, directed_ceff, _SPDReuse, _softmax0,
                       DIODE_BACKWARD_FLOOR, DIODE_SMOOTH_DELTA, DIRECTED_TOL)


REFF_EPS = 1e-12
IEFF_EPS = 1e-12

SELFTEST_TOL = 1e-9

SRC_SUPERNODE = ("__terminal__", "source")
SNK_SUPERNODE = ("__terminal__", "sink")


@dataclass
class AtomGraph:
    nodes: list
    edges: list
    gp: np.ndarray
    gm: np.ndarray
    meta: dict = field(default_factory=dict)
    idx: dict = field(default=None, repr=False)
    _by_met: dict = field(default=None, repr=False)

    def __post_init__(self):
        self.gp = np.asarray(self.gp, float)
        self.gm = np.asarray(self.gm, float)
        if self.idx is None:
            self.idx = {nd: i for i, nd in enumerate(self.nodes)}
        if self._by_met is None:
            by = defaultdict(list)
            for nd in self.nodes:
                by[nd[0]].append(nd)
            self._by_met = {m: sorted(v, key=lambda n: n[1]) for m, v in by.items()}

    @property
    def n(self) -> int:
        return len(self.nodes)

    @property
    def m(self) -> int:
        return len(self.edges)

    def metabolites(self) -> list:
        return sorted(self._by_met)

    def atoms_of(self, met: str) -> list:
        return list(self._by_met.get(met, ()))

    def ranks_of(self, met: str) -> list:
        return [nd[1] for nd in self._by_met.get(met, ())]

    @classmethod
    def from_edge_records(cls, records, meta=None):
        nodes, idx, edges, gp, gm = [], {}, [], [], []

        def _i(k):
            j = idx.get(k)
            if j is None:
                j = idx[k] = len(nodes)
                nodes.append(k)
            return j

        for a, b, p, mn in records:
            edges.append((_i(a), _i(b)))
            gp.append(float(p))
            gm.append(float(mn))
        return cls(nodes, edges, np.asarray(gp, float), np.asarray(gm, float),
                   dict(meta or {}), idx)

    def save(self, path):
        import json as _json
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        e = np.asarray(self.edges, np.int64).reshape(-1, 2) if self.edges else \
            np.zeros((0, 2), np.int64)
        meta = {k: v for k, v in self.meta.items() if not hasattr(v, "shape")
                and k != "edge_reactions"}
        np.savez_compressed(
            path,
            node_met=np.array([n[0] for n in self.nodes], dtype=object),
            node_rank=np.array([n[1] for n in self.nodes], dtype=np.int64),
            tail=e[:, 0], head=e[:, 1], gp=self.gp, gm=self.gm,
            meta=np.array(_json.dumps(meta, default=str)))
        return path

    @classmethod
    def load(cls, path):
        import json as _json
        z = np.load(Path(path), allow_pickle=True)
        nodes = list(zip(z["node_met"].tolist(), (int(r) for r in z["node_rank"])))
        edges = list(zip(z["tail"].tolist(), z["head"].tolist()))
        return cls(nodes, edges, z["gp"], z["gm"], _json.loads(str(z["meta"])))

    def with_ratios(self, ratio: float):
        return AtomGraph(list(self.nodes), list(self.edges), self.gp.copy(),
                         ratio * self.gp, dict(self.meta), dict(self.idx))


@dataclass(frozen=True)
class Terminal:
    label: str
    nodes: frozenset
    metabolites: tuple = ()
    missing: tuple = ()

    @classmethod
    def metabolite(cls, graph: AtomGraph, mnxm: str, mask=None, label=None) -> "Terminal":
        atoms = graph.atoms_of(mnxm)
        if mask is not None:
            keep = set(int(r) for r in mask)
            atoms = [a for a in atoms if a[1] in keep]
        return cls(label or mnxm, frozenset(atoms), (mnxm,),
                   () if atoms else (mnxm,))

    @classmethod
    def merge(cls, graph: AtomGraph, mnxms, mask=None, label="ground") -> "Terminal":
        mnxms = tuple(mnxms)
        atoms, missing = set(), []
        for m in mnxms:
            a = graph.atoms_of(m)
            if mask and m in mask:
                keep = set(int(r) for r in mask[m])
                a = [x for x in a if x[1] in keep]
            if not a:
                missing.append(m)
            atoms.update(a)
        return cls(label, frozenset(atoms), mnxms, tuple(missing))

    @classmethod
    def of_nodes(cls, label: str, nodes) -> "Terminal":
        nodes = frozenset(nodes)
        return cls(label, nodes, tuple(sorted({n[0] for n in nodes})))

    def __len__(self):
        return len(self.nodes)

    def __bool__(self):
        return bool(self.nodes)


class Solution:
    __slots__ = ("graph", "source", "sink", "total", "injected", "converged",
                 "_phi_c", "_cidx", "_cmap", "_cur", "_otail", "_ohead", "_nin",
                 "_oedge", "iters", "used_newton", "note")

    def __init__(self, graph, source, sink, *, total, phi_c, cidx, cmap, cur,
                 otail, ohead, converged, oedge=None, iters=0, used_newton=True, note=""):
        self.graph = graph
        self.source = source
        self.sink = sink
        self.total = float(total)
        self.injected = 1.0
        self.converged = bool(converged)
        self.iters = int(iters)
        self.used_newton = bool(used_newton)
        self.note = note
        self._phi_c = phi_c
        self._cidx = cidx
        self._cmap = cmap
        self._cur = cur
        self._otail = otail
        self._ohead = ohead
        self._oedge = (np.arange(len(cur), dtype=np.int64) if oedge is None
                       else np.asarray(oedge, np.int64))
        self._nin = graph.n

    def original_edge_index(self) -> np.ndarray:
        return self._oedge

    def edge_currents(self) -> tuple:
        return self._oedge, self._cur

    def voltage(self, node) -> float:
        ck = self._cmap.get(node)
        if ck is None:
            return float("nan")
        j = self._cidx.get(ck)
        return float("nan") if j is None else float(self._phi_c[j])

    def drop(self, a, b) -> float:
        return self.voltage(a) - self.voltage(b)

    def voltage_metabolite(self, mnxm) -> dict:
        atoms = self.graph.atoms_of(mnxm)
        per = {a[1]: self.voltage(a) for a in atoms}
        wts = {a[1]: self.throughput(a) for a in atoms}
        finite = [(per[r], wts[r]) for r in per if per[r] == per[r]]
        wsum = sum(w for _, w in finite)
        mean = (sum(v * w for v, w in finite) / wsum) if wsum > 0 else (
            float(np.mean([v for v, _ in finite])) if finite else float("nan"))
        return dict(per_atom=per, weighted_mean=float(mean), n_atoms=len(atoms))

    def _mask(self, nodes) -> np.ndarray:
        mk = np.zeros(self._nin, dtype=bool)
        gi = self.graph.idx
        for nd in nodes:
            j = gi.get(nd)
            if j is not None:
                mk[j] = True
        return mk

    def _net_inflow(self, nodes) -> float:
        if self._cur.size == 0:
            return 0.0
        mk = self._mask(nodes)
        return float(np.sum(self._cur * (mk[self._ohead].astype(float)
                                         - mk[self._otail].astype(float))))

    def _boundary_flux(self, nodes) -> tuple:
        if self._cur.size == 0:
            return 0.0, 0.0
        mk = self._mask(nodes)
        c = self._cur * (mk[self._ohead].astype(float) - mk[self._otail].astype(float))
        return float(np.sum(np.maximum(c, 0.0))), float(np.sum(np.maximum(-c, 0.0)))

    def _boundary_abs(self, nodes) -> float:
        i, o = self._boundary_flux(nodes)
        return max(i, o)

    def current(self, node) -> float:
        return self._net_inflow((node,))

    def throughput(self, node) -> float:
        return self._boundary_abs((node,))

    def delivered(self, mnxm) -> float:
        return self._net_inflow(self.graph.atoms_of(mnxm))

    def throughput_metabolite(self, mnxm) -> float:
        return self._boundary_abs(self.graph.atoms_of(mnxm))

    def share(self, mnxm) -> float:
        return self.delivered(mnxm) / self.injected if self.injected else 0.0

    def delivered_all(self, mnxms) -> dict:
        return {m: self.delivered(m) for m in mnxms}

    def conservation_error(self) -> float:
        tot = sum(self.delivered(m) for m in self.sink.metabolites)
        return abs(tot - self.injected)

    def __repr__(self):
        return (f"Solution(total={self.total:.6g}, {self.source.label}->{self.sink.label}, "
                f"converged={self.converged})")


def _zero_solution(graph, source, sink, note):
    empty_i = np.zeros(0, dtype=np.int64)
    return Solution(graph, source, sink, total=0.0, phi_c=np.zeros(0),
                    cidx={}, cmap={}, cur=np.zeros(0), otail=empty_i, ohead=empty_i,
                    converged=True, used_newton=False, note=note)


def _contract(graph: AtomGraph, source: Terminal, sink: Terminal):
    cmap = {}
    for nd in source.nodes:
        cmap[nd] = SRC_SUPERNODE
    for nd in sink.nodes:
        cmap[nd] = SNK_SUPERNODE

    cnodes, cidx = [SRC_SUPERNODE, SNK_SUPERNODE], {SRC_SUPERNODE: 0, SNK_SUPERNODE: 1}

    def _ci(key):
        j = cidx.get(key)
        if j is None:
            j = cidx[key] = len(cnodes)
            cnodes.append(key)
        return j

    nodes = graph.nodes
    ct, ch, gp, gm, ot, oh, oe = [], [], [], [], [], [], []
    for e, (a, b) in enumerate(graph.edges):
        ka, kb = nodes[a], nodes[b]
        ca, cb = cmap.get(ka, ka), cmap.get(kb, kb)
        if ca == cb:
            continue
        ct.append(_ci(ca)); ch.append(_ci(cb))
        gp.append(graph.gp[e]); gm.append(graph.gm[e])
        ot.append(a); oh.append(b); oe.append(e)
    for nd in nodes:
        if nd not in cmap:
            cmap[nd] = nd
    return (cnodes, cidx, cmap, np.asarray(ct, np.int64), np.asarray(ch, np.int64),
            np.asarray(gp, float), np.asarray(gm, float),
            np.asarray(ot, np.int64), np.asarray(oh, np.int64),
            np.asarray(oe, np.int64))


def _component(n, tail, head, seed):
    A = sp.coo_matrix((np.ones(tail.size), (tail, head)), shape=(n, n)).tocsr()
    lab = sp.csgraph.connected_components(A, directed=False)[1]
    return np.flatnonzero(lab == lab[seed])


def _undirected_phi(B, gp, s, t, g):
    n = B.shape[1]
    keep = np.arange(n) != g
    L = (B.T @ sp.diags(gp) @ B).tocsc()
    rhs = np.zeros(n); rhs[s] = 1.0; rhs[t] = -1.0
    phi = np.zeros(n)
    phi[keep] = splu(L[keep][:, keep].tocsc(), permc_spec="MMD_AT_PLUS_A").solve(rhs[keep])
    return phi


def solve(graph: AtomGraph, source: Terminal, sink: Terminal, *, tol=None, warm=True,
          floor=DIODE_BACKWARD_FLOOR, delta=DIODE_SMOOTH_DELTA) -> Solution:
    if source.nodes & sink.nodes:
        raise ValueError(
            f"terminals overlap on {len(source.nodes & sink.nodes)} atom node(s): "
            f"{source.label} and {sink.label} would short into one node")
    if not source.nodes or not sink.nodes:
        return _zero_solution(graph, source, sink, "empty terminal")

    (cnodes, cidx, cmap, ct, ch, gp, gm, ot, oh, oe) = _contract(graph, source, sink)
    if ct.size == 0:
        return _zero_solution(graph, source, sink, "no edges after contraction")

    sub = _component(len(cnodes), ct, ch, 0)
    inc = np.zeros(len(cnodes), bool)
    inc[sub] = True
    if not inc[1]:
        return _zero_solution(graph, source, sink, "terminals disconnected")

    keepe = inc[ct]
    pos = np.full(len(cnodes), -1, np.int64)
    pos[sub] = np.arange(sub.size)
    remap = {int(u): int(pos[u]) for u in sub}
    tail = pos[ct[keepe]]
    head = pos[ch[keepe]]
    gp_k, gm_k = gp[keepe], gm[keepe]
    ot_k, oh_k, oe_k = ot[keepe], oh[keepe], oe[keepe]
    si, ti = remap[0], remap[1]

    B = build_incidence(list(zip(tail.tolist(), head.tolist())), len(sub))
    gm_eff = np.maximum(gm_k, floor * gp_k)
    symmetric = bool(np.allclose(gm_eff, gp_k, rtol=1e-12, atol=0.0))

    if symmetric:
        phi = _undirected_phi(B, gp_k, si, ti, ti)
        converged, iters, used_newton = True, 0, False
    else:
        phi0 = _undirected_phi(B, gp_k, si, ti, ti) if warm else None
        kw = dict(g=ti, floor=floor, delta=delta, phi0=phi0, return_phi=True,
                  return_iters=True, return_converged=True,
                  reuse=_SPDReuse(B, np.arange(len(sub)) != ti))
        if tol is not None:
            kw["tol"] = tol
        _, iters, phi, converged = directed_ceff(B, gp_k, gm_k, si, ti, **kw)
        used_newton = True

    dv = float(phi[si] - phi[ti])
    total = 1.0 / dv if abs(dv) > REFF_EPS else 0.0

    x = B @ phi
    hx = _softmax0(x, delta)
    cur = gp_k * hx + gm_eff * (x - hx)

    cidx_solved = {cnodes[u]: remap[u] for u in sub}
    return Solution(graph, source, sink, total=total, phi_c=np.asarray(phi, float),
                    cidx=cidx_solved, cmap=cmap, cur=np.asarray(cur, float),
                    otail=ot_k, ohead=oh_k, oedge=oe_k, converged=converged, iters=iters,
                    used_newton=used_newton)


def derive_ieff(r_base: float, r_aug: float) -> tuple:
    rb = max(r_base, IEFF_EPS)
    ra = max(r_aug, IEFF_EPS)
    g_base = 1.0 / rb
    g_aug = 1.0 / ra
    d = g_aug - g_base
    if d < IEFF_EPS:
        d = 0.0
    return d, g_base, g_aug


OMEGA = ("__OMEGA__", 0)


def attach_leak(graph: AtomGraph, precursors=None, *, leak=1e-6, port=1.0):
    prec = set(precursors or ())
    nodes = list(graph.nodes) + [OMEGA]
    omega = len(nodes) - 1
    edges = list(graph.edges)
    gp = list(np.asarray(graph.gp, float))
    gm = list(np.asarray(graph.gm, float))

    leak_edges = {}
    for met in graph.metabolites():
        atoms = graph.atoms_of(met)
        if not atoms:
            continue
        g = (port if met in prec else leak) / len(atoms)
        idxs = []
        for a in atoms:
            idxs.append(len(edges))
            edges.append((graph.idx[a], omega))
            gp.append(g)
            gm.append(g)
        leak_edges[met] = idxs

    meta = dict(graph.meta)
    meta.update(leak=float(leak), port=float(port),
                n_precursors=len(prec & set(graph.metabolites())),
                leak_mode=True)
    g2 = AtomGraph(nodes, edges, np.asarray(gp, float), np.asarray(gm, float), meta)
    return g2, leak_edges


def measure_leak(graph: AtomGraph, source: Terminal, precursors=None, *,
                 leak=1e-6, port=1.0, tol=None):
    g2, leak_edges = attach_leak(graph, precursors, leak=leak, port=port)
    omega_t = Terminal.of_nodes("OMEGA", [OMEGA])
    src = Terminal(source.label, frozenset(source.nodes), source.metabolites,
                   source.missing)
    sol = solve(g2, src, omega_t, tol=tol)

    oidx, cur = sol.edge_currents()
    by_edge = {int(e): float(c) for e, c in zip(oidx, cur)}
    prec = set(precursors or ())
    draw, prec_sum, leak_sum = {}, 0.0, 0.0
    for met, idxs in leak_edges.items():
        d = sum(by_edge.get(i, 0.0) for i in idxs)
        draw[met] = d
        if met in prec:
            prec_sum += d
        else:
            leak_sum += d

    tot = prec_sum + leak_sum
    return dict(
        draw=draw, total=float(sol.total), converged=bool(sol.converged),
        prec_share=(prec_sum / tot) if tot else float("nan"),
        leak_frac=(leak_sum / tot) if tot else float("nan"),
        injected=float(sol.injected), n_metabolites=len(draw),
        missing=tuple(source.missing), leak=float(leak), port=float(port),
    )


def sweep_leak(graph: AtomGraph, source: Terminal, precursors, leaks, *,
               port=1.0, top=50, tol=None):
    runs = []
    for lk in leaks:
        r = measure_leak(graph, source, precursors, leak=lk, port=port, tol=tol)
        order = [m for m, _ in sorted(r["draw"].items(), key=lambda kv: -kv[1])[:top]]
        runs.append(dict(leak=lk, total=r["total"], leak_frac=r["leak_frac"],
                         prec_share=r["prec_share"], order=order, draw=r["draw"]))

    def _rho(a, b):
        common = [m for m in a if m in set(b)]
        if len(common) < 3:
            return float("nan")
        ra = {m: i for i, m in enumerate(a)}
        rb = {m: i for i, m in enumerate(b)}
        n = len(common)
        d2 = sum((ra[m] - rb[m]) ** 2 for m in common)
        return 1.0 - 6.0 * d2 / (n * (n * n - 1))

    for i in range(1, len(runs)):
        runs[i]["rho_vs_prev"] = _rho(runs[i - 1]["order"], runs[i]["order"])
    if runs:
        runs[0]["rho_vs_prev"] = float("nan")
    return runs
