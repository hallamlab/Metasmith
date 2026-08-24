"""The direction table as a differentiable parameter vector over a frozen topology.

WHAT IS BEING FITTED. One scalar per reaction, ``a_r = log10(g_rev/g_fwd)``, which is the
baked ``dG'`` in disguise (``a = dG' / DIR_DECADE``). Everything else about the network --
which atoms exist, which reactions carry which transfers, the existence conductance -- is
held fixed, so a fit can only ever redistribute flow, never manufacture it.

WHY THE TOPOLOGY CAN BE FROZEN AT ALL. ``build.graph_from_pairs`` REVERSES an edge whose
ratio exceeds 1 rather than amplifying it, so the edge list moves discontinuously as a ratio
crosses 1 and a naive parametrisation would be non-differentiable exactly where the
interesting reactions sit. But reversing an edge and swapping ``(gp, gm)`` is a mathematical
no-op -- the rectified law reads the same element either way round -- so the same model is
available with the edge list held at its baked orientation and BOTH conductances made
functions of the parameter:

    gp_row = G_row * 10 ** -max(s, 0)      s = o_r * a_r
    gm_row = G_row * 10 ** min(s, 0)       o_r = -1 where the baked ratio flipped the edge

At ``s = 0`` both equal ``G_row``, so the map is continuous everywhere and kinked only at
the fully-reversible point. That is a ReLU-style kink, not a discontinuity, and it is the
best available: direction evidence may only throttle, so ``max(gp, gm) = G_row`` is an
invariant of the instrument and no smooth reparametrisation can lift it.

AND THE KINK IS EXACTLY WHERE THE FIT LIVES, which is why it gets smoothed rather than
tolerated. A reaction with no direction evidence is baked at ratio 1.0, i.e. ``a_r = 0`` --
half the table, and every tier-0 reaction a fit would be trying to learn. The subgradient
there is the whole interval between the two one-sided slopes, and handing back the
convenient element (zero) freezes precisely the parameters the fit exists to move. So
``eta`` replaces the hinge with

    u = sqrt(s^2 + eta^2)      gp_row = G_row * 10 ** -(u + s)/2
                               gm_row = G_row * 10 ** -(u - s)/2

which keeps ``gm/gp = 10**s`` EXACTLY -- the model's meaning is untouched -- and pays for
smoothness only in the overall level, damping both branches by at most ``10 ** -eta/2``.
Damping, never amplification, so the "direction may only throttle" invariant survives. It
is the same trade ``directed.DIODE_SMOOTH_DELTA`` already makes one layer down, for the same
reason. ``eta = 0`` recovers the builder's hinge exactly and is the default, so every
REPORTED number is the unsmoothed one; only the optimiser runs at ``eta > 0``.

THE GRADIENT IS EXACT, not a finite difference and not autodiff through the Newton loop.
:mod:`ecspr.model.sensitivity` differentiates the converged solve by its adjoint; this module
only chains that onto the parameters, which is a sparse scatter-add over pair rows.

``check_gradient.py`` is the gate: this file is wrong until it agrees with a central
difference on the real host graph.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

import ecspr.model.build as EB                                        # noqa: E402
from ecspr.model.graph import AtomGraph, Terminal, solve              # noqa: E402
from ecspr.model.sensitivity import edge_sensitivity                  # noqa: E402

BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
ANNOT = BAKE / "seams/direction_annotation.parquet"
HOSTS = ROOT / "data/fabfos/benchmarks/hosts"
CACHE = Path(__file__).resolve().parent / "cache"

GLUCOSE = "MNXM1364061"
LN10 = float(np.log(10.0))

# The bake clamps |dG'| at three decades and ``build.DIRECTION_DECADE_CAP`` matches it, so a
# parameter outside that box describes a table the instrument would not read anyway.
DECADE_CAP = EB.DIRECTION_DECADE_CAP


def load_bake(element: str = "C") -> tuple:
    # ``(pairs, annotation)`` for one element, in the column names ``build`` expects.
    v = pd.read_parquet(BAKE / "vocab.parquet")
    mol = v[v.kind == "met"].set_index("code").symbol.to_dict()
    rxn = v[v.kind == "rxn"].set_index("code").symbol.to_dict()
    ele = v[v.kind == "element"].set_index("code").symbol.to_dict()
    code = next(k for k, s in ele.items() if s == element)

    ap = pd.read_parquet(BAKE / "atom_pairs.parquet")
    ap = ap[ap.element == code]
    pairs = pd.DataFrame({
        "mnxr": ap.rxn.map(rxn), "element": element,
        "substrate": ap.tail_met.map(mol), "product": ap.head_met.map(mol),
        "sub_idx": ap.tail_rank.to_numpy(), "prod_idx": ap.head_rank.to_numpy(),
        "pair_w": ap.pair_w.to_numpy(),
    }).dropna(subset=["mnxr", "substrate", "product"])
    pairs = pairs[pairs.mnxr != "EMPTY"]

    ann = pd.read_parquet(ANNOT)[["mnxr", "ratio", "dir_tier", "dir_confidence"]]
    return pairs, ann[ann.mnxr != "EMPTY"].reset_index(drop=True)


def host_weights(host: str) -> dict:
    # Curated lane only. The de-novo tables carry a ``pbert`` channel whose landmark index is
    # under repair, so a number taken from them is not a number about this instrument.
    gpr = pd.read_parquet(HOSTS / host / "gpr_gem.parquet")
    lanes = set(gpr.lane_set.unique())
    if lanes != {"curated"}:
        raise ValueError(f"{host} is not the curated arm: lane_set={sorted(lanes)}")
    return {m: 1.0 for m in gpr.mnxr.dropna().unique()}


class DirectionModel:
    # A host graph whose direction table is a parameter vector.
    #
    # Construction freezes the edge list against ``ratios_ref`` (the baked table by default).
    # ``params`` is indexed by :attr:`reactions`, in ``log10`` ratio, and ``forward`` /
    # ``gradient`` are the only two entry points.

    def __init__(self, pairs: pd.DataFrame, element: str, weights: dict,
                 ratios_ref: dict, *, cap: float = DECADE_CAP, eta: float = 0.0):
        self.cap = float(cap)
        self.eta = float(eta)
        ref = EB.cap_direction_ratios(ratios_ref, cap) if cap else dict(ratios_ref)
        self.graph = EB.graph_from_pairs(pairs, element, weights, ref, with_provenance=True)
        prov = self.graph.meta["edge_reactions"]

        self.reactions = pd.Index(pd.unique(prov.mnxr))
        rid = pd.Series(np.arange(len(self.reactions)), index=self.reactions)
        self._row_edge = prov.edge.to_numpy(np.int64)
        self._row_rxn = prov.mnxr.map(rid).to_numpy(np.int64)
        self._row_G = prov.gp.to_numpy(float)

        # ``o_r`` records which reactions the builder reversed, so the frozen edge list can
        # be read against the parameter's own sign. It is per REACTION, not per row, because
        # the flip is decided from the reaction's ratio.
        tiny = np.finfo(float).tiny
        a_ref = np.log10(np.maximum([float(ref.get(r, 1.0)) for r in self.reactions], tiny))
        self._o = np.where(a_ref > 0, -1.0, 1.0)
        self.a_ref = a_ref
        self._ne = self.graph.m

    def __len__(self) -> int:
        return len(self.reactions)

    def _branches(self, a: np.ndarray) -> tuple:
        # ``(s_row, gp_row, gm_row, du/ds)`` -- the per-row branch conductances and the one
        # derivative the chain rule needs. ``eta = 0`` is the builder's exact hinge.
        s = self._o * np.asarray(a, float)
        srow = s[self._row_rxn]
        if self.eta > 0:
            u = np.sqrt(srow * srow + self.eta * self.eta)
            duds = srow / u
        else:
            u = np.abs(srow)
            duds = np.sign(srow)
        gp_row = self._row_G * 10.0 ** (-0.5 * (u + srow))
        gm_row = self._row_G * 10.0 ** (-0.5 * (u - srow))
        return srow, gp_row, gm_row, duds

    def conductances(self, a: np.ndarray) -> tuple:
        srow, gp_row, gm_row, duds = self._branches(a)
        return (np.bincount(self._row_edge, gp_row, minlength=self._ne),
                np.bincount(self._row_edge, gm_row, minlength=self._ne),
                srow, gp_row, gm_row, duds)

    def with_eta(self, eta: float) -> "DirectionModel":
        m = object.__new__(DirectionModel)
        m.__dict__.update(getattr(self, "__dict__", {}))
        for k in ("cap", "eta", "graph", "reactions", "_row_edge", "_row_rxn",
                  "_row_G", "_o", "a_ref", "_ne"):
            setattr(m, k, getattr(self, k))
        m.eta = float(eta)
        return m

    def graph_at(self, a: np.ndarray) -> AtomGraph:
        gp, gm, *_ = self.conductances(a)
        return AtomGraph(self.graph.nodes, self.graph.edges, gp, gm,
                         dict(self.graph.meta), dict(self.graph.idx))

    def terminal(self, mnxm: str) -> Terminal:
        return Terminal.metabolite(self.graph, mnxm)

    def forward(self, a: np.ndarray, source: Terminal, sink: Terminal):
        return solve(self.graph_at(a), source, sink)

    def gradient(self, a: np.ndarray, source: Terminal, sink: Terminal) -> tuple:
        # ``(log C_eff, d log C_eff / d a)`` -- one solve plus one adjoint solve.
        gp, gm, srow, gp_row, gm_row, duds = self.conductances(a)
        g = AtomGraph(self.graph.nodes, self.graph.edges, gp, gm,
                      dict(self.graph.meta), dict(self.graph.idx))
        sol = solve(g, source, sink)
        if not sol.total > 0:
            return -np.inf, np.zeros(len(self))
        sens = edge_sensitivity(sol)

        dgp = np.zeros(self._ne)
        dgm = np.zeros(self._ne)
        dgp[sens.oedge] = sens.d_logceff_dgp
        dgm[sens.oedge] = sens.d_logceff_dgm

        half = 0.5 * LN10
        d_srow = (dgp[self._row_edge] * (-half * (duds + 1.0) * gp_row)
                  + dgm[self._row_edge] * (-half * (duds - 1.0) * gm_row))
        grad = np.bincount(self._row_rxn, d_srow, minlength=len(self)) * self._o
        return float(np.log(sol.total)), grad


def build(host: str = "e_coli_k12", element: str = "C", *, cap: float = DECADE_CAP,
          pairs=None, ann=None) -> tuple:
    if pairs is None or ann is None:
        pairs, ann = load_bake(element)
    ratios = dict(zip(ann.mnxr, ann.ratio.astype(float)))
    return DirectionModel(pairs, element, host_weights(host), ratios, cap=cap), pairs, ann
