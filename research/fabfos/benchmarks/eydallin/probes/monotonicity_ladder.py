#!/usr/bin/env python3
"""When can widening an edge make the readout go DOWN?

Rayleigh says a two-terminal effective conductance is non-decreasing in every edge
conductance, and three sessions of this benchmark took that as closing the direction
question. It closes it for one readout. This walks a ladder of circuits, each the smallest
one that can settle a specific claim, to find where a signed response becomes available:

  A  fork, dead-end diversion .. the diverting edge is INVISIBLE, not merely non-negative
  B  fork, diversion rejoins ... monotone up, as Rayleigh says
  C  fork, competing GROUND ... the share is signed: widening the shunt lowers it
  D  alternative path, one sink  the control: a path is not enough, it must reach ground
  E  the same as C through `attach_leak`, i.e. the probe ECSPr actually ships
  F  shunt walked along a chain  how far upstream a diversion can still be felt
  G  leak sweep ............... where in leak-space the signed response lives
  H  target with an exit ...... the glycogen case: an outflow edge is a negative lever

Two identities decide the whole thing, and both are just homogeneity. Scaling every
conductance by t scales the two-point conductance by t, so its elasticities sum to +1 --
they are a partition and, by Rayleigh, they are all non-negative. Scaling every conductance
by t leaves a SHARE unchanged, so a share's elasticities sum to 0 -- which forces them to
come in both signs. The sign is not a property of the network. It is a property of the
readout.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/monotonicity_ladder.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.graph import (AtomGraph, Terminal, solve,  # noqa: E402
                               attach_leak, measure_leak, OMEGA)

OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"


def circuit(edges) -> AtomGraph:
    return AtomGraph.from_edge_records(
        [((a, 0), (b, 0), g, g) for (a, b), g in edges.items()], dict(kind="ladder"))


def _with(edges, key, value) -> dict:
    out = dict(edges)
    out[key] = value
    return out


def ceff(edges, src, snk) -> float:
    g = circuit(edges)
    return float(solve(g, Terminal.metabolite(g, src),
                       Terminal.merge(g, snk) if isinstance(snk, (list, tuple))
                       else Terminal.metabolite(g, snk)).total)


def delivered(edges, src, drains, at) -> float:
    g = circuit(edges)
    sol = solve(g, Terminal.metabolite(g, src), Terminal.merge(g, drains))
    return float(sol.delivered(at))


def elasticity(fn, edges, key, fold=1.0001) -> float:
    base = fn(edges)
    up = fn(_with(edges, key, edges[key] * fold))
    if base <= 0 or up <= 0:
        return float("nan")
    return float(np.log(up / base) / np.log(fold))


def _banner(title):
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}", file=sys.stderr)


def rung_a(rows):
    _banner("A -- fork with a dead-end diversion: the shunt is INVISIBLE to C_eff")
    base = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "D"): 1.0}
    for d in (0.01, 1.0, 100.0):
        e = _with(base, ("M", "D"), d)
        c = ceff(e, "S", "T")
        print(f"  g(M-D)={d:<8g}  C_eff(S,T) = {c:.12f}", file=sys.stderr)
        rows.append(dict(rung="A", param=d, readout="ceff", value=c))
    eps = elasticity(lambda e: ceff(e, "S", "T"), base, ("M", "D"))
    print(f"  elasticity of the diverting edge = {eps:.3e}  "
          f"(exactly zero: it carries no current)", file=sys.stderr)
    rows.append(dict(rung="A", param=np.nan, readout="eps_shunt", value=eps))
    assert abs(eps) < 1e-6


def rung_b(rows):
    _banner("B -- diversion that REJOINS the sink: monotone up (Rayleigh)")
    base = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "N"): 1.0, ("N", "T"): 1.0}
    prev = -1.0
    for d in (0.01, 0.1, 1.0, 10.0, 100.0):
        c = ceff(_with(base, ("M", "N"), d), "S", "T")
        print(f"  g(M-N)={d:<8g}  C_eff(S,T) = {c:.9f}", file=sys.stderr)
        rows.append(dict(rung="B", param=d, readout="ceff", value=c))
        assert c > prev, "Rayleigh violated"
        prev = c


def rung_c(rows):
    _banner("C -- competing ground: C_eff still rises, the share falls")
    base = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "D"): 1.0}
    print("  drains = {T, D}; readout = current arriving at T", file=sys.stderr)
    prev_c, prev_i = -1.0, 1e9
    for d in (0.01, 0.1, 1.0, 10.0, 100.0):
        e = _with(base, ("M", "D"), d)
        c = ceff(e, "S", ["T", "D"])
        i = delivered(e, "S", ["T", "D"], "T")
        exact = 1.0 / (1.0 + d)
        print(f"  g(M-D)={d:<8g}  C_eff = {c:.6f}  I_T = {i:.9f}  "
              f"(divider says {exact:.9f})", file=sys.stderr)
        rows.append(dict(rung="C", param=d, readout="ceff", value=c))
        rows.append(dict(rung="C", param=d, readout="I_T", value=i))
        assert abs(i - exact) < 1e-7, "the divider is not a divider"
        assert c > prev_c and i < prev_i, "expected C_eff up and I_T down"
        prev_c, prev_i = c, i

    eps_c = elasticity(lambda e: ceff(e, "S", ["T", "D"]), base, ("M", "D"))
    eps_i = elasticity(lambda e: delivered(e, "S", ["T", "D"], "T"), base, ("M", "D"))
    eps_t = elasticity(lambda e: delivered(e, "S", ["T", "D"], "T"), base, ("M", "T"))
    print(f"\n  shunt elasticity: C_eff {eps_c:+.4f}   I_T {eps_i:+.4f}", file=sys.stderr)
    print(f"  probe-edge elasticity of I_T: {eps_t:+.4f}", file=sys.stderr)
    rows += [dict(rung="C", param=np.nan, readout=k, value=v) for k, v in
             (("eps_ceff_shunt", eps_c), ("eps_IT_shunt", eps_i), ("eps_IT_probe", eps_t))]
    assert eps_i < -0.4 and eps_t > 0.4, "the share must respond in both directions"

    tot_i = sum(elasticity(lambda e: delivered(e, "S", ["T", "D"], "T"), base, k)
                for k in base)
    tot_c = sum(elasticity(lambda e: ceff(e, "S", ["T", "D"]), base, k) for k in base)
    print(f"\n  sum of elasticities: C_eff {tot_c:+.9f} (must be 1)   "
          f"I_T {tot_i:+.9f} (must be 0)", file=sys.stderr)
    rows += [dict(rung="C", param=np.nan, readout="sum_eps_ceff", value=tot_c),
             dict(rung="C", param=np.nan, readout="sum_eps_IT", value=tot_i)]
    assert abs(tot_c - 1.0) < 1e-4 and abs(tot_i) < 1e-4


def rung_d(rows):
    _banner("D -- control: one sink, so KCL pins the delivered current at 1 whatever "
            "the topology")
    base = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "N"): 1.0, ("N", "T"): 1.0}
    for d in (0.01, 1.0, 100.0):
        e = _with(base, ("M", "N"), d)
        i = delivered(e, "S", ["T"], "T")
        c = ceff(e, "S", "T")
        print(f"  g(M-N)={d:<8g}  I_T = {i:.12f}   C_eff = {c:.6f}", file=sys.stderr)
        rows.append(dict(rung="D", param=d, readout="I_T", value=i))
        assert abs(i - 1.0) < 1e-6, "with one sink the delivered current is not a readout"
    print("  -> with a single sink the only readout IS the conductance, and Rayleigh "
          "binds it.", file=sys.stderr)


def rung_e(rows):
    _banner("E -- through attach_leak: T as a port, everything else leaking")
    base = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "D"): 1.0}
    prev = 1e9
    for d in (0.01, 0.1, 1.0, 10.0, 100.0):
        g = circuit(_with(base, ("M", "D"), d))
        r = measure_leak(g, Terminal.metabolite(g, "S"), precursors=["T", "D"],
                         leak=1e-6, port=1.0)
        draw = r["draw"]["T"]
        print(f"  g(M-D)={d:<8g}  draw[T] = {draw:.9f}  "
              f"draw[D] = {r['draw']['D']:.9f}  leak_frac = {r['leak_frac']:.2e}",
              file=sys.stderr)
        rows.append(dict(rung="E", param=d, readout="draw_T", value=float(draw)))
        assert draw < prev, "the shipped leak probe must also fall"
        prev = draw


def rung_f(rows):
    _banner("F -- a shunt walked along a 6-step chain: does distance mute the sign?")
    k = 6
    for j in range(1, k):
        edges = {(f"M{i}", f"M{i + 1}"): 1.0 for i in range(k)}
        edges[(f"M{j}", "D")] = 1.0
        e_shunt = elasticity(
            lambda ee: delivered(ee, "M0", ["M6", "D"], "M6"), edges, (f"M{j}", "D"))
        e_probe = elasticity(
            lambda ee: delivered(ee, "M0", ["M6", "D"], "M6"), edges, ("M5", "M6"))
        base_i = delivered(edges, "M0", ["M6", "D"], "M6")
        print(f"  shunt at M{j} ({j} steps from the source, {k - j} from the target): "
              f"I_T = {base_i:.5f}  eps_shunt = {e_shunt:+.4f}  "
              f"eps_last_step = {e_probe:+.4f}", file=sys.stderr)
        rows.append(dict(rung="F", param=j, readout="eps_shunt", value=e_shunt))
        rows.append(dict(rung="F", param=j, readout="I_T", value=base_i))
        assert e_shunt < 0


def rung_g(rows):
    _banner("G -- leak sweep: the shunt's elasticity against the background leak")
    base = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "D"): 1.0}

    def draw_T(edges, leak):
        g = circuit(edges)
        return measure_leak(g, Terminal.metabolite(g, "S"), precursors=["T"],
                            leak=leak, port=1.0)["draw"]["T"]

    print("   leak      draw[T]      eps(shunt)   eps(probe edge)   share of injected",
          file=sys.stderr)
    for leak in (1e-9, 1e-6, 1e-3, 1e-2, 1e-1, 1.0, 10.0):
        b = draw_T(base, leak)
        es = elasticity(lambda e: draw_T(e, leak), base, ("M", "D"))
        ep = elasticity(lambda e: draw_T(e, leak), base, ("M", "T"))
        print(f"  {leak:<9g} {b:<12.6f} {es:+.5f}      {ep:+.5f}         {b:.4f}",
              file=sys.stderr)
        rows.append(dict(rung="G", param=leak, readout="eps_shunt", value=es))
        rows.append(dict(rung="G", param=leak, readout="draw_T", value=float(b)))
    print("\n  D is NOT a port here -- it drains only through the background leak, so the\n"
          "  shunt can only steal current in proportion to how much ground it reaches.\n"
          "  AT leak=1e-6, THE VALUE THE COHORT PANELS RAN AT, the shunt elasticity is zero\n"
          "  to five decimals: the probe was signed in principle and flat in practice.",
          file=sys.stderr)


def _net(edges, target, drains, *, ratios=None, source="S") -> float:
    g = circuit(edges)
    if ratios:
        keys = list(edges)
        gm = np.asarray(g.gm, float).copy()
        for k, r in ratios.items():
            gm[keys.index(k)] = r * g.gp[keys.index(k)]
        g = AtomGraph(g.nodes, g.edges, g.gp, gm, dict(g.meta))
    sol = solve(g, Terminal.metabolite(g, source), Terminal.merge(g, drains))
    return float(sol.delivered(target))


def _draw(edges, target, ports, *, ratios=None, leak=1e-6, port=1.0, source="S") -> float:
    g = circuit(edges)
    if ratios:
        keys = list(edges)
        gm = np.asarray(g.gm, float).copy()
        for k, r in ratios.items():
            gm[keys.index(k)] = r * g.gp[keys.index(k)]
        g = AtomGraph(g.nodes, g.edges, g.gp, gm, dict(g.meta))
    return measure_leak(g, Terminal.metabolite(g, source), precursors=ports,
                        leak=leak, port=port)["draw"][target]


def rung_h(rows):
    _banner("H -- one circuit, three lever kinds: feed (+), shunt (-), exit (-)")
    base = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "D"): 1.0, ("T", "X"): 1.0}
    ports = ["T", "D", "X"]
    b = _draw(base, "T", ports)
    part = 0.0
    for label, key in (("feed  M->T", ("M", "T")), ("shunt M->D", ("M", "D")),
                       ("exit  T->X", ("T", "X"))):
        e = elasticity(lambda ee: _draw(ee, "T", ports), base, key)
        part += e
        print(f"  {label:12} eps(draw[T]) = {e:+.5f}", file=sys.stderr)
        rows.append(dict(rung="H", param=np.nan, readout=f"eps_{key[0]}{key[1]}", value=e))
        if label.startswith("feed"):
            assert e > 0
        else:
            assert e < 0

    scaled = _draw({k: v * 7.0 for k, v in base.items()}, "T", ports,
                   leak=7e-6, port=7.0)
    print(f"\n  draw[T] = {b:.9f}; every conductance x7 gives {scaled:.9f} "
          f"-- a share is homogeneous of degree 0", file=sys.stderr)
    print(f"  the four circuit edges carry {part:+.5f} of the zero; the leak and port "
          f"edges carry the rest", file=sys.stderr)
    rows.append(dict(rung="H", param=np.nan, readout="sum_eps_circuit", value=part))
    assert abs(scaled - b) < 1e-9

    _banner("H2 -- the SAME edge is a feed or an exit, and orientation gates which")
    up = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "D"): 1.0, ("T", "X"): 0.2,
          ("S", "U"): 1.0}                       # U fed from the source: a second feed
    down = {("S", "M"): 1.0, ("M", "T"): 1.0, ("M", "D"): 1.0, ("T", "X"): 0.2,
            ("U", "W"): 1.0}                     # U drains through W: an exit
    print("  placement          orientation             draw[T]      eps(T-U edge)",
          file=sys.stderr)
    for place, edges, ports in (("U upstream ", up, ["T", "D", "X"]),
                                ("U downstream", down, ["T", "D", "X", "W"])):
        for label, key, r in (("aligned with the flow", None, 1e-3),
                              ("symmetric (abstain)  ", None, 1.0),
                              ("opposed to the flow  ", None, 1e-3)):
            fill = (("U", "T") if place.startswith("U upstream") else ("T", "U"))
            drain = (fill[1], fill[0])
            key = fill if "aligned" in label or "symmetric" in label else drain
            e2 = dict(edges)
            e2[key] = 1.0
            d = _draw(e2, "T", ports, ratios={key: r})
            eps = elasticity(lambda ee: _draw(ee, "T", ports, ratios={key: r}), e2, key)
            print(f"  {place}  {label}   {d:.6f}   {eps:+.5f}", file=sys.stderr)
            rows.append(dict(rung="H2", param=r, readout=f"{place.strip()}:{label.strip()}",
                             value=eps))
    print("\n  Where the partner sits decides the sign the edge CAN carry; the orientation\n"
          "  decides whether it carries it at all -- an edge pointed against the flow is\n"
          "  inert, which is the same invisibility rung A found. Topology and direction are\n"
          "  both required, and the ensemble abstains on exactly the polymer edges that\n"
          "  decide it.", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    rows: list = []
    for fn in (rung_a, rung_b, rung_c, rung_d, rung_e, rung_f, rung_g, rung_h):
        fn(rows)

    out = args.out_dir / "monotonicity_ladder.tsv"
    pd.DataFrame(rows).to_csv(out, sep="\t", index=False)
    print(f"\n[ladder] wrote {out}", file=sys.stderr)
    print("\nVERDICT: a fold can lower the readout exactly when the readout is a SHARE of\n"
          "the injected current and the widened edge reaches ground without passing\n"
          "through the target. The conductance never falls; the share must be able to,\n"
          "because its elasticities sum to zero.", file=sys.stderr)


if __name__ == "__main__":
    main()
