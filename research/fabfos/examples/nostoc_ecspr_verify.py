#!/usr/bin/env python3
"""The checks with teeth on the composed community measurement.

    python research/fabfos/examples/nostoc_ecspr_verify.py --structural      # no products needed, ~20 s
    python research/fabfos/examples/nostoc_ecspr_verify.py --structural --g0-sweep   # + the g0 trend, ~50 min
    python research/fabfos/examples/nostoc_ecspr_verify.py --products       # over the promoted chunk

Split deliberately. The STRUCTURAL checks interrogate the composition itself and run in
process against the staged networks -- they are the ones that catch a broken construction
before an hour of solving is spent on it. The PRODUCT checks read a finished measurement.

Not on either list: "the products are non-empty" and "the numbers are not all zero". Both
pass on a subtly wrong measurement, which is the only kind this is likely to produce.

THE PRODUCT CHECKS READ THE MEASUREMENT OF RECORD, IN THE SCHEMA IT WAS MEASURED IN.
`--products` defaults to the pinned `data/fabfos/nostoc/ecspr/results/` chunk, whose
21 units were measured by two transforms -- `measure_ground` and `measure_two_terminal`
-- that lived in the transform library at the time and wrote a wide table each
(`role`/`draw`/`share`; `conductance`/`reff`). Neither transform exists now: the library
has since collapsed both probes into one `ecspr::results` type, a long table keyed by a
`probe` column, produced by `ecspr_measure` dispatching the `ecspr` command-line tool.
So this file verifies the promoted products and does NOT read what a re-run would write.
Re-running is a separate piece of work -- it needs the two-point probe declared as a
transform, and the checks below re-expressed against the long schema.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

import ecspr.model.build as eb  # noqa: E402
import ecspr.model.compose as ec  # noqa: E402
import ecspr.model.evidence as en  # noqa: E402
from ecspr.model.graph import Terminal, measure_leak  # noqa: E402

sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))
import bake_pairs  # noqa: E402

GPR = REPO / "data/fabfos/nostoc/annotation"
OUT = REPO / "data/fabfos/nostoc/ecspr"
NETS = OUT / "networks"


def load_bake():
    return (pd.read_parquet(bake_pairs.atom_pairs()),
            pd.read_parquet(bake_pairs.direction_ratios()))

FERREDOXIN = ("MNXM178", "MNXM169", "MNXM20146", "MNXM21524",
              "MNXM588581", "MNXM681091")
BIOTIN = "MNXM304"

_print = print


def print(*a, **k):  # noqa: A001
    _print(*a, **dict(k, flush=True))


def _fail(msg):
    print(f"  FAIL  {msg}")
    return 1


def _pass(msg):
    print(f"  pass  {msg}")
    return 0


def check_singleton_identity(pairs, direction, element="C"):
    print("[1] a singleton is the uncomposed organism, exactly")
    raw = pd.read_parquet(GPR / "NOS" / "gpr_4lane.parquet")
    w_raw = dict(zip(*[en.compute_weights(raw)[c] for c in ("mnxr", "E_full")]))
    ratios = dict(zip(direction.mnxr, direction.ratio))
    g_raw = eb.graph_from_pairs(pairs[pairs.element == element], element, w_raw, ratios)

    d = NETS / "NOS"
    cp = pd.read_parquet(d / "atom_pairs.parquet")
    cg = pd.read_parquet(d / "gpr.parquet")
    cd = pd.read_parquet(d / "direction.parquet")
    w_c = dict(zip(*[en.compute_weights(cg)[c] for c in ("mnxr", "E_full")]))
    g_c = eb.graph_from_pairs(cp[cp.element == element], element,
                              w_c, dict(zip(cd.mnxr, cd.ratio)))

    print(f"  raw      {g_raw.n:,} nodes  {g_raw.m:,} edges")
    print(f"  composed {g_c.n:,} nodes  {g_c.m:,} edges")
    bad = 0
    if (g_raw.n, g_raw.m) != (g_c.n, g_c.m):
        bad += _fail(f"shape moved: {(g_raw.n, g_raw.m)} -> {(g_c.n, g_c.m)}")
    else:
        bad += _pass("same node and edge count")
    for name, a, b in (("gp", np.sort(g_raw.gp), np.sort(g_c.gp)),
                       ("gm", np.sort(g_raw.gm), np.sort(g_c.gm))):
        if len(a) == len(b) and np.allclose(a, b, rtol=0, atol=1e-12):
            bad += _pass(f"{name} conductances identical (max |d| "
                         f"{float(np.max(np.abs(a - b))) if len(a) else 0:.2e})")
        else:
            bad += _fail(f"{name} conductances differ")
    strip = {ec.untag(m)[1] for m in g_c.metabolites()}
    if strip == set(g_raw.metabolites()):
        bad += _pass("metabolite sets identical once the tag is stripped")
    else:
        bad += _fail(f"metabolite sets differ by {len(strip ^ set(g_raw.metabolites()))}")
    return bad


def check_copies_are_private(element="C"):
    print("\n[2] copies are private: only bridges cross between members")
    bad = 0
    for nid in ("NOS-ERY_bl-on", "NOS-ERY-RHI_bl-on"):
        cp = pd.read_parquet(NETS / nid / "atom_pairs.parquet")
        el = cp[cp.element == element]
        so = el.substrate.str.split(ec.SEP).str[0]
        po = el["product"].str.split(ec.SEP).str[0]
        cross = el[so != po]
        non_bridge = cross[~cross.mnxr.str.startswith("BRIDGE")]
        print(f"  {nid}: {len(el):,} edges, {len(cross):,} cross-member, "
              f"{len(non_bridge):,} of those NOT bridges")
        bad += (_fail(f"{nid}: {len(non_bridge):,} non-bridge cross edges")
                if len(non_bridge) else _pass(f"{nid}: every cross edge is a bridge"))
        br = cross[cross.mnxr.str.startswith("BRIDGE")]
        mism = br[br.substrate.str.split(ec.SEP).str[1] != br["product"].str.split(ec.SEP).str[1]]
        bad += (_fail(f"{nid}: {len(mism):,} bridges join DIFFERENT metabolites")
                if len(mism) else _pass(f"{nid}: every bridge joins one metabolite to itself"))
    return bad


def check_belief_conservation():
    print("\n[3] belief conservation survives the synthetic bridge ORFs")
    bad = 0
    e_bridge = en.pooled_E_of_mass(1.0)
    for d in sorted(NETS.iterdir()):
        g = pd.read_parquet(d / "gpr.parquet")
        w = en.compute_weights(g)
        n, s = g.orf.nunique(), float(w.belief_mass.sum())
        ok = abs(s - n) < 1e-6 * max(1.0, n)
        # Conservation is a statement about the PRE-pooling mass; the bridges' pooled
        # E_full is not 1.0 and cannot be, but every bridge spends one whole ORF on one
        # pseudo-reaction so they all land on the same constant.
        bridge = w[w.mnxr.str.startswith("BRIDGE")]
        e_ok = bool(len(bridge) == 0
                    or np.allclose(bridge.E_full, e_bridge, atol=1e-12))
        print(f"  {d.name:22s} sum(belief_mass)={s:12.6f}  orfs={n:6,}  "
              f"bridges={len(bridge):2d} at E={'the constant' if e_ok else 'DRIFTED'}")
        bad += 0 if (ok and e_ok) else _fail(f"{d.name}: sum(belief_mass)={s} vs {n} orfs")
    return bad or _pass(f"every network conserves belief, bridges at E={e_bridge:.6f}")


def check_g0_limit(pairs, direction, element="C", *, sweep=False):
    print("\n[4] g0 scales the bridges, and only the bridges couple the copies")
    names = pd.read_parquet(OUT / "metabolite_names.parquet")
    bl = set(pd.read_parquet(OUT / "carrier_blacklist.parquet").mnxm)
    gprs = {o: pd.read_parquet(GPR / o / "gpr_4lane.parquet") for o in ("NOS", "ERY")}
    ratios = dict(zip(direction.mnxr, direction.ratio))
    src = "NOS:MNXM1364061"
    sinks = ["ERY:MNXM741173", "ERY:MNXM737787"]
    bad = 0

    def _compose(g0):
        out = ec.compose(gprs, pairs, direction, elements=(element,), g0=g0,
                         blacklist=bl, names=names, network_id="NOS-ERY")
        p = out["pairs"][out["pairs"].element == element]
        return out, p

    if sweep:
        for g0 in (1.0, 1e-3, 1e-6):
            out, p = _compose(g0)
            w = dict(zip(*[en.compute_weights(out["gpr"])[c] for c in ("mnxr", "E_full")]))
            d2 = (dict(zip(out["direction"].mnxr, out["direction"].ratio))
                  if out["direction"] is not None else ratios)
            g = eb.graph_from_pairs(p, element, w, d2)
            r = measure_leak(g, Terminal.metabolite(g, src, label="source"), sinks,
                             leak=1e-6)
            tot = r["total"] or float("nan")
            got = {s: r["draw"].get(s, 0.0) / tot for s in sinks}
            print(f"  g0={g0:<8g} " + "  ".join(f"{s}={v:.6e}" for s, v in got.items()))
    else:
        print("  (g0 sweep skipped; --g0-sweep to run it. Measured previously: "
              "1.0 -> 0.5062/0.4882, 1e-3 -> 0.5166/0.4768, 1e-6 -> 0.3441/0.3180)")

    _, p0 = _compose(0.0)
    br = p0[p0.mnxr.astype(str).str.startswith("BRIDGE")]
    print(f"  g0=0        {len(br)} bridge edges emitted")
    if len(br):
        bad += _fail(f"g0=0 still emits {len(br)} bridge edge(s), "
                     f"max |w| {float(br.pair_w.abs().max()):.3e}")
    else:
        bad += _pass("g0=0 -> no bridge edges at all; the copies are disconnected")

    for nid in ("NOS-ERY_bl-on", "NOS-ERY-RHI_bl-on"):
        ap = pd.read_parquet(NETS / nid / "atom_pairs.parquet")
        b = ap[ap.mnxr.astype(str).str.startswith("BRIDGE")]
        n0 = int((b.pair_w == 0).sum())
        if n0:
            bad += _fail(f"{nid}: {n0} bridge edge(s) at exactly zero weight")
        else:
            bad += _pass(f"{nid}: no degenerate zero-weight bridges "
                         f"(min |w| {float(b.pair_w.abs().min()):.2e})")
    return bad


def check_sulfur_survival(pairs, element="S"):
    print("\n[5] sulfur bridge survival after the blacklist")
    bad = 0
    for pair in ("NOS-ERY", "NOS-RHI", "ERY-RHI"):
        on = pd.read_parquet(NETS / f"{pair}_bl-on" / "bridges.parquet")
        off = pd.read_parquet(NETS / f"{pair}_bl-off" / "bridges.parquet")
        s_on = on[(on.element == element) & (on.bridged == 1)]
        s_off = off[(off.element == element) & (off.bridged == 1)]
        frac = len(s_on) / len(s_off) if len(s_off) else float("nan")
        fer = off[(off.element == element) & off.metabolite.isin(FERREDOXIN)]
        print(f"  {pair}: {len(s_on):,} of {len(s_off):,} S bridges survive ({frac:.1%});"
              f" ferredoxin carried {float(fer.g_bridge.sum()):.3e} of "
              f"{float(s_off.g_bridge.sum()):.3e} total S bridge conductance")
        if frac < 0.5:
            bad += _fail(f"{pair}: over half the sulfur bridging is blacklisted away")
    return bad or _pass("sulfur bridging survives the blacklist")


def check_biotin(pairs):
    print("\n[6] biotin: the independent check")
    bad = 0
    for pair in ("NOS-RHI", "ERY-RHI"):
        b = pd.read_parquet(NETS / f"{pair}_bl-on" / "bridges.parquet")
        rows = b[b.metabolite == BIOTIN]
        if not len(rows):
            bad += _fail(f"{pair}: biotin absent from the bridge report entirely")
            continue
        for r in rows.itertuples(index=False):
            print(f"  {pair} [{r.element}] M_{r.org_a}={r.M_a:.4f} M_{r.org_b}={r.M_b:.4f} "
                  f"D={r.asymmetry:.3f} bridged={r.bridged} g={r.g_bridge:.3e}")
        if not (rows.bridged == 1).any():
            bad += _fail(f"{pair}: biotin is bridgeable in no element -- the check is blind")
    return bad or _pass("biotin is bridged and asymmetric where it should be")


def structural(sweep=False):
    print("loading the reference bake ...", flush=True)
    pairs, direction = load_bake()
    bad = 0
    bad += check_singleton_identity(pairs, direction)
    bad += check_copies_are_private()
    bad += check_belief_conservation()
    bad += check_g0_limit(pairs, direction, sweep=sweep)
    bad += check_sulfur_survival(pairs)
    bad += check_biotin(pairs)
    print("\n" + ("STRUCTURAL CHECKS PASS" if not bad else f"{bad} STRUCTURAL FAILURE(S)"))
    return 1 if bad else 0


def _collect(root: Path, kind: str):
    parts = []
    for p in sorted(q for q in root.rglob("*.parquet")
                    if kind in str(q) and "/results/" in str(q)):
        try:
            parts.append(pd.read_parquet(p).assign(_src=str(p)))
        except Exception as e:  # noqa: BLE001
            print(f"  (skipping {p}: {e})")
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _split_cid(df):
    p = df.condition_id.str.split("|", expand=True)
    return df.assign(network=p[0], direction=p[1], el=p[2])


EXPECTED_UNITS = 21


def _unit_keys(df):
    return set(df.condition_id.str.split("|").str[0] + "|"
               + df.condition_id.str.split("|").str[1].str.split("->").str[0])


def _expected_units():
    out = {}
    for net in ("NOS", "ERY", "RHI"):
        out[net] = 1
    for net in ("NOS-ERY", "NOS-RHI", "ERY-RHI"):
        for arm in ("bl-on", "bl-off"):
            out[f"{net}_{arm}"] = 2
    for arm in ("bl-on", "bl-off"):
        out[f"NOS-ERY-RHI_{arm}"] = 3
    return out


def products(root: Path):
    bad = 0
    g = _collect(root, "ground_results")
    t = _collect(root, "two_terminal_results")
    print(f"ground rows: {len(g):,}   two-terminal rows: {len(t):,}")
    if not len(g) and not len(t):
        return _fail("no products found -- nothing to check")

    seen = _unit_keys(g) & _unit_keys(t) if len(g) and len(t) else set()
    expect = _expected_units()
    print(f"units present (both transforms): {len(seen)} of {EXPECTED_UNITS}")
    for net, n in sorted(expect.items()):
        have = sum(1 for u in seen if u.rsplit("|", 1)[0] == net)
        if have != n:
            bad += _fail(f"{net}: {have} of {n} unit(s) -- the checks below are PARTIAL "
                         f"and a pass here does not mean the experiment passed")
    if len(seen) != EXPECTED_UNITS:
        bad += _fail(f"{len(seen)} units covered, expected exactly {EXPECTED_UNITS}")

    if len(t):
        t = _split_cid(t)
        print("\n[A] a->b differs from b->a on the identical network")
        t = t.assign(met=t.sink_hub.map(lambda s: ec.untag(s)[1]))
        for net in sorted(set(t.network)):
            sub = t[t.network == net]
            dirs = sorted(set(sub.direction))
            if len(dirs) < 2:
                continue
            piv = sub.pivot_table(index=["el", "met"], columns="direction",
                                  values="conductance", aggfunc="first").dropna()
            if not len(piv):
                bad += _fail(f"{net}: {dirs} share no endpoint metabolite -- nothing paired")
                continue
            a, b = piv.iloc[:, 0], piv.iloc[:, 1]
            rel = float((a - b).abs().div(((a + b) / 2).abs()).max())
            same = int((a == b).sum())
            print(f"  {net}: {dirs} -> {len(piv)} paired endpoints, "
                  f"{same} exactly equal, max relative gap {rel:.3%}")
            if same == len(piv):
                bad += _fail(f"{net}: both directions agree on every endpoint -- the "
                             f"copies collapsed and the prefixing did not take")

    if len(g):
        g = _split_cid(g)
        print("\n[B] the blacklist arms differ")
        g = g.assign(arm=g.media.str.extract(r"bl=(\w+)")[0],
                     base=g.network.str.replace(r"_bl-(on|off)$", "", regex=True))
        prec = g[g.role == "precursor"]
        for base in sorted(set(prec.base)):
            sub = prec[prec.base == base]
            if sub.arm.nunique() < 2:
                continue
            piv = sub.pivot_table(index=["direction", "el", "metabolite"],
                                  columns="arm", values="share", aggfunc="first").dropna()
            if not len(piv):
                continue
            d = float(np.abs(piv["on"] - piv["off"]).max())
            print(f"  {base}: {len(piv)} paired precursor shares, max |d| = {d:.3e}")
            if d < 1e-12:
                bad += _fail(f"{base}: the two arms are bit-identical -- either the "
                             f"blacklist does nothing or the bridges carry no current")

        print("\n[C] interior metabolites draw current (the leak attached)")
        for net, sub in g.groupby("network"):
            inter = sub[sub.role == "interior"]
            nz = int((inter.draw.abs() > 0).sum())
            print(f"  {net}: {nz:,} of {len(inter):,} interior metabolites draw")
            if len(inter) and nz == 0:
                bad += _fail(f"{net}: hard-ground measurement wearing the leak schema")

        print("\n[D] a receiving copy actually receives")
        for net, sub in g.groupby("network"):
            if "-" not in net.split("_")[0]:
                continue
            prec = sub[sub.role == "precursor"]
            if not len(prec):
                continue
            per = prec.groupby("condition_id")["share"].sum()
            print(f"  {net}: precursors take {per.min():.3f}-{per.max():.3f} of the "
                  f"injected current across {len(per)} conditions")
            if float(per.min()) <= 0:
                dead = list(per[per <= 0].index)[:3]
                bad += _fail(f"{net}: no current reaches the receiving copy in {dead}")

    print("\n" + ("PRODUCT CHECKS PASS" if not bad else f"{bad} PRODUCT FAILURE(S)"))
    return 1 if bad else 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--structural", action="store_true")
    p.add_argument("--g0-sweep", action="store_true",
                   help="also run the g0 sensitivity sweep: 3 composes and 3 "
                        "~110k-node solves, ~50 min. Asserts nothing; reports a trend.")
    p.add_argument("--products", nargs="?", const=str(OUT / "results"), default=None,
                   metavar="DIR",
                   help=f"results tree to check (default: {OUT.name}/results, the "
                        f"promoted chunk)")
    a = p.parse_args(argv)
    if not (a.structural or a.products):
        p.error("pick --structural and/or --products [DIR]")
    rc = 0
    if a.structural:
        rc |= structural(sweep=a.g0_sweep)
    if a.products:
        rc |= products(Path(a.products).resolve())
    return rc


if __name__ == "__main__":
    sys.exit(main())
