#!/usr/bin/env python3
"""Is each declared Fuhrer axis reachable at all, before any sweep is run?

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/fuhrer/sinks/probe_axes.py --channel gem
    ... --channel denovo

One two-point solve per axis on the unperturbed BW25113 background: unit current injected at
D-glucose's carbon atoms, drawn at the sink's, `Solution.total` the effective conductance
between the two atom sets. Every axis shares the source and the ratio's denominator, so this
also measures the denominator once, as its own row.

WHY THIS RUNS BEFORE THE SWEEP. A sweep over an unreachable sink returns a column of exact
zeros and a flat delta for all 3,806 mutants -- indistinguishable, in the output, from a real
null. `research/fabfos/benchmarks/woodruff/REPORT.md` established the failure with the
`ctl_kdo2lipida` control: Kdo2-lipid A scores exactly 0 on the curated channel with
`sink_is_node=True` and `converged=True`, and 20.37 on the de-novo channel. So a zero here
carries no information until the node check has separated the three cases this file's output
distinguishes by column rather than by a comment:

    sink_is_node=False  the id is not in this background's carbon graph at all
    sink_is_node=True, g=0     it is, and no atom-resolved carbon route reaches it
    g>0                        reachable

THE BACKGROUND IS iML1515, WHICH IS THIS SCREEN'S ACTUAL HOST rather than a proxy. Fuhrer
screened the Keio collection, whose parent is BW25113, and iML1515 is a BW25113-curated
model. Eydallin's 2010 arm had to borrow DH1's model for AG1; nothing is borrowed here, and
the numbers are therefore not comparable to that arm's for a reason that has nothing to do
with the method.

THE SOURCE IS D-GLUCOSE BECAUSE THE MEDIUM IS. `sinks/declare_axes.py` fixes it, and both
files read it from `axes.tsv` rather than either carrying a constant.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
# `bake_pairs` and `reaction_chemistry` live in the eydallin tree and are shared across
# studies exactly as `fang/sinks/probe_axes.py` shares them; a second copy is how two arms of
# one campaign come to read two different bakes.
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))

import bake_pairs                                                            # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                # noqa: E402

AXES = ROOT / "data/fabfos/benchmarks/fuhrer/axes.tsv"
HOST = "e_coli_bw25113"
OUT_DIR = ROOT / "data/fabfos/runs/fuhrer_clones/ecspr"

FIELDS = ("host", "channel", "role", "axis", "src_mnxm", "sink_mnxm", "sink_name",
          "element", "n_rxn", "g", "g_denominator", "ratio", "converged", "src_is_node",
          "sink_is_node", "n_src_atoms", "n_sink_atoms", "n_nodes", "n_edges",
          "n_reactions_used", "note")

DENOMINATOR_AXIS = "denominator"


def probe_name(stem: str, channel: str, host: str, element: str) -> str:
    """The probe output's filename. One function, imported by the sweep, so a panel and its
    reachability table cannot drift apart by one of the two spelling the name itself."""
    return (f"axis_probe_{channel}_{host}_{element}"
            + ("" if stem == "axes" else f"_{stem}") + ".tsv")


def check_bake_cache() -> None:
    cache = Path(bake_pairs.CACHE)
    if not cache.is_dir():
        return
    stale = [c.name for c in cache.glob("*.parquet")
             if any(c.stat().st_mtime < b.stat().st_mtime
                    for b in bake_pairs.BAKE.glob("*.parquet"))]
    if stale:
        raise SystemExit(f"[probe_axes] stale bake cache {stale} in {cache}: "
                         f"delete the directory and rerun")


def atom_universe() -> set:
    """The transport-excluded atom-mapped reaction set, computed rather than read.

    `runs/e_coli_k12/gpr/gpr_denovo.parquet` carries `in_atom_universe` as all-null -- the
    de-novo builder leaves the column for its consumer -- so filtering on it yields an empty
    background and a graph with no nodes, which then reads as "nothing is reachable" instead
    of as a missing filter. The curated table populates the column and is filtered on it."""
    sys.path.insert(0, str(ROOT / "src/fabfos/build_references/resources/buildlib"))
    import bench_universe as bu                                              # noqa: E402
    bake = ROOT / "data/fabfos/processed/metabolism_bake"
    universe, stats = bu.atom_universe(
        bake / "vocab.parquet", bake / "atom_pairs.parquet",
        exclude=bu.transport_mnxrs(bu.reac_prop_path(ROOT / "data/fabfos/originals/metanetx")))
    print(bu.universe_line(stats, "fuhrer"), file=sys.stderr)
    return universe


def host_weights(host: str, channel: str) -> dict:
    p = ROOT / f"data/fabfos/runs/{host}/gpr/gpr_{channel}.parquet"
    df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
    if channel == "gem":
        keep = df.in_atom_universe.fillna(False).infer_objects(copy=False).astype(bool)
    else:
        keep = df.mnxr.astype(str).isin(atom_universe())
    return {m: 1.0 for m in sorted(df[keep].mnxr.dropna().astype(str).unique())}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--channel", choices=("gem", "denovo"), default="gem")
    ap.add_argument("--element", default="C")
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--axes", type=Path, default=AXES)
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()

    check_bake_cache()
    panel = pd.read_csv(a.axes, sep="\t", dtype=str).fillna("").to_dict("records")
    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    weights = host_weights(a.host, a.channel)
    sub = pairs[pairs.mnxr.isin(weights)]
    nodes = set(sub.substrate.astype(str)) | set(sub["product"].astype(str))
    g = graph_from_pairs(pairs, a.element, weights, ratios)
    print(f"[probe_axes] basis {bake_pairs.BAKE}", file=sys.stderr)
    print(f"=== {a.host} | {a.channel} | {len(weights):,} in-universe reactions "
          f"({g.meta['n_reactions_used']:,} used) | {len(nodes):,} metabolite nodes | "
          f"{g.n:,} atom nodes / {g.m:,} edges ===")

    def one(src_id: str, sink_id: str, label: str):
        src = Terminal.metabolite(g, src_id, label="source")
        snk = Terminal.metabolite(g, sink_id, label=label)
        assert (src_id in nodes) == (not src.missing), src_id
        assert (sink_id in nodes) == (not snk.missing), sink_id
        if src.missing or snk.missing:
            return None, "", "terminal is not a node of this background's carbon graph", \
                src, snk
        sol = solve(g, src, snk)
        conv = getattr(sol, "converged", None)
        note = getattr(sol, "note", "") or ""
        if conv is False:
            note = (note + "; " if note else "") + "SOLVER DID NOT CONVERGE"
        return float(sol.total), ("" if conv is None else bool(conv)), note, src, snk

    den_row = next((r for r in panel if r["axis"] == DENOMINATOR_AXIS), None)
    if den_row is None:
        raise SystemExit(f"[probe_axes] {a.axes} declares no `{DENOMINATOR_AXIS}` row")
    g_den, _, _, _, _ = one(den_row["src_mnxm"], den_row["sink_mnxm"], "CO2")
    if not g_den:
        raise SystemExit(f"[probe_axes] the shared denominator "
                         f"{den_row['src_mnxm']} -> {den_row['sink_mnxm']} reads {g_den} on "
                         f"the {a.channel} channel: there is no ratio to take")

    rows = []
    print(f"  {'axis':22} {'role':8} {'sink':14} {'node?':>5} {'atoms':>6} "
          f"{'g':>12} {'ratio':>12}")
    for r in panel:
        val, conv, note, src, snk = one(r["src_mnxm"], r["sink_mnxm"], r["sink_name"])
        ratio = "" if not val else val / g_den
        rows.append(dict(
            host=a.host, channel=a.channel, role=r["role"], axis=r["axis"],
            src_mnxm=r["src_mnxm"], sink_mnxm=r["sink_mnxm"], sink_name=r["sink_name"],
            element=a.element, n_rxn=len(weights),
            g="" if val is None else val, g_denominator=g_den, ratio=ratio,
            converged=conv, src_is_node=r["src_mnxm"] in nodes,
            sink_is_node=r["sink_mnxm"] in nodes,
            n_src_atoms=len(src) if not src.missing else 0,
            n_sink_atoms=len(snk) if not snk.missing else 0,
            n_nodes=g.n, n_edges=g.m,
            n_reactions_used=g.meta["n_reactions_used"], note=note))
        shown = "n/a" if val is None else f"{val:.6g}"
        rshown = "" if ratio == "" else f"{ratio:.6g}"
        print(f"  {r['axis']:22} {r['role']:8} {r['sink_mnxm']:14} "
              f"{str(r['sink_mnxm'] in nodes):>5} {rows[-1]['n_sink_atoms']:>6} "
              f"{shown:>12} {rshown:>12}  {note}")

    a.out_dir.mkdir(parents=True, exist_ok=True)
    out = a.out_dir / probe_name(a.axes.stem, a.channel, a.host, a.element)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(FIELDS), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    live = [r["axis"] for r in rows if r["role"] == "target" and r["g"] not in ("", 0.0)]
    dead = [r["axis"] for r in rows if r["role"] == "target" and r["g"] in ("", 0.0)]
    print(f"\n[probe_axes] {len(live)} of {len(live) + len(dead)} target axes reach on the "
          f"{a.channel} channel: {live}", file=sys.stderr)
    if dead:
        print(f"[probe_axes] unreachable, do not sweep: {dead}", file=sys.stderr)
    print(f"[probe_axes] -> {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
