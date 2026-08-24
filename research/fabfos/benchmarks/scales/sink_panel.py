#!/usr/bin/env python3
"""Effective conductance glucose -> each declared sink, per host, on the curated channel.

    mamba run -n build-refs-cobra python research/fabfos/benchmarks/scales/sink_panel.py

WHAT THIS MEASURES. One two-point solve per (host, axis): unit current injected at
D-glucose's carbon atoms, drawn at the sink's, and `Solution.total` is the effective
conductance between the two atom sets. The panel is not written here -- it is read from
the `role=target` rows of the study's declared axis file, so the axes this run reports
are the axes that were declared before any number existed.

The background is the host as the curated (GEM) channel sees it. `graph_from_pairs`
builds a graph out of exactly the reactions its weight dict names, so THE WEIGHT DICT IS
THE ORGANISM: the dict is the host's `in_atom_universe` reactions at uniform weight 1.0,
and nothing else reaches the graph. LW06's engineered pdc/adhB edges are deliberately
absent -- the host transform only subtracts -- so this is the host background alone.

WHY THE NODE CHECK IS HERE, AND WHAT IT DOES NOT BUY. The decisive claim of this panel is
a zero. `solve` returns a definite 0.0 for disconnected terminals rather than raising --
but a metabolite that is not a node of the host-restricted carbon graph at all returns
exactly the same 0.0, so A ZERO WITHOUT THE NODE CHECK IS UNINTERPRETABLE. Every row
carries `src_is_node`/`sink_is_node`, computed off the atom-pair table rather than off the
built graph, so it is an independent statement and not a restatement of `Terminal.missing`.
Membership in the MetaNetX vocabulary is NOT the same check and must not be substituted:
cross-references resolve polymer ids that are in the vocabulary and are not graph nodes.

`sink_is_node=True` with `g=0` DOES NOT, HOWEVER, ESTABLISH "genuine disconnection", and an
adversarial review of this file established that with a counter-example now carried as the
`ctl_kdo2lipida` control row. The node check rules out a missing id; it does not rule out
unmapped carbon somewhere across the intervening chain, which is the dominant failure mode
on large molecules. A zero here means "no atom-resolved carbon route in THIS basis", which
is a claim about the basis as much as about the organism, and any biological reading of it
needs evidence from outside this panel.

THE PANEL RUNS ITS CONTROLS IN THE SAME PASS as its targets, and carries `role` into the
output so a claim is distinguishable from a check on the claim. The size controls exist
because effective conductance rises with sink size and shared substructure, so a sink that
is literally two glucoses starts high for reasons that are not the paper's biology; the
classifier arm prints a size control beside every AUC and this arm had none until they were
added.
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
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))

import bake_pairs                                                            # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                # noqa: E402

PANEL_TSV = ROOT / "data/fabfos/originals/benchmarks/scales/gof_scales.tsv"
HOSTS = ("e_coli_bw25113", "e_coli_lw06")
OUT_TSV = ROOT / "data/fabfos/runs/scales/ecspr/sink_panel_gem_C.tsv"

INSERTION = ROOT / "data/fabfos/runs/scales/gpr/gpr_insertion.parquet"

FIELDS = ("host", "background", "channel", "role", "axis", "src_mnxm", "sink_mnxm",
          "sink_name", "element", "n_rxn", "g", "converged", "src_is_node",
          "sink_is_node", "expected_dir", "n_src_atoms", "n_sink_atoms", "n_nodes",
          "n_edges", "n_reactions_used", "note")


def check_bake_cache() -> None:
    cache = Path(bake_pairs.CACHE)
    if not cache.is_dir():
        return
    stale = [c.name for c in cache.glob("*.parquet")
             if any(c.stat().st_mtime < b.stat().st_mtime
                    for b in bake_pairs.BAKE.glob("*.parquet"))]
    if stale:
        raise SystemExit(f"[sink_panel] stale bake cache {stale} in {cache}: "
                         f"delete the directory and rerun")


def read_panel(path: Path) -> list[dict]:
    df = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    keep = df[df.role.isin(("target", "control"))]
    if keep.empty:
        raise SystemExit(f"[sink_panel] no role=target/control rows in {path}")
    return (keep[keep.role == "target"].to_dict("records")
            + keep[keep.role == "control"].to_dict("records"))


def host_weights(host: str, channel: str = "gem", universe: set | None = None) -> dict:
    if channel == "gem":
        p = ROOT / f"data/fabfos/runs/{host}/gpr/gpr_gem.parquet"
        df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
        keep = df.in_atom_universe.fillna(False).astype(bool)
    else:
        p = ROOT / f"data/fabfos/runs/{host}/gpr/gpr_denovo.parquet"
        if not p.exists():
            raise SystemExit(f"[sink_panel] no de-novo table at {p.relative_to(ROOT)}")
        df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
        if universe is None:
            raise SystemExit("[sink_panel] the de-novo background needs the atom universe")
        keep = df.mnxr.astype(str).isin(universe)
    return {m: 1.0 for m in sorted(df[keep].mnxr.dropna().astype(str).unique())}


def insertion_weights(path: Path) -> dict:
    df = pd.read_parquet(path, columns=["mnxr", "in_atom_universe", "feature_name"])
    df = df[df.in_atom_universe.fillna(False).astype(bool)]
    return {m: 1.0 for m in sorted(df.mnxr.dropna().astype(str).unique())}


def host_metabolite_nodes(pairs: pd.DataFrame, weights: dict) -> set:
    sub = pairs[pairs.mnxr.isin(weights)]
    return set(sub.substrate.astype(str)) | set(sub["product"].astype(str))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--panel", type=Path, default=PANEL_TSV)
    ap.add_argument("--hosts", nargs="+", default=list(HOSTS))
    ap.add_argument("--element", default="C")
    ap.add_argument("--out", type=Path, default=None,
                    help="default: sink_panel_<channel>_<element>.tsv beside the run")
    ap.add_argument("--channel", choices=("gem", "denovo"), default="gem")
    ap.add_argument("--insertion", action="store_true",
                    help="also solve LW06 with the attTn7 pdc/adhB edges unioned in")
    a = ap.parse_args()
    if a.out is None:
        a.out = OUT_TSV.parent / f"sink_panel_{a.channel}_{a.element}.tsv"

    check_bake_cache()
    panel = read_panel(a.panel)
    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    print(f"[sink_panel] basis {bake_pairs.BAKE} | {len(pairs):,} {a.element} pair rows | "
          f"{len(panel)} declared axes", file=sys.stderr)

    universe = None
    if a.channel == "denovo":
        sys.path.insert(0, str(ROOT / "src/fabfos/build_references/resources/buildlib"))
        import bench_universe as bu
        universe, ustats = bu.atom_universe(
            ROOT / "data/fabfos/processed/metabolism_bake/vocab.parquet",
            ROOT / "data/fabfos/processed/metabolism_bake/atom_pairs.parquet",
            exclude=bu.transport_mnxrs(
                bu.reac_prop_path(ROOT / "data/fabfos/originals/metanetx")))
        print(bu.universe_line(ustats, "scales"), file=sys.stderr)

    backgrounds = [(h, "host", host_weights(h, a.channel, universe)) for h in a.hosts]
    if a.insertion:
        ins = insertion_weights(INSERTION)
        for h in a.hosts:
            if h != "e_coli_lw06":
                continue
            w = dict(host_weights(h, a.channel, universe))
            new = sorted(set(ins) - set(w))
            w.update(ins)
            print(f"[sink_panel] {h}+attTn7: {len(ins)} insertion reactions, "
                  f"{len(new)} not already in the host background ({new})", file=sys.stderr)
            backgrounds.append((h, "host+attTn7", w))

    rows = []
    for host, background, weights in backgrounds:
        nodes = host_metabolite_nodes(pairs, weights)
        g = graph_from_pairs(pairs, a.element, weights, ratios)
        print(f"\n=== {host} [{background}] | gem | {len(weights):,} in-universe reactions "
              f"({g.meta['n_reactions_used']:,} used) | {len(nodes):,} metabolite nodes | "
              f"{g.n:,} atom nodes / {g.m:,} edges ===")
        print(f"  {'axis':20} {'sink':14} {'sink_name':34} {'exp':>3} "
              f"{'src?':>5} {'sink?':>5} {'atoms':>6} {'g':>12}")
        for r in panel:
            src_id, sink_id = r["src_mnxm"], r["sink_mnxm"]
            src_node, sink_node = src_id in nodes, sink_id in nodes
            src = Terminal.metabolite(g, src_id, label="source")
            snk = Terminal.metabolite(g, sink_id, label=r["sink_name"])
            assert src_node == (not src.missing), (host, src_id)
            assert sink_node == (not snk.missing), (host, sink_id)
            if src.missing or snk.missing:
                val, note, conv = "", "terminal not a node of this host's graph", ""
            else:
                sol = solve(g, src, snk)
                val, note = float(sol.total), getattr(sol, "note", "") or ""
                conv = getattr(sol, "converged", None)
                conv = "" if conv is None else bool(conv)
                if conv is False:
                    note = (note + "; " if note else "") + "SOLVER DID NOT CONVERGE"
            rows.append(dict(
                host=host, background=background, channel=a.channel,
                role=r.get("role", "target"), axis=r["gene"], src_mnxm=src_id,
                sink_mnxm=sink_id, sink_name=r["sink_name"], element=a.element,
                n_rxn=len(weights), g=val, src_is_node=src_node, sink_is_node=sink_node,
                converged=conv, expected_dir=r["expected_dir"],
                n_src_atoms=len(src), n_sink_atoms=len(snk),
                n_nodes=g.n, n_edges=g.m, n_reactions_used=g.meta["n_reactions_used"],
                note=note))
            shown = f"{val:.6g}" if val != "" else "n/a"
            print(f"  {r['gene']:20} {sink_id:14} {r['sink_name'][:34]:34} "
                  f"{r['expected_dir']:>3} {str(src_node):>5} {str(sink_node):>5} "
                  f"{len(snk):>6} {shown:>12}  {note}")

    a.out.parent.mkdir(parents=True, exist_ok=True)
    with a.out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(FIELDS), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"\n[sink_panel] wrote {len(rows)} rows -> {a.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
