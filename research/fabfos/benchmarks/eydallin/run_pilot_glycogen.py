#!/usr/bin/env python3
"""Eydallin pilot: does glycogen draw current, and does one condition move it?

The Eydallin cohort measures ONE metabolite (glycogen) across many gene-level
perturbations, so before any scoring can mean anything the target has to be a node the
media-to-ground probe can actually reach. This script answers that first, and only then
applies a single condition.

Two things it deliberately does NOT inherit from the study tier:

- The perturbation is modelled as a CONDUCTANCE FOLD-CHANGE on the perturbed reaction,
  not as an edge addition. Eydallin 2010 is an ASKA *overexpression* screen and every
  hit gene is already native to K-12, so "add the reaction" is a no-op; raising its
  weight is the only operation that means anything in a conductance model. (The study
  tier currently labels this cohort `arm=lof` / `n_del=1`, which is the opposite
  perturbation -- see the README.)

  That holds for a gene the HOST GEM carries, which is only 32 of the cohort's 86.
  For the other 54 the gene is native to K-12 but iML1515 gives it no reaction, so a
  fold-change multiplies a weight that does not exist and the perturbed solve is the
  base solve. `--rxn`/`--weight` is the addition model those need: name the reaction
  the clone supplies and set its conductance absolutely. aspP is the case that forced
  it -- ADP-sugar pyrophosphatase is `MNXR152881` (biggR:ADPGLC), atom-mapped on
  carbon over exactly the ids the host route uses, and absent from the GEM.

- The target is read off the built graph rather than assumed present. `measure_leak`'s
  draw dict only has keys for metabolites that became NODES; a missing key is a coverage
  gap, not a zero, and the two must never be collapsed.

Reference basis is the bake, atom pairs and direction from the same directory. It was
tier4 atom pairs + bake direction when this pilot was written and run; tier4's pin is
retired (the chunk stays reachable from the commit that carried it), and the swap is
not cosmetic -- glgA and
glgP have carbon rows here and had none there, so the numbers cached under `cache/` do
NOT reproduce on this basis. Re-run before quoting any of them. That also breaks
comparability with main/benchmarks/laser/pilot/run_pilot.py until it moves too.

    docker run --rm -v $PWD:/ws -w /ws fabfos:local \
        python main/benchmarks/eydallin/run_pilot_glycogen.py --gene glgC --fold 2.0
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, measure_leak, solve                        # noqa: E402
from ecspr.model.directed import _HAVE_CHOLMOD                                     # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_identity                                                          # noqa: E402
import bake_pairs                                                             # noqa: E402

ATOM_PAIRS = bake_pairs.atom_pairs()
CHEM_PROP = ROOT / "data" / "fabfos" / "originals" / "metanetx" / "4.5" / "chem_prop.tsv"
HOST_GEM = ROOT / "data" / "fabfos" / "runs" / "e_coli_k12" / "gpr" / "gpr_gem.parquet"
BAKE = ROOT / "data" / "fabfos" / "processed" / "metabolism_bake"
OUT_DIR = Path(__file__).resolve().parent / "cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)

GLYCOGEN = {"MNXM738130": "Glycogen (BiGG, iML1515 species)",
            "MNXM738131": "Glycogen (KEGG C00182)"}
WAYPOINTS = {"MNXM1364111": "D-glucose 6-phosphate (HEX1 product)",
             "MNXM1364212": "D-glucopyranose 1-phosphate (PGMT product, GLGC substrate)",
             "MNXM1105977": "ADP-alpha-D-glucose (GLGC substrate as written)",
             "MNXM8348": "Branching glycogen"}
SOURCE_NAME = "D-glucose"


def build_direction_ratios(out_path: Path) -> Path:
    # Stamped with the bake it came from -- this cache is the campaign's baseline, and
    # an unstamped one is how the r7 delivery split outlived the r8 repin.
    return bake_identity.build_direction_ratios(out_path, BAKE)


def resolve_source(pairs: pd.DataFrame) -> str:
    cp = pd.read_csv(CHEM_PROP, sep="\t", comment="#",
                     names=["id", "name", "reference", "formula", "charge", "mass",
                            "inchi", "inchikey", "smiles"])
    hit = cp[cp["name"].astype(str).str.lower() == SOURCE_NAME.lower()]
    universe = set(pairs["substrate"]) | set(pairs["product"])
    for mnxm in hit["id"]:
        if mnxm in universe:
            return mnxm
    raise SystemExit(f"[pilot] {SOURCE_NAME} resolves to {list(hit['id'])}, none of which "
                     f"is in the atom-pair universe for this element")


def gene_reactions(gene: str) -> list:
    host = pd.read_parquet(HOST_GEM)
    rows = host[host["feature_name"].astype(str) == gene]
    if not len(rows):
        raise SystemExit(f"[pilot] gene {gene!r} carries no reaction in the host GEM")
    return sorted(set(rows["mnxr"].dropna().astype(str)))


def incident_report(graph, mnxm: str) -> dict:
    nodes = [i for i, nd in enumerate(graph.nodes)
             if isinstance(nd, tuple) and nd[0] == mnxm]
    inc = [(graph.nodes[a], graph.nodes[b]) for (a, b) in graph.edges
           if a in nodes or b in nodes]
    partners = sorted({p[0] for e in inc for p in e
                       if isinstance(p, tuple) and p[0] != mnxm})
    return dict(n_nodes=len(nodes), n_incident_edges=len(inc), partner_metabolites=partners)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--gene", default="glgC",
                   help="host gene to perturb (feature_name in the host GEM); with "
                        "--rxn it only labels the run")
    p.add_argument("--fold", type=float, default=2.0,
                   help="conductance fold-change on that gene's reactions; "
                        "0 deletes them (LOF), >1 is the overexpression model")
    p.add_argument("--rxn", action="append", default=None, metavar="MNXR",
                   help="perturb these reactions directly instead of looking the gene "
                        "up in the host GEM; repeatable")
    p.add_argument("--weight", type=float, default=None,
                   help="SET the perturbed reactions' conductance to this value rather "
                        "than multiplying by --fold. Required for a reaction the host "
                        "GEM does not carry, where a fold-change multiplies zero")
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    args = p.parse_args()

    pairs = load_pairs(ATOM_PAIRS, element=args.element)
    ratios = load_direction_ratios(build_direction_ratios(OUT_DIR / "direction_ratios.parquet"))
    host = pd.read_parquet(HOST_GEM)
    base_w = {m: 1.0 for m in host["mnxr"].dropna().astype(str).unique()}

    rxns = sorted(set(args.rxn)) if args.rxn else gene_reactions(args.gene)
    pert_w = dict(base_w)
    if args.weight is not None:
        mode = f"set weight {args.weight}"
        for r in rxns:
            if args.weight == 0:
                pert_w.pop(r, None)
            else:
                pert_w[r] = args.weight
    else:
        mode = f"fold {args.fold}"
        for r in rxns:
            if args.fold == 0:
                pert_w.pop(r, None)
            else:
                pert_w[r] = base_w.get(r, 0.0) * args.fold
    absent = [r for r in rxns if r not in base_w]
    if absent and args.weight is None:
        raise SystemExit(f"[pilot] {absent} carry no weight in the host GEM, so --fold "
                         f"multiplies zero and the perturbed solve IS the base solve. "
                         f"Pass --weight to model the clone supplying the reaction.")
    print(f"[pilot] {args.gene}: {len(rxns)} reaction(s) {rxns} at {mode}"
          f"{f' (ADDED, absent from host GEM: {absent})' if absent else ''}",
          file=sys.stderr)

    src_mnxm = resolve_source(pairs)
    probes = {**GLYCOGEN, **WAYPOINTS}

    def solve(weights, tag):
        g = graph_from_pairs(pairs, args.element, weights, ratios)
        print(f"[pilot] {tag}: {g.n:,} nodes / {g.m:,} edges from "
              f"{g.meta['n_reactions_used']:,} reactions "
              f"(AAM gap {g.meta['n_aam_gap']:,})", file=sys.stderr)
        src = Terminal.metabolite(g, src_mnxm, label="source")
        if src.missing:
            raise SystemExit(f"[pilot] source {src_mnxm} absent from the built graph")
        r = measure_leak(g, src, [], leak=args.leak)
        return g, r

    g_base, r_base = solve(base_w, "base")
    g_pert, r_pert = solve(pert_w, "pert")

    out = dict(
        gene=args.gene, gene_reactions=rxns, fold=args.fold,
        element=args.element, leak=args.leak, ground="universal",
        source=SOURCE_NAME, source_mnxm=src_mnxm,
        reference_basis="metabolism_bake atom pairs + direction",
        cholmod_available=_HAVE_CHOLMOD,
        base=dict(total=r_base["total"], converged=r_base["converged"],
                  n_metabolites=r_base["n_metabolites"],
                  n_nodes=g_base.n, n_edges=g_base.m),
        pert=dict(total=r_pert["total"], converged=r_pert["converged"],
                  n_metabolites=r_pert["n_metabolites"],
                  n_nodes=g_pert.n, n_edges=g_pert.m),
        probes={},
    )
    for mnxm, label in probes.items():
        db, dp = r_base["draw"].get(mnxm), r_pert["draw"].get(mnxm)
        out["probes"][mnxm] = dict(
            name=label,
            draw_base=db, draw_pert=dp,
            delta=(None if db is None or dp is None else dp - db),
            in_draw_base=db is not None, in_draw_pert=dp is not None,
            graph_base=incident_report(g_base, mnxm),
        )

    path = OUT_DIR / f"pilot_{args.gene}_fold{args.fold}_{args.element}.json"
    path.write_text(json.dumps(out, indent=1, sort_keys=True))
    print(json.dumps(out, indent=1, sort_keys=True))
    print(f"\nwrote {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
