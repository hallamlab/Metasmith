#!/usr/bin/env python3
"""The null the panel needs: how does a target rank when the deleted gene is not its own?

`run_ko_panel.py` finds the required metabolite near the top of the depletion ranking
under three of four conditions -- but three of those four move it by 1e-5 to 1e-7 of a
unit-1 share, which is small enough that "near the top of 991" has to be earned rather
than asserted. A rank is only evidence against a distribution of ranks.

So: delete a reaction the condition did not delete, size-matched at one reaction, and
ask where L-arginine / L-histidine / L-tryptophan land. Two pools, because they fail
differently:

  keio  -- the reactions the OTHER Keio genes delete. Matched in character: every one
           is a real biosynthetic step some auxotroph lost, so this pool cannot be
           dismissed as "random reactions are in dead corners of the graph".
  graph -- uniform over every reaction that is both in the host GEM and in the element's
           atom-pair table. Broader, and the only pool that says anything about the
           graph as a whole rather than about biosynthesis.

The empirical p is one-sided on the same statistic the panel reports (`frac_beaten`,
the share of the field the target is strictly more depleted than), with the standard
+1/+1 so a pool that never reaches the observed value reports 1/(K+1) rather than zero.

    docker run --rm -v "$PWD":/ws -w /ws fabfos:local \
        python main/benchmarks/keio/run_null_pool.py --k 30
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_ko_panel import (  # noqa: E402
    ATOM_PAIRS, CACHE, CHEM_PROP, EDITS, HOST_GEM, OUT, PANEL, SOURCE_ALIASES,
    SOURCE_NAME, build_direction_ratios, run, score_field, rank_of,
)

SEED = 20260809  # one seed for both pools and every arm -- see the LASER README's
                 # POOL_SEED note: re-drawing per arm voids every head-to-head p.


def in_graph_reactions(element):
    gem = pd.read_parquet(HOST_GEM)
    gem_rxn = set(gem.mnxr.dropna().astype(str))
    pairs = pd.read_parquet(ATOM_PAIRS, columns=["mnxr", "element"])
    return sorted(gem_rxn & set(pairs[pairs.element == element].mnxr.astype(str)))


def keio_reactions(element, exclude):
    """Every reaction some Keio gene deletes, minus the panel's own, restricted to the
    ones that are actually in the built graph -- a deletion of a reaction the graph
    never had is a no-op and would pad the null with exact zeros."""
    edits = pd.read_parquet(EDITS)
    cand = set(edits[edits.action == "del"].mnxr.dropna().astype(str))
    return sorted((cand & set(in_graph_reactions(element))) - set(exclude))


def solve_del(mnxr, direction_path, source_mnxm, element, leak):
    out = CACHE / f"null_{mnxr}_{element}.json"
    if out.exists():
        return json.loads(out.read_text())
    w = CACHE / f"null_weights_{mnxr}.json"
    if not w.exists():
        base_w = json.loads((CACHE / "weights___base__.json").read_text())
        base_w.pop(mnxr, None)
        w.write_text(json.dumps(base_w, indent=1, sort_keys=True))
    run("solve", "--ground", "universal", "--atom-pairs", ATOM_PAIRS,
        "--direction", direction_path, "--weights", w, "--element", element,
        "--source", source_mnxm, "--leak", leak, "--out", out, want_json=False)
    return json.loads(out.read_text())


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--k", type=int, default=30, help="draws per pool")
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    p.add_argument("--floor", type=float, default=1e-15)
    args = p.parse_args()

    base_path = CACHE / f"solve___base___{args.element}.json"
    if not base_path.exists():
        raise SystemExit("run run_ko_panel.py first -- this reuses its background solve")
    base_draw = json.loads(base_path.read_text())["draw"]
    direction_path = build_direction_ratios(CACHE / "direction_ratios.parquet")

    source = run("resolve-metabolite", "--name", SOURCE_NAME, "--exact-names",
                 *SOURCE_ALIASES, "--atom-pairs", ATOM_PAIRS, "--chem-prop", CHEM_PROP,
                 "--element", args.element)
    tnames = sorted({v[0] for v in PANEL.values()})
    targets = {n: run("resolve-metabolite", "--name", n, "--exact-names", n,
                      "--atom-pairs", ATOM_PAIRS, "--chem-prop", CHEM_PROP,
                      "--element", args.element)["mnxm"] for n in tnames}

    observed = pd.read_csv(OUT / "panel_matrix.tsv", sep="\t")
    panel_del = set()
    for e in pd.read_parquet(EDITS).itertuples(index=False):
        if e.condition_id in PANEL and pd.notna(e.mnxr):
            panel_del.add(str(e.mnxr))

    rng = random.Random(SEED)
    pools = {
        "keio": keio_reactions(args.element, panel_del),
        "graph": sorted(set(in_graph_reactions(args.element)) - panel_del),
    }
    draws = {}
    for name, cand in pools.items():
        k = min(args.k, len(cand))
        draws[name] = rng.sample(cand, k)
        print(f"[null] pool {name}: {k} of {len(cand)} candidates", file=sys.stderr)

    rows = []
    for name, sel in draws.items():
        for i, mnxr in enumerate(sel, 1):
            print(f"[null] {name} {i}/{len(sel)} {mnxr}", file=sys.stderr)
            ko = solve_del(mnxr, direction_path, source["mnxm"], args.element, args.leak)
            scored = score_field(base_draw, ko["draw"], args.floor)
            for tname, mnxm in targets.items():
                r = rank_of(scored, mnxm)
                rows.append(dict(pool=name, del_mnxr=mnxr, target=tname,
                                 rel_change=None if r is None else r["rel_change"],
                                 n_more_depleted=None if r is None else r["n_more_depleted"],
                                 frac_beaten=None if r is None else r["frac_beaten"],
                                 n_scored=None if r is None else r["n_scored"]))

    null = pd.DataFrame(rows)
    null.to_csv(OUT / "null_pool.tsv", sep="\t", index=False)

    # one-sided p on frac_beaten, against each pool, for every observed cell
    ps = []
    for o in observed.itertuples(index=False):
        for name in pools:
            n = null[(null.pool == name) & (null.target == o.target)]
            ge = int((n.frac_beaten >= o.frac_beaten).sum())
            ps.append(dict(condition_id=o.condition_id, role=o.role, target=o.target,
                           is_required=o.is_required, pool=name,
                           observed_frac_beaten=o.frac_beaten,
                           observed_rel_change=o.rel_change,
                           null_k=len(n), null_ge=ge,
                           null_median_frac_beaten=float(n.frac_beaten.median()),
                           p_empirical=(ge + 1) / (len(n) + 1),
                           p_floor=1 / (len(n) + 1)))
    pdf = pd.DataFrame(ps)
    pdf.to_csv(OUT / "null_pvalues.tsv", sep="\t", index=False)
    print(f"[null] wrote {OUT/'null_pool.tsv'} and {OUT/'null_pvalues.tsv'}", file=sys.stderr)
    print(pdf[pdf.is_required == 1].to_string(index=False))


if __name__ == "__main__":
    main()
