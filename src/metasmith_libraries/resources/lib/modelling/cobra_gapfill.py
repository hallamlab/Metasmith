"""Add the smallest set of universe reactions that makes biomass feasible.

    cobra_gapfill.py <draft.xml> <media.tsv> <reac_prop.tsv> <chem_prop.tsv> \\
                     <chem_xref.tsv> <out.xml>

Without this a missing annotation returns zero growth, which reads as a
biological result. Every reaction added is labelled `gapfill` in its notes and
gets `GAPFILL` in its id prefix, so a later reader can tell a gapfill from an
annotation instead of having to diff two models.

The universe is NOT all of MetaNetX. It is restricted to reactions whose
metabolites the draft already carries, because a gapfill that is free to invent
metabolites will always find a solution and the solution means nothing: the MILP
is over which reactions connect what the model has, not over what it could have.
`MetBridge` is what maps a MetaNetX metabolite onto the model's own dialect, so
this works on a BiGG-derived model as well as on one `gem_from_gpr` wrote.
"""
import sys

import cobra
from cobra.flux_analysis import gapfill

import bridge as met_bridge
import media as media_mod
import mnx

MAX_UNIVERSE = 20_000


def main(argv: list[str]) -> int:
    model_path, media_path, reac_prop_path, chem_prop_path, chem_xref_path, out_path = argv

    model = cobra.io.read_sbml_model(model_path)
    biomass = mnx.biomass_reaction(model)
    assert biomass is not None, (
        f"[{model_path}] has neither an objective nor a reaction named for biomass; "
        "there is nothing for a gapfill to make feasible"
    )
    model.objective = biomass

    table = media_mod.load(media_path)
    media_mod.apply(model, table)
    before = model.slim_optimize()
    print(f"objective [{biomass.id}] before gapfill: {before}")

    chem_prop = mnx.load_chem_prop(chem_prop_path)
    bridge = met_bridge.MetBridge(model, chem_prop, chem_xref_path)
    have = set(bridge.by_mnxm) | {
        m.id[:-2] for m in model.metabolites if m.id.endswith("_c")
    }

    universe = cobra.Model("mnx_universe")
    props = mnx.load_reac_prop(reac_prop_path)
    added = 0
    for mnxr, prop in props.items():
        if mnxr in model.reactions:
            continue
        if str(prop.get("is_transport")).upper() in ("T", "TRUE", "1"):
            continue
        stoich = mnx.parse_mnx_equation(prop.get("equation"))
        if stoich is None or not set(stoich).issubset(have):
            continue
        reaction = cobra.Reaction(f"GAPFILL_{mnxr}")
        reaction.lower_bound, reaction.upper_bound = -1000.0, 1000.0
        universe.add_reactions([reaction])
        reaction.add_metabolites({bridge.get(m)[0]: c for m, c in stoich.items()})
        added += 1
        if added >= MAX_UNIVERSE:
            print(f"universe capped at {MAX_UNIVERSE} reactions")
            break
    print(f"universe: {added} candidate reactions over metabolites the draft already has")

    filled = []
    if before is None or before < 1e-6:
        try:
            solutions = gapfill(model, universe, demand_reactions=False, iterations=1)
            filled = list(solutions[0]) if solutions else []
        except Exception as e:                                     # noqa: BLE001
            print(f"gapfill found no solution: {type(e).__name__}: {e}")
    else:
        print("model already grows on this medium; nothing to fill")

    for reaction in filled:
        reaction.notes["source"] = "gapfill"
    model.add_reactions(filled)
    after = model.slim_optimize()
    print(f"added {len(filled)} reactions: {[r.id for r in filled]}")
    print(f"objective after gapfill: {after}")

    cobra.io.write_sbml_model(model, out_path)
    print(f"{len(model.reactions)} reactions -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
