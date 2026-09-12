"""Borrow CarveMe's biomass equation and map it into our own MNXM namespace.

    carveme_biomass.py <draft.xml> <carveme_universe.xml.gz> <chem_xref.tsv> \\
                       <chem_prop.tsv> <out.xml> [biomass_reaction_id]

`gem_from_gpr.py` deliberately sets no objective: a GPR table names no biomass
equation, and inventing one would make an unfalsifiable growth number. This
script is the other half of that trade, made explicit rather than silent --
Tony's approved design is to borrow CarveMe's `Growth` reaction (57 metabolites
over `universe_bacteria.xml.gz`, 64 over `universe_gramneg.xml.gz`) rather than
derive one, and to say so in the model: every reaction this writes carries
`notes["source"] = "carveme_biomass:<universe file>:<reaction id>"`.

The 57 (or 64) BiGG metabolites are mapped to MNXM through the same three tiers
`bridge.py` uses, in the same priority, for the same reason (an acid and its
anion must NOT collapse together just because they share a skeleton):

  1. direct    -- MetaNetX's own chem_xref carries a `biggM:<id>` row.
  2. inchikey  -- chem_xref has no direct row; fall back to the metabolite's own
                  structural InChIKey, as CarveMe's SBML annotates it (a property
                  of the compound, not a MetaNetX release pointer), matched by its
                  first 14 characters -- the skeleton, not the protonation state.
  3. formula_charge -- last resort, exact (formula, charge) equality.

A biomass metabolite that matches none of the three is DROPPED from the
equation and named on stderr rather than silently omitted: a biomass missing a
precursor is a different, easier problem than the one CarveMe's authors solved,
and gapfill downstream must not be credited for closing a gap that was never
opened. Measured against `universe_bacteria.xml.gz`: 57/57 direct, 0 inchikey,
0 formula_charge, 0 unmatched -- MetaNetX's own BiGG cross-reference already
names every one of CarveMe's biomass precursors, so this model happens not to
exercise tiers 2 and 3 at all. That is a property of THIS universe file, not a
guarantee; the tier breakdown is printed every run so a future universe or a
different biomass reaction shows its own numbers rather than inheriting this one.
"""
import sys

import cobra

import mnx

DEFAULT_BIOMASS_ID = "Growth"


def tier_map(bigg_bases: dict, mnxm_by_bigg: dict, universe_model, chem_prop) -> tuple[dict, dict]:
    """bigg_id -> (mnxm, tier), plus the per-tier bigg-id lists, for a set of BiGG ids."""
    chem_idx = chem_prop.set_index("mnxm")
    by_ikey14: dict[str, str] = {}
    by_fc: dict[tuple[str, str], str] = {}
    for row in chem_prop.itertuples(index=False):
        ik = row.inchikey
        if isinstance(ik, str) and ik:
            by_ikey14.setdefault(str(ik)[:14], row.mnxm)
        f, c = row.formula, row.charge
        if isinstance(f, str) and f and f != "*" and c is not None and c == c:
            by_fc.setdefault((f, str(c)), row.mnxm)

    resolved: dict[str, tuple[str, str]] = {}
    tiers = {"direct": [], "inchikey": [], "formula_charge": [], "unmatched": []}
    for bid, base in bigg_bases.items():
        direct = mnxm_by_bigg.get(base)
        if direct:
            resolved[bid] = (direct[0], "direct")
            tiers["direct"].append(bid)
            continue
        met = universe_model.metabolites.get_by_id(bid)
        ik = met.annotation.get("inchikey") if met.annotation else None
        ik14 = str(ik)[:14] if isinstance(ik, str) and ik else None
        cand = by_ikey14.get(ik14) if ik14 else None
        if cand:
            resolved[bid] = (cand, "inchikey")
            tiers["inchikey"].append(bid)
            continue
        f, c = met.formula, met.charge
        fc = (f, str(c)) if isinstance(f, str) and f and f != "*" and c is not None else None
        cand = by_fc.get(fc) if fc else None
        if cand:
            resolved[bid] = (cand, "formula_charge")
            tiers["formula_charge"].append(bid)
            continue
        tiers["unmatched"].append(bid)
    return resolved, tiers


def main(argv: list[str]) -> int:
    draft_path, universe_path, chem_xref_path, chem_prop_path, out_path = argv[:5]
    biomass_id = argv[5] if len(argv) > 5 else DEFAULT_BIOMASS_ID

    model = cobra.io.read_sbml_model(draft_path)
    universe = cobra.io.read_sbml_model(universe_path)
    biomass = universe.reactions.get_by_id(biomass_id)

    bigg_bases = {m.id: mnx.strip_compartment(m.id) for m in biomass.metabolites}
    bridge = mnx.bigg_bridge(chem_xref_path)
    mnxm_by_bigg = bridge.groupby("bigg").mnxm.apply(list).to_dict()
    chem_prop = mnx.load_chem_prop(chem_prop_path)
    resolved, tiers = tier_map(bigg_bases, mnxm_by_bigg, universe, chem_prop)

    print(f"=== biomass tier yield: {biomass_id} over {universe_path} "
          f"({len(bigg_bases)} metabolites) ===")
    for k in ("direct", "inchikey", "formula_charge", "unmatched"):
        print(f"  {k}: {len(tiers[k])}")
    if tiers["unmatched"]:
        print(f"  DROPPED from the biomass equation (no MNXM found): {tiers['unmatched']}")

    chem_by_mnxm = chem_prop.set_index("mnxm")
    formulas = chem_by_mnxm.formula.to_dict()
    stoich = {}
    for met_obj, coef in biomass.metabolites.items():
        bid = met_obj.id
        mapping = resolved.get(bid)
        if mapping is None:
            continue
        mnxm, _tier = mapping
        mid = f"{mnxm}_c"
        if mid in model.metabolites:
            met = model.metabolites.get_by_id(mid)
        else:
            f = formulas.get(mnxm)
            met = cobra.Metabolite(
                mid, compartment="c", name=str(bid),
                formula=(f if isinstance(f, str) and f != "*" else None),
            )
            model.add_metabolites([met])
        stoich[met] = coef

    reaction = cobra.Reaction(biomass_id)
    reaction.lower_bound, reaction.upper_bound = biomass.lower_bound, biomass.upper_bound
    reaction.notes["source"] = f"carveme_biomass:{universe_path}:{biomass_id}"
    model.add_reactions([reaction])
    reaction.add_metabolites(stoich)
    model.objective = reaction

    cobra.io.write_sbml_model(model, out_path)
    print(f"biomass [{reaction.id}] over {len(stoich)}/{len(bigg_bases)} precursors "
          f"-> {len(model.reactions)} reactions, {len(model.metabolites)} metabolites -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
