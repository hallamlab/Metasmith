#!/usr/bin/env python3
"""Build the host models this arm compares clones against, one per GPR channel.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin_fba/build_models.py --report

Both channels use `iECDH1ME8569_1439` as the stoichiometric frame, because a GPR table
carries gene-to-reaction evidence and no stoichiometry at all.  What differs is which of
that frame's reactions each channel's evidence keeps, and what each channel adds.

**The GEM host** removes the reactions the AG1 host GPR table does not assert.  AG1 borrows
DH1's model and the borrow withholds one reaction, GTPDPK, for relA1.  Two independent
sources say so and the build asserts against both: `check_ag1_identity.py`'s declared
`EXPECTED_LOST_REACTIONS`, and the table's own coverage of the frame.  A future strain edit
that moves one source and not the other fails here rather than silently.

**The de-novo host** runs four steps.

1. Carve the frame to reactions whose MNXR the de-novo host lanes assert.  Keep every
   reaction that carries no gene rule -- exchanges, demands, biomass, maintenance and the
   spontaneous reactions -- because no annotation lane can speak to those.
2. Insert lane-asserted MNXR the GEM never named, through `mnx_insert`.
3. Close the energy-generating cycles that reversible insertion creates.
4. Gapfill back to biomass feasibility, drawing only on reactions the carve removed.
5. Close the cycles again.  Gapfill returns partners the carve had taken away, so a cycle
   shut in step 3 can reopen.  At two lanes exactly one does, and leaving it open puts the
   de-novo host above the curated one, at 1.367 per hour against 0.982.

**WARNING** The carve is deep.  At two lanes it keeps 1,260 of 2,755 reactions, 530 of them
ungoverned, and biomass goes to zero.  Step 4 therefore does real work and its count is a
result rather than a detail.  Gapfill runs as an L1 relaxation because GLPK is the only
solver here and the exact minimum-cardinality MILP does not finish on a 1,495-reaction
donor.  `--milp` selects the exact form for anyone with a better solver.

**CAUTION** `min_lanes` is the one knob that moves everything downstream.  REPORT.md
documents two lanes as the fix for the ProtBERT lane's promiscuity, and two is the right
default for a question about annotation quality in general.  It is the wrong default here:
no two lanes agree on any reaction of the glycogen pathway, so a two-lane carve removes the
phenotype under study and only gapfill puts it back.  This arm defaults to one lane and
`--sweep` builds all three, so what the threshold costs stays visible.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402
import kornberg as K  # noqa: E402
import mnx_insert as MI  # noqa: E402

EXPECTED_LOST = {"GTPDPK"}
MODELS = C.OUT / "models"


def _demand(model):
    """The glycogen demand every arm maximises.  Idempotent."""
    if C.DEMAND in model.reactions:
        return model.reactions.get_by_id(C.DEMAND)
    met = model.metabolites.get_by_id(C.GLYCOGEN_MET)
    return model.add_boundary(met, type="demand", reaction_id=C.DEMAND)


def gem_host():
    """The frame minus whatever the AG1 host GPR table does not assert."""
    model = C.load_model()
    frame = {r.id for r in model.reactions}
    asserted = C.host_gem_bigg()
    lost = frame - asserted
    if lost != EXPECTED_LOST:
        raise SystemExit(
            f"AG1 host GPR omits {sorted(lost)}; check_ag1_identity.py declares "
            f"{sorted(EXPECTED_LOST)}.  The two sources disagree, so one of them moved.")
    model.remove_reactions(sorted(lost), remove_orphans=True)
    model.id = "ag1_gem"
    _demand(model)
    return model, {"channel": "gem", "removed": sorted(lost)}


def denovo_host(min_lanes: int = 2, gapfill: bool = True, milp: bool = False):
    model = C.load_model()
    model.id = f"ag1_denovo_l{min_lanes}"
    mnxr_by_rid = C.model_mnxr_map()
    supported = C.host_denovo_mnxr(min_lanes)

    ungoverned = [r.id for r in model.reactions if not r.gene_reaction_rule.strip()]
    keep = set(ungoverned) | {r.id for r in model.reactions
                              if mnxr_by_rid.get(r.id) in supported}
    drop = sorted({r.id for r in model.reactions} - keep)

    donor = _donor(model, drop)
    model.remove_reactions(drop, remove_orphans=False)
    _demand(model)

    bridge = MI.bridge_for(model)
    want = sorted(supported - set(mnxr_by_rid.values()))
    inserted, _rejected, insert_census = MI.insert(model, want, bridge)
    closed = MI.close_energy_cycles(model, list(inserted.values()))

    census = {"channel": "denovo", "min_lanes": min_lanes,
              "frame_reactions": len(keep) + len(drop),
              "ungoverned_kept": len(ungoverned),
              "carved_away": len(drop),
              "kept": len(keep),
              "insert": insert_census,
              "energy_cycles_closed": closed}

    census["growth_carved"] = round(_growth(model, K.m9(model)), 6)
    if not gapfill:
        census["gapfill"] = "skipped"
        return model, census

    census["gapfill"] = _gapfill(model, donor) if milp else _gapfill_lp(model, donor)
    census["growth_gapfilled"] = round(_growth(model, K.m9(model)), 6)

    # Gapfill hands the inserted reactions partners the carve had taken away, so a cycle
    # that was closed before it can reopen.  At two lanes exactly one does, and leaving it
    # open puts the de-novo host above the curated one at 1.367 per hour against 0.982.
    reclosed = MI.close_energy_cycles(model, list(inserted.values()))
    census["energy_cycles_closed_after_gapfill"] = reclosed
    census["growth_final"] = round(_growth(model, K.m9(model)), 6)
    return model, census


def _donor(model, drop):
    """A universal model holding exactly the reactions the carve removes."""
    import cobra
    u = cobra.Model("carved_away")
    u.add_reactions([model.reactions.get_by_id(r).copy() for r in drop])
    return u


GLYCOGEN_FLOOR = 0.01


def _gapfill_lp(model, donor, frac: float = 0.1, glycogen_floor: float = GLYCOGEN_FLOOR):
    """Restore biomass and glycogen feasibility by an L1-minimal draw on the leftovers.

    Exact gapfill is a MILP and GLPK is the only solver this environment has, so the
    minimum-cardinality formulation does not finish on a 1,495-reaction donor.  This is the
    standard relaxation: put every donor reaction back, require growth and glycogen, then
    minimise total absolute flux through donor reactions alone.  What still carries flux is
    what the carve should not have taken.  The draw is small rather than provably minimal.

    The requirement is growth *and* glycogen.  No two annotation lanes agree on any reaction
    of the glycogen pathway -- GLGC, GLCS1, GLBRAN2 and PGMT each rest on one lane -- so a
    two-lane carve deletes glycogen synthesis outright and every clone's percent difference
    goes undefined.  Asking for a token 0.01 mmol/gDW/h restores the capability without
    dictating the capacity, and what it costs measures how far the lanes fell short.

    **CAUTION** The work happens on `model` itself, not on a copy.  Solving on a copy and
    transplanting the solution's support does not reproduce it: the transplant came back at
    zero growth while the copy it came from was feasible.  Keeping the model that solved the
    problem removes that whole class of mismatch.
    """
    ref = C.load_model()
    with ref:
        ref.medium = K.m9(ref)
        target = frac * (ref.slim_optimize() or 0.0)

    bio = [r for r in model.reactions if r.objective_coefficient]
    if not bio:
        return {"needed": None, "reactions": [], "status": "no_biomass"}
    bio = bio[0]
    keep_obj = model.objective

    added = []
    for r in donor.reactions:
        if r.id not in model.reactions:
            model.add_reactions([r.copy()])
            added.append(r.id)

    aux, obj = [], []
    for rid in added:
        r = model.reactions.get_by_id(rid)
        fwd = model.problem.Variable(f"_gf_p_{rid}", lb=0)
        rev = model.problem.Variable(f"_gf_n_{rid}", lb=0)
        cons = model.problem.Constraint(r.flux_expression - fwd + rev, lb=0, ub=0,
                                        name=f"_gf_c_{rid}")
        model.add_cons_vars([fwd, rev])
        model.add_cons_vars(cons)
        aux += [fwd, rev, cons]
        obj += [fwd, rev]

    old_medium = dict(model.medium)
    old_bio_lb = bio.lower_bound
    old_dm_lb = model.reactions.get_by_id(C.DEMAND).lower_bound
    model.medium = {k: v for k, v in K.m9(model).items() if k in model.reactions}
    bio.lower_bound = target
    model.reactions.get_by_id(C.DEMAND).lower_bound = glycogen_floor
    model.objective = model.problem.Objective(sum(obj), direction="min")
    sol = model.optimize()

    bio.lower_bound = old_bio_lb
    model.reactions.get_by_id(C.DEMAND).lower_bound = old_dm_lb
    model.remove_cons_vars(aux)
    model.objective = keep_obj
    model.objective.direction = "max"
    model.medium = {k: v for k, v in old_medium.items() if k in model.reactions}

    if sol.status != "optimal":
        model.remove_reactions(added, remove_orphans=False)
        return {"needed": None, "reactions": [], "status": "infeasible",
                "target_growth": round(target, 6)}

    used = sorted(rid for rid in added if abs(sol.fluxes.get(rid, 0.0)) > 1e-12)
    model.remove_reactions([r for r in added if r not in set(used)],
                           remove_orphans=False)
    got = _growth(model, K.m9(model))
    return {"needed": len(used), "reactions": used,
            "status": "ok" if got > 1e-6 else "verify_failed",
            "target_growth": round(target, 6), "growth_reached": round(got, 6),
            "method": "l1_relaxation"}


def _gapfill(model, donor, timeout_iterations: int = 1):
    """Restore biomass feasibility using only reactions the carve took away.

    Drawing on the carve's own leftovers rather than a foreign universal model keeps the
    de-novo host a subset of the curated one plus its MetaNetX insertions.  A gapfilled
    reaction is then a statement about what the annotation missed, not about what some
    other organism can do.
    """
    from cobra.flux_analysis import gapfill as cobra_gapfill

    with model:
        model.medium = K.m9(model)
        if (model.slim_optimize() or 0.0) > 1e-6:
            return {"needed": 0, "reactions": []}
    try:
        with model:
            model.medium = K.m9(model)
            sols = cobra_gapfill(model, donor, demand_reactions=False,
                                 iterations=timeout_iterations)
    except Exception as e:  # cobra raises RuntimeError when the MILP finds nothing
        return {"needed": None, "reactions": [], "status": "failed",
                "error": f"{type(e).__name__}: {e}"}
    add = [r.id for r in sols[0]]
    model.add_reactions([donor.reactions.get_by_id(r).copy() for r in add])
    return {"needed": len(add), "reactions": sorted(add), "status": "ok"}


def _growth(model, medium) -> float:
    with model:
        model.medium = {k: v for k, v in medium.items() if k in model.reactions}
        v = model.slim_optimize()
    return float(v) if v is not None and v == v else 0.0


def _max_glycogen(model, medium, pin: float) -> float:
    with model:
        model.medium = {k: v for k, v in medium.items() if k in model.reactions}
        mu = model.slim_optimize()
        if mu is None or mu != mu:
            return 0.0
        for r in model.reactions:
            if r.objective_coefficient:
                r.lower_bound = max(r.lower_bound, pin * mu)
        model.objective = C.DEMAND
        v = model.slim_optimize()
    return float(v) if v is not None and v == v else 0.0


def profile(model, label: str) -> dict:
    prop = C.chem_prop()
    formulas = K.formula_by_bigg(prop, C.bigg_bridge())
    kb, _absent = K.kornberg(model, formulas)
    # `model.genes` keeps every gene the frame shipped with, carved or not, so count the
    # ones that still govern a reaction.
    live = {g.id for r in model.reactions for g in r.genes}
    out = {"model": label,
           "reactions": len(model.reactions),
           "metabolites": len({m.id for r in model.reactions
                               for m in r.metabolites}),
           "genes": len(live)}
    for name, med in (("m9", K.m9(model)), ("kornberg", kb)):
        out[f"growth_{name}"] = round(_growth(model, med), 6)
        for pin in (0.9, 0.5, 0.1):
            out[f"glycogen_{name}_pin{pin}"] = round(_max_glycogen(model, med, pin), 6)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-lanes", type=int, default=1)
    ap.add_argument("--sweep", action="store_true",
                    help="build the de-novo host at one, two and three lanes")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--no-gapfill", action="store_true")
    ap.add_argument("--milp", action="store_true",
                    help="use cobra's exact minimum-cardinality gapfill instead of the "
                         "L1 relaxation; does not finish under GLPK on this donor")
    a = ap.parse_args()

    MODELS.mkdir(parents=True, exist_ok=True)
    import cobra

    census, profiles = [], []

    gem, c = gem_host()
    census.append(c)
    cobra.io.save_json_model(gem, str(MODELS / "ag1_gem.json"))
    profiles.append(profile(gem, "ag1_gem"))

    lanes = (1, 2, 3) if a.sweep else (a.min_lanes,)
    for k in lanes:
        dn, c = denovo_host(k, gapfill=not a.no_gapfill, milp=a.milp)
        census.append(c)
        cobra.io.save_json_model(dn, str(MODELS / f"ag1_denovo_l{k}.json"))
        profiles.append(profile(dn, f"ag1_denovo_l{k}"))

    (C.OUT / "host_census.json").write_text(json.dumps(census, indent=2))
    (C.OUT / "host_profile.json").write_text(json.dumps(profiles, indent=2))

    if a.report:
        print(json.dumps(census, indent=2))
        print()
        keys = ["model", "reactions", "metabolites", "genes",
                "growth_m9", "growth_kornberg",
                "glycogen_m9_pin0.1", "glycogen_kornberg_pin0.1"]
        print("\t".join(keys))
        for p in profiles:
            print("\t".join(str(p.get(k)) for k in keys))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
