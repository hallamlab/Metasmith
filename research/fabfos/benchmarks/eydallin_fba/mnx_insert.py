#!/usr/bin/env python3
"""Turn a set of MetaNetX reaction ids into cobra reactions on an existing frame.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin_fba/mnx_insert.py

The de-novo channel asserts reactions in MNXR space that the curated GEM never names.
Carving the GEM to lane-supported reactions therefore loses them, and only an insertion
step puts them back.  This module is that step, and `laser/vs_gem` already holds the
machinery it needs: `parse_mnx_equation` reads MetaNetX's `1 MNXM01@MNXD1 = ...` form,
`load_reac_prop` fetches the rows, and the four-tier `MetBridge` maps an MNXM onto the
frame's own metabolite by identity, InChIKey prefix, or formula and charge.

Four admission rules sit on top of that machinery.  Each one exists because it was
measured to matter on this frame, and the numbers below come from the host lanes at a
two-lane threshold: 1,713 MNXR the GEM lacks, 131 of which survive to insertion.

1. **Take only balanced, non-transport MetaNetX reactions.** MetaNetX marks 39,073 of its
   reactions unbalanced.  An unbalanced reaction is a free source or sink for whatever it
   fails to conserve.  675 of the 1,713 fall here, and a further 38 are transport.

2. **Insert only when the frame holds every metabolite, each as a distinct one.**
   `MetBridge` will create a metabolite on demand.  A created metabolite has one producer
   and one consumer -- the inserted reaction itself -- so it carries no flux at steady
   state and the reaction is structurally dead.  380 of the 1,713 fall here.

   Two metabolites collapsing onto one is the same failure wearing a different face.  An
   epimerase reads `substrate + 2 H+ = product`, and both epimers carry one formula and
   one charge, so the loosest bridge tier maps them to a single frame metabolite.  The
   substrate and the product cancel and `2 H+ ->` survives as a free proton sink.  25 fall
   here, and a further rule rejects whatever reduces to protons and water.

3. **Re-balance on the frame, correcting protons only.** MetaNetX balances against its own
   protonation and BiGG protonates differently, so a reaction can leave MetaNetX balanced
   and land on the frame carrying spare protons.  `fadh2_c + 2 h_c + oaa_c = fad_c +
   mal__L_c` is one of seven such, and each one alone raises growth from 0.982 to 1.384
   per hour by acting as a proton sink.  25 insertions take a proton correction.  7 stay
   unbalanced after it, which means the bridge reached the wrong metabolite, and those go.

4. **Let a carboxy-lyase run only forwards.** See `_direction`.

**CAUTION** MetaNetX ships no direction, and full reversibility is the right prior for an
annotation that says only "this gene catalyses this reaction".  It is also how an inserted
reaction completes an energy-generating cycle.  Two of these 131 make 1000 units of ATP
from an empty medium.  Every build calls `close_energy_cycles` after inserting.  It is not
optional and it is not a diagnostic.

Together the four rules and the cycle closure leave the de-novo insertion worth 1.5% of
growth rate rather than 41%.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402

BIG = 1000.0
HET_PREFIX = C.HET_PREFIX

REJECTIONS = ("no_stoichiometry", "transport", "unbalanced", "unparsable",
              "orphan_metabolite", "collapsed", "trivial", "degenerate",
              "no_formula", "unbalanced_on_frame", "already_present")

FREE = ("h_c", "h_p", "h_e", "h2o_c", "h2o_p", "h2o_e")


def _direction(classifs, mets: dict, bounds):
    """Bounds for one insertion.  A carboxy-lyase may only decarboxylate.

    MetaNetX ships no direction, so an EC 4.1.1 reaction inserted reversibly gives the
    model a carboxylase that fixes CO2 for free.  One such insertion here, the reverse of
    `co2_c + dhap_c = h_c + ohpb_c`, raises growth from 0.982 to 1.102 on glucose alone.
    Physiological CO2 does not drive a decarboxylase backwards, so the reverse direction
    closes.
    """
    lo, hi = float(bounds[0]), float(bounds[1])
    ecs = str(classifs or "")
    if "4.1.1." not in ecs:
        return lo, hi
    co2 = [c for m, c in mets.items() if m.id == "co2_c"]
    if not co2:
        return lo, hi
    if co2[0] > 0:
        return max(lo, 0.0), hi
    return lo, min(hi, 0.0)


def _rebalance(model, mets: dict):
    """Balance a bridged reaction on the frame, adjusting protons only.

    MetaNetX and BiGG protonate differently, so a reaction MetaNetX calls balanced can
    arrive on the frame carrying spare protons.  `fadh2_c + 2 h_c + oaa_c = fad_c +
    mal__L_c` is one of the seven this insertion set produced, and each one alone raises
    growth from 0.982 to 1.384 by acting as a proton sink.  Correcting the proton
    coefficient is the standard fix and is the only correction allowed here.  Anything
    still unbalanced after it is a bridge that mapped the wrong metabolite.

    Returns (metabolites, outcome) where outcome is "balanced", "protonated",
    "no_formula" or "unbalanced_on_frame".
    """
    import cobra
    probe = cobra.Reaction("_probe")
    probe.add_metabolites(dict(mets))
    try:
        imb = probe.check_mass_balance()
    except Exception:
        return mets, "no_formula"
    if not imb:
        return mets, "balanced"
    if set(imb) - {"charge", "H"}:
        return mets, "unbalanced_on_frame"
    d_h = imb.get("H", 0.0)
    d_q = imb.get("charge", 0.0)
    if abs(d_h - d_q) > 1e-9 or "h_c" not in model.metabolites:
        return mets, "unbalanced_on_frame"
    h = model.metabolites.get_by_id("h_c")
    out = dict(mets)
    out[h] = out.get(h, 0.0) - d_h
    if out[h] == 0.0:
        out.pop(h)
    probe2 = cobra.Reaction("_probe2")
    probe2.add_metabolites(out)
    try:
        if probe2.check_mass_balance():
            return mets, "unbalanced_on_frame"
    except Exception:
        return mets, "no_formula"
    if len([m for m in out if m.id not in FREE]) < 2:
        return mets, "unbalanced_on_frame"
    return out, "protonated"


def _stoich_key(mets: dict) -> frozenset:
    """A reaction's identity up to scale and direction, for duplicate detection."""
    items = sorted((m.id, v) for m, v in mets.items())
    if not items:
        return frozenset()
    scale = abs(items[0][1])
    fwd = frozenset((k, round(v / scale, 6)) for k, v in items)
    rev = frozenset((k, round(-v / scale, 6)) for k, v in items)
    return min(fwd, rev, key=lambda s: sorted(s))


def frame_stoich_keys(model) -> set:
    """Stoichiometry keys for every reaction the frame already carries."""
    return {_stoich_key({m: c for m, c in r.metabolites.items()})
            for r in model.reactions}


def insert(model, mnxrs, bridge, *, bounds=(-BIG, BIG), prefix=HET_PREFIX,
           skip_duplicates: bool = True):
    """Insert `mnxrs` into `model`.  Returns (inserted, rejected, census).

    `inserted` maps MNXR to the new reaction id.  `rejected` maps MNXR to one of
    REJECTIONS.  `census` counts both.  The model is modified in place.
    """
    import cobra

    mnxrs = sorted(set(str(m) for m in mnxrs))
    rp = _reac_prop(set(mnxrs))
    seen = frame_stoich_keys(model) if skip_duplicates else set()
    existing = {r.id for r in model.reactions}

    inserted, rejected, new = {}, {}, []
    protonated = 0
    oneway = 0
    for mnxr in mnxrs:
        info = rp.get(mnxr)
        if info is None:
            rejected[mnxr] = "no_stoichiometry"
            continue
        if str(info.get("is_transport") or "").strip() == "T":
            rejected[mnxr] = "transport"
            continue
        if str(info.get("is_balanced") or "").strip() != "B":
            rejected[mnxr] = "unbalanced"
            continue
        stoich = _parse(info["equation"])
        if not stoich:
            rejected[mnxr] = "unparsable"
            continue

        mets, orphan = {}, False
        for mnxm, coef in stoich.items():
            met, _tier = bridge.get(mnxm, create=False)
            if met is None:
                orphan = True
                break
            mets[met] = mets.get(met, 0.0) + coef
        if orphan:
            rejected[mnxr] = "orphan_metabolite"
            continue
        if len(mets) < len(stoich):
            rejected[mnxr] = "collapsed"
            continue
        mets = {k: v for k, v in mets.items() if v != 0.0}
        if not mets:
            rejected[mnxr] = "degenerate"
            continue
        if len([m for m in mets if m.id not in FREE]) < 2:
            rejected[mnxr] = "trivial"
            continue

        mets, fix = _rebalance(model, mets)
        if fix == "no_formula":
            rejected[mnxr] = "no_formula"
            continue
        if fix == "unbalanced_on_frame":
            rejected[mnxr] = "unbalanced_on_frame"
            continue
        protonated += (fix == "protonated")

        key = _stoich_key(mets)
        if skip_duplicates and key in seen:
            rejected[mnxr] = "already_present"
            continue

        rid = prefix + mnxr
        if rid in existing:
            rejected[mnxr] = "already_present"
            continue
        lo, hi = _direction(info.get("classifs"), mets, bounds)
        if (lo, hi) != tuple(float(b) for b in bounds):
            oneway += 1
        r = cobra.Reaction(rid, name=f"MetaNetX {mnxr}",
                           lower_bound=lo, upper_bound=hi)
        r.add_metabolites(mets)
        new.append(r)
        inserted[mnxr] = rid
        existing.add(rid)
        seen.add(key)

    if new:
        model.add_reactions(new)

    census = {"requested": len(mnxrs), "inserted": len(inserted),
              "proton_corrected": protonated,
              "decarboxylase_oneway": oneway}
    for why in REJECTIONS:
        census[why] = sum(1 for v in rejected.values() if v == why)
    return inserted, rejected, census


DISSIPATION = {
    "EGC_atp": {"atp_c": -1.0, "h2o_c": -1.0, "adp_c": 1.0, "pi_c": 1.0, "h_c": 1.0},
    "EGC_nadh": {"nadh_c": -1.0, "nad_c": 1.0, "h_c": 1.0},
    "EGC_nadph": {"nadph_c": -1.0, "nadp_c": 1.0, "h_c": 1.0},
    "EGC_fadh2": {"fadh2_c": -1.0, "fad_c": 1.0, "h_c": 2.0},
    "EGC_q8h2": {"q8h2_c": -1.0, "q8_c": 1.0, "h_c": 2.0},
    "EGC_proton": {"h_p": -1.0, "h_c": 1.0},
}


def _seal(model):
    """Close every route by which mass enters the model."""
    for r in model.exchanges:
        r.lower_bound = 0.0
    for r in model.reactions:
        if r.id.startswith(("DM_", "SK_", "EGC_")):
            r.lower_bound = 0.0
        if r.id == "ATPM":
            r.lower_bound = 0.0


def energy_cycles(model, tol: float = 1e-6) -> dict:
    """Maximise each cofactor dissipation reaction on a sealed model.

    A non-zero value means the network makes that cofactor from nothing, which is a
    thermodynamically infeasible cycle rather than metabolism.  Returns the ones that fire.
    """
    import cobra
    out = {}
    with model:
        _seal(model)
        for name, stoich in DISSIPATION.items():
            mets = {}
            for mid, coef in stoich.items():
                if mid not in model.metabolites:
                    mets = None
                    break
                mets[model.metabolites.get_by_id(mid)] = coef
            if mets is None:
                continue
            r = cobra.Reaction(name, lower_bound=0.0, upper_bound=BIG)
            r.add_metabolites(mets)
            model.add_reactions([r])
            model.objective = name
            v = model.slim_optimize()
            if v is not None and v == v and v > tol:
                out[name] = float(v)
    return out


def close_energy_cycles(model, reaction_ids, tol: float = 1e-6,
                        max_rounds: int = 500) -> dict:
    """Constrain inserted reactions until no cofactor is made from an empty medium.

    Solves the sealed dissipation problem, finds which of `reaction_ids` carries flux, and
    closes the direction the worst offender used.  Repeats until the cycle is gone.  Only
    the named reactions are touched, so the curated frame keeps every bound it shipped with.

    Returns reaction id -> the direction closed, one of "forward" or "reverse".
    """
    import cobra
    ids = [r for r in reaction_ids if r in model.reactions]
    closed = {}
    for _ in range(max_rounds):
        live = energy_cycles(model, tol)
        if not live:
            break
        name = max(live, key=live.get)
        with model:
            _seal(model)
            stoich = DISSIPATION[name]
            mets = {model.metabolites.get_by_id(k): v for k, v in stoich.items()}
            r = cobra.Reaction(name, lower_bound=0.0, upper_bound=BIG)
            r.add_metabolites(mets)
            model.add_reactions([r])
            model.objective = name
            sol = model.optimize()
            if sol.status != "optimal":
                break
            hot = [(abs(sol.fluxes[i]), i, sol.fluxes[i]) for i in ids
                   if abs(sol.fluxes.get(i, 0.0)) > tol]
        if not hot:
            break
        _mag, rid, flux = max(hot)
        rxn = model.reactions.get_by_id(rid)
        if flux > 0:
            rxn.upper_bound = 0.0
            closed[rid] = "forward"
        else:
            rxn.lower_bound = 0.0
            closed[rid] = "reverse"
    return closed


def unbounded_targets(model, reaction_ids, tol: float = 1e-6) -> dict:
    """Maximise each id alone and report the ones that reach BIG.

    A reaction that optimises to the solver's own ceiling is on a loop, not on a pathway.
    Run this after every insertion pass.
    """
    out = {}
    for rid in reaction_ids:
        if rid not in model.reactions:
            continue
        with model:
            model.objective = rid
            v = model.slim_optimize()
        if v is not None and v == v and v >= BIG - tol:
            out[rid] = float(v)
    return out


def bridge_for(model):
    """A `MetBridge` over `model`, wired to this arm's MetaNetX caches."""
    FS, _LB = C.laser_bridge()
    return FS.MetBridge(model, C.chem_prop())


def _reac_prop(needed: set) -> dict:
    """`load_reac_prop` plus the `classifs` column, which `_direction` reads."""
    import pandas as pd
    rp = pd.read_csv(C.REAC_PROP, sep="\t", comment="#", header=None,
                     names=["mnxr", "equation", "reference", "classifs",
                            "is_balanced", "is_transport"], dtype=str,
                     low_memory=False)
    rp = rp[rp.mnxr.isin(needed)]
    return {r.mnxr: dict(equation=r.equation, classifs=r.classifs,
                         is_balanced=r.is_balanced, is_transport=r.is_transport)
            for r in rp.itertuples(index=False)}


def _parse(eq: str):
    FS, _LB = C.laser_bridge()
    return FS.parse_mnx_equation(eq)


if __name__ == "__main__":
    import json

    model = C.load_model()
    native = set(C.model_mnxr_map().values())
    want = C.host_denovo_mnxr(min_lanes=2) - native
    print(f"frame {len(model.reactions)} reactions, "
          f"{len(native)} native MNXR, {len(want)} lane-asserted MNXR the GEM lacks")

    print("growth before:", model.slim_optimize())
    mb = bridge_for(model)
    ins, rej, census = insert(model, want, mb)
    print(json.dumps(census, indent=2))
    print("bridge tiers:", json.dumps(mb.tiers))
    print(f"frame now {len(model.reactions)} reactions")
    print("growth after insertion:", model.slim_optimize())
    print("energy cycles:", json.dumps(energy_cycles(model)))

    closed = close_energy_cycles(model, list(ins.values()))
    print(f"closed {len(closed)} directions:", json.dumps(closed))
    print("energy cycles now:", json.dumps(energy_cycles(model)))
    print("growth after closure:", model.slim_optimize())
