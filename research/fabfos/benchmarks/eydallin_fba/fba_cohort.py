#!/usr/bin/env python3
"""Maximum glycogen for every Eydallin clone against its host, per channel and medium.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin_fba/fba_cohort.py \\
        --channel gem --medium kornberg

Each clone is one chromosomal ORF on a high-copy plasmid in AG1.  A GPR table says which
reactions that ORF's product catalyses and says nothing about stoichiometry or capacity, so
turning "this gene is overexpressed" into an FBA edit needs a convention.  Three run here,
because no one of them is right and they fail in different directions.

  add    Put the clone's reactions into the host model if the host lacks them, opened both
         ways, taking them from the curated frame when it has them and from MetaNetX when
         it does not.  This is the only convention that reads the GPR as evidence about *presence*
         rather than about capacity, and it is the only one the de-novo channel needs: the
         carve removes reactions the clone then asserts.  It is vacuous on the GEM channel,
         where the host already carries every reaction the curated GPR names.

  relax  Cap every reaction at max(|v_ref|, VMIN), then multiply the clone's caps by FOLD.
         An LP optimum is non-decreasing in every bound, so this can only predict up or
         flat.  It is the monotone control and it is reported as one.

  force  Require the clone's reactions to carry FOLD x max(|v_ref|, VMIN) in whichever
         direction the reference ran them.  "Twice the enzyme, and it is running."  This one
         is signed and it can go infeasible.

The reference state is the host at the growth pin with glycogen maximised, taken through
pFBA, so v_ref means "what a glycogen-maximising cell at this growth rate would run".
`relax` compares against a capped host rather than the plain one, because the capping is
itself a restriction and the clone must be scored against the same restriction.

**CAUTION** The pin is a fraction of each model's own maximum growth rate, not an absolute
rate.  The de-novo hosts grow far slower than the curated one, and an absolute pin would
compare them at different fractions of their capacity.

**CAUTION** An added reaction can complete an energy-generating cycle that the host build
had closed.  Every `add` re-runs the cycle check, and a clone whose cycle cannot be closed
is recorded unanswered rather than scored.

**CAUTION** A solve that goes infeasible is an unanswered condition.  It is never a
prediction of "down".
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402
import kornberg as K  # noqa: E402
import mnx_insert as MI  # noqa: E402

FOLD = 2.0
VMIN = 0.1
DEGENERATE_ABS = 1e-6
PINS = (0.9, 0.5, 0.1)
ARMS = ("add", "relax", "force")


def _biomass(model):
    from cobra.util.solver import linear_reaction_coefficients
    return list(linear_reaction_coefficients(model))[0]


def reference_state(model, medium, pin):
    """(biomass reaction, gmax, pinned glycogen optimum, pFBA fluxes) under `medium`."""
    from cobra.flux_analysis import pfba
    bio = _biomass(model)
    model.medium = {k: v for k, v in medium.items() if k in model.reactions}
    gmax = model.slim_optimize()
    if gmax is None or gmax != gmax:
        return bio, 0.0, None, {}
    with model:
        bio.lower_bound = pin * gmax
        model.objective = model.reactions.get_by_id(C.DEMAND)
        try:
            sol = pfba(model)
        except Exception:
            return bio, float(gmax), None, {}
    return bio, float(gmax), float(sol.fluxes[C.DEMAND]), dict(sol.fluxes)


def _solve(model, bio, gmax, pin):
    with model:
        bio.lower_bound = pin * gmax
        model.objective = model.reactions.get_by_id(C.DEMAND)
        v = model.slim_optimize()
    return None if v is None or v != v else float(v)


def _capped(model, vref, scale=None):
    scale = scale or {}
    for r in model.reactions:
        if r.id == C.DEMAND or r.boundary:
            continue
        cap = max(abs(vref.get(r.id, 0.0)), VMIN) * scale.get(r.id, 1.0)
        r.lower_bound = max(r.lower_bound, -cap)
        r.upper_bound = min(r.upper_bound, cap)


def _forced(model, vref, rxn_ids):
    for rid in rxn_ids:
        r = model.reactions.get_by_id(rid)
        t = FOLD * max(abs(vref.get(rid, 0.0)), VMIN)
        if vref.get(rid, 0.0) >= 0:
            r.lower_bound = min(t, r.upper_bound)
        else:
            r.upper_bound = max(-t, r.lower_bound)


class Clones:
    """What each clone asserts, resolved onto one host model.

    `present` are reaction ids the host already carries.  `absent` are the assertions the
    host does not, which only the `add` arm uses.
    """

    def __init__(self, model, channel: str, min_lanes: int):
        self.model = model
        self.channel = channel
        self.frame = C.load_model()
        if channel == "gem":
            self.asserted = C.clone_gem_rxns()
        else:
            self.asserted = C.clone_denovo_mnxr(min_lanes)
            self.by_mnxr, self.frame_by_mnxr = {}, {}
            for rid, mnxr in C.model_mnxr_map().items():
                self.frame_by_mnxr.setdefault(mnxr, rid)
                if rid in model.reactions:
                    self.by_mnxr.setdefault(mnxr, rid)
            for r in model.reactions:
                if r.id.startswith(MI.HET_PREFIX):
                    self.by_mnxr.setdefault(r.id[len(MI.HET_PREFIX):], r.id)
            self.bridge = MI.bridge_for(model)

    def resolve(self, key: str):
        items = self.asserted.get(key, [])
        if self.channel == "gem":
            present = [x for x in items if x in self.model.reactions]
            absent = [x for x in items if x not in self.model.reactions
                      and x in self.frame.reactions]
        else:
            present = [self.by_mnxr[x] for x in items if x in self.by_mnxr]
            absent = [x for x in items if x not in self.by_mnxr]
        return sorted(set(present)), sorted(set(absent))

    def insert(self, absent):
        """Put the absent assertions into the model.  Call inside `with model:`."""
        if self.channel == "gem":
            new = [self.frame.reactions.get_by_id(r).copy() for r in absent]
            self.model.add_reactions(new)
            return [r.id for r in new]
        # An assertion the carve removed exists in the curated frame already, balanced and
        # in BiGG form.  Take it from there and go to MetaNetX only for what the frame
        # never had.  Skipping this step leaves the arm with nothing to add: of 424 absent
        # assertions across 50 clones, MetaNetX admits none -- 250 are unbalanced, 75 span
        # compartments, 58 reach a metabolite the frame does not hold.
        from_frame = [self.frame_by_mnxr[x] for x in absent if x in self.frame_by_mnxr]
        new = [self.frame.reactions.get_by_id(r).copy() for r in sorted(set(from_frame))
               if r not in self.model.reactions]
        if new:
            self.model.add_reactions(new)
        rest = [x for x in absent if x not in self.frame_by_mnxr]
        got, _rej, _cen = MI.insert(self.model, rest, self.bridge)
        return [r.id for r in new] + list(got.values())


def run_channel(channel: str, min_lanes: int, media: dict, pins=PINS) -> pd.DataFrame:
    import cobra

    path = (C.OUT / "models" /
            ("ag1_gem.json" if channel == "gem" else f"ag1_denovo_l{min_lanes}.json"))
    model = cobra.io.load_json_model(str(path))
    clones = Clones(model, channel, min_lanes)
    meas = C.measured().set_index("_k")

    rows = []
    for medium_name, medium in media.items():
        for pin in pins:
            bio, gmax, host_g, vref = reference_state(model, medium, pin)
            if host_g is None:
                continue
            with model:
                _capped(model, vref)
                host_relax = _solve(model, bio, gmax, pin)

            for key, row in meas.iterrows():
                base = dict(channel=channel, min_lanes=min_lanes, medium=medium_name,
                            pin=pin, condition_id=row.condition_id, gene=row.gene,
                            measured_pct_wt=row.pct_wt)
                present, absent = clones.resolve(key)
                base.update(n_asserted=len(present) + len(absent),
                            n_in_host=len(present), n_absent=len(absent))
                if not present and not absent:
                    rows += [dict(base, arm=arm, status="no_gpr") for arm in ARMS]
                    continue
                for arm in ARMS:
                    rows.append(_one(model, clones, bio, gmax, pin, vref, host_g,
                                     host_relax, present, absent, arm, base))
    return pd.DataFrame(rows)


def _one(model, clones, bio, gmax, pin, vref, host_g, host_relax,
         present, absent, arm, base):
    rec = dict(base, arm=arm)
    if arm == "add":
        rec["host_glycogen"] = host_g
        if not absent:
            rec["status"] = "nothing_to_add"
            return rec
        with model:
            added = clones.insert(absent)
            if not added:
                rec["status"] = "insertion_refused"
                return rec
            MI.close_energy_cycles(model, added)
            if MI.energy_cycles(model):
                rec["status"] = "energy_cycle"
                return rec
            v = _solve(model, bio, gmax, pin)
        rec["n_added"] = len(added)
    elif arm == "relax":
        rec["host_glycogen"] = host_relax
        if not present:
            rec["status"] = "no_reaction_in_host"
            return rec
        with model:
            _capped(model, vref, scale={rid: FOLD for rid in present})
            v = _solve(model, bio, gmax, pin)
    else:
        rec["host_glycogen"] = host_g
        if not present:
            rec["status"] = "no_reaction_in_host"
            return rec
        with model:
            _forced(model, vref, present)
            v = _solve(model, bio, gmax, pin)

    rec["clone_glycogen"] = v
    h = rec["host_glycogen"]
    if v is None:
        rec["status"] = "infeasible"
    elif h is None or abs(h) < 1e-12:
        rec["status"] = "host_zero"
    else:
        rec["delta"] = v - h
        rec["pct_diff"] = 100.0 * (v - h) / h
        rec["status"] = "ok" if abs(v - h) > DEGENERATE_ABS else "degenerate"
    return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", choices=("gem", "denovo", "both"), default="both")
    ap.add_argument("--medium", choices=("kornberg", "m9", "both"), default="both")
    ap.add_argument("--min-lanes", type=int, default=1)
    ap.add_argument("--out", type=Path, default=C.OUT / "cohort.tsv")
    a = ap.parse_args()

    import cobra
    probe = cobra.io.load_json_model(str(C.OUT / "models" / "ag1_gem.json"))
    formulas = K.formula_by_bigg(C.chem_prop(), C.bigg_bridge())
    kb, _absent = K.kornberg(probe, formulas)
    media = {"kornberg": kb, "m9": K.m9(probe)}
    if a.medium != "both":
        media = {a.medium: media[a.medium]}

    channels = ("gem", "denovo") if a.channel == "both" else (a.channel,)
    frames = [run_channel(ch, a.min_lanes, media) for ch in channels]
    out = pd.concat(frames, ignore_index=True)
    out.to_csv(a.out, sep="\t", index=False)

    print(f"{len(out)} rows -> {a.out}")
    print(out.groupby(["channel", "arm", "status"]).size().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
