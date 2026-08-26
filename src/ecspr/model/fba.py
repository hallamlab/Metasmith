# The flux balance arm: the same two tables, asked a different question.
#
# A condition (`ecspr.model.conditions`) resolves to a weighted set of GPR rows. The
# conductance arm turns that into edge weights on an atom graph. This arm turns it into a
# CARVE of a stoichiometric frame -- a curated genome-scale model -- and then solves an LP
# on what survives. Both arms read the same conditions table, so a construction measured
# one way and the other is the same construction rather than two hand-written approximations
# of one.
#
# WHAT A CONDITION CAN AND CANNOT SAY TO AN LP, stated plainly because the column names
# invite the wrong reading:
#
#   `factor` IS EVIDENCE IN NATS AND IT IS NOT A FLUX BOUND. Doubling a gene doubles the
#   belief that the cell can run the reaction. It says nothing about throughput, and
#   raising a bound in an FBA is a bit-exact no-op whenever the bound was not the binding
#   constraint -- which it almost never is. So dosage reaches the LP through exactly one
#   route: `carve_threshold`, where more belief can put a reaction INTO the model. It never
#   moves a bound, and this arm does not pretend otherwise.
#
#   `min_lanes` IS A LANE COUNT AND READS NO WEIGHT AT ALL. It admits a reaction when at
#   least that many distinct annotation channels assert it. It is the threshold the
#   benchmark shipped with, and it is deliberately blind to calibration -- which is why an
#   evidence-weighting change scored through `min_lanes` alone comes back as a null.
#   `carve_threshold` is the knob that sees one.
#
# **CAUTION** `min_lanes` DEFAULTS TO 1 AND THAT IS DELIBERATE. Two lanes is the documented
# fix for the ProtBERT lane's promiscuity and is right for a question about annotation
# quality in general. It is wrong for a question about a specific pathway: no two lanes
# agree on any reaction of the E. coli glycogen pathway, so a two-lane carve deletes the
# phenotype under study and only gapfill puts it back.
#
# **WARNING** THE CARVE IS DEEP AND BIOMASS GOES TO ZERO. At two lanes it keeps 1,260 of
# 2,755 reactions. Gapfill therefore does real work, and its count is a result rather than
# a detail. It draws only on reactions the carve itself removed, so a gapfilled reaction is
# a statement about what the annotation missed and not about what some other organism can
# do.
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from . import mnx
from .conditions import resolve
from .gpr import UNIT_COL, weights_from_rows
from .probes import load_corpus

#: Mirrors `ecspr.model.probes.RESULT_COLUMNS` so the two arms are read the same way.
#: `objective` names the reaction that was maximised; a `_`-prefixed readout is a
#: diagnostic, carried in the same table so a result cannot be read without it.
RESULT_COLUMNS = ("test_id", "objective", "readout", "value")

MEDIA_COLUMNS = ("media", "exchange", "lower_bound", "upper_bound")

DEMAND_PREFIX = "DM_"

#: What a gapfill asks for: this fraction of the untouched frame's own growth rate under
#: the same medium, plus a token flux to the target so a deleted pathway comes back as a
#: capability without being handed a capacity.
GAPFILL_GROWTH_FRAC = 0.1
GAPFILL_TARGET_FLOOR = 0.01

#: A carve this shallow gets its reactions named in the census. A curated GPR table
#: carving one reaction away is a strain edit, and the caller checks it by name.
CARVE_NAMED_LIMIT = 20


# --------------------------------------------------------------------------- frames


def load_frame(path):
    """A curated genome-scale model, by extension. cobra is imported by the caller."""
    import cobra
    p = Path(path)
    if p.suffix == ".json":
        return cobra.io.load_json_model(str(p))
    if p.suffix in (".xml", ".sbml"):
        return cobra.io.read_sbml_model(str(p))
    if p.suffix == ".mat":
        return cobra.io.load_matlab_model(str(p))
    raise ValueError(f"{p} is not a model this reads (.json, .xml, .sbml, .mat)")


def frame_index(paths) -> dict:
    """`{name: path}` for the frames staged, keyed by BASENAME AND by STEM.

    A `gem_tests` table's `frame` column names one of these. Both spellings resolve
    because a hand-written table says `iECDH1ME8569_1439` and a staged file is
    `iECDH1ME8569_1439.json`, and refusing one of them is a typo hunt rather than a
    contract.
    """
    out = {}
    for p in paths or ():
        p = Path(p)
        for key in (p.name, p.stem):
            if key in out and out[key] != p:
                raise ValueError(f"two staged frames are both called {key!r}")
            out[key] = p
    return out


# --------------------------------------------------------------------------- media


def load_media(path) -> dict:
    """`{media name: {exchange id: (lower_bound, upper_bound)}}`."""
    p = Path(path)
    df = (pd.read_parquet(p) if p.suffix == ".parquet"
          else pd.read_csv(p, sep="\t" if p.suffix in (".tsv", ".txt") else ","))
    missing = set(MEDIA_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"media table {p} has no {sorted(missing)} column(s); "
                         f"the schema is {list(MEDIA_COLUMNS)}")
    out: dict = {}
    for row in df.to_dict("records"):
        out.setdefault(str(row["media"]), {})[str(row["exchange"])] = (
            float(row["lower_bound"]), float(row["upper_bound"]))
    return out


def apply_medium(model, bounds: dict) -> int:
    """Set the model's exchange bounds to this medium. Returns how many it did not have.

    EVERY OTHER EXCHANGE CLOSES for uptake. A medium that only opened what it named would
    be the frame's shipped medium plus the named compounds, which is a different
    experiment from the one the table describes.
    """
    for r in model.exchanges:
        r.lower_bound = 0.0
    missing = 0
    for rid, (lo, hi) in bounds.items():
        if rid not in model.reactions:
            missing += 1
            continue
        r = model.reactions.get_by_id(rid)
        r.lower_bound, r.upper_bound = lo, hi
    return missing


def write_media(rows, path):
    df = pd.DataFrame(list(rows), columns=list(MEDIA_COLUMNS))
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".parquet":
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, sep="\t", index=False)
    return p


# --------------------------------------------------------------------- the carve


#: Where a frame keeps its own MetaNetX id. Every BiGG-derived model carries it, and it is
#: the FALLBACK rather than the source -- see `crosswalk`.
MNXR_ANNOTATION = "metanetx.reaction"


def crosswalk(model, corpus: dict | None = None, *, log=None) -> tuple:
    """`({frame reaction id: mnxr}, census)`, from the corpus first and the frame second.

    **WARNING** AN MNXR IS ONLY MEANINGFUL INSIDE ONE MNXref RELEASE, and a BiGG model's
    `metanetx.reaction` annotation is whichever release BiGG used. Measured on
    `iECDH1ME8569_1439` against this project's pinned MetaNetX 4.5: the two crosswalks
    agree on 1,520 reactions and DISAGREE ON 1,231 of 2,751. Carving with the frame's own
    annotation keeps 1,095 reactions where the pinned-release crosswalk keeps 1,720, and
    nothing raises -- the model is simply wrong and smaller.

    So a curated GPR table for this frame is the source: it was built against the same
    release the assertions are in. The frame's annotation fills in what no table covers,
    and the census counts how far the two disagree, which is the measurement that says
    whether the frame is in your release at all.

    **CAUTION** The corpus cannot supply this on its own either. A de-novo table's
    `intermediate_id` is a KO or an EC number, so it maps nothing onto a frame; a
    de-novo-only corpus yields an empty crosswalk, and `carve` refuses rather than
    returning the 530 ungoverned reactions as though they were a deep carve.
    """
    ids = {r.id for r in model.reactions}
    out = {}
    for df in (corpus or {}).values():
        if not {"intermediate_id", "mnxr"} <= set(df.columns):
            continue
        g = df[["intermediate_id", "mnxr"]].dropna()
        for iid, mnxr in zip(g.intermediate_id.astype(str), g.mnxr.astype(str)):
            if iid in ids:
                out.setdefault(iid, mnxr)
    n_corpus = len(out)

    disagree = filled = 0
    for r in model.reactions:
        v = (r.annotation or {}).get(MNXR_ANNOTATION)
        if isinstance(v, (list, tuple)):
            v = v[0] if v else None
        if not v:
            continue
        if r.id not in out:
            out[r.id] = str(v)
            filled += 1
        elif out[r.id] != str(v):
            disagree += 1

    census = dict(crosswalk=len(out), crosswalk_from_corpus=n_corpus,
                  crosswalk_from_frame=filled, crosswalk_disagree=disagree)
    if log and filled > n_corpus:
        log(f"[fba] {filled} of {len(out)} crosswalk entries come from the frame's own "
            f"metanetx.reaction annotation, which is whichever MNXref release the frame "
            f"was published against. Stage a curated GPR table for this frame if the "
            f"carve looks too deep")
    return out, census


@dataclass
class Assertions:
    """What one condition claims the cell can do, in both id spaces."""
    ids: set = field(default_factory=set)
    mnxrs: set = field(default_factory=set)
    census: dict = field(default_factory=dict)

    @property
    def key(self) -> tuple:
        """WHAT ACTUALLY DECIDES THE MODEL, so two conditions that carve identically
        share one build.

        The condition id does not decide it. On the Eydallin cohort every clone is one
        chromosomal *E. coli* ORF the host already carries, so 86 perturbed conditions
        produce one assertion set -- and keying a build on the condition would run 86
        identical carves, insertions and gapfills, which is hours rather than minutes.
        """
        return (frozenset(self.ids), frozenset(self.mnxrs))


def assertions(condition, corpus: dict, *, min_lanes: int = 1, carve_threshold=None,
               weighting: str = "belief") -> Assertions:
    """The reactions a condition asserts, after both admission thresholds.

    `min_lanes` counts distinct channels. `carve_threshold` is a floor on the pooled
    belief `E_r`, which is the ONLY path by which a calibrated `llr` or a condition's
    dosage `factor` reaches the model at all.
    """
    rows = resolve(condition, corpus)
    if rows.empty:
        return Assertions(census=dict(n_rows=0, n_units=0, n_mnxr=0, n_ids=0))

    rows = rows[rows["mnxr"].notna()].copy()
    rows["mnxr"] = rows["mnxr"].astype(str)
    lanes = rows.groupby("mnxr")["channel"].nunique()
    passing = set(lanes[lanes >= int(min_lanes)].index)

    n_before_belief = len(passing)
    if carve_threshold is not None:
        E = weights_from_rows(rows, weighting)
        passing = {r for r in passing if float(E.get(r, 0.0)) >= float(carve_threshold)}

    keep = rows[rows["mnxr"].isin(passing)]
    ids = (set(keep["intermediate_id"].dropna().astype(str))
           if "intermediate_id" in keep.columns else set())
    return Assertions(
        ids=ids, mnxrs=passing,
        census=dict(n_rows=int(len(rows)),
                    n_units=int(rows[UNIT_COL].nunique())
                    if UNIT_COL in rows.columns else 0,
                    n_mnxr=len(passing), n_ids=len(ids),
                    n_mnxr_before_belief=n_before_belief,
                    n_mnxr_all_lanes=int(lanes.size)))


def carve(model, a: Assertions, xwalk: dict) -> tuple:
    """Remove every reaction the condition does not support. Returns `(kept, dropped)`.

    A reaction survives when its own id is asserted, or when the frame's crosswalk maps it
    to an asserted MNXR, or when it carries no gene rule at all. THE LAST CLAUSE IS NOT A
    CONVENIENCE: exchanges, demands, biomass, maintenance and the spontaneous reactions
    have no gene, so no annotation lane can speak to them and dropping them would be a
    claim the evidence never made.
    """
    ungoverned = {r.id for r in model.reactions if not r.gene_reaction_rule.strip()}
    keep = ungoverned | {r.id for r in model.reactions
                         if r.id in a.ids or xwalk.get(r.id) in a.mnxrs}
    if a.mnxrs and keep == ungoverned:
        # A CARVE THAT KEEPS NOTHING GOVERNED IS A BROKEN CROSSWALK, not a deep carve.
        # It happened: a de-novo-only corpus maps KO ids onto no frame reaction at all,
        # and the model that came out was 530 reactions of exchanges and biomass which
        # gapfill then repaired back to something plausible-looking.
        raise ValueError(
            f"the condition asserts {len(a.mnxrs)} reaction(s) and not one of them "
            f"reaches this frame, so every governed reaction was carved away. The "
            f"frame's crosswalk holds {len(xwalk)} entries -- check that it annotates "
            f"{MNXR_ANNOTATION!r}, or stage a curated GPR table for it")
    drop = sorted({r.id for r in model.reactions} - keep)
    return sorted(keep), drop


# --------------------------------------------------------------------- the build


def _donor(model, drop):
    import cobra
    u = cobra.Model("carved_away")
    u.add_reactions([model.reactions.get_by_id(r).copy() for r in drop])
    return u


def _biomass(model):
    from cobra.util.solver import linear_reaction_coefficients
    got = list(linear_reaction_coefficients(model))
    return got[0] if got else None


def demand_for(model, metabolite_id: str):
    """The demand reaction the target flux is read on. Idempotent."""
    rid = DEMAND_PREFIX + metabolite_id
    if rid in model.reactions:
        return model.reactions.get_by_id(rid)
    if metabolite_id not in model.metabolites:
        return None
    met = model.metabolites.get_by_id(metabolite_id)
    return model.add_boundary(met, type="demand", reaction_id=rid)


def _growth(model) -> float:
    v = model.slim_optimize()
    return float(v) if v is not None and v == v else 0.0


def gapfill(model, donor, medium: dict, *, target=None, frame_path=None,
            frac=GAPFILL_GROWTH_FRAC, floor=GAPFILL_TARGET_FLOOR) -> dict:
    """Restore feasibility by an L1-minimal draw on what the carve removed.

    Exact gapfill is a MILP and GLPK is the only solver these environments carry, so the
    minimum-cardinality formulation does not finish on a 1,495-reaction donor. This is the
    standard relaxation: put every donor reaction back, require growth and the target,
    then minimise total absolute flux through the donor reactions alone. What still
    carries flux is what the carve should not have taken. The draw is small rather than
    provably minimal.

    **CAUTION** The work happens on `model` itself, never on a copy. Solving on a copy and
    transplanting the solution's support does not reproduce it -- the transplant came back
    at zero growth while the copy it came from was feasible. Keeping the model that solved
    the problem removes that whole class of mismatch.
    """
    bio = _biomass(model)
    if bio is None:
        return {"needed": None, "reactions": [], "status": "no_biomass"}

    target_flux = 0.0
    if frame_path is not None:
        ref = load_frame(frame_path)
        with ref:
            apply_medium(ref, medium)
            target_flux = frac * _growth(ref)

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

    dm = demand_for(model, target) if target else None
    old_bio_lb = bio.lower_bound
    old_dm_lb = dm.lower_bound if dm is not None else None
    apply_medium(model, medium)
    bio.lower_bound = target_flux
    if dm is not None:
        dm.lower_bound = floor
    model.objective = model.problem.Objective(sum(obj), direction="min")
    sol = model.optimize()

    bio.lower_bound = old_bio_lb
    if dm is not None:
        dm.lower_bound = old_dm_lb
    model.remove_cons_vars(aux)
    model.objective = keep_obj
    model.objective.direction = "max"

    if sol.status != "optimal":
        model.remove_reactions(added, remove_orphans=False)
        return {"needed": None, "reactions": [], "status": "infeasible",
                "target_growth": round(target_flux, 6)}

    used = sorted(rid for rid in added if abs(sol.fluxes.get(rid, 0.0)) > 1e-12)
    model.remove_reactions([r for r in added if r not in set(used)],
                           remove_orphans=False)
    return {"needed": len(used), "reactions": used, "status": "ok",
            "target_growth": round(target_flux, 6), "method": "l1_relaxation"}


def build(frame_path, condition, corpus: dict, medium: dict, *, min_lanes=1,
          carve_threshold=None, weighting="belief", target=None, release=None,
          do_gapfill=True, log=None) -> tuple:
    """A condition plus a frame -> a carved, repaired cobra model and its census."""
    a = assertions(condition, corpus, min_lanes=min_lanes,
                   carve_threshold=carve_threshold, weighting=weighting)
    return build_from(frame_path, a, None, medium, target=target, release=release,
                      do_gapfill=do_gapfill, log=log, label=condition.condition_id,
                      corpus=corpus)


def build_from(frame_path, a: Assertions, xwalk: dict | None, medium: dict, *,
               target=None, release=None, do_gapfill=True, log=None, label="",
               corpus=None) -> tuple:
    """An assertion set plus a frame -> a carved, repaired cobra model and its census.

    `medium` IS THE MEDIUM THE MODEL IS BUILT AND REPORTED UNDER, which need not be the
    one a later test measures in. The gapfill happens in it, so it decides what the model
    IS.

    The order is load, carve, insert, close cycles, gapfill, CLOSE THE CYCLES AGAIN.
    The second closure is not belt and braces: gapfill hands the inserted reactions the
    partners the carve had taken away, so a cycle shut before it can reopen. At two lanes
    exactly one does, and leaving it open puts the de-novo host above the curated one, at
    1.367 per hour against 0.982.
    """
    model = load_frame(frame_path)
    xcensus = {}
    if xwalk is None:
        xwalk, xcensus = crosswalk(model, corpus, log=log)
    keep, drop = carve(model, a, xwalk)

    census = dict(a.census, **xcensus)
    census.update(frame_reactions=len(keep) + len(drop), kept=len(keep),
                  carved_away=len(drop),
                  ungoverned=sum(1 for r in model.reactions
                                 if not r.gene_reaction_rule.strip()))
    # A shallow carve is a strain edit and the caller checks it by name. A deep one is a
    # thousand ids that belong in the model rather than in a census.
    if len(drop) <= CARVE_NAMED_LIMIT:
        census["carved_away_ids"] = list(drop)
    donor = _donor(model, drop)
    model.remove_reactions(drop, remove_orphans=False)
    if target:
        demand_for(model, target)

    inserted = {}
    if release is not None:
        native = set(xwalk.values())
        want = sorted(a.mnxrs - native)
        bridge = release.bridge(model)
        inserted, _rejected, ins_census = mnx.insert(model, want, bridge, release)
        census["insert"] = ins_census
        census["closed_after_insert"] = len(
            mnx.close_energy_cycles(model, list(inserted.values())))
    else:
        census["insert"] = {"requested": 0, "inserted": 0, "skipped": "no MetaNetX release"}
        census["closed_after_insert"] = 0

    with model:
        census["media_missing"] = apply_medium(model, medium)
        census["growth_carved"] = round(_growth(model), 6)

    if do_gapfill:
        census["gapfill"] = gapfill(model, donor, medium, target=target,
                                    frame_path=frame_path)
        with model:
            apply_medium(model, medium)
            census["growth_after_gapfill"] = round(_growth(model), 6)
        # **CAUTION** THE SECOND CLOSURE CAN UNDO THE REPAIR. Gapfill verifies
        # feasibility and the closure then removes a direction the repair was using.
        # Measured on the de-novo host gapfilled under Kornberg: four directions close
        # and growth goes from 0.063 back to zero, where the same host gapfilled under M9
        # loses two and keeps 0.246. Both numbers are in the census, because a gapfill
        # that reports `ok` beside a `growth_final` of zero is the only way to see it.
        census["closed_after_gapfill"] = len(
            mnx.close_energy_cycles(model, list(inserted.values())))
    else:
        census["gapfill"] = {"needed": 0, "reactions": [], "status": "skipped"}
        census["growth_after_gapfill"] = None
        census["closed_after_gapfill"] = 0

    with model:
        apply_medium(model, medium)
        census["growth_final"] = round(_growth(model), 6)
    census["energy_cycles_open"] = len(mnx.energy_cycles(model))
    if log:
        log(f"[fba] {label}: kept {len(keep)}, carved {len(drop)}, "
            f"inserted {len(inserted)}, gapfilled "
            f"{census['gapfill'].get('needed')}, growth {census['growth_final']}")
    return model, census


# --------------------------------------------------------------------- the solve


def _rows(tid, objective, pairs) -> list:
    return [dict(test_id=tid, objective=objective, readout=k,
                 value=(float(v) if v is not None else None)) for k, v in pairs]


def _flatten(census: dict, prefix="_") -> list:
    out = []
    for k, v in census.items():
        if isinstance(v, dict):
            out += _flatten(v, prefix=f"{prefix}{k}_")
            continue
        if isinstance(v, bool):
            out.append((f"{prefix}{k}", float(v)))
        elif isinstance(v, (int, float)):
            out.append((f"{prefix}{k}", float(v)))
    return out


def measure(model, t, medium: dict, census: dict, *, emit="summary") -> list:
    """One flux balance test on one built model.

    Maximise growth under the medium, pin it at `growth_pin` of that maximum, then
    maximise flux to the target. The pin is a FRACTION of each model's own maximum and not
    an absolute rate: a carved model grows far slower than the frame it came from, and an
    absolute pin would compare the two at different fractions of their capacity.
    """
    objective_id = str(t.settings.get("objective") or "")
    target = str(t.settings.get("target_hub") or "")
    pin = float(t.settings.get("growth_pin") or 0.0)

    diag = _flatten(census)
    diag += [("_n_reactions", len(model.reactions)),
             ("_n_metabolites", len(model.metabolites)),
             ("_n_media_missing", census.get("media_missing", 0))]

    out = []
    with model:
        apply_medium(model, medium)
        if objective_id:
            if objective_id not in model.reactions:
                diag.append(("_abstained", 1))
                return _rows(t.test_id, objective_id, out + diag)
            model.objective = objective_id
        bio = _biomass(model)
        gmax = _growth(model)
        out.append(("growth", gmax))
        obj_name = bio.id if bio is not None else objective_id

        if not target:
            diag.append(("_abstained", 0))
            return _rows(t.test_id, obj_name, out + diag)

        dm = demand_for(model, target)
        if dm is None:
            diag += [("_abstained", 1), ("_missing_target", 1)]
            return _rows(t.test_id, obj_name, out + diag)
        diag.append(("_missing_target", 0))
        if gmax <= 0.0:
            diag += [("_abstained", 1), ("_host_zero", 1)]
            return _rows(t.test_id, obj_name, out + diag)

        if bio is not None:
            bio.lower_bound = max(bio.lower_bound, pin * gmax)
        model.objective = dm.id
        sol = model.optimize()
        feasible = sol.status == "optimal"
        out.append(("target", float(sol.objective_value) if feasible else None))
        diag += [("_feasible", int(feasible)), ("_abstained", 0),
                 ("_growth_pin", pin * gmax)]

        if feasible and emit in ("fluxes", "reduced-costs"):
            # ONE LP, NOT ONE PER PERTURBATION. Reduced costs read off the baseline solve
            # rank the whole reaction set at once, which is the cheapest thing in this arm
            # that carries magnitude rather than direction.
            series = sol.fluxes if emit == "fluxes" else sol.reduced_costs
            tag = "flux:" if emit == "fluxes" else "rc:"
            out += [(f"{tag}{rid}", float(v)) for rid, v in series.items()
                    if abs(float(v)) > 1e-12]

    return _rows(t.test_id, obj_name, out + diag)


def run(frames, gpr_paths, conditions, tests, media, *, release=None, weighting="belief",
        emit="summary", do_gapfill=True, gapfill_media=None, census_out=None,
        log=print) -> pd.DataFrame:
    """Every gem test, on the condition it names and the frame it names.

    A BUILD IS KEYED ON THE ASSERTION SET, not on the condition. Two conditions that carve
    the frame identically get one model, and on the Eydallin cohort that is 87 conditions
    to one build per channel -- each clone is a chromosomal *E. coli* ORF the host already
    carries, so the perturbation changes the network and not the carve. A carve plus an
    insertion plus a gapfill is minutes, so this is the difference between minutes and
    hours. The per-condition counts still come from the condition, so a shared build never
    hides which rows went into it.

    `gapfill_media` NAMES ONE MEDIUM FOR EVERY REPAIR, and giving it makes the built model
    a claim about the organism rather than about the broth. Without it each medium gets
    its own gapfill and therefore its own model, so a comparison across media compares two
    different models -- which is defensible for asking what a medium costs, and wrong for
    asking what a perturbation does.
    """
    corpus = load_corpus(gpr_paths)
    by_id = {c.condition_id: c for c in conditions}
    missing = sorted({t.network for t in tests if t.network not in by_id})
    if missing:
        raise ValueError(f"{len(missing)} test(s) name networks with no condition: "
                         f"{missing[:5]}. Known: {sorted(by_id)[:5]}")
    index = frame_index(frames)

    xwalks: dict = {}
    built: dict = {}
    asserted: dict = {}
    census_log: dict = {}
    rows = []
    for i, t in enumerate(tests):
        frame_name = str(t.settings.get("frame") or "")
        if frame_name not in index:
            raise ValueError(f"test {t.test_id!r} names frame {frame_name!r}, which was "
                             f"not staged (have {sorted(index)})")
        media_name = str(t.settings.get("media") or "")
        if media_name and media_name not in media:
            raise ValueError(f"test {t.test_id!r} names medium {media_name!r}, which the "
                             f"media table does not carry (has {sorted(media)})")
        medium = media.get(media_name, {})
        min_lanes = int(t.settings.get("min_lanes") or 1)
        threshold = t.settings.get("carve_threshold")
        threshold = None if threshold in (None, "", 0) else float(threshold)
        target = str(t.settings.get("target_hub") or "")

        akey = (t.network, min_lanes, threshold)
        if akey not in asserted:
            asserted[akey] = assertions(by_id[t.network], corpus, min_lanes=min_lanes,
                                        carve_threshold=threshold, weighting=weighting)
        a = asserted[akey]

        if gapfill_media is not None and gapfill_media not in media:
            raise ValueError(f"--gapfill-media names {gapfill_media!r}, which the media "
                             f"table does not carry (has {sorted(media)})")
        repair = media[gapfill_media] if gapfill_media is not None else None

        # The medium is part of the build key only when each medium gets its own repair.
        key = (frame_name, target, a.key,
               media_name if gapfill_media is None else gapfill_media)
        if key not in built:
            if frame_name not in xwalks:
                xwalks[frame_name] = crosswalk(load_frame(index[frame_name]), corpus,
                                               log=log)
            xwalk, xcensus = xwalks[frame_name]
            model, census = build_from(index[frame_name], a, xwalk,
                                       repair if repair is not None else medium,
                                       target=target, release=release,
                                       do_gapfill=do_gapfill, log=log, label=t.network)
            census.update(xcensus, build_media=gapfill_media or media_name)
            built[key] = (model, census)
            census_log[f"{frame_name}|{gapfill_media or media_name}|"
                       f"{t.network}"] = _jsonable(census)
        model, census = built[key]
        rows += measure(model, t, medium, dict(census, **a.census), emit=emit)
        if i == 0 or (i + 1) % 10 == 0 or i + 1 == len(tests):
            log(f"[fba] {i + 1}/{len(tests)} tests ({len(built)} model(s) built)")

    if census_out:
        Path(census_out).parent.mkdir(parents=True, exist_ok=True)
        Path(census_out).write_text(json.dumps(census_log, indent=2))
    if not rows:
        return pd.DataFrame(columns=list(RESULT_COLUMNS))
    return pd.DataFrame(rows)[list(RESULT_COLUMNS)]


def _jsonable(v):
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (int, float, str, bool)) or v is None:
        return v
    return str(v)


def write_results(df: pd.DataFrame, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".parquet":
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, sep="\t", index=False)
    return p


def read_results(path) -> pd.DataFrame:
    p = Path(path)
    return (pd.read_parquet(p) if p.suffix == ".parquet"
            else pd.read_csv(p, sep="\t", float_precision="round_trip"))
