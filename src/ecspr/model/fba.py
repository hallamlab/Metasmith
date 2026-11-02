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

import hashlib
import json
from itertools import groupby
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from . import mnx
from .conditions import resolve
from .gpr import UNIT_COL, weights_from_rows
from .probes import load_corpus

#: Mirrors `ecspr.model.probes.RESULT_COLUMNS` so the two arms are read the same way.
#: `objective` names the reaction that was maximised, `target` the metabolite the flux was
#: taken to, and a `_`-prefixed readout is a diagnostic, carried in the same table so a
#: result cannot be read without it. `(objective, target, readout)` is what
#: `scoring.KEYS["gem"]` identifies a measurement by -- see `_rows` for why the target has
#: to be in the table rather than only in the tests file.
RESULT_COLUMNS = ("test_id", "objective", "target", "readout", "value")

MEDIA_COLUMNS = ("media", "exchange", "lower_bound", "upper_bound")

DEMAND_PREFIX = "DM_"

#: What a gapfill asks for: this fraction of the untouched frame's own growth rate under
#: the same medium, plus a token flux to the target so a deleted pathway comes back as a
#: capability without being handed a capacity.
GAPFILL_GROWTH_FRAC = 0.1
GAPFILL_TARGET_FLOOR = 0.01

#: WHAT COUNTS AS NOT GROWING. An LP does not return an exact zero: a lethal deletion comes
#: back at 3e-15, which is zero, and comparing it to `0.0` lets it through. The growth pin is
#: a FRACTION of each model's own maximum, so a pin of 0.1 x 3e-15 constrains nothing and the
#: target is then maximised unpinned -- which made two dead Eydallin mutants the largest
#: movers in their cohort, larger than the two genes that genuinely carry the pathway.
GROWTH_TOL = 1e-9

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
    _key: str = field(default="", repr=False, compare=False)

    @property
    def key(self) -> str:
        """WHAT ACTUALLY DECIDES THE MODEL, so two conditions that carve identically
        share one build.

        The condition id does not decide it. On the Eydallin cohort every clone is one
        chromosomal *E. coli* ORF the host already carries, so 86 perturbed conditions
        produce one assertion set -- and keying a build on the condition would run 86
        identical carves, insertions and gapfills, which is hours rather than minutes.

        A DIGEST AND NOT THE SETS THEMSELVES, and it is cached. This key goes into a
        build key, a difference key and a sort key, and a run holds one per test: on the
        Fuhrer cohort's 64,719 tests, returning a fresh pair of frozensets over ~5,000
        strings each time cost 35 GB and the process was killed. Sixteen bytes per test
        costs nothing.

        `blake2b` and not `hash()`: `hash()` is salted per interpreter, so the same two
        conditions would share a build in one process and not in the next.
        """
        if not self._key:
            h = hashlib.blake2b(digest_size=16)
            for part in (self.ids, self.mnxrs):
                h.update(b"\x00")
                for v in sorted(str(x) for x in part):
                    h.update(v.encode())
                    h.update(b"\x01")
            self._key = h.hexdigest()
        return self._key


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


@dataclass
class Baseline:
    """A built model, plus everything a PERTURBATION of it needs to move it by hand.

    The four extras are what make `apply_delta` possible without a second build:
    `xwalk` says which frame reaction is which MNXR, `donor` holds copies of what the
    carve took away so a native reaction can come back, `inserted` says where each
    heterologous MNXR landed, and `a` is the assertion set the whole thing was carved to.
    """
    model: object
    census: dict
    a: Assertions
    xwalk: dict
    donor: object
    inserted: dict = field(default_factory=dict)

    def mnxr_of(self, rid: str):
        """The MNXR a reaction of this model stands for, native or inserted."""
        if not hasattr(self, "_by_rid"):
            object.__setattr__(self, "_by_rid",
                               {v: k for k, v in (self.inserted or {}).items()})
        return self.xwalk.get(rid) or self._by_rid.get(rid)

    def asserts(self, rid: str, a: Assertions) -> bool:
        if rid in a.ids:
            return True
        m = self.mnxr_of(rid)
        return bool(m) and m in a.mnxrs


def build(frame_path, condition, corpus: dict, medium: dict, *, min_lanes=1,
          carve_threshold=None, weighting="belief", target=None, release=None,
          do_gapfill=True, log=None) -> tuple:
    """A condition plus a frame -> a carved, repaired cobra model and its census."""
    a = assertions(condition, corpus, min_lanes=min_lanes,
                   carve_threshold=carve_threshold, weighting=weighting)
    return build_from(frame_path, a, None, medium, target=target, release=release,
                      do_gapfill=do_gapfill, log=log, label=condition.condition_id,
                      corpus=corpus)


def build_from(frame_path, a: Assertions, xwalk: dict | None, medium: dict, **kw) -> tuple:
    """`(model, census)` -- `build_baseline` for callers that want only the two."""
    b = build_baseline(frame_path, a, xwalk, medium, **kw)
    return b.model, b.census


def build_baseline(frame_path, a: Assertions, xwalk: dict | None, medium: dict, *,
                   target=None, release=None, do_gapfill=True, log=None, label="",
                   corpus=None) -> Baseline:
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
    return Baseline(model=model, census=census, a=a, xwalk=xwalk, donor=donor,
                    inserted=dict(inserted))


@contextmanager
def perturbed(baseline: Baseline, a: Assertions, *, release=None, target=None,
              log=None, label=""):
    """`(model, census)` for a perturbed condition, as a context.

    A CONTEXT BECAUSE A DELETION NEED NOT COPY THE MODEL. Removing a reaction and pinning
    it to zero flux are the same linear program -- the column is there and can carry
    nothing -- and cobra unwinds a bound change when the context closes. Measured on
    iML1515: a `model.copy()` is 0.373 s and a knockout inside a context plus a solve is
    0.006 s. Over the Fuhrer cohort's 3,806 deletions on eight axes that is three hours of
    copying against three minutes, which decides whether the arm is a coffee break or a
    cluster job.

    So a withdrawal-only difference moves bounds on the baseline itself and puts them
    back; anything that adds a reaction copies, because a structure change cannot be
    unwound by a bound. The census says which route was taken, because `_n_reactions`
    means different things on the two: on the bounds route the withdrawn reactions are
    still counted and are carrying nothing.
    """
    withdraw, back, want = delta_sets(baseline, a)
    if not (withdraw or back or want):
        census = dict(baseline.census, **a.census)
        census["delta"] = dict(withdrawn=0, reintroduced=0, inserted=0, requested=0,
                               closed_after_insert=0, unmet=0, empty=1)
        yield baseline.model, census
        return
    if withdraw and not (back or want):
        census = dict(baseline.census, **a.census)
        baseline_final = census.pop("growth_final", None)
        for k in ("growth_carved", "growth_after_gapfill", "carved_away_ids"):
            census.pop(k, None)
        census["delta"] = dict(withdrawn=len(withdraw), reintroduced=0, inserted=0,
                               requested=0, closed_after_insert=0, unmet=0, empty=0,
                               by_bounds=1, baseline_growth_final=baseline_final)
        census["gapfill"] = dict(baseline.census.get("gapfill", {}), inherited=1)
        with baseline.model:
            for rid in withdraw:
                baseline.model.reactions.get_by_id(rid).knock_out()
            if log:
                log(f"[fba] {label}: delta -{len(withdraw)} by bounds on the baseline")
            yield baseline.model, census
        return
    yield apply_delta(baseline, a, release=release, target=target, log=log, label=label)


def delta_sets(baseline: Baseline, a: Assertions) -> tuple:
    """`(withdraw, reintroduce, insert)` -- the three moves, as id and MNXR lists."""
    base = baseline.a
    ref = baseline.model
    present = {r.id for r in ref.reactions}
    native = set(baseline.xwalk.values())
    withdraw = [r.id for r in ref.reactions
                if r.gene_reaction_rule.strip()
                and baseline.asserts(r.id, base) and not baseline.asserts(r.id, a)]
    back = [r.id for r in baseline.donor.reactions
            if r.id not in present and baseline.asserts(r.id, a)
            and not baseline.asserts(r.id, base)]
    want = sorted(a.mnxrs - base.mnxrs - native - set(baseline.inserted))
    return withdraw, back, want


def apply_delta(baseline: Baseline, a: Assertions, *, release=None, target=None,
                log=None, label="") -> tuple:
    """A perturbed condition as a DIFFERENCE on the baseline model. `(model, census)`.

    A PERTURBATION IS NOT A REBUILD. Rebuilding was both slow and wrong, and the same
    change fixes the two:

      WRONG, because `build_from` hands its gapfill a donor made of exactly the
      reactions the carve removed. On a loss-of-function cohort the deleted reaction is
      therefore the first thing the repair draws back, and the model reports a plausible
      growth rate for a strain that no longer has the gene. Here the repair belongs to
      the BASELINE and is not re-run, so a deletion stays deleted.

      SLOW, because keying a build on the assertion set gives every deletion its own
      load, carve, insertion, cycle closure and gapfill -- 3,806 of them on the Fuhrer
      cohort. A difference is one model copy and a handful of reaction edits.

    THREE MOVES, and each one has to be there:

      withdraw     a governed reaction the baseline asserted and this condition does not.
                   Gapfilled reactions are asserted by NEITHER, so they are left alone --
                   they are part of what the baseline IS.
      reintroduce  a reaction the baseline's carve took away that this condition asserts.
                   This is the carve re-evaluated, not a repair: it draws only on
                   `baseline.donor`, which is the frame's own reactions.
      insert       an MNXR this condition asserts that the frame never named. Same
                   admission rules as the build, and the cycles are closed afterwards --
                   adding reactions to an already-repaired model can reopen one that the
                   build shut, which is the lesson `build_baseline`'s double closure
                   already encodes.
    """
    base = baseline.a
    ref = baseline.model
    withdraw, back, want = delta_sets(baseline, a)

    if not (withdraw or back or want):
        # AN EMPTY DIFFERENCE IS THE COMMON CASE AND IT MUST COST NOTHING. Two conditions
        # can carry different assertion sets and still carve one frame identically:
        # `Assertions.key` counts every `intermediate_id`, and a de-novo table's are KO
        # and EC numbers that reach no frame reaction at all. On the Eydallin cohort that
        # is the whole story -- its thirteen "distinct models" were one carve, one repair
        # and one growth rate, separated only by a KO string per clone.
        census = dict(baseline.census, **a.census)
        census["delta"] = dict(withdrawn=0, reintroduced=0, inserted=0, requested=0,
                               closed_after_insert=0, unmet=0, empty=1)
        return ref, census

    model = ref.copy()
    if withdraw:
        model.remove_reactions(withdraw, remove_orphans=False)
    if back:
        model.add_reactions([baseline.donor.reactions.get_by_id(r).copy() for r in back])

    inserted, closed = {}, 0
    if want and release is not None:
        inserted, _rejected, ins_census = mnx.insert(model, want, release.bridge(model),
                                                     release)
        closed = len(mnx.close_energy_cycles(model, list(inserted.values())))
    if target:
        demand_for(model, target)

    census = dict(baseline.census, **a.census)
    # THE BASELINE'S GROWTH NUMBERS DO NOT DESCRIBE THIS MODEL and carrying them would
    # put the baseline's rate in every perturbed row under a name that reads like the
    # row's own. `measure` reports this model's growth under the test's own medium.
    baseline_final = census.pop("growth_final", None)
    for k in ("growth_carved", "growth_after_gapfill", "carved_away_ids"):
        census.pop(k, None)
    census["delta"] = dict(withdrawn=len(withdraw), reintroduced=len(back),
                           inserted=len(inserted), requested=len(want),
                           closed_after_insert=closed,
                           unmet=len(want) - len(inserted), empty=0,
                           baseline_growth_final=baseline_final)
    census["gapfill"] = dict(baseline.census.get("gapfill", {}), inherited=1)
    if log:
        log(f"[fba] {label}: delta -{len(withdraw)} +{len(back)} native "
            f"+{len(inserted)} inserted on the baseline build")
    return model, census


# --------------------------------------------------------------------- the solve


def _rows(tid, objective, target, pairs) -> list:
    """The long form: one row per readout, carrying what identifies the measurement.

    `target` IS PART OF THE IDENTITY, not a label. A cohort measured on eight axes takes
    every one of them on the same objective and the same `target` readout name, so a
    scorer keyed on `(objective, readout)` alone pools eight chemically different fluxes
    into one null -- and their magnitudes differ by more than an order of magnitude, so
    the pooled spread is whichever axis moves most and every other axis is scored against
    it. The column is blank for a test that names no target, and for every diagnostic.
    """
    return [dict(test_id=tid, objective=objective, target=target, readout=k,
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
                return _rows(t.test_id, objective_id, target, out + diag)
            model.objective = objective_id
        bio = _biomass(model)
        gmax = _growth(model)
        out.append(("growth", gmax))
        obj_name = bio.id if bio is not None else objective_id

        if not target:
            diag.append(("_abstained", 0))
            return _rows(t.test_id, obj_name, target, out + diag)

        dm = demand_for(model, target)
        if dm is None:
            diag += [("_abstained", 1), ("_missing_target", 1)]
            return _rows(t.test_id, obj_name, target, out + diag)
        diag.append(("_missing_target", 0))
        if gmax <= GROWTH_TOL:
            diag += [("_abstained", 1), ("_host_zero", 1)]
            return _rows(t.test_id, obj_name, target, out + diag)

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

    return _rows(t.test_id, obj_name, target, out + diag)


def _root_test(tid: str, by_test: dict) -> str:
    """The test a test is ultimately read against; itself when it is a baseline."""
    seen, cur = [], tid
    while True:
        nxt = (by_test[cur].read_against or "").strip()
        if not nxt or nxt == cur:
            return cur
        if nxt not in by_test:
            raise ValueError(f"test {cur!r} reads against {nxt!r}, which is not a test "
                             f"in this table")
        if nxt in seen:
            raise ValueError("`read_against` forms a cycle: "
                             + " -> ".join(seen + [nxt]))
        seen.append(cur)
        cur = nxt


def _plan(t, media, index, gapfill_media, do_gapfill=True):
    """What one test needs: its frame, medium, thresholds and assertion-set key.

    THE TARGET IS PART OF THE BUILD ONLY WHEN THERE IS A GAPFILL. It reaches the
    construction through exactly one door -- the repair's requirement that the target carry
    a token flux -- and `measure` adds the demand reaction itself. So with the repair off,
    every target shares one model, and a cohort measured on seventeen axes builds ONE
    baseline rather than seventeen and differences each mutant once rather than seventeen
    times.
    """
    frame_name = str(t.settings.get("frame") or "")
    if frame_name not in index:
        raise ValueError(f"test {t.test_id!r} names frame {frame_name!r}, which was "
                         f"not staged (have {sorted(index)})")
    media_name = str(t.settings.get("media") or "")
    if media_name and media_name not in media:
        raise ValueError(f"test {t.test_id!r} names medium {media_name!r}, which the "
                         f"media table does not carry (has {sorted(media)})")
    threshold = t.settings.get("carve_threshold")
    return dict(
        frame=frame_name, media=media_name, medium=media.get(media_name, {}),
        min_lanes=int(t.settings.get("min_lanes") or 1),
        threshold=None if threshold in (None, "", 0) else float(threshold),
        target=(str(t.settings.get("target_hub") or "") if do_gapfill else ""),
        build_media=gapfill_media if gapfill_media is not None else media_name)


def _shard_name(t, signature: str) -> str:
    """One test's shard. The signature carries every RUN-level setting that changes the
    answer, so a directory written under a different repair or a different differencing
    rule does not resume into this one and quietly mix two constructions."""
    h = hashlib.blake2b(f"{t.test_id}\x00{signature}".encode(),
                        digest_size=8).hexdigest()
    return f"{h}.json"


def run(frames, gpr_paths, conditions, tests, media, *, release=None, weighting="belief",
        emit="summary", do_gapfill=True, gapfill_media=None, delta=True, census_out=None,
        shard_dir=None, resume=True, part=None, of=None, log=print) -> pd.DataFrame:
    """Every gem test, on the condition it names and the frame it names.

    A PERTURBATION IS A DIFFERENCE ON ITS BASELINE, not a build of its own. `read_against`
    already names each test's baseline, so the baseline condition is built once -- carve,
    insertion, cycle closure, gapfill -- and every test reading against it is measured on
    a copy of that model with its own condition's reactions withdrawn, reintroduced or
    inserted. `apply_delta` says why that is the correct construction and not merely the
    cheap one; the short of it is that re-running the gapfill on a deletion draws the
    deleted reaction straight back.

    TWO CONDITIONS THAT CARVE IDENTICALLY STILL SHARE ONE MODEL. On the Eydallin cohort
    every clone is a chromosomal *E. coli* ORF the host already carries, so 86 perturbed
    conditions produce one assertion set and the difference is empty. The per-condition
    counts still come from the condition, so a shared model never hides which rows went
    into it.

    ONLY BASELINES ARE KEPT. A delta model is measured and dropped, because a cohort of
    3,806 deletions would otherwise hold 3,806 cobra models at once. Tests are therefore
    walked in an order grouped by (baseline, assertion set) so that a condition carrying
    several tests is differenced once; the rows come back in the table's own order.

    `gapfill_media` NAMES ONE MEDIUM FOR EVERY REPAIR, and giving it makes the built model
    a claim about the organism rather than about the broth. Without it each medium gets
    its own gapfill and therefore its own baseline, so a comparison across media compares
    two different models -- defensible for asking what a medium costs, and wrong for
    asking what a perturbation does.

    `delta=False` restores the older behaviour of building every distinct assertion set
    from the frame. It is a diagnostic: on a cohort whose perturbations change no
    assertion the two must agree bit for bit, and that is the check that says the
    difference is being applied correctly.

    `shard_dir` MAKES A KILLED RUN RESUMABLE, one JSON file per test, exactly as the
    conductance arm shards. `part`/`of` split the work for a cluster array, and THEY SPLIT
    BASELINES RATHER THAN TESTS: the unit of work is a test but the expensive object is a
    build, so a boundary drawn through a baseline would have every task rebuild it. Whole
    baseline groups go to one part. A test outside this part is not solved; if its shard
    is on disk it is read, which is what lets a final run with no `part` assemble every
    array task's output without solving anything.
    """
    corpus = load_corpus(gpr_paths)
    by_id = {c.condition_id: c for c in conditions}
    missing = sorted({t.network for t in tests if t.network not in by_id})
    if missing:
        raise ValueError(f"{len(missing)} test(s) name networks with no condition: "
                         f"{missing[:5]}. Known: {sorted(by_id)[:5]}")
    if gapfill_media is not None and gapfill_media not in media:
        raise ValueError(f"--gapfill-media names {gapfill_media!r}, which the media "
                         f"table does not carry (has {sorted(media)})")
    index = frame_index(frames)
    by_test = {t.test_id: t for t in tests}

    asserted: dict = {}

    def assertion_set(network, min_lanes, threshold):
        akey = (network, min_lanes, threshold)
        if akey not in asserted:
            asserted[akey] = assertions(by_id[network], corpus, min_lanes=min_lanes,
                                        carve_threshold=threshold, weighting=weighting)
        return asserted[akey]

    # ---- one pass to work out what each test is measured on, before anything is built.
    plans = []
    for t in tests:
        pl = _plan(t, media, index, gapfill_media, do_gapfill)
        pl["a"] = a = assertion_set(t.network, pl["min_lanes"], pl["threshold"])
        root = _root_test(t.test_id, by_test) if delta else t.test_id
        rt = by_test[root]
        rpl = pl if root == t.test_id else _plan(rt, media, index,
                                                 gapfill_media, do_gapfill)
        # A BASELINE ON DIFFERENT SETTINGS IS NOT THIS TEST'S BASELINE. Differencing
        # across a frame, a target, a threshold or a repair medium would silently measure
        # the perturbation plus the setting change, so such a test builds on its own.
        same = all(rpl[k] == pl[k] for k in
                   ("frame", "target", "min_lanes", "threshold", "build_media"))
        if not same:
            root, rt, rpl = t.test_id, t, pl
        pl["root"] = root
        pl["root_a"] = ra = (a if root == t.test_id
                             else assertion_set(rt.network, rpl["min_lanes"],
                                                rpl["threshold"]))
        pl["root_network"] = rt.network
        pl["repair_medium"] = (media[gapfill_media] if gapfill_media is not None
                               else rpl["medium"])
        pl["base_key"] = (rpl["frame"], rpl["target"], ra.key, rpl["build_media"])
        pl["delta_key"] = (pl["base_key"], a.key)
        plans.append(pl)

    # A STABLE GROUPING ORDER, and `hash()` is not one: it is salted per interpreter, so
    # an assertion set would sort differently in two processes and the difference count
    # would stop being reproducible. First appearance is the ordering instead.
    rank: dict = {}
    def _rank(k):
        return rank.setdefault(k, len(rank))
    order = sorted(range(len(tests)),
                   key=lambda i: (_rank(plans[i]["base_key"]), _rank(plans[i]["a"].key)))

    # THE UNIT A PART MUST NOT SPLIT IS THE DIFFERENCE, not the baseline. Every test of one
    # (baseline, assertion set) shares one perturbed model, so splitting there would build
    # it twice; a baseline, by contrast, is ONE build that a task can afford to repeat.
    # Partitioning on the baseline was right only while each target had its own -- with the
    # target out of the build key a whole cohort is one baseline group, and `--of` could
    # not split it at all.
    groups: dict = {}
    for i in order:
        groups.setdefault(plans[i]["delta_key"], []).append(i)
    mine = set(range(len(tests)))
    if of:
        if not (0 <= int(part) < int(of)):
            raise ValueError(f"--part {part} is not in range for --of {of}")
        # BALANCE ON TESTS, NOT ON GROUPS. Dealing whole groups round-robin assumes they
        # are the same size and they are not: 2,458 of fuhrer's 3,806 deletions name a gene
        # that carves the frame identically, so they collapse into ONE group holding
        # two thirds of the sweep. Strided over 32 parts that group went to task 0 alone,
        # which then ran for an hour while thirty-one tasks finished in a minute each.
        #
        # A contiguous cut by test position keeps every group whole except the one or two
        # that straddle a boundary, and those cost one extra difference in the neighbouring
        # part -- cheap beside the imbalance it removes.
        sized, pos = [], 0
        for k in groups:
            sized.append((k, pos, pos + len(groups[k])))
            pos += len(groups[k])
        lo = pos * int(part) // int(of)
        hi = pos * (int(part) + 1) // int(of)
        mine = {i for k, a, b in sized if a < hi and b > lo
                for n, i in enumerate(groups[k], start=a) if lo <= n < hi}
        split = sum(1 for _k, a, b in sized if a < lo < b or a < hi < b)
        log(f"[fba] part {part}/{of}: {len(mine)} of {len(tests)} test(s), "
            f"contiguous positions [{lo}, {hi}) over {len(groups)} difference group(s)"
            + (f"; {split} group(s) straddle a boundary and are built here too"
               if split else ""))

    shard_dir = Path(shard_dir) if shard_dir else None
    if shard_dir:
        shard_dir.mkdir(parents=True, exist_ok=True)
    # THE SHARD SIGNATURE CARRIES THE ROW SCHEMA. A shard is a cached measurement, and a
    # run that emits a column the cached rows do not have would resume straight over the
    # gap -- the run reports success and the assembled table is missing the column for
    # every resumed test. Naming the columns here retires those shards instead.
    signature = json.dumps(dict(weighting=weighting, emit=emit, do_gapfill=do_gapfill,
                                gapfill_media=gapfill_media, delta=delta,
                                columns=list(RESULT_COLUMNS)),
                           sort_keys=True)

    xwalks: dict = {}
    built: dict = {}
    census_log: dict = {}
    by_index: dict = {}
    n_delta = n_empty = n_resumed = n_skipped = 0
    todo = []
    for i in order:
        t = tests[i]
        shard = shard_dir / _shard_name(t, signature) if shard_dir else None
        if shard is not None and resume and shard.exists():
            by_index[i] = json.loads(shard.read_text())
            n_resumed += 1
            continue
        if i not in mine:
            n_skipped += 1
            continue
        todo.append(i)

    done = 0

    def _emit(model, census):
        """Measure every test of one group on the model that group is about."""
        nonlocal done
        for i in chunk:
            t = tests[i]
            by_index[i] = rows_i = measure(model, t, plans[i]["medium"],
                                           dict(census, **plans[i]["a"].census),
                                           emit=emit)
            if shard_dir is not None:
                (shard_dir / _shard_name(t, signature)).write_text(json.dumps(rows_i))
            done += 1
            if done == 1 or done % 10 == 0 or done == len(todo):
                log(f"[fba] {done}/{len(todo)} tests to solve "
                    f"({len(built)} baseline(s) built, {n_delta} difference(s) applied)")

    # ONE DIFFERENCE PER GROUP, not per test. `order` puts every test of one
    # (baseline, assertion set) together, so a condition carrying several growth pins or
    # several media is differenced once and measured that many times inside the one
    # perturbed model.
    for _dkey, group in groupby(todo, key=lambda i: plans[i]["delta_key"]):
        chunk = list(group)
        pl = plans[chunk[0]]
        if pl["base_key"] not in built:
            frame_name = pl["base_key"][0]
            if frame_name not in xwalks:
                xwalks[frame_name] = crosswalk(load_frame(index[frame_name]), corpus,
                                               log=log)
            xwalk, xcensus = xwalks[frame_name]
            b = build_baseline(index[frame_name], pl["root_a"], xwalk,
                               pl["repair_medium"], target=pl["base_key"][1],
                               release=release, do_gapfill=do_gapfill, log=log,
                               label=pl["root_network"])
            b.census.update(xcensus, build_media=pl["base_key"][3], builds=1)
            built[pl["base_key"]] = b
            census_log[f"{frame_name}|{pl['base_key'][3]}|{pl['root_network']}"] = \
                _jsonable(b.census)
        b = built[pl["base_key"]]

        if pl["a"].key == pl["root_a"].key:
            _emit(b.model, b.census)
            continue
        with perturbed(b, pl["a"], release=release, target=pl["base_key"][1],
                       log=None, label=tests[chunk[0]].network) as (model, census):
            n_delta += 1
            n_empty += int(bool(census.get("delta", {}).get("empty")))
            census_log[f"{pl['base_key'][0]}|{pl['base_key'][3]}|"
                       f"{tests[chunk[0]].network}"] = _jsonable(census)
            _emit(model, census)

    log(f"[fba] {len(built)} baseline build(s), {n_delta} difference(s), "
        f"{len(tests)} test(s)"
        + (f" ({n_empty} difference(s) empty: those conditions carve the frame "
           f"identically)" if n_empty else "")
        + (f", {n_resumed} resumed" if n_resumed else "")
        + (f", {n_skipped} not in this part" if n_skipped else ""))
    rows = [r for i in range(len(tests)) for r in by_index.get(i, ())]

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
