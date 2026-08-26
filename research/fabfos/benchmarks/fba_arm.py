"""One cohort -> the three tables `ecspr fba` reads. Shared by every flux balance arm.

WHY THIS IS SHARED AND NOT COPIED. Fuhrer, the Eydallin 2007 mutants and Keio are the same
experiment three times: a host GPR table, a table of ORFs each mutant deletes, and a set of
metabolites to measure the flux to. A second copy of that reader is a second answer, which
is the same reason `ecspr.model.fba` exists rather than a loader per arm.

A COHORT THAT ALREADY HAS A CONDITIONS TABLE PASSES IT IN, and that is the case worth
having: the SCADC fosmids were built through `ecspr conditions` for the conductance arm, so
the flux balance arm reads that very table and the two arms are measuring one construction
rather than two that were written to agree. `conditions_path` is how a cohort says so, and
`conditions()` below is only for a cohort that has none.

WHAT A COHORT STILL HAS TO SAY FOR ITSELF is its frame, its host table, its deletion table,
its medium and its sinks. Those are the `Cohort` fields, and they are the only thing an arm's
own `build_tables.py` should hold.

THE SUBTRACT GOES ON `orf`. Keying it on `mnxr` renormalises the surviving nominators -- by
as much as 41% on a fan-out of three -- and deletes an isozyme's contribution along with the
deleted gene's. `orf` is a nominator: subtracting one leaves another gene's row for a
reaction two genes nominate, which is what a single-gene knockout actually does.

A MUTANT WHOSE GENE CARRIES NO REACTION OF THE FRAME STILL GETS A CONDITION, and it comes
out identical to the baseline. That is the honest answer -- the model cannot represent that
deletion -- and it keeps the population the one the screen measured rather than the subset
the model happens to cover. Those mutants are marked `is_control`: a real perturbation that
cannot move the measurement is what a noise floor is made of.

DO NOT GAPFILL A DELETION COHORT'S BASELINE WITHOUT READING ITS CARVE FIRST. A gapfill draws
on exactly what the carve removed, so if the carve is reproducing the host strain's own
genotype -- BW25113 is daraBAD, dlacZ, drhaBAD against the MG1655 model iML1515 -- the repair
puts those deletions back. `probe_axes` prints the carved set for that reason.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


def repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = repo_root(Path(__file__).resolve())
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from ecspr.model import conditions as cond_mod                        # noqa: E402
from ecspr.model import fba as fba_mod                                # noqa: E402
from ecspr.model import mnx                                           # noqa: E402
from ecspr.model import tests as test_mod                             # noqa: E402

MNX_DIR = ROOT / "data/fabfos/originals/metanetx/4.5"
GROWTH_PIN = 0.1
GLC_UPTAKE = 10.0


@dataclass
class Cohort:
    """What one deletion screen has to say for itself."""
    name: str
    frame: Path
    host_gpr: Path
    #: `(condition_id, orf)` rows: which ORFs each mutant deletes. `action == "del"`.
    cohort_gpr: Path
    #: Every condition_id the screen measured, deletions with no frame reaction included.
    condition_ids: tuple
    #: `[(axis, role, sink_mnxm, sink_name)]` -- what the flux is measured to.
    sinks: tuple
    media: str = "m9"
    baseline_id: str = ""
    out: Path = field(default=Path("cache"))
    #: An existing arm-neutral conditions table to read instead of building one.
    conditions_path: Path = None
    #: Extra GPR tables a passed-in conditions table names, beyond `host_gpr`.
    extra_gpr: tuple = ()
    #: `[(leg, source_mnxm, sink_mnxm)]` -- the CONDUCTANCE arm's two-point legs over the
    #: same conditions. Given, `build` writes an `ecspr_tests` table beside the gem one,
    #: so `ecspr draw --like-tests <that> --like-gem-tests <the gem one>` emits one drawn
    #: pool that both arms measure. Two arms on two pools have adjacent error bars rather
    #: than comparable ones, and that is the whole point of the comparison.
    legs: tuple = ()
    element: str = "C"
    #: `(column, table)` -- how big a construction IS, counted in the units a null draws.
    #: Unset, a perturbation is one genome_unit and `n_units` is 1, which is right for a
    #: deletion screen. It is WRONG for a library: a fosmid is one unit carrying 9 to 81
    #: ORFs, so a null matched on unit count can only redraw the observed inserts. Set it
    #: to the column `ecspr draw --size-from` is given and the strata agree on both sides.
    size_column: tuple = ()

    def __post_init__(self):
        if not self.baseline_id:
            self.baseline_id = f"{self.name}:BASELINE"

    @property
    def host_table(self) -> str:
        return self.host_gpr.name


def conditions(c: Cohort) -> list:
    if c.conditions_path is not None:
        got = cond_mod.read(c.conditions_path)
        print(f"[conditions] {len(got):,} read from {c.conditions_path} -- the same "
              f"table the conductance arm measured")
        return got
    background = (cond_mod.Step(table=c.host_table, type="", set=(), operation="union"),)
    cohort = pd.read_parquet(c.cohort_gpr)
    dropped = (cohort.groupby("condition_id").orf
               .apply(lambda s: tuple(sorted(set(s.astype(str))))).to_dict())
    out = [cond_mod.Condition(c.baseline_id, background)]
    for cid in c.condition_ids:
        orfs = dropped.get(cid, ())
        steps = background + ((cond_mod.Step(table=c.host_table, type="orf", set=orfs,
                                             operation="subtract"),) if orfs else ())
        out.append(cond_mod.Condition(cid, steps))
    return out


def targets(c: Cohort) -> list:
    """`[(axis, role, frame metabolite id, tier)]`, resolved through the MetaNetX bridge.

    NOT BY NAME. A name grep has pinned the wrong metabolite in this project before, and
    the two arms have to be aimed at one molecule for their answers to be about one thing.
    """
    model = fba_mod.load_frame(c.frame)
    bridge = mnx.Release(MNX_DIR).bridge(model)
    out, absent = [], []
    for axis, role, mnxm, name in c.sinks:
        met, tier = bridge.get(mnxm, create=False)
        if met is None:
            absent.append(f"{axis} ({name}, {mnxm})")
            continue
        out.append((axis, role, met.id, tier))
    if absent:
        print(f"[axes] {len(absent)} declared sink(s) are NOT metabolites of this frame, "
              f"so flux balance cannot be asked about them at all: {absent}")
    return out


def sizes(c: Cohort, conds: list) -> dict:
    """`{condition_id: how many draw-units it perturbs}` -- the stratum both arms match on.

    A NULL AND AN OBSERVATION HAVE TO BE SIZED THE SAME WAY OR THE MATCH SILENTLY MISSES.
    `ecspr score` looks the observation's `n_units` up among the null's strata; where the
    null was drawn with `--size-from orf` and the observation still says 1, no stratum
    matches and `p_stratum` comes back NaN for every row.
    """
    if not c.size_column:
        return {}
    from ecspr.model.nulls import size_in
    column, table = c.size_column
    pool = pd.read_parquet(next(p for p in (c.cohort_gpr, *c.extra_gpr)
                                if Path(p).name == table))
    return {x.condition_id: size_in(pool, x, column, table) for x in conds}


def gem_tests(c: Cohort, conds: list, axis_targets: list) -> list:
    settings = dict(frame=Path(c.frame).stem, media=c.media, objective="",
                    growth_pin=GROWTH_PIN, min_lanes=1)
    n_of = sizes(c, conds)
    out = []
    for axis, _role, met, _tier in axis_targets:
        base_id = f"{axis}.{c.baseline_id}"
        out.append(test_mod.Test(
            test_id=base_id, network=c.baseline_id, arm="gem",
            settings=dict(settings, target_hub=met),
            meta=dict(cohort=c.name, is_control=1, stratum=0, n_units=0, draw_id=None)))
        for cond in conds:
            if cond.condition_id == c.baseline_id:
                continue
            out.append(test_mod.Test(
                test_id=f"{axis}.{cond.condition_id}", network=cond.condition_id,
                arm="gem", read_against=base_id,
                settings=dict(settings, target_hub=met),
                # `n_units` IS WHAT THE NULL IS SIZE-MATCHED ON, and the matched null for
                # a deletion screen is "delete one random gene". So it is 1 for every
                # mutant, and a mutant whose gene carries no reaction of this frame is a
                # real perturbation that cannot move the measurement -- a control.
                meta=dict(cohort=c.name, is_control=int(len(cond.steps) == 1),
                          stratum=n_of.get(cond.condition_id, 1),
                          n_units=n_of.get(cond.condition_id, 1), draw_id=None)))
    return out


def ecspr_tests(c: Cohort, conds: list) -> list:
    """The conductance arm's tests over the SAME conditions the gem tests read.

    One test per (leg, condition). `two-point` names its readout after the sink, so two
    legs of a ratio come back on distinct rows of `scoring.KEYS["ecspr"]` and are never
    pooled -- the quotient is taken downstream, from the two legs' own deltas.
    """
    n_of = sizes(c, conds)
    out = []
    for leg, src, snk in c.legs:
        base_id = f"{leg}.{c.baseline_id}"
        settings = dict(element=c.element, source_hub=src, sink_hub=snk)
        out.append(test_mod.Test(
            test_id=base_id, network=c.baseline_id, arm="ecspr", settings=dict(settings),
            meta=dict(cohort=c.name, is_control=1, stratum=0, n_units=0, draw_id=None)))
        for cond in conds:
            if cond.condition_id == c.baseline_id:
                continue
            out.append(test_mod.Test(
                test_id=f"{leg}.{cond.condition_id}", network=cond.condition_id,
                arm="ecspr", read_against=base_id, settings=dict(settings),
                meta=dict(cohort=c.name, is_control=int(len(cond.steps) == 1),
                          stratum=n_of.get(cond.condition_id, 1),
                          n_units=n_of.get(cond.condition_id, 1), draw_id=None)))
    return out


def media_rows(c: Cohort) -> list:
    """The frame's own minimal medium at a fixed glucose uptake."""
    model = fba_mod.load_frame(c.frame)
    med = dict(model.medium)
    med["EX_glc__D_e"] = GLC_UPTAKE
    return [dict(media=c.media, exchange=rid, lower_bound=-abs(v), upper_bound=1000.0)
            for rid, v in sorted(med.items())]


def build(c: Cohort) -> dict:
    """Write the three tables and report what went into them."""
    c.out.mkdir(parents=True, exist_ok=True)
    conds = conditions(c)
    perturbing = sum(1 for x in conds if len(x.steps) > 1)
    if c.conditions_path is None:
        print(f"[conditions] {len(conds):,} ({perturbing:,} remove at least one reaction "
              f"of this frame, {len(conds) - 1 - perturbing:,} are the baseline over "
              f"again because the gene carries none)")
    tgt = targets(c)
    print(f"[axes] {len(tgt)} of {len(c.sinks)} declared sinks are on this frame")
    ts = gem_tests(c, conds, tgt)
    rows = media_rows(c)
    cond_mod.write(conds, c.out / "conditions.parquet")
    test_mod.write(ts, c.out / "gem_tests.parquet", "gem")
    fba_mod.write_media(rows, c.out / "media.parquet")
    print(f"[tests] {len(ts):,} -> {c.out / 'gem_tests.parquet'}")
    if c.legs:
        es = ecspr_tests(c, conds)
        test_mod.write(es, c.out / "ecspr_tests.parquet", "ecspr")
        print(f"[tests] {len(es):,} conductance -> {c.out / 'ecspr_tests.parquet'} "
              f"({len(c.legs)} leg(s) on the same conditions)")
    print(f"[media] {len(rows)} bounds -> {c.out / 'media.parquet'}")
    return dict(conditions=len(conds), perturbing=perturbing, tests=len(ts),
                axes=len(tgt))


def probe(c: Cohort, *, do_gapfill=False) -> pd.DataFrame:
    """Can the unperturbed model carry flux to each sink at all?

    RUN THIS BEFORE THE SWEEP. A sweep over a sink the model cannot make returns a column
    of exact zeros and a flat delta for every mutant, which is indistinguishable in the
    output from a real null. The three cases are separated by column here rather than
    inferred downstream from a float.
    """
    corpus = fba_mod.load_corpus([c.host_gpr, *c.extra_gpr])
    conds = {x.condition_id: x for x in cond_mod.read(c.out / "conditions.parquet")}
    medium = fba_mod.load_media(c.out / "media.parquet")[c.media]
    a = fba_mod.assertions(conds[c.baseline_id], corpus)
    base = fba_mod.build_baseline(c.frame, a, None, medium, corpus=corpus,
                                  do_gapfill=do_gapfill, log=print, label=c.baseline_id)
    model = base.model
    bridge = mnx.Release(MNX_DIR).bridge(model)
    carved = base.census.get("carved_away_ids")
    print(f"[probe] the carve removed {base.census['carved_away']} reaction(s)"
          + (f": {carved}" if carved else "")
          + ". A gapfill draws on exactly these, so read them before repairing.")
    with model:
        fba_mod.apply_medium(model, medium)
        gmax = fba_mod._growth(model)
    print(f"[probe] unperturbed growth {gmax:.6f} /h on {c.media}")

    rows = []
    for axis, role, mnxm, name in c.sinks:
        met, tier = bridge.get(mnxm, create=False)
        row = dict(axis=axis, role=role, sink_mnxm=mnxm, sink_name=name,
                   metabolite="", bridge_tier=tier, growth=round(gmax, 6),
                   target_at_pin=None, target_unpinned=None, state="absent")
        if met is not None:
            row["metabolite"] = met.id
            for pinned in (True, False):
                with model:
                    fba_mod.apply_medium(model, medium)
                    dm = fba_mod.demand_for(model, met.id)
                    bio = fba_mod._biomass(model)
                    if pinned and bio is not None:
                        bio.lower_bound = max(bio.lower_bound, GROWTH_PIN * gmax)
                    model.objective = dm.id
                    sol = model.optimize()
                    v = float(sol.objective_value) if sol.status == "optimal" else 0.0
                row["target_at_pin" if pinned else "target_unpinned"] = round(v, 9)
            row["state"] = ("reachable" if (row["target_unpinned"] or 0) > 1e-9
                            else "unreachable")
        rows.append(row)
    df = pd.DataFrame(rows)
    out = c.out / "probe_axes.tsv"
    df.to_csv(out, sep="\t", index=False)
    print(df.to_string(index=False))
    print(f"\n[probe] {df.state.value_counts().to_dict()} -> {out}")
    bad = df[(df.role == "target") & (df.state != "reachable")]
    if len(bad):
        print(f"[probe] {len(bad)} TARGET axis/axes cannot be answered by flux balance "
              f"on this frame and medium: {sorted(bad.axis)}. A sweep over them returns "
              f"a column of zeros, which is not a null result.")
    return df
