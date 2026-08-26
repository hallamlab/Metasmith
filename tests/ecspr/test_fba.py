"""The flux balance arm: what a condition can say to an LP, and what it cannot."""
from __future__ import annotations

import pandas as pd
import pytest

cobra = pytest.importorskip("cobra")

from ecspr.model import conditions as C  # noqa: E402
from ecspr.model import fba as F  # noqa: E402
from ecspr.model import tests as T  # noqa: E402


def _frame():
    """glc_e -> glc_c -> bio_c -> biomass. Two governed steps and one ungoverned sink."""
    m = cobra.Model("toy")
    glc_e = cobra.Metabolite("glc_e", compartment="e")
    glc_c = cobra.Metabolite("glc_c", compartment="c")
    bio_c = cobra.Metabolite("bio_c", compartment="c")
    m.add_metabolites([glc_e, glc_c, bio_c])

    ex = cobra.Reaction("EX_glc_e", lower_bound=-10.0, upper_bound=1000.0)
    ex.add_metabolites({glc_e: -1.0})
    t = cobra.Reaction("GLCt", lower_bound=0.0, upper_bound=1000.0)
    t.add_metabolites({glc_e: -1.0, glc_c: 1.0})
    t.gene_reaction_rule = "gT"
    r1 = cobra.Reaction("R1", lower_bound=0.0, upper_bound=1000.0)
    r1.add_metabolites({glc_c: -1.0, bio_c: 1.0})
    r1.gene_reaction_rule = "g1"
    bm = cobra.Reaction("BIOMASS", lower_bound=0.0, upper_bound=1000.0)
    bm.add_metabolites({bio_c: -1.0})
    m.add_reactions([ex, t, r1, bm])
    m.objective = "BIOMASS"
    return m


@pytest.fixture
def frame(tmp_path):
    p = tmp_path / "toy.json"
    cobra.io.save_json_model(_frame(), str(p))
    return p


def _row(unit, orf, rid, mnxr, llr, channel="manual_gpr"):
    return dict(source=unit, orf=orf, gene=orf, channel=channel, mnxr=mnxr,
                intermediate_id=rid, intermediate_name=rid, llr=llr,
                llr_route="asserted", projection_via="x", evidence_quality="reviewed",
                lane_set="curated", build_id="b", host="h", genome_unit=unit,
                in_atom_universe=True)


# GLCt rests on one weakly-supported ORF, R1 on a strongly-supported one. That is the
# separation `carve_threshold` reads and `min_lanes` cannot see.
CORPUS = {"host.parquet": pd.DataFrame([
    _row("dh1", "bT", "GLCt", "MNXR1", 1.0),
    _row("dh1", "b1", "R1", "MNXR2", 6.0),
])}

BOTH = C.Condition("both", (C.Step(table="host.parquet", type="", set=(),
                                   operation="union"),))
MEDIUM = {"EX_glc_e": (-10.0, 1000.0)}


def _build(cond, frame, **kw):
    kw.setdefault("do_gapfill", False)
    return F.build(frame, cond, CORPUS, MEDIUM, **kw)


def test_an_ungoverned_reaction_survives_a_carve_that_keeps_nothing(frame):
    # Exchanges, demands, biomass and maintenance carry no gene rule, so no annotation
    # lane can speak to them. Dropping them would be a claim the evidence never made.
    empty = C.Condition("none", (C.Step(table="host.parquet", type="mnxr",
                                        set=("MNXR9",), operation="union",
                                        allow_empty=True),))
    model, census = _build(empty, frame)
    assert {r.id for r in model.reactions} == {"EX_glc_e", "BIOMASS"}
    assert census["carved_away"] == 2


def test_the_carve_keeps_what_the_condition_asserts(frame):
    model, census = _build(BOTH, frame)
    assert {"GLCt", "R1"} <= {r.id for r in model.reactions}
    assert census["growth_final"] == pytest.approx(10.0)


def test_min_lanes_counts_channels_and_reads_no_weight(frame):
    # THE POINT OF THE SECOND THRESHOLD. Every row here is one channel, so a two-lane
    # carve deletes both reactions whatever their evidence is worth -- which is why an
    # evidence-weighting change scored through `min_lanes` alone comes back as a null.
    model, census = _build(BOTH, frame, min_lanes=2)
    assert census["n_mnxr"] == 0
    assert census["growth_final"] == 0.0


def test_carve_threshold_reads_the_calibrated_weight(frame):
    # GLCt's single ORF at 1.0 nats pools to 0.106; R1's at 6.0 nats to 0.899. A floor
    # between them keeps the transporter out and the pathway dead.
    model, census = _build(BOTH, frame, carve_threshold=0.5)
    assert census["n_mnxr"] == 1
    assert "GLCt" not in {r.id for r in model.reactions}
    assert census["growth_final"] == 0.0


def test_dosage_can_put_a_reaction_into_the_model(frame):
    # THE ONLY ROUTE BY WHICH `factor` REACHES AN LP. Four more copies of the weak ORF
    # take its pooled belief across the same floor. It never moves a bound, because
    # raising a bound that was not binding is a bit-exact no-op.
    dosed = C.Condition("dosed", (
        C.Step(table="host.parquet", type="", set=(), operation="union"),
        C.Step(table="host.parquet", type="mnxr", set=("MNXR1",), operation="union",
               factor=4.0),
    ))
    model, census = _build(dosed, frame, carve_threshold=0.5)
    assert census["n_mnxr"] == 2
    assert census["growth_final"] == pytest.approx(10.0)


def test_a_medium_closes_the_exchanges_it_does_not_name():
    m = _frame()
    missing = F.apply_medium(m, {})
    assert missing == 0
    assert m.reactions.get_by_id("EX_glc_e").lower_bound == 0.0
    assert m.slim_optimize() == pytest.approx(0.0)
    assert F.apply_medium(m, {"EX_nope_e": (-1.0, 1.0)}) == 1


def test_the_growth_pin_is_a_fraction_of_the_model_s_own_maximum(frame):
    # An absolute pin would compare a carved model and its frame at different fractions
    # of their capacity, which is the comparison this arm exists to avoid.
    model, census = _build(BOTH, frame)
    t = T.Test(test_id="t", network="both", arm="gem",
               settings=dict(frame="toy", media="m", target_hub="bio_c",
                             growth_pin=0.5))
    rows = {r["readout"]: r["value"] for r in F.measure(model, t, MEDIUM, census)}
    assert rows["growth"] == pytest.approx(10.0)
    assert rows["_growth_pin"] == pytest.approx(5.0)
    # Every unit of glucose goes to biomass or to the demand, so pinning half the growth
    # leaves exactly half for the target.
    assert rows["target"] == pytest.approx(5.0)


def test_a_test_naming_an_unstaged_frame_is_refused(frame, tmp_path):
    ts = [T.Test(test_id="t", network="both", arm="gem",
                 settings=dict(frame="somewhere_else", media="m"))]
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    with pytest.raises(ValueError, match="not staged"):
        F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, log=lambda _m: None)


def test_a_test_naming_an_unknown_medium_is_refused(frame, tmp_path):
    ts = [T.Test(test_id="t", network="both", arm="gem",
                 settings=dict(frame="toy", media="lb"))]
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    with pytest.raises(ValueError, match="media table does not carry"):
        F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, log=lambda _m: None)


def test_one_build_serves_every_test_that_shares_it(frame, tmp_path):
    # A pin sweep is three tests and one carve. Rebuilding per test would triple a step
    # that is minutes on a real frame.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    ts = [T.Test(test_id=f"pin{p}", network="both", arm="gem",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=p))
          for p in (0.9, 0.5, 0.1)]
    seen = []
    df = F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, do_gapfill=False,
               census_out=tmp_path / "census.json", log=seen.append)
    assert seen[-1] == f"[fba] 1 baseline build(s), 0 difference(s), {len(ts)} test(s)"
    got = {(r.test_id, r.readout): r.value for r in df.itertuples()}
    assert got[("pin0.9", "target")] == pytest.approx(1.0)
    assert got[("pin0.1", "target")] == pytest.approx(9.0)


def test_two_conditions_that_carve_alike_share_one_build(frame, tmp_path):
    # THE EYDALLIN SHAPE. Every clone is a chromosomal E. coli ORF the host already
    # carries, so a perturbed condition changes the network and not the carve. Keying the
    # build on the condition would run 86 identical carves, insertions and gapfills.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    clone = tmp_path / "clone.parquet"
    pd.DataFrame([_row("clone_g1", "c1", "R1", "MNXR2", 6.0)]).to_parquet(
        clone, index=False)
    # A second copy of a reaction the host already carries: a different network, the
    # same carve.
    dosed = C.Condition("clone", (
        C.Step(table="host.parquet", type="", set=(), operation="union"),
        C.Step(table="clone.parquet", type="", set=(), operation="union"),
    ))
    ts = [T.Test(test_id=c, network=c, arm="gem",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=0.1))
          for c in ("both", "clone")]
    seen = []
    df = F.run([frame], [gpr, clone], [BOTH, dosed], ts, {"m": MEDIUM}, do_gapfill=False,
               log=seen.append)
    assert seen[-1] == f"[fba] 1 baseline build(s), 0 difference(s), {len(ts)} test(s)"
    # ...and the per-condition counts still come from the condition, so a shared build
    # cannot hide which rows went into it.
    rows = {(r.test_id, r.readout): r.value for r in df.itertuples()}
    assert rows[("both", "_n_units")] == 1 and rows[("clone", "_n_units")] == 2


def test_the_media_table_round_trips(tmp_path):
    p = F.write_media([("kornberg", "EX_glc_e", -10.0, 1000.0),
                       ("kornberg", "EX_ala__L_e", -0.35, 1000.0),
                       ("m9", "EX_glc_e", -10.0, 1000.0)],
                      tmp_path / "media.parquet")
    back = F.load_media(p)
    assert set(back) == {"kornberg", "m9"}
    assert back["kornberg"]["EX_ala__L_e"] == (-0.35, 1000.0)


def test_a_media_table_missing_a_column_is_refused(tmp_path):
    p = tmp_path / "bad.tsv"
    pd.DataFrame({"media": ["m"], "mnxr": ["MNXR1"]}).to_csv(p, sep="\t", index=False)
    with pytest.raises(ValueError, match="exchange"):
        F.load_media(p)


def test_a_curated_table_outranks_the_frame_s_own_annotation():
    # AN MNXR IS ONLY MEANINGFUL INSIDE ONE MNXref RELEASE. A BiGG model's annotation is
    # whichever release BiGG used, and on iECDH1ME8569_1439 against MetaNetX 4.5 the two
    # disagree on 1,231 of 2,751 reactions. Trusting the frame carved 625 reactions away
    # that belonged in the model, and nothing raised.
    m = _frame()
    m.reactions.get_by_id("GLCt").annotation = {F.MNXR_ANNOTATION: ["MNXR_STALE"]}
    m.reactions.get_by_id("BIOMASS").annotation = {F.MNXR_ANNOTATION: ["MNXR_ONLY"]}
    x, census = F.crosswalk(m, CORPUS)
    assert x["GLCt"] == "MNXR1", "the pinned-release table wins"
    assert x["BIOMASS"] == "MNXR_ONLY", "and the frame fills in what no table covers"
    assert census["crosswalk_disagree"] == 1


def test_a_de_novo_corpus_with_no_crosswalk_is_refused():
    # WHAT THIS GUARD IS FOR. Keeping only the ungoverned reactions is not a deep carve,
    # it is an empty crosswalk -- and gapfill then repairs the wreckage back into
    # something that reports plausible numbers.
    m = _frame()
    denovo = {"lanes.parquet": pd.DataFrame([
        _row("dh1", "b1", "K00001", "MNXR2", 6.0, channel="kofam")])}
    a = F.assertions(C.Condition("x", (C.Step(table="lanes.parquet", type="", set=(),
                                              operation="union"),)), denovo)
    with pytest.raises(ValueError, match="not one of them reaches this frame"):
        F.carve(m, a, F.crosswalk(m, denovo)[0])


def test_results_mirror_the_conductance_schema(frame, tmp_path):
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    ts = [T.Test(test_id="t", network="both", arm="gem",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=0.1))]
    df = F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, do_gapfill=False,
               log=lambda _m: None)
    assert list(df.columns) == list(F.RESULT_COLUMNS)
    assert (df.readout.str.startswith("_")).any(), \
        "a result must carry its own diagnostics, so it cannot be read without them"
    # THE TARGET IS IN THE TABLE, not only in the tests file. `scoring.KEYS["gem"]` keys
    # on it, so a cohort measured on several axes is scored against a null per axis
    # rather than against all of them pooled. `RESULT_COLUMNS` is what decides whether it
    # survives into the written table, and it once did not while `_rows` emitted it.
    assert "target" in F.RESULT_COLUMNS
    assert set(df.target) == {"bio_c"}


# ---------------------------------------------------------- a perturbation is a delta


def _ko_g1():
    """The host, minus the gene that nominates R1. A knockout subtracts on `gene`."""
    return C.Condition("ko", (
        C.Step(table="host.parquet", type="", set=(), operation="union"),
        C.Step(table="host.parquet", type="gene", set=("b1",), operation="subtract"),
    ))


def _ko_tests():
    common = dict(frame="toy", media="m", target_hub="bio_c", growth_pin=0.1)
    return [T.Test(test_id="base", network="both", arm="gem", settings=dict(common)),
            T.Test(test_id="ko", network="ko", arm="gem", read_against="base",
                   settings=dict(common))]


def test_a_deleted_gene_stays_deleted_through_the_gapfill(frame, tmp_path):
    # THE DEFECT THIS TASK EXISTS FOR. `build_from` hands its gapfill a donor made of
    # exactly the reactions the carve removed, so on a deletion cohort the deleted
    # reaction is the FIRST thing the repair draws back -- and the model then reports a
    # plausible growth rate for a strain that no longer has the gene. Differencing
    # against the baseline leaves the repair where it belongs, on the baseline.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    seen = []
    df = F.run([frame], [gpr], [BOTH, _ko_g1()], _ko_tests(), {"m": MEDIUM},
               do_gapfill=True, log=seen.append)
    rows = {(r.test_id, r.readout): r.value for r in df.itertuples()}
    assert rows[("base", "growth")] == pytest.approx(10.0)
    assert rows[("ko", "growth")] == 0.0
    assert rows[("ko", "_delta_withdrawn")] == 1.0
    assert seen[-1] == "[fba] 1 baseline build(s), 1 difference(s), 2 test(s)"


def test_rebuilding_a_deletion_lets_the_gapfill_draw_it_back(frame, tmp_path):
    # WHAT `--no-delta` COSTS, kept as a test so the old path cannot be mistaken for a
    # second opinion. The same knockout, rebuilt from the frame, grows exactly as fast as
    # the strain that still has the gene -- because the repair put the gene's reaction
    # back and nothing in the census says so under a name that reads like a warning.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    df = F.run([frame], [gpr], [BOTH, _ko_g1()], _ko_tests(), {"m": MEDIUM},
               do_gapfill=True, delta=False, log=lambda _m: None)
    rows = {(r.test_id, r.readout): r.value for r in df.itertuples()}
    assert rows[("ko", "growth")] == pytest.approx(10.0)
    assert rows[("ko", "_gapfill_needed")] == 1.0


def test_a_perturbation_that_changes_no_assertion_is_measured_identically(frame,
                                                                          tmp_path):
    # THE EQUIVALENCE CHECK, and it is what says the difference is being applied rather
    # than merely being cheap. A second copy of a reaction the host already carries
    # changes the network and not the carve, so differencing and rebuilding have to agree
    # bit for bit -- and they do it from one build instead of two.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    clone = tmp_path / "clone.parquet"
    pd.DataFrame([_row("clone_g1", "c1", "R1", "MNXR2", 6.0)]).to_parquet(clone,
                                                                          index=False)
    dosed = C.Condition("clone", (
        C.Step(table="host.parquet", type="", set=(), operation="union"),
        C.Step(table="clone.parquet", type="", set=(), operation="union"),
    ))
    common = dict(frame="toy", media="m", target_hub="bio_c", growth_pin=0.1)
    ts = [T.Test(test_id="base", network="both", arm="gem", settings=dict(common)),
          T.Test(test_id="clone", network="clone", arm="gem", read_against="base",
                 settings=dict(common))]
    seen = []
    a = F.run([frame], [gpr, clone], [BOTH, dosed], ts, {"m": MEDIUM}, do_gapfill=True,
              log=seen.append)
    b = F.run([frame], [gpr, clone], [BOTH, dosed], ts, {"m": MEDIUM}, do_gapfill=True,
              delta=False, log=lambda _m: None)
    key = ["test_id", "readout"]
    shared = sorted(set(a.readout) & set(b.readout) - {"_gapfill_inherited"})
    ai = a[a.readout.isin(shared)].set_index(key).value.sort_index()
    bi = b[b.readout.isin(shared)].set_index(key).value.sort_index()
    pd.testing.assert_series_equal(ai, bi)
    assert seen[-1] == "[fba] 1 baseline build(s), 0 difference(s), 2 test(s)"


def test_a_baseline_on_other_settings_is_not_differenced_against(frame, tmp_path):
    # A `read_against` that crosses a threshold, a frame, a target or a repair medium is
    # not this test's baseline: differencing there would measure the perturbation PLUS
    # the setting change and report it as the perturbation. Such a test builds on its own.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    ts = [T.Test(test_id="base", network="both", arm="gem",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=0.1, carve_threshold=0.5)),
          T.Test(test_id="ko", network="ko", arm="gem", read_against="base",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=0.1))]
    seen = []
    F.run([frame], [gpr], [BOTH, _ko_g1()], ts, {"m": MEDIUM}, do_gapfill=False,
          log=seen.append)
    assert seen[-1] == "[fba] 2 baseline build(s), 0 difference(s), 2 test(s)"


def test_read_against_cannot_form_a_cycle(frame, tmp_path):
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    common = dict(frame="toy", media="m", target_hub="bio_c", growth_pin=0.1)
    ts = [T.Test(test_id="a", network="both", arm="gem", read_against="b",
                 settings=dict(common)),
          T.Test(test_id="b", network="both", arm="gem", read_against="a",
                 settings=dict(common))]
    with pytest.raises(ValueError, match="forms a cycle"):
        F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, do_gapfill=False,
              log=lambda _m: None)


# ------------------------------------------------------------- shards, resume and parts


MEDIUM2 = {"EX_glc_e": (-5.0, 1000.0)}


def _two_media_tests():
    common = dict(frame="toy", target_hub="bio_c", growth_pin=0.1)
    out = []
    for med in ("m", "m2"):
        out.append(T.Test(test_id=f"base_{med}", network="both", arm="gem",
                          settings=dict(common, media=med)))
        out.append(T.Test(test_id=f"ko_{med}", network="ko", arm="gem",
                          read_against=f"base_{med}", settings=dict(common, media=med)))
    return out


def _gpr(tmp_path):
    p = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(p, index=False)
    return p


def test_a_shard_directory_resumes_without_re_solving(frame, tmp_path):
    gpr = _gpr(tmp_path)
    shards = tmp_path / "shards"
    args = ([frame], [gpr], [BOTH, _ko_g1()], _two_media_tests(),
            {"m": MEDIUM, "m2": MEDIUM2})
    first = F.run(*args, do_gapfill=False, shard_dir=shards, log=lambda _m: None)
    assert len(list(shards.glob("*.json"))) == 4
    seen = []
    again = F.run(*args, do_gapfill=False, shard_dir=shards, log=seen.append)
    assert seen[-1] == "[fba] 0 baseline build(s), 0 difference(s), 4 test(s), 4 resumed"
    pd.testing.assert_frame_equal(first, again)


def test_a_shard_written_under_other_settings_does_not_resume_into_this_run(frame,
                                                                            tmp_path):
    # The shard key carries the run-level settings, so a directory written with the
    # repair off cannot hand its rows to a run with the repair on. Without that the two
    # constructions mix and nothing says so.
    gpr = _gpr(tmp_path)
    shards = tmp_path / "shards"
    args = ([frame], [gpr], [BOTH, _ko_g1()], _two_media_tests(),
            {"m": MEDIUM, "m2": MEDIUM2})
    F.run(*args, do_gapfill=False, shard_dir=shards, log=lambda _m: None)
    seen = []
    F.run(*args, do_gapfill=True, shard_dir=shards, log=seen.append)
    assert "resumed" not in seen[-1]
    assert len(list(shards.glob("*.json"))) == 8


def test_parts_split_differences_and_reassemble_into_the_whole(frame, tmp_path):
    # THE SPLIT IS OVER DIFFERENCES, and every test of one perturbed condition stays with
    # its own. Running both parts and then assembling with no part reproduces the
    # single-process answer exactly.
    gpr = _gpr(tmp_path)
    args = ([frame], [gpr], [BOTH, _ko_g1()], _two_media_tests(),
            {"m": MEDIUM, "m2": MEDIUM2})
    whole = F.run(*args, do_gapfill=False, log=lambda _m: None)

    # Each part gets its own directory, which is what two array tasks running at once
    # actually see -- neither can resume from work the other has not finished.
    shards = tmp_path / "array"
    shards.mkdir()
    for i in (0, 1):
        seen = []
        F.run(*args, do_gapfill=False, shard_dir=tmp_path / f"task{i}", part=i, of=2,
              log=seen.append)
        # Each part builds the baselines ITS differences need -- repeating a baseline
        # across tasks is one build, and cheap beside not splitting a difference.
        assert "2 not in this part" in seen[-1]
        for f in (tmp_path / f"task{i}").glob("*.json"):
            (shards / f.name).write_text(f.read_text())
    seen = []
    assembled = F.run(*args, do_gapfill=False, shard_dir=shards, log=seen.append)
    assert seen[-1] == "[fba] 0 baseline build(s), 0 difference(s), 4 test(s), 4 resumed"
    pd.testing.assert_frame_equal(whole, assembled)


def test_a_part_out_of_range_is_refused(frame, tmp_path):
    gpr = _gpr(tmp_path)
    with pytest.raises(ValueError, match="not in range"):
        F.run([frame], [gpr], [BOTH, _ko_g1()], _two_media_tests(),
              {"m": MEDIUM, "m2": MEDIUM2}, do_gapfill=False, part=2, of=2,
              log=lambda _m: None)


def test_a_deletion_by_bounds_and_by_removal_are_the_same_program(frame):
    # WHY A DELETION NEED NOT COPY THE MODEL. A reaction removed and a reaction pinned to
    # zero flux are the same linear program, so the cheap route has to give the same
    # number as the expensive one -- and the baseline has to come back unharmed, because
    # every other perturbation is measured on it afterwards.
    base_a = F.assertions(BOTH, CORPUS)
    ko_a = F.assertions(_ko_g1(), CORPUS)
    b = F.build_baseline(frame, base_a, None, MEDIUM, corpus=CORPUS, do_gapfill=False)
    before = {r.id: (r.lower_bound, r.upper_bound) for r in b.model.reactions}

    with F.perturbed(b, ko_a) as (m1, c1):
        with m1:
            F.apply_medium(m1, MEDIUM)
            by_bounds = F._growth(m1)
    m2, c2 = F.apply_delta(b, ko_a)
    with m2:
        F.apply_medium(m2, MEDIUM)
        by_removal = F._growth(m2)

    assert by_bounds == by_removal == 0.0
    assert c1["delta"]["withdrawn"] == c2["delta"]["withdrawn"] == 1
    assert c1["delta"]["by_bounds"] == 1 and "by_bounds" not in c2["delta"]
    # the copy really did lose the reaction; the bounds route kept it, carrying nothing
    assert "R1" not in {r.id for r in m2.reactions}
    assert {r.id: (r.lower_bound, r.upper_bound) for r in b.model.reactions} == before
    # ...and the baseline still grows, which is what says the context unwound
    with b.model:
        F.apply_medium(b.model, MEDIUM)
        assert F._growth(b.model) == pytest.approx(10.0)


def test_a_model_that_does_not_grow_abstains_rather_than_reading_unpinned(frame,
                                                                          tmp_path):
    # AN LP DOES NOT RETURN AN EXACT ZERO. A lethal deletion comes back at 3e-15, and the
    # growth pin is a FRACTION of each model's own maximum -- so a pin of 0.1 x 3e-15
    # constrains nothing, the target is maximised unpinned, and the dead strain reads as
    # the largest mover in its cohort. It happened: two dead Eydallin mutants outranked
    # the two genes that genuinely carry the glycogen pathway.
    gpr = _gpr(tmp_path)
    # GLCt is the only way carbon enters, so deleting its gene stops growth outright.
    ko_t = C.Condition("ko_t", (
        C.Step(table="host.parquet", type="", set=(), operation="union"),
        C.Step(table="host.parquet", type="gene", set=("bT",), operation="subtract"),
    ))
    common = dict(frame="toy", media="m", target_hub="bio_c", growth_pin=0.1)
    ts = [T.Test(test_id="base", network="both", arm="gem", settings=dict(common)),
          T.Test(test_id="dead", network="ko_t", arm="gem", read_against="base",
                 settings=dict(common))]
    df = F.run([frame], [gpr], [BOTH, ko_t], ts, {"m": MEDIUM}, do_gapfill=False,
               log=lambda _m: None)
    rows = {(r.test_id, r.readout): r.value for r in df.itertuples()}
    assert rows[("dead", "growth")] < F.GROWTH_TOL
    assert rows[("dead", "_host_zero")] == 1.0 and rows[("dead", "_abstained")] == 1.0
    assert ("dead", "target") not in rows, \
        "a model that cannot grow has no pinned target to report"
