"""The builder: the families the tree hand-writes, and the two things it derives."""
from __future__ import annotations

import pandas as pd
import pytest

from ecspr.model import conditions as C
from ecspr.model import recipes as R
from ecspr.model import tests as T
from ecspr.model.gpr import resolve_weights


def _rows(unit, orf, mnxrs, *, gene, cond=None, universe=True):
    return [dict(source=unit, orf=orf, gene=gene, channel="manual_gpr", mnxr=r,
                 intermediate_id=r, intermediate_name=r, llr=2.99,
                 llr_route="asserted", projection_via="x", evidence_quality="reviewed",
                 lane_set="curated", build_id="b", host="h", genome_unit=unit,
                 in_atom_universe=universe, condition_id=cond)
            for r in mnxrs]


HOST = pd.DataFrame(_rows("dh1", "b1", ["R1"], gene="glgA")
                    + _rows("dh1", "b2", ["R2"], gene="glgB")
                    + _rows("dh1", "b3", ["R3"], gene="glgC"))

# Three clones. `cD` reaches a reaction that is NOT in the atom universe, so it is a real
# perturbation that cannot move a conductance measurement -- the noise floor.
CLONES = pd.DataFrame(
    _rows("clone_glgA", "cA", ["R1"], gene="glgA")
    + _rows("clone_glgD", "cD", ["R9"], gene="glgD", universe=False)
    + _rows("clone_big", "cB", ["R2", "R3"], gene="glgE"))

CORPUS = {"host.parquet": HOST, "clones.parquet": CLONES}
BACKGROUND = (C.Step(table="host.parquet", type="genome_unit", set=("dh1",),
                     operation="union"),)
TEMPLATE = T.Test(test_id="", network="", arm="ecspr",
                  settings=dict(element="C", source_hub="GLC", sink_hub="R1|R2",
                                probe="ground"))


def test_gof_each_is_one_arm_per_value():
    conds, ts = R.sweep(CORPUS, "clones.parquet", "gene", background=BACKGROUND,
                        template=TEMPLATE, family="gof_each")
    assert [c.condition_id for c in conds] == ["baseline", "glgA", "glgD", "glgE"]
    # Every arm carries the background, so the deltas are against the same host.
    assert all(c.steps[0] == BACKGROUND[0] for c in conds)
    assert [t.read_against for t in ts] == ["", "baseline", "baseline", "baseline"]


def test_the_baseline_is_measured_like_any_other_arm():
    conds, ts = R.sweep(CORPUS, "clones.parquet", "gene", background=BACKGROUND,
                        template=TEMPLATE)
    base = [t for t in ts if t.test_id == "baseline"][0]
    arm = [t for t in ts if t.test_id == "glgA"][0]
    assert base.settings == arm.settings, \
        "a baseline computed another way would put the two on different footings"


def test_a_control_is_derived_from_the_atom_universe_not_declared():
    # THE THING THAT MUST NOT BE HAND-DECLARED. A control is an arm that reaches no
    # reaction the measurement can carry current through -- real, and unable to move the
    # answer. Declared by hand, a sweep ends up with controls that are not controls.
    _, ts = R.sweep(CORPUS, "clones.parquet", "gene", background=BACKGROUND,
                    template=TEMPLATE)
    by_id = {t.test_id: t for t in ts}
    assert by_id["glgD"].is_control is True
    assert by_id["glgA"].is_control is False
    assert by_id["glgE"].is_control is False


def test_n_units_counts_copies_not_rows():
    # `glgE` is one clone carrying TWO reactions. A null matched on rows would draw
    # two-unit pools against a one-unit arm.
    _, ts = R.sweep(CORPUS, "clones.parquet", "gene", background=BACKGROUND,
                    template=TEMPLATE)
    by_id = {t.test_id: t for t in ts}
    assert by_id["glgE"].meta["n_units"] == 1
    assert by_id["glgE"].meta["stratum"] == 1


def test_gof_all_is_the_ceiling():
    conds, _ = R.sweep(CORPUS, "clones.parquet", "gene", background=BACKGROUND,
                       template=TEMPLATE, family="gof_all")
    all_arm = conds[1]
    assert all_arm.steps[-1].set == ("glgA", "glgD", "glgE")
    w, _ = resolve_weights(all_arm, CORPUS)
    each, _ = resolve_weights(
        R.sweep(CORPUS, "clones.parquet", "gene", background=BACKGROUND,
                template=TEMPLATE)[0][1], CORPUS)
    assert w["R1"] >= each["R1"]


def test_lof_each_subtracts_the_nominator():
    conds, _ = R.sweep(CORPUS, "host.parquet", "gene", background=BACKGROUND,
                       template=TEMPLATE, family="lof_each")
    ko = [c for c in conds if c.condition_id == "glgA"][0]
    w, _ = resolve_weights(ko, CORPUS)
    assert set(w) == {"R2", "R3"}


def test_a_knockout_may_not_key_on_a_reaction():
    # Subtracting one of a gene's reactions renormalises its belief onto the siblings --
    # measured at +41.1% for a fan-out of three -- which is a claim about a gene the
    # experiment did not make.
    with pytest.raises(ValueError, match="renormalises"):
        R.sweep(CORPUS, "host.parquet", "mnxr", background=BACKGROUND,
                template=TEMPLATE, family="lof_each")


def test_lof_named_is_one_construction():
    conds, _ = R.sweep(CORPUS, "host.parquet", "gene", values=("glgA", "glgB"),
                       background=BACKGROUND, template=TEMPLATE, family="lof_named")
    w, _ = resolve_weights(conds[1], CORPUS)
    assert set(w) == {"R3"}


def test_the_arm_order_is_the_table_s_own():
    # A sweep's shard directory and results file are laid out by arm order, so re-sorting
    # makes a rerun look like a different experiment.
    assert R.distinct(CORPUS, "clones.parquet", "gene") == ("glgA", "glgD", "glgE")


def test_a_grouping_the_orf_name_encodes_becomes_a_column():
    df = pd.DataFrame({"orf": ["pool1:k141_1:0-1200_1", "pool1:k141_1:0-1200_2",
                               "pool1:k141_9:5-99_1"]})
    out = R.insert_column(df, "insert", frm="orf", sep="_")
    assert list(out["insert"]) == ["pool1:k141_1:0-1200", "pool1:k141_1:0-1200",
                                   "pool1:k141_9:5-99"]
    # and it is then selectable, which string surgery inside three consumers is not
    corpus = {"t.parquet": out.assign(gene=None)}
    assert R.distinct(corpus, "t.parquet", "insert") == (
        "pool1:k141_1:0-1200", "pool1:k141_9:5-99")


def test_a_sweep_round_trips_through_both_tables(tmp_path):
    conds, ts = R.sweep(CORPUS, "clones.parquet", "gene", background=BACKGROUND,
                        template=TEMPLATE)
    cp = C.write(conds, tmp_path / "conditions.parquet")
    tp = T.write(ts, tmp_path / "ecspr_tests.parquet", "ecspr")
    back_c = {c.condition_id: c for c in C.read(cp)}
    back_t = T.read(tp, "ecspr")
    assert set(back_c) == {c.condition_id for c in conds}
    assert all(t.network in back_c for t in back_t)
    w, _ = resolve_weights(back_c["glgA"], CORPUS)
    assert "R1" in w
