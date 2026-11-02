"""The conditions table's set algebra: order, dosage, and what an empty step does."""
from __future__ import annotations

import pandas as pd
import pytest

from ecspr.model import conditions as C
from ecspr.model.gpr import resolve_weights

ASSERTION_LLR = 2.99


def _rows(unit, orf, mnxrs, *, gene=None, channel="gem_gpr"):
    return [dict(source=unit, orf=orf, gene=gene or orf, channel=channel, mnxr=r,
                 intermediate_id=r, intermediate_name=r, llr=ASSERTION_LLR,
                 llr_route="asserted", projection_via="x",
                 evidence_quality="reviewed", lane_set="curated",
                 build_id="t", host="h", genome_unit=unit)
            for r in mnxrs]


HOST = pd.DataFrame(
    _rows("dh1", "b1", ["R1"], gene="glgA")
    + _rows("dh1", "b2", ["R2"], gene="glgB")
    + _rows("dh1", "b3", ["R3", "R4", "R5"], gene="promiscuous"))

CLONES = pd.DataFrame(
    _rows("clone_glgA", "cA", ["R1"], gene="glgA", channel="manual_gpr")
    + _rows("clone_glgC", "cC", ["R9"], gene="glgC", channel="manual_gpr"))

CORPUS = {"host.parquet": HOST, "clones.parquet": CLONES}


def _cond(cid, *steps):
    return C.Condition(condition_id=cid, steps=tuple(steps))


def _step(table, type, values, op, **kw):
    return C.Step(table=table, type=type, set=tuple(values), operation=op, **kw)


BACKGROUND = _step("host.parquet", "genome_unit", ("dh1",), "union")


def _E(cond):
    w, _ = resolve_weights(cond, CORPUS)
    return w


# ---------------------------------------------------------------- the fold

def test_a_background_is_one_union():
    assert set(_E(_cond("bg", BACKGROUND))) == {"R1", "R2", "R3", "R4", "R5"}


def test_a_gain_of_function_adds_a_second_unit():
    # The host's glgA and the clone's are two `genome_unit`s, so their conductances add
    # rather than collapsing. That is the whole account of an overexpression.
    bg = _E(_cond("bg", BACKGROUND))
    gof = _E(_cond("gof", BACKGROUND,
                   _step("clones.parquet", "gene", ("glgA",), "union")))
    assert gof["R1"] > bg["R1"]
    assert gof["R2"] == pytest.approx(bg["R2"])   # untouched


def test_a_knockout_is_a_subtract_on_gene():
    lof = _E(_cond("lof", BACKGROUND,
                   _step("host.parquet", "gene", ("glgA",), "subtract")))
    assert "R1" not in lof
    assert set(lof) == {"R2", "R3", "R4", "R5"}


def test_subtract_on_a_reaction_renormalises_the_gene_s_others():
    # BY DESIGN, and the reason a knockout must key on `gene`. Belief conservation is
    # recomputed on the survivors, so removing one of b3's three reactions pushes its
    # belief onto the other two.
    bg = _E(_cond("bg", BACKGROUND))
    cut = _E(_cond("cut", BACKGROUND,
                   _step("host.parquet", "mnxr", ("R3",), "subtract")))
    assert "R3" not in cut
    assert cut["R4"] > bg["R4"]
    # and subtracting the whole gene does not touch anything that survives
    gone = _E(_cond("gone", BACKGROUND,
                    _step("host.parquet", "gene", ("promiscuous",), "subtract")))
    assert set(gone) == {"R1", "R2"}
    assert gone["R1"] == pytest.approx(bg["R1"])


def test_order_matters():
    # Subtract-then-union puts the rows back; union-then-subtract takes them away. The
    # two tables differ only in `step`, which is why it is a column.
    after = _cond("after", BACKGROUND,
                  _step("clones.parquet", "gene", ("glgA",), "union"),
                  _step("clones.parquet", "gene", ("glgA",), "subtract"))
    before = _cond("before", BACKGROUND,
                   _step("clones.parquet", "gene", ("glgA",), "subtract"),
                   _step("clones.parquet", "gene", ("glgA",), "union"))
    assert _E(after)["R1"] == pytest.approx(_E(_cond("bg", BACKGROUND))["R1"])
    assert _E(before)["R1"] > _E(after)["R1"]


def test_intersect_restricts_to_one_table():
    # `P(r)` is false for every row of another table by its first clause, so an
    # intersect scoped to `clones` drops the host entirely. That is the definition, and
    # what makes intersect mean "restrict to".
    c = _cond("only", BACKGROUND,
              _step("clones.parquet", "gene", ("glgA",), "union"),
              _step("clones.parquet", "gene", ("glgA",), "intersect"))
    assert set(_E(c)) == {"R1"}


# ---------------------------------------------------------------- dosage

def test_a_repeated_union_doubles_the_weight_not_the_rows():
    once = _cond("x1", BACKGROUND,
                 _step("clones.parquet", "gene", ("glgA",), "union"))
    twice = _cond("x2", BACKGROUND,
                  _step("clones.parquet", "gene", ("glgA",), "union"),
                  _step("clones.parquet", "gene", ("glgA",), "union"))
    rows_once = C.resolve(once, CORPUS)
    rows_twice = C.resolve(twice, CORPUS)
    assert len(rows_once) == len(rows_twice)              # no duplicated edges
    assert rows_twice[C.WEIGHT_COL].max() == 2.0
    assert _E(twice)["R1"] > _E(once)["R1"]


def test_a_factor_of_one_equals_a_physically_relabelled_copy():
    # THE EQUIVALENCE THE `factor` COLUMN RESTS ON, and the reason there is no `copy`
    # operation. `pool_logodds` reads `llr` linearly inside its saturating term, so
    # doubling the weight of one row and adding a second row under a new
    # `genome_unit` + `orf` produce the same conductance to FLOAT EQUALITY.
    doubled = _cond("doubled", BACKGROUND,
                    _step("clones.parquet", "gene", ("glgA",), "union"),
                    _step("clones.parquet", "gene", ("glgA",), "union", factor=1.0))

    second = pd.concat([CLONES, pd.DataFrame(
        _rows("clone_glgA_copy2", "cA_copy2", ["R1"], gene="glgA",
              channel="manual_gpr"))], ignore_index=True)
    relabelled = _cond("relabelled", BACKGROUND,
                       _step("clones2.parquet", "gene", ("glgA",), "union"))
    w2, _ = resolve_weights(relabelled, {**CORPUS, "clones2.parquet": second})

    assert _E(doubled)["R1"] == w2["R1"]


def test_a_half_dose_is_between_the_background_and_a_whole_one():
    bg = _E(_cond("bg", BACKGROUND))["R1"]
    half = _E(_cond("half", BACKGROUND,
                    _step("clones.parquet", "gene", ("glgA",), "union",
                          factor=0.5)))["R1"]
    whole = _E(_cond("whole", BACKGROUND,
                     _step("clones.parquet", "gene", ("glgA",), "union")))["R1"]
    assert bg < half < whole


# ---------------------------------------------------------------- refusals

def test_a_step_that_selects_nothing_is_refused():
    c = _cond("typo", BACKGROUND,
              _step("clones.parquet", "gene", ("glgZ",), "union"))
    with pytest.raises(ValueError, match="selects no rows"):
        _E(c)


def test_an_intended_empty_step_is_allowed_when_the_row_says_so():
    c = _cond("maybe", BACKGROUND,
              _step("clones.parquet", "gene", ("glgZ",), "union", allow_empty=True))
    assert set(_E(c)) == {"R1", "R2", "R3", "R4", "R5"}


def test_an_unknown_column_names_the_condition_and_the_table():
    c = _cond("bad", _step("host.parquet", "nope", ("x",), "union"))
    with pytest.raises(ValueError, match="'bad'.*'nope'.*'host.parquet'"):
        _E(c)


def test_an_unstaged_table_is_refused():
    c = _cond("missing", _step("elsewhere.parquet", "gene", ("glgA",), "union"))
    with pytest.raises(ValueError, match="was not staged"):
        _E(c)


# ---------------------------------------------------------------- round trip

@pytest.mark.parametrize("ext", [".parquet", ".tsv"])
def test_the_long_table_round_trips(tmp_path, ext):
    conds = [
        _cond("bg", BACKGROUND),
        _cond("gof", BACKGROUND,
              _step("clones.parquet", "gene", ("glgA", "glgC"), "union", factor=0.5),
              _step("host.parquet", "mnxr", ("R3",), "subtract", allow_empty=True)),
    ]
    back = C.read(C.write(conds, tmp_path / f"conds{ext}"))
    assert [c.condition_id for c in back] == ["bg", "gof"]
    assert back[1].steps[1].set == ("glgA", "glgC")
    assert back[1].steps[1].factor == 0.5
    assert back[1].steps[2].allow_empty is True
    assert back[1].tables == ("host.parquet", "clones.parquet")


def test_step_order_survives_a_shuffled_table(tmp_path):
    c = _cond("gof", BACKGROUND,
              _step("clones.parquet", "gene", ("glgA",), "union"),
              _step("clones.parquet", "gene", ("glgA",), "subtract"))
    p = C.write([c], tmp_path / "conds.tsv")
    df = pd.read_csv(p, sep="\t").iloc[::-1]           # a spreadsheet round trip
    df.to_csv(p, sep="\t", index=False)
    back = C.read(p)[0]
    assert [s.operation for s in back.steps] == ["union", "union", "subtract"]
