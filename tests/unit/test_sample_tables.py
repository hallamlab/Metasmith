"""Parsing, substitution and expansion of a sample table.

The library-touching half is here rather than in `tests/gui/` because none of it
is about a route: `ops.samples` reads the sheet, `ops.inputs.sync` writes the
library, and the GUI is a veneer over both.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.ops import inputs as op_inputs
from metasmith.ops import samples as op_samples

CSV = b"sample,fwd,rev\nS1,a_R1.fq,a_R2.fq\nS2,b_R1.fq,b_R2.fq\n"


def _library(tmp_path: Path) -> Path:
    types = DataTypeLibrary()
    types["marker"] = Endpoint(properties={"marker"})
    types["fwd"] = Endpoint(properties={"fwd"})
    types["rev"] = Endpoint(properties={"rev"})
    types_path = tmp_path / "mock.yml"
    types.Save(types_path)

    path = tmp_path / "wf" / "input.xgdb"
    lib = DataInstanceLibrary(path)
    lib.AddTypeLibrary(types_path, namespace="mock")
    lib.Save()
    return path


def _rows(**over) -> list[dict]:
    rows = [
        {"id": "a", "mode": "value", "name": "{sample}.id", "value": "{sample}",
         "dtype": "mock::marker", "parents": []},
        {"id": "b", "mode": "file", "path": "/data/{fwd}",
         "dtype": "mock::fwd", "parents": ["#a"]},
        {"id": "c", "mode": "file", "path": "/data/{rev}",
         "dtype": "mock::rev", "parents": ["#a"]},
    ]
    for r in rows:
        r.update(over.get(r["id"], {}))
    return rows


# -- parsing -----------------------------------------------------------------


def test_parse_csv_keeps_cells_as_written():
    t = op_samples.parse_table(b"sample,n\n007,\nS2,3\n", filename="s.csv")
    assert t["columns"] == ["sample", "n"]
    # left to itself pandas makes these `7` and `NaN`
    assert t["rows"][0] == {"sample": "007", "n": ""}
    assert t["row_count"] == 2


def test_parse_sniffs_a_tab_delimited_sheet():
    t = op_samples.parse_table(b"a\tb\n1\t2\n", filename="s.txt")
    assert t["columns"] == ["a", "b"]
    assert t["rows"] == [{"a": "1", "b": "2"}]


def test_parse_refuses_a_repeated_column():
    with pytest.raises(AssertionError, match="more than once"):
        op_samples.parse_table(b"a,a\n1,2\n", filename="s.csv")


def test_attach_stores_the_upload_verbatim(tmp_path):
    op_samples.attach_table(tmp_path, CSV, filename="sheet.csv")
    stored = op_samples.attached_table_path(tmp_path)
    assert stored.read_bytes() == CSV
    assert op_samples.read_attached_table(tmp_path)["row_count"] == 2
    op_samples.detach_table(tmp_path)
    assert op_samples.attached_table_path(tmp_path) is None


# -- sample arrays -----------------------------------------------------------


def test_tokens_and_substitution():
    assert op_samples.columns_in("/d/{a}_{b}.fq") == ["a", "b"]
    assert op_samples.substitute("/d/{a}_{b}.fq", {"a": "x", "b": "y"}) == "/d/x_y.fq"
    assert not op_samples.is_array_row({"path": "/d/plain.fq"})
    assert op_samples.is_array_row({"mode": "value", "name": "{s}.id", "value": "x"})


def test_array_rows_order_parents_first():
    order = [t["id"] for t in op_samples.order_array_rows(_rows())]
    assert order[0] == "a"


# -- validation --------------------------------------------------------------


def _table():
    return op_samples.parse_table(CSV, filename="s.csv")


def test_validate_accepts_the_ordinary_shape(tmp_path):
    lib = _library(tmp_path)
    assert op_samples.validate(str(lib), _table(), _rows())["problems"] == []


def test_validate_accepts_ordinary_dag_rows_with_no_index_field(tmp_path):
    """No row is ever "the index" any more -- a plain parent-wired array set
    validates cleanly with no `index` key anywhere in the row dicts."""
    lib = _library(tmp_path)
    rows = _rows(a={"parents": []})
    for r in rows:
        assert "index" not in r
    assert op_samples.validate(str(lib), _table(), rows)["problems"] == []


def test_validate_refuses_an_unknown_column(tmp_path):
    lib = _library(tmp_path)
    problems = op_samples.validate(str(lib), _table(), _rows(b={"path": "/d/{nope}"}))["problems"]
    assert any("does not have" in p["message"] for p in problems)


def test_validate_accepts_a_path_that_does_not_vary(tmp_path):
    """A token that happens to hold the same value on every row is a deliberate
    grouping now -- the same declared column landing on the same path again
    collapses onto one shared instance, not an error."""
    lib = _library(tmp_path)
    table = op_samples.parse_table(
        b"sample,fwd,rev,batch\nS1,a_R1.fq,a_R2.fq,B\nS2,b_R1.fq,b_R2.fq,B\n",
        filename="s.csv",
    )
    problems = op_samples.validate(str(lib), table, _rows(b={"path": "/d/{batch}.fq"}))["problems"]
    assert problems == []


def test_validate_refuses_two_rows_landing_on_one_path(tmp_path):
    lib = _library(tmp_path)
    rows = _rows(b={"path": "/d/{sample}.fq"}, c={"path": "/d/{sample}.fq"})
    problems = op_samples.validate(str(lib), _table(), rows)["problems"]
    assert any("also what" in p["message"] for p in problems)


def test_validate_refuses_an_empty_cell(tmp_path):
    lib = _library(tmp_path)
    table = op_samples.parse_table(b"sample,fwd,rev\nS1,,a_R2.fq\n", filename="s.csv")
    problems = op_samples.validate(str(lib), table, _rows())["problems"]
    assert any("nothing under" in p["message"] for p in problems)


# -- expansion ---------------------------------------------------------------


def test_expand_registers_one_item_per_array_row_per_sheet_row(tmp_path):
    lib_path = _library(tmp_path)
    out = op_inputs.sync(str(lib_path), _rows(), _table())
    assert out["row_count"] == 2
    assert out["counts"] == {"a": 2, "b": 2, "c": 2}

    lib = DataInstanceLibrary.Load(lib_path)
    assert {str(p) for p in lib.manifest} == {
        "S1.id", "S2.id", "/data/a_R1.fq", "/data/a_R2.fq",
        "/data/b_R1.fq", "/data/b_R2.fq",
    }
    # each row's reads name that row's marker, not the other one's
    parents = {str(p): sorted(str(m.path) for m in ms) for p, ms in lib.parents.items()}
    assert parents["/data/a_R1.fq"] == ["S1.id"]
    assert parents["/data/b_R2.fq"] == ["S2.id"]


def _grouped_table():
    # two accessions share pangenome "P1", one is alone under "P2"
    return op_samples.parse_table(
        b"pangenome,accession\nP1,GCF_1\nP1,GCF_2\nP2,GCF_3\n", filename="s.csv",
    )


def _grouped_rows(**over) -> list[dict]:
    rows = [
        {"id": "pan", "mode": "value", "name": "{pangenome}.pan", "value": "{pangenome}",
         "dtype": "mock::marker", "parents": []},
        {"id": "acc", "mode": "value", "name": "{accession}.acc", "value": "{accession}",
         "dtype": "mock::fwd", "parents": ["#pan"]},
    ]
    for r in rows:
        r.update(over.get(r["id"], {}))
    return rows


def test_expand_groups_rows_that_share_a_column_value(tmp_path):
    lib_path = _library(tmp_path)
    out = op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    # 3 accession rows but only 2 distinct pangenome names
    assert out["counts"] == {"pan": 3, "acc": 3}

    lib = DataInstanceLibrary.Load(lib_path)
    assert {str(p) for p in lib.manifest} == {
        "P1.pan", "P2.pan", "GCF_1.acc", "GCF_2.acc", "GCF_3.acc",
    }
    parents = {str(p): sorted(str(m.path) for m in ms) for p, ms in lib.parents.items()}
    # both accessions under P1 parent to the *same* single P1.pan instance
    assert parents["GCF_1.acc"] == ["P1.pan"]
    assert parents["GCF_2.acc"] == ["P1.pan"]
    assert parents["GCF_3.acc"] == ["P2.pan"]


def test_validate_refuses_a_shared_name_with_disagreeing_values(tmp_path):
    lib = _library(tmp_path)
    table = op_samples.parse_table(
        b"pangenome,accession\nP1,GCF_1\nP1,GCF_2\n", filename="s.csv",
    )
    rows = _grouped_rows(pan={
        # the pangenome's own value diverges even though its name does not
        "value": "{pangenome}-{accession}",
    })
    problems = op_samples.validate(str(lib), table, rows)["problems"]
    assert any("different values" in p["message"] for p in problems)


def test_clear_then_reexpand_a_grouped_shape_round_trips(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    removed = op_inputs.sync(str(lib_path), [])["removed"]
    assert set(removed) == {"P1.pan", "P2.pan", "GCF_1.acc", "GCF_2.acc", "GCF_3.acc"}
    assert DataInstanceLibrary.Load(lib_path).manifest == {}

    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    assert {str(p) for p in DataInstanceLibrary.Load(lib_path).manifest} == {
        "P1.pan", "P2.pan", "GCF_1.acc", "GCF_2.acc", "GCF_3.acc",
    }


def test_re_expanding_takes_back_exactly_what_it_put_down(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _rows(), _table())
    # a hand-registered row that no expansion owns
    lib = DataInstanceLibrary.Load(lib_path)
    lib.AddItem(Path("/data/ref.db"), "mock::fwd")
    lib.Save()

    smaller = op_samples.parse_table(b"sample,fwd,rev\nS9,z_R1.fq,z_R2.fq\n", filename="s.csv")
    out = op_inputs.sync(str(lib_path), _rows(), smaller)
    assert out["row_count"] == 1

    lib = DataInstanceLibrary.Load(lib_path)
    assert {str(p) for p in lib.manifest} == {
        "/data/ref.db", "S9.id", "/data/z_R1.fq", "/data/z_R2.fq",
    }
    # nothing is left naming a parent that is gone
    for path in lib.manifest:
        for meta in lib.parents.get(path, []):
            assert meta.path in lib.manifest


def test_clear_unregisters_the_generation(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _rows(), _table())
    removed = op_inputs.sync(str(lib_path), [])["removed"]
    assert len(removed) == 6
    assert DataInstanceLibrary.Load(lib_path).manifest == {}
