"""Parsing, substitution and expansion of a sample table.

The library-touching half is here rather than in `tests/gui/` because none of it
is about a route: `ops.samples` is the one implementation and the GUI is a
veneer over it.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
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
         "dtype": "mock::marker", "parents": [], "index": True},
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


# -- templates ---------------------------------------------------------------


def test_tokens_and_substitution():
    assert op_samples.columns_in("/d/{a}_{b}.fq") == ["a", "b"]
    assert op_samples.substitute("/d/{a}_{b}.fq", {"a": "x", "b": "y"}) == "/d/x_y.fq"
    assert not op_samples.is_template({"path": "/d/plain.fq"})
    assert op_samples.is_template({"mode": "value", "name": "{s}.id", "value": "x"})


def test_templates_order_parents_first():
    order = [t["id"] for t in op_samples.order_templates(_rows())]
    assert order[0] == "a"


# -- validation --------------------------------------------------------------


def _table():
    return op_samples.parse_table(CSV, filename="s.csv")


def test_validate_accepts_the_ordinary_shape(tmp_path):
    lib = _library(tmp_path)
    assert op_samples.validate(str(lib), _table(), _rows())["problems"] == []


def test_validate_refuses_no_index(tmp_path):
    lib = _library(tmp_path)
    rows = _rows(a={"index": False})
    problems = op_samples.validate(str(lib), _table(), rows)["problems"]
    assert any("sample index" in p["message"] for p in problems)


def test_validate_refuses_two_indexes(tmp_path):
    lib = _library(tmp_path)
    problems = op_samples.validate(str(lib), _table(), _rows(b={"index": True}))["problems"]
    assert any("exactly one" in p["message"] for p in problems)


def test_validate_refuses_a_parented_index(tmp_path):
    """A shared ancestor above the index collapses every sample into one view."""
    lib = _library(tmp_path)
    rows = _rows() + [{"id": "z", "mode": "file", "path": "/d/meta.json",
                       "dtype": "mock::marker", "parents": []}]
    rows[0]["parents"] = ["#z"]
    problems = op_samples.validate(str(lib), _table(), rows)["problems"]
    assert any("cannot descend from anything" in p["message"] for p in problems)


def test_validate_refuses_a_template_that_misses_the_index(tmp_path):
    lib = _library(tmp_path)
    problems = op_samples.validate(str(lib), _table(), _rows(c={"parents": []}))["problems"]
    assert any("does not descend from the sample index" in p["message"] for p in problems)


def test_validate_refuses_an_unknown_column(tmp_path):
    lib = _library(tmp_path)
    problems = op_samples.validate(str(lib), _table(), _rows(b={"path": "/d/{nope}"}))["problems"]
    assert any("does not have" in p["message"] for p in problems)


def test_validate_refuses_a_path_that_does_not_vary(tmp_path):
    """A token that happens to hold the same value on every row is the usual
    cause -- the row registers once and then collides with itself."""
    lib = _library(tmp_path)
    table = op_samples.parse_table(
        b"sample,fwd,rev,batch\nS1,a_R1.fq,a_R2.fq,B\nS2,b_R1.fq,b_R2.fq,B\n",
        filename="s.csv",
    )
    problems = op_samples.validate(str(lib), table, _rows(b={"path": "/d/{batch}.fq"}))["problems"]
    assert any("does not vary per row" in p["message"] for p in problems)


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


def test_expand_registers_one_item_per_template_per_row(tmp_path):
    lib_path = _library(tmp_path)
    out = op_samples.expand(str(lib_path), _table(), _rows())
    assert out["row_count"] == 2
    assert out["counts"] == {"a": 2, "b": 2, "c": 2}
    assert out["sample_type"] == "mock::marker"

    lib = DataInstanceLibrary.Load(lib_path)
    assert {str(p) for p in lib.manifest} == {
        "S1.id", "S2.id", "/data/a_R1.fq", "/data/a_R2.fq",
        "/data/b_R1.fq", "/data/b_R2.fq",
    }
    # each row's reads name that row's marker, not the other one's
    parents = {str(p): sorted(str(m.path) for m in ms) for p, ms in lib.parents.items()}
    assert parents["/data/a_R1.fq"] == ["S1.id"]
    assert parents["/data/b_R2.fq"] == ["S2.id"]


def test_expanded_library_splits_into_one_sample_per_row(tmp_path):
    lib_path = _library(tmp_path)
    op_samples.expand(str(lib_path), _table(), _rows())
    lib = DataInstanceLibrary.Load(lib_path)
    masks = sorted(
        (sorted(str(p) for p in v._mask) for v in lib.AsSamples("mock::marker")),
        key=lambda m: m[0],
    )
    assert masks == [
        ["/data/a_R1.fq", "/data/a_R2.fq", "S1.id"],
        ["/data/b_R1.fq", "/data/b_R2.fq", "S2.id"],
    ]


def test_re_expanding_takes_back_exactly_what_it_put_down(tmp_path):
    lib_path = _library(tmp_path)
    op_samples.expand(str(lib_path), _table(), _rows())
    # a hand-registered row that no expansion owns
    lib = DataInstanceLibrary.Load(lib_path)
    lib.AddItem(Path("/data/ref.db"), "mock::fwd")
    lib.Save()

    smaller = op_samples.parse_table(b"sample,fwd,rev\nS9,z_R1.fq,z_R2.fq\n", filename="s.csv")
    out = op_samples.expand(str(lib_path), smaller, _rows())
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
    op_samples.expand(str(lib_path), _table(), _rows())
    removed = op_samples.clear(str(lib_path))["removed"]
    assert len(removed) == 6
    assert DataInstanceLibrary.Load(lib_path).manifest == {}
