from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.ops import inputs as op_inputs
from metasmith.ops import samples as op_samples

CSV = (
    b"sample,fwd,rev\n"
    b"S1,/data/a_R1.fq,/data/a_R2.fq\n"
    b"S2,/data/b_R1.fq,/data/b_R2.fq\n"
)


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


def _ids(lib_path) -> dict[str, str]:
    lib = DataInstanceLibrary.Load(lib_path)
    return {
        str(p): lib._resolve_instance_meta(p, dtype)["instance_id"]
        for p, dtype in lib.manifest.items()
    }


def _val(*cols) -> list[dict]:
    return [{"key": k, "value": "", "column": c} for k, c in cols]


def _rows(**over) -> list[dict]:
    rows = [
        {"id": "a", "mode": "value", "values": _val(("", "sample")),
         "dtype": "mock::marker", "parents": []},
        {"id": "b", "mode": "file", "path": "", "column": "fwd",
         "dtype": "mock::fwd", "parents": ["#a"]},
        {"id": "c", "mode": "file", "path": "", "column": "rev",
         "dtype": "mock::rev", "parents": ["#a"]},
    ]
    for r in rows:
        r.update(over.get(r["id"], {}))
    return rows


def test_parse_csv_keeps_cells_as_written():
    t = op_samples.parse_table(b"sample,n\n007,\nS2,3\n", filename="s.csv")
    assert t["columns"] == ["sample", "n"]
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


def test_a_rows_fields_are_the_columns_it_binds():
    assert op_samples.bound_fields({"mode": "file", "column": "fwd"}) == [("path", "fwd")]
    assert not op_samples.is_bound({"mode": "file", "path": "/d/{fwd}"})
    assert op_samples.is_bound({"mode": "file", "column": "fwd"})

    keyed = {"mode": "value", "values": _val(("sample", "fwd"))}
    assert op_samples.bound_fields(keyed) == [("[sample]", "fwd")]
    assert op_samples.is_bound(keyed)


def test_array_rows_order_parents_first():
    order = [t["id"] for t in op_samples.order_array_rows(_rows())]
    assert order[0] == "a"


def _table():
    return op_samples.parse_table(CSV, filename="s.csv")


def test_validate_accepts_the_ordinary_shape(tmp_path):
    lib = _library(tmp_path)
    assert op_samples.validate(str(lib), _table(), _rows())["problems"] == []


def test_validate_accepts_ordinary_dag_rows_with_no_index_field(tmp_path):
    lib = _library(tmp_path)
    rows = _rows(a={"parents": []})
    for r in rows:
        assert "index" not in r
    assert op_samples.validate(str(lib), _table(), rows)["problems"] == []


def test_validate_refuses_a_binding_the_sheet_cannot_honour(tmp_path):
    lib = _library(tmp_path)
    problems = op_samples.validate(str(lib), _table(), _rows(b={"column": "nope"}))["problems"]
    assert any("does not have" in p["message"] for p in problems)


def test_a_row_that_binds_nothing_is_reported_rather_than_refused(tmp_path):
    lib = _library(tmp_path)
    rows = _rows(b={"column": ""})
    assert op_samples.validate(str(lib), _table(), rows)["problems"] == []
    (said,) = op_samples.unbound_problems(rows)
    assert "no column chosen for its path" in said["message"]
    assert op_inputs.problems(rows, _table()) == [said["message"]]

    out = op_inputs.sync(str(lib), rows, _table())
    assert out["counts"] == {"a": 2, "c": 2}, "the other rows are unaffected"


def test_validate_accepts_a_column_that_does_not_vary(tmp_path):
    lib = _library(tmp_path)
    table = op_samples.parse_table(
        b"sample,fwd,rev,batch\nS1,/d/a.fq,/d/a2.fq,/d/B.fq\nS2,/d/b.fq,/d/b2.fq,/d/B.fq\n",
        filename="s.csv",
    )
    problems = op_samples.validate(str(lib), table, _rows(b={"column": "batch"}))["problems"]
    assert problems == []


def test_validate_refuses_two_rows_landing_on_one_path(tmp_path):
    lib = _library(tmp_path)
    rows = _rows(b={"column": "fwd"}, c={"column": "fwd"})
    problems = op_samples.validate(str(lib), _table(), rows)["problems"]
    assert any("also what" in p["message"] for p in problems)


def test_validate_refuses_an_empty_cell(tmp_path):
    lib = _library(tmp_path)
    table = op_samples.parse_table(b"sample,fwd,rev\nS1,,/data/a_R2.fq\n", filename="s.csv")
    problems = op_samples.validate(str(lib), table, _rows())["problems"]
    assert any("nothing under" in p["message"] for p in problems)


def test_expand_registers_one_item_per_array_row_per_sheet_row(tmp_path):
    lib_path = _library(tmp_path)
    out = op_inputs.sync(str(lib_path), _rows(), _table())
    assert out["row_count"] == 2
    assert out["counts"] == {"a": 2, "b": 2, "c": 2}

    lib = DataInstanceLibrary.Load(lib_path)
    paths = {str(p) for p in lib.manifest}
    assert {p for p in paths if p.startswith("/data/")} == {
        "/data/a_R1.fq", "/data/a_R2.fq", "/data/b_R1.fq", "/data/b_R2.fq",
    }
    markers = sorted(p for p in paths if not p.startswith("/data/"))
    assert len(markers) == 2 and all(len(m) == 32 for m in markers), markers
    parents = {str(p): sorted(str(m.path) for m in ms) for p, ms in lib.parents.items()}
    assert parents["/data/a_R1.fq"] == parents["/data/a_R2.fq"]
    assert parents["/data/b_R1.fq"] == parents["/data/b_R2.fq"]
    assert parents["/data/a_R1.fq"] != parents["/data/b_R1.fq"]
    assert set(parents["/data/a_R1.fq"]) | set(parents["/data/b_R1.fq"]) == set(markers)


def _grouped_table():
    return op_samples.parse_table(
        b"pangenome,accession\nP1,GCF_1\nP1,GCF_2\nP2,GCF_3\n", filename="s.csv",
    )


def _grouped_rows(**over) -> list[dict]:
    rows = [
        {"id": "pan", "mode": "value", "values": _val(("", "pangenome")),
         "dtype": "mock::marker", "parents": []},
        {"id": "acc", "mode": "value", "values": _val(("", "accession")),
         "dtype": "mock::fwd", "parents": ["#pan"]},
    ]
    for r in rows:
        r.update(over.get(r["id"], {}))
    return rows


def test_expand_groups_rows_that_share_a_column_value(tmp_path):
    lib_path = _library(tmp_path)
    out = op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    assert out["counts"] == {"pan": 3, "acc": 3}

    lib = DataInstanceLibrary.Load(lib_path)
    assert len(lib.manifest) == 5
    by_value = {
        (lib_path / p).read_text(): str(p)
        for p in lib.manifest if not str(p).startswith("/")
    }
    assert set(by_value) == {"P1", "P2", "GCF_1", "GCF_2", "GCF_3"}
    parents = {str(p): sorted(str(m.path) for m in ms) for p, ms in lib.parents.items()}
    assert parents[by_value["GCF_1"]] == [by_value["P1"]]
    assert parents[by_value["GCF_2"]] == [by_value["P1"]]
    assert parents[by_value["GCF_3"]] == [by_value["P2"]]


def test_a_rows_grouping_follows_the_columns_it_binds(tmp_path):
    lib_path = _library(tmp_path)
    table = op_samples.parse_table(
        b"pangenome,accession\nP1,GCF_1\nP1,GCF_2\n", filename="s.csv",
    )
    op_inputs.sync(str(lib_path), _grouped_rows(), table)
    lib = DataInstanceLibrary.Load(lib_path)
    assert len(lib.manifest) == 3
    assert {(lib_path / p).read_text() for p in lib.manifest} == {"P1", "GCF_1", "GCF_2"}

    second = tmp_path / "second"
    second.mkdir()
    lib_path = _library(second)
    rows = _grouped_rows(
        pan={"values": _val(("p", "pangenome"), ("a", "accession"))},
    )
    assert op_samples.validate(str(lib_path), table, rows)["problems"] == []
    op_inputs.sync(str(lib_path), rows, table)
    lib = DataInstanceLibrary.Load(lib_path)
    assert len(lib.manifest) == 4
    assert {(lib_path / p).read_text() for p in lib.manifest} == {
        '{"p": "P1", "a": "GCF_1"}', '{"p": "P1", "a": "GCF_2"}', "GCF_1", "GCF_2",
    }


def test_clear_then_reexpand_a_grouped_shape_round_trips(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    before = {str(p) for p in DataInstanceLibrary.Load(lib_path).manifest}
    removed = op_inputs.sync(str(lib_path), [])["removed"]
    assert set(removed) == before
    assert DataInstanceLibrary.Load(lib_path).manifest == {}

    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    lib = DataInstanceLibrary.Load(lib_path)
    assert len(lib.manifest) == 5
    assert {(lib_path / p).read_text() for p in lib.manifest} == {
        "P1", "P2", "GCF_1", "GCF_2", "GCF_3",
    }


def test_re_expanding_takes_back_exactly_what_it_put_down(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _rows(), _table())
    lib = DataInstanceLibrary.Load(lib_path)
    lib.AddItem(Path("/data/ref.db"), "mock::fwd")
    lib.Save()

    smaller = op_samples.parse_table(
        b"sample,fwd,rev\nS9,/data/z_R1.fq,/data/z_R2.fq\n", filename="s.csv",
    )
    out = op_inputs.sync(str(lib_path), _rows(), smaller)
    assert out["row_count"] == 1

    lib = DataInstanceLibrary.Load(lib_path)
    paths = {str(p) for p in lib.manifest}
    assert {p for p in paths if p.startswith("/data/")} == {
        "/data/ref.db", "/data/z_R1.fq", "/data/z_R2.fq",
    }
    minted = [p for p in paths if not p.startswith("/data/")]
    assert len(minted) == 1 and (lib_path / minted[0]).read_text() == "S9"
    for path in lib.manifest:
        for meta in lib.parents.get(path, []):
            assert meta.path in lib.manifest


def test_clear_unregisters_the_generation(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _rows(), _table())
    removed = op_inputs.sync(str(lib_path), [])["removed"]
    assert len(removed) == 6
    assert DataInstanceLibrary.Load(lib_path).manifest == {}


def test_reordering_the_sheet_changes_nothing(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    before = _ids(lib_path)

    reversed_sheet = op_samples.parse_table(
        b"pangenome,accession\nP2,GCF_3\nP1,GCF_2\nP1,GCF_1\n", filename="s.csv",
    )
    out = op_inputs.sync(str(lib_path), _grouped_rows(), reversed_sheet)
    assert out["changed"] is False
    assert _ids(lib_path) == before


def test_adding_a_sheet_row_mints_only_that_one(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    before = _ids(lib_path)

    bigger = op_samples.parse_table(
        b"pangenome,accession\nP1,GCF_1\nP1,GCF_2\nP2,GCF_3\nP2,GCF_4\n",
        filename="s.csv",
    )
    op_inputs.sync(str(lib_path), _grouped_rows(), bigger)
    after = _ids(lib_path)
    assert set(before) < set(after)
    assert len(after) == len(before) + 1
    for path, iid in before.items():
        assert after[path] == iid


def test_giving_a_field_a_key_keeps_the_items(tmp_path):
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    before = _ids(lib_path)

    rows = _grouped_rows(pan={"values": _val(("of", "pangenome"))})
    op_inputs.sync(str(lib_path), rows, _grouped_table())
    assert _ids(lib_path) == before
    lib = DataInstanceLibrary.Load(lib_path)
    assert '{"of": "P1"}' in {(lib_path / p).read_text() for p in lib.manifest}


def test_every_field_of_a_value_row_has_to_bind_one():
    half = {"mode": "value", "values": [
        {"key": "a", "value": "1", "column": ""},
        {"key": "b", "value": "", "column": "sample"},
    ]}
    assert not op_samples.is_bound(half)
    assert [f for f, c in op_samples.bound_fields(half) if not c] == ["[a]"]
    both = {"mode": "value", "values": _val(("a", "fwd"), ("b", "sample"))}
    assert op_samples.is_bound(both)
    assert not op_samples.is_bound({"mode": "value", "value": "x"})


def test_a_field_binding_a_missing_column_is_named_by_its_key(tmp_path):
    lib = _library(tmp_path)
    rows = [{
        "id": "v", "mode": "value", "path": "", "name": "", "dtype": "mock::marker",
        "values": _val(("of", "sample"), ("depth", "nope")),
        "parents": [],
    }]
    (problem,) = op_samples.validate(str(lib), _table(), rows)["problems"]
    assert "[nope]" in problem["message"] and "in its [depth]" in problem["message"]


def test_a_value_row_is_labelled_by_its_first_field():
    kv = lambda *p: [{"key": k, "value": v} for k, v in p]
    assert op_samples.row_label({"mode": "value", "values": kv(("", "GCF_1"))}) == "GCF_1"
    assert op_samples.row_label({"mode": "value", "values": kv(("of", "P1"))}) == "of: P1"
    assert op_samples.row_label({"mode": "value", "values": _val(("", "pangenome"))}) == "pangenome"
    assert op_samples.row_label({"mode": "file", "path": "", "column": "fwd"}) == "fwd"
