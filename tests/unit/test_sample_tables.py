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


def _ids(lib_path) -> dict[str, str]:
    """{path: instance_id} — what a re-mint would change and a re-expand must not."""
    lib = DataInstanceLibrary.Load(lib_path)
    return {
        str(p): lib._resolve_instance_meta(p, dtype)["instance_id"]
        for p, dtype in lib.manifest.items()
    }


def _rows(**over) -> list[dict]:
    rows = [
        {"id": "a", "mode": "value", "value": "{sample}",
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
    paths = {str(p) for p in lib.manifest}
    # the file rows keep their spelled-out paths; the two markers are minted
    assert {p for p in paths if p.startswith("/data/")} == {
        "/data/a_R1.fq", "/data/a_R2.fq", "/data/b_R1.fq", "/data/b_R2.fq",
    }
    markers = sorted(p for p in paths if not p.startswith("/data/"))
    assert len(markers) == 2 and all(len(m) == 32 for m in markers), markers
    # each row's reads name that row's marker, not the other one's -- asserted
    # by identity now that the marker has no name to assert on
    parents = {str(p): sorted(str(m.path) for m in ms) for p, ms in lib.parents.items()}
    assert parents["/data/a_R1.fq"] == parents["/data/a_R2.fq"]
    assert parents["/data/b_R1.fq"] == parents["/data/b_R2.fq"]
    assert parents["/data/a_R1.fq"] != parents["/data/b_R1.fq"]
    assert set(parents["/data/a_R1.fq"]) | set(parents["/data/b_R1.fq"]) == set(markers)


def _grouped_table():
    # two accessions share pangenome "P1", one is alone under "P2"
    return op_samples.parse_table(
        b"pangenome,accession\nP1,GCF_1\nP1,GCF_2\nP2,GCF_3\n", filename="s.csv",
    )


def _grouped_rows(**over) -> list[dict]:
    rows = [
        {"id": "pan", "mode": "value", "value": "{pangenome}",
         "dtype": "mock::marker", "parents": []},
        {"id": "acc", "mode": "value", "value": "{accession}",
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
    # 2 pangenomes + 3 accessions. A path minted per *sheet row* rather than per
    # distinct pangenome would make this 6, silently: three pangenomes with one
    # genome each, which plans fine and is the wrong science.
    assert len(lib.manifest) == 5
    by_value = {
        (lib_path / p).read_text(): str(p)
        for p in lib.manifest if not str(p).startswith("/")
    }
    assert set(by_value) == {"P1", "P2", "GCF_1", "GCF_2", "GCF_3"}
    parents = {str(p): sorted(str(m.path) for m in ms) for p, ms in lib.parents.items()}
    # both accessions under P1 parent to the *same* single P1 instance
    assert parents[by_value["GCF_1"]] == [by_value["P1"]]
    assert parents[by_value["GCF_2"]] == [by_value["P1"]]
    assert parents[by_value["GCF_3"]] == [by_value["P2"]]


def test_a_rows_grouping_follows_the_columns_its_value_reads(tmp_path):
    """The refusal this replaces cannot happen any more, and here is why.

    It used to be possible for two sheet rows to agree on a value row's *name*
    and disagree on its contents -- name and value were two independent
    templates. There is no name now: the path is keyed on exactly the columns
    the value reads, and `substitute` reads exactly those, so one key implies
    one value by construction.

    What is left is the useful half of that, which nothing asserted before:
    widening the value to read a second column *is* how you say these are no
    longer the same thing, and the grouping follows.
    """
    lib_path = _library(tmp_path)
    table = op_samples.parse_table(
        b"pangenome,accession\nP1,GCF_1\nP1,GCF_2\n", filename="s.csv",
    )
    op_inputs.sync(str(lib_path), _grouped_rows(), table)
    lib = DataInstanceLibrary.Load(lib_path)
    # one shared pangenome + two accessions
    assert len(lib.manifest) == 3
    assert {(lib_path / p).read_text() for p in lib.manifest} == {"P1", "GCF_1", "GCF_2"}

    # now the pangenome row reads the accession too, so the two sheet rows no
    # longer name one pangenome -- two instances, and no problem reported
    second = tmp_path / "second"
    second.mkdir()
    lib_path = _library(second)
    rows = _grouped_rows(pan={"value": "{pangenome}-{accession}"})
    assert op_samples.validate(str(lib_path), table, rows)["problems"] == []
    op_inputs.sync(str(lib_path), rows, table)
    lib = DataInstanceLibrary.Load(lib_path)
    assert len(lib.manifest) == 4
    assert {(lib_path / p).read_text() for p in lib.manifest} == {
        "P1-GCF_1", "P1-GCF_2", "GCF_1", "GCF_2",
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
    # a hand-registered row that no expansion owns
    lib = DataInstanceLibrary.Load(lib_path)
    lib.AddItem(Path("/data/ref.db"), "mock::fwd")
    lib.Save()

    smaller = op_samples.parse_table(b"sample,fwd,rev\nS9,z_R1.fq,z_R2.fq\n", filename="s.csv")
    out = op_inputs.sync(str(lib_path), _rows(), smaller)
    assert out["row_count"] == 1

    lib = DataInstanceLibrary.Load(lib_path)
    paths = {str(p) for p in lib.manifest}
    assert {p for p in paths if p.startswith("/data/")} == {
        "/data/ref.db", "/data/z_R1.fq", "/data/z_R2.fq",
    }
    minted = [p for p in paths if not p.startswith("/data/")]
    assert len(minted) == 1 and (lib_path / minted[0]).read_text() == "S9"
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


# -- the mint is a mint ------------------------------------------------------


def test_reordering_the_sheet_changes_nothing(tmp_path):
    """The whole point of keying on cells rather than on sheet position.

    A record keyed by position would re-mint every item the moment someone
    sorted their spreadsheet, and every downstream cache_key with it -- with
    nothing on screen to say so.
    """
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
    # P2 was already there, so only the new accession is new
    assert set(before) < set(after)
    assert len(after) == len(before) + 1
    for path, iid in before.items():
        assert after[path] == iid


def test_editing_the_text_around_a_token_keeps_the_items(tmp_path):
    """Contents change, paths and identities do not.

    The payoff for keying on the columns a value reads rather than on the text
    it produces: rewording a template is not a re-registration.
    """
    lib_path = _library(tmp_path)
    op_inputs.sync(str(lib_path), _grouped_rows(), _grouped_table())
    before = _ids(lib_path)

    rows = _grouped_rows(pan={"value": "pangenome: {pangenome}"})
    op_inputs.sync(str(lib_path), rows, _grouped_table())
    assert _ids(lib_path) == before
    lib = DataInstanceLibrary.Load(lib_path)
    assert "pangenome: P1" in {(lib_path / p).read_text() for p in lib.manifest}
