from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.ops import inputs as op_inputs
from metasmith.ops import samples as op_samples


@pytest.fixture
def lib_path(tmp_path) -> Path:
    types = DataTypeLibrary()
    types["assembly"] = Endpoint(properties={"assembly"})
    types["reads"] = Endpoint(properties={"reads"})
    types["bam"] = Endpoint(properties={"bam"})
    types_path = tmp_path / "mock.yml"
    types.Save(types_path)

    path = tmp_path / "wf" / "input.xgdb"
    lib = DataInstanceLibrary(path)
    lib.AddTypeLibrary(types_path, namespace="mock")
    lib.Save()
    return path


def row(rid, path="", dtype="mock::assembly", parents=None, **over):
    return {
        "id": rid, "mode": "file", "path": str(path), "name": "", "value": "",
        "dtype": dtype, "parents": list(parents or []),
    } | over


def _file(p: Path, text=">x\nACGT\n") -> Path:
    p.write_text(text)
    return p


def loaded(lib_path) -> DataInstanceLibrary:
    return DataInstanceLibrary.Load(lib_path)


def ids(lib_path) -> dict[str, str]:
    lib = loaded(lib_path)
    return {
        str(p): lib._resolve_instance_meta(p, dtype)["instance_id"]
        for p, dtype in lib.manifest.items()
    }


def test_the_type_changes_in_place(lib_path, tmp_path):
    parent = _file(tmp_path / "reads.fa")
    child = _file(tmp_path / "reads.bam", "bam")
    rows = [row("p", parent), row("c", child, dtype="mock::bam", parents=["#p"])]
    op_inputs.sync(str(lib_path), rows)

    rows[0]["dtype"] = "mock::reads"
    op_inputs.sync(str(lib_path), rows)

    lib = loaded(lib_path)
    assert lib.manifest[parent] == "mock::reads"
    assert [(str(m.path), m.name) for m in lib.parents[child]] == [(str(parent), "mock::reads")]
    assert parent.is_file() and child.is_file(), "a type is a label, not a file operation"


def test_an_unknown_type_is_refused(lib_path, tmp_path):
    f = _file(tmp_path / "reads.fa")
    op_inputs.sync(str(lib_path), [row("p", f)])
    with pytest.raises(AssertionError, match="not a type"):
        op_inputs.sync(str(lib_path), [row("p", f, dtype="mock::nonesuch")])
    assert loaded(lib_path).manifest[f] == "mock::assembly"


def test_the_path_changes_and_a_descendant_follows(lib_path, tmp_path):
    parent = _file(tmp_path / "run_a.fa")
    child = _file(tmp_path / "run_a.bam", "bam")
    moved_to = _file(tmp_path / "run_b.fa", ">y\nTTTT\n")
    rows = [row("p", parent), row("c", child, dtype="mock::bam", parents=["#p"])]
    op_inputs.sync(str(lib_path), rows)

    rows[0]["path"] = str(moved_to)
    op_inputs.sync(str(lib_path), rows)

    lib = loaded(lib_path)
    assert set(lib.manifest) == {moved_to, child}
    assert [str(m.path) for m in lib.parents[child]] == [str(moved_to)]
    assert parent.read_text().startswith(">x")
    assert moved_to.read_text().startswith(">y")


def test_a_collision_is_refused_and_the_rows_are_left_alone(lib_path, tmp_path):
    a, b = _file(tmp_path / "a.fa"), _file(tmp_path / "b.fa")
    rows = [row("a", a), row("b", b)]
    op_inputs.sync(str(lib_path), rows)
    rows[0]["path"] = str(b)
    with pytest.raises(AssertionError, match="one row, one path"):
        op_inputs.sync(str(lib_path), rows)
    assert set(loaded(lib_path).manifest) == {a, b}


def test_a_swap_is_refused_by_name(lib_path, tmp_path):
    a, b = _file(tmp_path / "a.fa"), _file(tmp_path / "b.fa")
    rows = [row("a", a), row("b", b)]
    op_inputs.sync(str(lib_path), rows)
    rows[0]["path"], rows[1]["path"] = str(b), str(a)
    with pytest.raises(AssertionError, match="swap places"):
        op_inputs.sync(str(lib_path), rows)


def test_a_value_rows_path_is_minted_and_never_moves(lib_path):
    rows = [row("v", mode="value", value="GCF_000005845.2")]
    out = op_inputs.sync(str(lib_path), rows)
    path = out["rows"]["v"]
    assert len(path) == 32 and set(path) <= set("0123456789abcdef"), path
    assert (lib_path / path).read_text() == "GCF_000005845.2"

    before = ids(lib_path)
    rows[0]["value"] = "GCF_000005845.3"
    out = op_inputs.sync(str(lib_path), rows)
    assert out["rows"]["v"] == path, "the mint is a mint, not a re-roll"
    assert (lib_path / path).read_text() == "GCF_000005845.3"
    assert ids(lib_path) == before


def test_a_value_row_whose_contents_change_keeps_its_identity(lib_path):
    rows = [row("v", mode="value", value="one")]
    out = op_inputs.sync(str(lib_path), rows)
    path = out["rows"]["v"]
    before = ids(lib_path)
    rows[0]["value"] = "two"
    op_inputs.sync(str(lib_path), rows)
    assert (lib_path / path).read_text() == "two"
    assert ids(lib_path) == before


def test_two_value_rows_with_the_same_contents_are_two_items(lib_path):
    rows = [row("a", mode="value", value="same"), row("b", mode="value", value="same")]
    out = op_inputs.sync(str(lib_path), rows)
    pa, pb = out["rows"]["a"], out["rows"]["b"]
    assert pa != pb
    assert len(set(ids(lib_path).values())) == 2


def test_a_legacy_named_value_row_keeps_the_file_it_already_has(lib_path):
    lib = loaded(lib_path)
    lib.AddValue("K12", "GCF_000005845.2", "mock::assembly")
    lib.Save()
    op_samples.write_record(str(lib_path), {"adopted": True, "rows": {"v": "K12"}})

    before = ids(lib_path)
    out = op_inputs.sync(str(lib_path), [row("v", mode="value", value="GCF_000005845.2")])
    assert out["rows"]["v"] == "K12"
    assert ids(lib_path) == before


def test_a_legacy_named_value_row_with_no_record_is_bound_once(lib_path):
    lib = loaded(lib_path)
    lib.AddValue("K12", "GCF_000005845.2", "mock::assembly")
    lib.Save()
    before = ids(lib_path)

    out = op_inputs.sync(
        str(lib_path), [row("v", mode="value", name="K12", value="GCF_000005845.2")]
    )
    assert out["rows"]["v"] == "K12"
    assert ids(lib_path) == before

    out = op_inputs.sync(str(lib_path), [row("v", mode="value", value="GCF_000005845.2")])
    assert out["rows"]["v"] == "K12"
    assert ids(lib_path) == before


def test_switching_a_deferred_row_to_a_value_stays_inside_the_library(lib_path):
    rows = [row("a")]
    op_inputs.sync(str(lib_path), rows)
    rows[0].update(mode="value", value="x")
    out = op_inputs.sync(str(lib_path), rows)
    path = out["rows"]["a"]
    assert not Path(path).is_absolute(), path
    assert (lib_path / path).read_text() == "x"
    assert not Path("/msm_deferred").exists()


def test_a_file_row_cannot_be_library_owned(lib_path, tmp_path):
    f = _file(tmp_path / "reads.fa")
    op_inputs.sync(str(lib_path), [row("p", f)])
    with pytest.raises(AssertionError, match="relative path"):
        op_inputs.sync(str(lib_path), [row("p", "reads.fa")])
    assert [str(p) for p in loaded(lib_path).manifest] == [str(f)]


def test_a_lineage_loop_is_refused(lib_path, tmp_path):
    parent = _file(tmp_path / "reads.fa")
    child = _file(tmp_path / "reads.bam", "bam")
    rows = [row("p", parent), row("c", child, dtype="mock::bam", parents=["#p"])]
    op_inputs.sync(str(lib_path), rows)

    for loop in (["#c"], ["#p"]):
        rows[0]["parents"] = loop
        with pytest.raises(AssertionError, match="loop"):
            op_inputs.sync(str(lib_path), rows)

    lib = loaded(lib_path)
    assert [str(m.path) for m in lib.parents[child]] == [str(parent)]
    assert lib.parents.get(parent, []) == []


def test_a_grandparent_link_still_works(lib_path, tmp_path):
    a, b, c = (_file(tmp_path / f"{n}.fa") for n in ("a", "b", "c"))
    rows = [
        row("a", a),
        row("b", b, parents=["#a"]),
        row("c", c, parents=["#b", "#a"]),
    ]
    op_inputs.sync(str(lib_path), rows)
    assert {str(m.path) for m in loaded(lib_path).parents[c]} == {str(a), str(b)}


def test_a_parent_link_can_be_taken_back(lib_path, tmp_path):
    a, b, c = (_file(tmp_path / f"{n}.fa") for n in ("a", "b", "c"))
    rows = [row("a", a), row("b", b, parents=["#a"]), row("c", c, parents=["#b", "#a"])]
    op_inputs.sync(str(lib_path), rows)
    rows[2]["parents"] = ["#b"]
    op_inputs.sync(str(lib_path), rows)
    assert op_inputs.immediate_parents(loaded(lib_path), c) == [b]


def test_a_row_that_goes_takes_its_item_with_it(lib_path, tmp_path):
    a, b = _file(tmp_path / "a.fa"), _file(tmp_path / "b.fa")
    rows = [row("a", a), row("b", b, dtype="mock::bam", parents=["#a"])]
    op_inputs.sync(str(lib_path), rows)
    out = op_inputs.sync(str(lib_path), rows[1:])
    assert out["removed"] == [str(a)]
    lib = loaded(lib_path)
    assert set(lib.manifest) == {b}
    assert lib.parents.get(b, []) == []


def test_an_incomplete_row_registers_nothing(lib_path, tmp_path):
    f = _file(tmp_path / "a.fa")
    op_inputs.sync(
        str(lib_path), [row("a", f, dtype=""), row("v", mode="value", dtype="", value="x")]
    )
    assert loaded(lib_path).manifest == {}


def test_a_row_with_no_path_is_a_deferred_input(lib_path):
    out = op_inputs.sync(str(lib_path), [row("a")])
    (path,) = out["rows"].values()
    assert path.startswith("/msm_deferred/")
    assert str(next(iter(loaded(lib_path).manifest))) == path


def test_an_unchanged_recipe_leaves_the_library_untouched(lib_path, tmp_path):
    rows = [row("a", _file(tmp_path / "a.fa")), row("b", _file(tmp_path / "b.fa"))]
    op_inputs.sync(str(lib_path), rows)
    before = ids(lib_path)
    index = lib_path / "_metadata" / "index.yml"
    stamp = index.stat().st_mtime_ns

    out = op_inputs.sync(str(lib_path), rows)
    assert out["changed"] is False
    assert ids(lib_path) == before
    assert index.stat().st_mtime_ns == stamp, "an unchanged library must not be rewritten"


def test_editing_one_row_leaves_the_others_identical(lib_path, tmp_path):
    a, b, c = (_file(tmp_path / f"{n}.fa") for n in ("a", "b", "c"))
    moved = _file(tmp_path / "moved.fa", ">z\nGGGG\n")
    rows = [row("a", a), row("b", b), row("c", c, dtype="mock::bam", parents=["#b"])]
    op_inputs.sync(str(lib_path), rows)
    before = ids(lib_path)

    rows[0]["path"] = str(moved)
    op_inputs.sync(str(lib_path), rows)
    after = ids(lib_path)
    assert str(a) not in after
    assert {k: v for k, v in after.items() if k != str(moved)} == {
        k: v for k, v in before.items() if k != str(a)
    }


def test_filling_in_a_deferred_path_moves_only_that_row(lib_path, tmp_path):
    rows = [row("a"), row("b", _file(tmp_path / "b.fa"))]
    op_inputs.sync(str(lib_path), rows)
    real = _file(tmp_path / "real.fa")
    rows[0]["path"] = str(real)
    op_inputs.sync(str(lib_path), rows)

    settled = ids(lib_path)[str(real)]
    rows.append(row("c", _file(tmp_path / "c.fa"), dtype="mock::reads"))
    op_inputs.sync(str(lib_path), rows)
    assert ids(lib_path)[str(real)] == settled


def test_a_deferred_path_survives_a_rewrite_of_the_request(lib_path):
    rows = [row("a")]
    minted = op_inputs.sync(str(lib_path), rows)["rows"]["a"]
    assert op_inputs.sync(str(lib_path), [row("a")])["rows"]["a"] == minted
    assert ids(lib_path)[minted]


def test_the_sheets_items_are_stable_across_a_re_expansion(lib_path, tmp_path):
    from metasmith.ops import samples as op_samples

    for n in ("s1", "s2"):
        _file(tmp_path / f"{n}.fa")
    table = op_samples.parse_table(
        f"sample,asm\ns1,{tmp_path}/s1.fa\ns2,{tmp_path}/s2.fa\n".encode(), filename="s.csv",
    )
    rows = [
        row("i", mode="value", values=[{"key": "", "value": "", "column": "sample"}],
            dtype="mock::reads"),
        row("a", column="asm", parents=["#i"]),
    ]
    op_inputs.sync(str(lib_path), rows, table)
    before = ids(lib_path)
    out = op_inputs.sync(str(lib_path), rows, table)
    assert out["changed"] is False
    assert ids(lib_path) == before


def test_adoption_makes_one_row_per_item(lib_path, tmp_path):
    from metasmith.models.paths import DEFERRED

    lib = loaded(lib_path)
    a = lib.AddItem(_file(tmp_path / "a.fa"), "mock::assembly")
    lib.AddValue("K12", "GCF_000005845.2", "mock::reads", parents=[a])
    lib.AddItem(DEFERRED, "mock::bam", parents=[a])
    lib.Save()

    out = op_inputs.adopt(str(lib_path), [])
    by_type = {r["dtype"]: r for r in out["rows"]}
    assert len(out["rows"]) == 3

    assert by_type["mock::assembly"]["mode"] == "file"
    assert by_type["mock::assembly"]["path"] == str(a)
    assert by_type["mock::reads"]["mode"] == "value"
    assert by_type["mock::reads"]["name"] == ""
    assert out["record"]["rows"][by_type["mock::reads"]["id"]] == "K12"
    assert by_type["mock::reads"]["values"] == [{"key": "", "value": "GCF_000005845.2"}]
    assert by_type["mock::bam"]["path"] == ""
    assert out["record"]["rows"][by_type["mock::bam"]["id"]].startswith("/msm_deferred/")
    parent = f"#{by_type['mock::assembly']['id']}"
    assert by_type["mock::reads"]["parents"] == [parent]


def test_adoption_is_idempotent_and_happens_once(lib_path, tmp_path):
    from metasmith.ops import samples as op_samples

    lib = loaded(lib_path)
    lib.AddItem(_file(tmp_path / "a.fa"), "mock::assembly")
    lib.Save()

    first = op_inputs.adopt(str(lib_path), [])
    op_samples.write_record(str(lib_path), first["record"])
    assert op_inputs.adopt(str(lib_path), first["rows"]) is None

    out = op_inputs.sync(str(lib_path), first["rows"])
    assert out["changed"] is False
    assert len(loaded(lib_path).manifest) == 1


def test_an_adopted_template_keeps_its_key(lib_path, tmp_path):
    from metasmith.models.paths import DEFERRED
    from metasmith.ops import samples as op_samples

    lib = loaded(lib_path)
    lib.AddItem(DEFERRED, "mock::assembly")
    lib.Save()
    before = ids(lib_path)

    out = op_inputs.adopt(str(lib_path), [])
    op_samples.write_record(str(lib_path), out["record"])
    op_inputs.sync(str(lib_path), out["rows"])
    assert ids(lib_path) == before


def test_an_adopted_json_object_arrives_as_keyed_fields(lib_path):
    from metasmith.ops import samples as op_samples

    lib = loaded(lib_path)
    lib.AddValue(
        "meta.json", '{"parity": "paired", "length_class": "short"}', "mock::reads",
    )
    lib.Save()
    before = ids(lib_path)

    out = op_inputs.adopt(str(lib_path), [])
    (adopted,) = out["rows"]
    assert adopted["values"] == [
        {"key": "parity", "value": "paired"},
        {"key": "length_class", "value": "short"},
    ]

    op_samples.write_record(str(lib_path), out["record"])
    assert op_inputs.sync(str(lib_path), out["rows"])["changed"] is False
    assert ids(lib_path) == before
    assert (lib_path / "meta.json").read_text() == (
        '{"parity": "paired", "length_class": "short"}'
    )


@pytest.mark.parametrize("value", [
    '{"n": "10"}',
    '{"nested": {"a": 1}}',
    '["a", "b"]',
    "{sample}",
])
def test_a_value_that_would_not_round_trip_keeps_its_single_entry(lib_path, value):
    lib = loaded(lib_path)
    lib.AddValue("v.txt", value, "mock::reads")
    lib.Save()

    (adopted,) = op_inputs.adopt(str(lib_path), [])["rows"]
    assert adopted["values"] == [{"key": "", "value": value}]


def test_a_library_owned_file_too_big_to_be_a_value_is_adopted_as_it_is(lib_path):
    from metasmith.ops import samples as op_samples

    lib = loaded(lib_path)
    lib.AddValue("big.txt", "x" * (op_inputs.MAX_VALUE_BYTES + 1), "mock::assembly")
    lib.Save()

    out = op_inputs.adopt(str(lib_path), [])
    (adopted,) = out["rows"]
    assert adopted["mode"] == "file" and adopted["path"] == "big.txt"
    op_samples.write_record(str(lib_path), out["record"])
    assert op_inputs.sync(str(lib_path), out["rows"])["changed"] is False


def kv(*pairs):
    return [{"key": k, "value": v} for k, v in pairs]


def test_one_unkeyed_field_writes_exactly_what_was_typed(lib_path):
    legacy = op_inputs.sync(str(lib_path), [row("v", mode="value", value="300\n")])
    path = legacy["rows"]["v"]
    before = ids(lib_path)

    listed = [row("v", mode="value", values=kv(("", "300\n")))]
    out = op_inputs.sync(str(lib_path), listed)
    assert out["changed"] is False
    assert out["rows"]["v"] == path
    assert (lib_path / path).read_text() == "300\n"
    assert ids(lib_path) == before


def test_keyed_fields_are_written_as_one_object(lib_path):
    rows = [row("v", mode="value", values=kv(("insert", "300"), ("paired", "true")))]
    out = op_inputs.sync(str(lib_path), rows)
    text = (lib_path / out["rows"]["v"]).read_text()
    assert text == '{"insert": 300, "paired": true}'


@pytest.mark.parametrize(
    "typed,written",
    [
        ("300", 300),
        ("1.5", 1.5),
        ("true", True),
        ("null", None),
        ('"300"', "300"),
        ("illumina", "illumina"),
        ("[1, 2]", "[1, 2]"),
        ('{"a": 1}', '{"a": 1}'),
    ],
)
def test_a_field_is_given_the_type_it_looks_like(typed, written):
    import json

    rendered = op_inputs.render_value(kv(("k", typed)))
    assert json.loads(rendered) == {"k": written}


def test_one_field_with_a_key_is_still_an_object(lib_path):
    rows = [row("v", mode="value", values=kv(("depth", "10")))]
    out = op_inputs.sync(str(lib_path), rows)
    assert (lib_path / out["rows"]["v"]).read_text() == '{"depth": 10}'


def _sheet():
    return op_samples.parse_table(
        b"sample,pangenome\ns1,pA\ns2,pA\ns3,pB\n", filename="s.csv",
    )


def bound(*pairs):
    return [{"key": k, "value": "", "column": c} for k, c in pairs]


def test_fields_may_bind_different_columns(lib_path):
    rows = [row(
        "v", mode="value", dtype="mock::reads",
        values=bound(("of", "pangenome"), ("id", "sample")),
    )]
    out = op_inputs.sync(str(lib_path), rows, _sheet())
    made = out["generated"]["v"]
    assert len(made) == 3 and len(set(made)) == 3, "one per (pangenome, sample) pair"
    assert (lib_path / made[0]).read_text() == '{"of": "pA", "id": "s1"}'

    before = ids(lib_path)
    again = op_inputs.sync(str(lib_path), rows, _sheet())
    assert again["changed"] is False and again["generated"]["v"] == made
    assert ids(lib_path) == before


def test_a_constant_under_a_sheet_is_a_column_repeated_down_it(lib_path):
    sheet = op_samples.parse_table(
        b"sample,pangenome,kit\ns1,pA,illumina\ns2,pA,illumina\ns3,pB,illumina\n",
        filename="s.csv",
    )
    rows = [row(
        "v", mode="value", dtype="mock::reads",
        values=bound(("of", "pangenome"), ("kit", "kit")),
    )]
    out = op_inputs.sync(str(lib_path), rows, sheet)
    made = out["generated"]["v"]
    assert len(made) == 3 and len(set(made)) == 2, "s1 and s2 share one pangenome"
    assert (lib_path / made[0]).read_text() == '{"of": "pA", "kit": "illumina"}'


def test_the_grouping_key_of_a_one_field_row_is_what_it_always_was(lib_path):
    record = {"sample": "s1", "pangenome": "pA"}
    assert op_inputs._group_key(
        {"mode": "value", "values": bound(("", "pangenome"))}, record,
    ) == '[["pangenome","pA"]]'


def test_a_field_keeps_both_answers_across_attach_and_detach(lib_path):
    def written(table=None):
        out = op_inputs.sync(str(lib_path), [r], table)
        return (lib_path / out["rows"]["v"]).read_text() if out["rows"] else None

    r = row("v", mode="value", dtype="mock::reads",
            values=[{"key": "", "value": "asdf", "column": ""}])
    assert written() == "asdf"

    assert op_inputs.problems([r], _sheet()) == ["[asdf] has no column chosen for its value"]
    assert written(_sheet()) is None

    r["values"][0]["column"] = "pangenome"
    out = op_inputs.sync(str(lib_path), [r], _sheet())
    assert len(set(out["generated"]["v"])) == 2
    assert r["values"][0]["value"] == "asdf", "the typed text is untouched by binding"

    assert written() == "asdf"
    r["values"][0]["value"] = "123"
    assert written() == "123"
    assert r["values"][0]["column"] == "pangenome"
    assert len(set(op_inputs.sync(str(lib_path), [r], _sheet())["generated"]["v"])) == 2


def test_a_binding_outlives_a_sheet_that_has_no_such_column(lib_path):
    r = row("v", mode="value", dtype="mock::reads", values=bound(("", "pangenome")))
    other = op_samples.parse_table(b"sample,depth\ns1,10\n", filename="s.csv")
    (said,) = op_samples.validate(str(lib_path), other, [r])["problems"]
    assert "[pangenome]" in said["message"] and "does not have" in said["message"]
    assert r["values"][0]["column"] == "pangenome"
    assert len(op_inputs.sync(str(lib_path), [r], _sheet())["generated"]["v"]) == 3


def test_a_finished_recipe_has_no_problems(lib_path, tmp_path):
    rows = [
        row("a", _file(tmp_path / "a.fa")),
        row("v", mode="value", values=kv(("", "GCF_000005845.2")), dtype="mock::reads"),
        row("m", mode="value", dtype="mock::reads",
            values=kv(("insert", "300"), ("paired", "true"))),
        row("x", dtype=""),
    ]
    assert op_inputs.problems(rows) == []


def test_the_blanks_in_a_recipe_are_named(lib_path):
    said = op_inputs.problems([
        row("a", "", dtype="mock::assembly"),
        row("v", mode="value", values=kv(("", "")), dtype="mock::reads"),
        row("m", mode="value", dtype="mock::reads",
            values=kv(("insert", "300"), ("", "true"))),
        row("d", mode="value", dtype="mock::reads",
            values=kv(("k", "1"), ("k", "2"))),
    ])
    assert len(said) == 4
    assert any("has no path" in s for s in said)
    assert any("has nothing in it" in s for s in said)
    assert any("field 2 has no key" in s for s in said)
    assert any("[k] twice" in s for s in said)


def test_a_recipe_with_blanks_still_syncs(lib_path):
    rows = [
        row("a", "", dtype="mock::assembly"),
        row("m", mode="value", dtype="mock::reads", values=kv(("", "300"), ("", ""))),
    ]
    out = op_inputs.sync(str(lib_path), rows)
    assert op_inputs.problems(rows)
    assert (lib_path / out["rows"]["m"]).read_text() == '{"": ""}'
