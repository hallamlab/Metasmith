"""Building an input library from the recipe's rows, and doing it in place.

`ops.inputs.sync` is the one writer of an input library. These are facts about
it rather than about a route -- there is no route that syncs on its own, because
there is no gesture in the page that does: the rows are saved as they are edited
and the library is built from them when the workflow is solved.

Half of this was a route suite (`TestInputRowEdits`) back when a completed row
was promoted into a library item and then corrected in place through five
mutation endpoints. The corrections are the same corrections; they are just what
editing a row *means* now. The other half -- what identity does across a
re-sync -- had no coverage at all, and is the whole reason the sync is
incremental rather than a rebuild.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.ops import inputs as op_inputs


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


# -- correcting a row --------------------------------------------------------


def test_the_type_changes_in_place(lib_path, tmp_path):
    """...and the row is still the same row.

    It was not removed and re-added, so what descends from it still does -- and
    under the *new* type name, since a parent record carries its parent's type
    as well as its path.
    """
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
    # the child descends from the row, not from the string it used to hold
    assert [str(m.path) for m in lib.parents[child]] == [str(moved_to)]
    # ...and neither file went anywhere: an absolute entry is a pointer at the
    # user's own file, so re-pointing it is a manifest edit
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
    """Two repoints each blocked by the other, and no way to say it in one move."""
    a, b = _file(tmp_path / "a.fa"), _file(tmp_path / "b.fa")
    rows = [row("a", a), row("b", b)]
    op_inputs.sync(str(lib_path), rows)
    rows[0]["path"], rows[1]["path"] = str(b), str(a)
    with pytest.raises(AssertionError, match="swap places"):
        op_inputs.sync(str(lib_path), rows)


def test_a_value_row_is_written_and_moved_inside_the_library(lib_path):
    """The one case where the file *is* the library's, so a move is right."""
    rows = [row("v", mode="value", name="K12", value="GCF_000005845.2")]
    op_inputs.sync(str(lib_path), rows)
    assert (lib_path / "K12").read_text() == "GCF_000005845.2"

    rows[0]["name"] = "K12_MG1655"
    op_inputs.sync(str(lib_path), rows)
    assert not (lib_path / "K12").exists()
    assert (lib_path / "K12_MG1655").read_text() == "GCF_000005845.2"
    assert [str(p) for p in loaded(lib_path).manifest] == ["K12_MG1655"]


def test_a_value_row_whose_contents_change_keeps_its_identity(lib_path):
    """Identity here is provenance, not bytes -- `fork` is the way to say new."""
    rows = [row("v", mode="value", name="K12", value="one")]
    op_inputs.sync(str(lib_path), rows)
    before = ids(lib_path)
    rows[0]["value"] = "two"
    op_inputs.sync(str(lib_path), rows)
    assert (lib_path / "K12").read_text() == "two"
    assert ids(lib_path) == before


def test_a_file_row_cannot_be_library_owned(lib_path, tmp_path):
    """Relative means the library owns the file; absolute means it does not.

    A file row points at a file of the user's, so a relative path would have the
    library claim one it never wrote. The row's own mode switch is how you say
    the other thing, so this is refused rather than guessed at.
    """
    f = _file(tmp_path / "reads.fa")
    op_inputs.sync(str(lib_path), [row("p", f)])
    with pytest.raises(AssertionError, match="relative path"):
        op_inputs.sync(str(lib_path), [row("p", "reads.fa")])
    assert [str(p) for p in loaded(lib_path).manifest] == [str(f)]


def test_a_lineage_loop_is_refused(lib_path, tmp_path):
    """Nothing downstream is defined over a cycle: `AsSamples` walks up to the
    ancestors and then back down to their descendants, so a loop makes every
    branch the whole library."""
    parent = _file(tmp_path / "reads.fa")
    child = _file(tmp_path / "reads.bam", "bam")
    rows = [row("p", parent), row("c", child, dtype="mock::bam", parents=["#p"])]
    op_inputs.sync(str(lib_path), rows)

    for loop in (["#c"], ["#p"]):
        rows[0]["parents"] = loop
        with pytest.raises(AssertionError, match="loop"):
            op_inputs.sync(str(lib_path), rows)

    # ...and the lineage that was there is untouched by the refusal
    lib = loaded(lib_path)
    assert [str(m.path) for m in lib.parents[child]] == [str(parent)]
    assert lib.parents.get(parent, []) == []


def test_a_grandparent_link_still_works(lib_path, tmp_path):
    """The guard is about loops, not about depth: a chain is still a chain."""
    a, b, c = (_file(tmp_path / f"{n}.fa") for n in ("a", "b", "c"))
    rows = [
        row("a", a),
        row("b", b, parents=["#a"]),
        row("c", c, parents=["#b", "#a"]),
    ]
    op_inputs.sync(str(lib_path), rows)
    assert {str(m.path) for m in loaded(lib_path).parents[c]} == {str(a), str(b)}


def test_a_parent_link_can_be_taken_back(lib_path, tmp_path):
    """The library reports ancestors and stores immediate parents; comparing the
    two raw would make the removal look like it never happened."""
    a, b, c = (_file(tmp_path / f"{n}.fa") for n in ("a", "b", "c"))
    rows = [row("a", a), row("b", b, parents=["#a"]), row("c", c, parents=["#b", "#a"])]
    op_inputs.sync(str(lib_path), rows)
    rows[2]["parents"] = ["#b"]
    op_inputs.sync(str(lib_path), rows)
    # `Load` expands the chain again, so `a` is still an ancestor -- through `b`,
    # which is the link that was kept. What must be gone is the declared one.
    assert op_inputs.immediate_parents(loaded(lib_path), c) == [b]


def test_a_row_that_goes_takes_its_item_with_it(lib_path, tmp_path):
    a, b = _file(tmp_path / "a.fa"), _file(tmp_path / "b.fa")
    rows = [row("a", a), row("b", b, dtype="mock::bam", parents=["#a"])]
    op_inputs.sync(str(lib_path), rows)
    out = op_inputs.sync(str(lib_path), rows[1:])
    assert out["removed"] == [str(a)]
    lib = loaded(lib_path)
    assert set(lib.manifest) == {b}
    # nothing is left naming a parent that is gone -- neither `Pack` nor `Unpack`
    # notices a dangling one, and the first `Get` on the child raises
    assert lib.parents.get(b, []) == []


def test_an_incomplete_row_registers_nothing(lib_path, tmp_path):
    """A row with no type is a row someone is still filling in."""
    f = _file(tmp_path / "a.fa")
    op_inputs.sync(str(lib_path), [row("a", f, dtype=""), row("v", mode="value", name="")])
    assert loaded(lib_path).manifest == {}


def test_a_row_with_no_path_is_a_deferred_input(lib_path):
    """"I will have one of these" is a complete statement about a plan. The
    refusal belongs at stage, which is where a deferred path is legible."""
    out = op_inputs.sync(str(lib_path), [row("a")])
    (path,) = out["rows"].values()
    assert path.startswith("/msm_deferred/")
    assert str(next(iter(loaded(lib_path).manifest))) == path


# -- identity ----------------------------------------------------------------
#
# The reason the sync is incremental rather than a rebuild. A rebuild is simpler
# to write, passes every test above, and silently re-mints every id on every
# solve -- which costs every user their cache with nothing on screen to say so.


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
    """The absolute branch of a repoint, which is now the common gesture.

    It used to move the manifest entry without its identity record, leaving the
    new path with none at all -- and the legacy fallback derives an id from the
    *library key*, which is a hash of the whole manifest. That row's id then
    moved every time any other row changed.
    """
    rows = [row("a"), row("b", _file(tmp_path / "b.fa"))]
    op_inputs.sync(str(lib_path), rows)
    real = _file(tmp_path / "real.fa")
    rows[0]["path"] = str(real)
    op_inputs.sync(str(lib_path), rows)

    settled = ids(lib_path)[str(real)]
    # ...and it does not drift when an unrelated row changes
    rows.append(row("c", _file(tmp_path / "c.fa"), dtype="mock::reads"))
    op_inputs.sync(str(lib_path), rows)
    assert ids(lib_path)[str(real)] == settled


def test_a_deferred_path_survives_a_rewrite_of_the_request(lib_path):
    """Minted once, and recorded beside the library rather than in the request:
    the browser rewrites that file wholesale on nearly every edit."""
    rows = [row("a")]
    minted = op_inputs.sync(str(lib_path), rows)["rows"]["a"]
    # the row comes back from the browser exactly as it went out -- blank
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
        row("i", mode="value", name="{sample}.id", value="{sample}", dtype="mock::reads"),
        row("a", "{asm}", parents=["#i"]),
    ]
    op_inputs.sync(str(lib_path), rows, table)
    before = ids(lib_path)
    out = op_inputs.sync(str(lib_path), rows, table)
    assert out["changed"] is False
    assert ids(lib_path) == before


# -- adoption ----------------------------------------------------------------


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
    # a library-owned file is a value: its contents are the row
    assert by_type["mock::reads"]["mode"] == "value"
    assert by_type["mock::reads"]["name"] == "K12"
    assert by_type["mock::reads"]["value"] == "GCF_000005845.2"
    # a minted path is remembered rather than shown, so identity does not move
    assert by_type["mock::bam"]["path"] == ""
    assert out["record"]["rows"][by_type["mock::bam"]["id"]].startswith("/msm_deferred/")
    # lineage arrives in row ids
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

    # ...and a solve over the adopted rows registers nothing new: the row it
    # made claims the item it was made from
    out = op_inputs.sync(str(lib_path), first["rows"])
    assert out["changed"] is False
    assert len(loaded(lib_path).manifest) == 1


def test_an_adopted_template_keeps_its_key(lib_path, tmp_path):
    """A template ships a library of deferred rows, copied verbatim so identity
    follows the path. Adopting it must not mint anything."""
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


def test_a_library_owned_file_too_big_to_be_a_value_is_adopted_as_it_is(lib_path):
    """...and a solve over the adopted rows leaves it alone.

    A file row's path must be absolute -- it points at a file of the user's --
    but a library-owned entry that cannot be read back as a value has nowhere
    else to go, and refusing it would make every solve fail over a row nobody
    had touched.
    """
    from metasmith.ops import samples as op_samples

    lib = loaded(lib_path)
    lib.AddValue("big.txt", "x" * (op_inputs.MAX_VALUE_BYTES + 1), "mock::assembly")
    lib.Save()

    out = op_inputs.adopt(str(lib_path), [])
    (adopted,) = out["rows"]
    assert adopted["mode"] == "file" and adopted["path"] == "big.txt"
    op_samples.write_record(str(lib_path), out["record"])
    assert op_inputs.sync(str(lib_path), out["rows"])["changed"] is False
