"""DEFERRED: an input whose path is not known yet.

The path half. What a deferred row does to a *solve* is in
`tests/flow/test_deferred_inputs.py`, because that needs transforms.

Two properties here are load-bearing and neither is obvious from reading the
constant:

* the minted value is distinct per row, because the manifest is keyed by path;
* the minted value survives its own render/parse round trip unchanged. `Source`
  did not, once, and a remote agent home grew a colon per save until nothing
  could reach it. A spec is rewritten on nearly every recipe edit, so the same
  bug here would compound faster.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.paths import (
    DEFERRED,
    DEFERRED_ROOT,
    DeferredPathError,
    is_deferred,
    mint_deferred_path,
)
from metasmith.models.remote import Logistics, Source
from metasmith.models.solver import Endpoint


def _library(tmp_path: Path) -> DataInstanceLibrary:
    tmp_path.mkdir(parents=True, exist_ok=True)
    types = DataTypeLibrary()
    types["reads"] = Endpoint(properties={"reads"})
    types_path = tmp_path / "mock.yml"
    types.Save(types_path)

    lib = DataInstanceLibrary(tmp_path / "input.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    return lib


# -- the constant and the minted value ---------------------------------------


def test_the_constant_is_recognised_as_deferred():
    assert is_deferred(DEFERRED)


def test_a_minted_path_is_absolute_under_the_reserved_root():
    p = mint_deferred_path()
    # absolute is not cosmetic: `ops.data.repoint_item` routes a *relative*
    # entry through `Rename`, a real file move, and refuses a re-point whose
    # old and new paths disagree on absoluteness.
    assert p.is_absolute()
    assert p.is_relative_to(DEFERRED_ROOT)
    assert is_deferred(p)


def test_two_minted_paths_differ():
    assert mint_deferred_path() != mint_deferred_path()


def test_ordinary_paths_are_not_deferred():
    for p in ("/data/reads.fq", "reads.fq", DEFERRED_ROOT, "", None, 7):
        assert not is_deferred(p), p


def test_render_parse_is_a_fixed_point():
    p = mint_deferred_path()
    once = Path(str(p))
    twice = Path(str(once))
    assert once == p
    assert twice == p
    assert is_deferred(str(p))


# -- what the library does with it -------------------------------------------


def test_add_item_takes_the_bare_constant(tmp_path: Path):
    lib = _library(tmp_path)
    path = lib.AddItem(DEFERRED, "mock::reads")
    assert is_deferred(path)
    assert lib.manifest[path] == "mock::reads"


def test_two_deferred_rows_stay_distinct(tmp_path: Path):
    lib = _library(tmp_path)
    a = lib.AddItem(DEFERRED, "mock::reads")
    b = lib.AddItem(DEFERRED, "mock::reads")
    assert a != b
    assert len(lib.manifest) == 2
    assert lib.Get(a).instance_id != lib.Get(b).instance_id


def test_the_id_is_derived_from_the_path_not_randomly(tmp_path: Path):
    """Two libraries that mint the same path arrive at the same id.

    This is what lets a spec rebuild its library from scratch and get the same
    task key back -- the property a template's DAG is pinned on.
    """
    lib = _library(tmp_path)
    path = lib.AddItem(DEFERRED, "mock::reads")
    first = lib.Get(path).instance_id

    other = _library(tmp_path / "second")
    other.AddItem(path, "mock::reads")
    assert other.Get(path).instance_id == first


def test_the_row_survives_save_and_load(tmp_path: Path):
    lib = _library(tmp_path)
    path = lib.AddItem(DEFERRED, "mock::reads")
    before = lib.Get(path).instance_id
    lib.Save()

    reloaded = DataInstanceLibrary.Load(lib.location)
    assert path in reloaded.manifest
    assert reloaded.Get(path).instance_id == before


# -- the guard ---------------------------------------------------------------


def test_filling_one_in_moves_no_file(tmp_path: Path):
    """The re-point route registers the real path and touches the filesystem not at all.

    This is the whole reason a minted path is absolute: `repoint_item` reads a
    *relative* entry as library-owned and delegates to `Rename`, a real move, and
    refuses a re-point whose old and new paths disagree on absoluteness.
    """
    from metasmith.ops.data import repoint_item

    lib = _library(tmp_path)
    deferred = lib.AddItem(DEFERRED, "mock::reads")
    lib.Save()

    real = tmp_path / "reads.fq"
    real.write_text("@r\nACGT\n+\n!!!!\n", encoding="utf-8")
    result = repoint_item(str(lib.location), str(deferred), str(real))

    assert result["moved"] is False
    assert real.exists(), "the user's own file must be left where it is"
    reloaded = DataInstanceLibrary.Load(lib.location)
    assert real in reloaded.manifest
    assert deferred not in reloaded.manifest


def test_logistics_refuses_a_deferred_source(tmp_path: Path):
    mover = Logistics()
    src = Source.FromLocal(mint_deferred_path())
    dest = Source.FromLocal(tmp_path / "somewhere")
    with pytest.raises(DeferredPathError):
        mover.QueueTransfer(src=src, dest=dest)
