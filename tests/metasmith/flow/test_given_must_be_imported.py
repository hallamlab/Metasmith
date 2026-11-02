"""What a plan will accept as a given.

A given's identity has to be a record. The pool is where a record comes from,
and an import is the act that writes one. Everything here is about the moment a
plan is built, because that is the last point at which a wrong identity is
still cheap: past it the id is baked into the plan key, the run directory is
named after it, and every shard the run writes is keyed on it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.paths import DEFERRED
from metasmith.models.workflow import GivenNotImportedError
from metasmith.testing import mock_transforms as mt
from metasmith.testing.pool_fixtures import pool_backed

from .conftest import (
    _MOCK_TYPE_PROPERTIES,
    _build_transform_lib,
    _build_type_lib,
    _generate_plan,
    build_linear_plan,
    run_and_load,
)


def _samples(tmp_path: Path, *, name: str = "samples.xgdb") -> DataInstanceLibrary:
    types_path = _build_type_lib(tmp_path / "types.yml")
    lib = DataInstanceLibrary(tmp_path / name)
    lib.AddTypeLibrary(types_path, namespace="mock")
    sdir = lib.location / "sample_00"
    sdir.mkdir(parents=True, exist_ok=True)
    (sdir / "assembly.txt").write_text(">s\nACGT\n", encoding="utf-8")
    lib.AddItem(Path("sample_00/assembly.txt"), "mock::assembly")
    return lib


def _plan(tmp_path: Path, lib: DataInstanceLibrary):
    tr_lib = _build_transform_lib(
        tmp_path / "tr",
        tmp_path / "types.yml",
        mt.identity_transform("mock::assembly", "mock::bam"),
    )
    return _generate_plan(
        lib,
        tr_lib,
        sample_dtype="assembly",
        target_props=[_MOCK_TYPE_PROPERTIES["bam"]],
        target_names=["bam"],
    )


class TestTheGivenPath:
    def test_a_path_registered_here_is_refused(self, tmp_path):
        lib = _samples(tmp_path)
        with pytest.raises(GivenNotImportedError) as excinfo:
            _plan(tmp_path, lib)
        assert "sample_00/assembly.txt" in str(excinfo.value)

    def test_the_refusal_names_the_import_call(self, tmp_path):
        lib = _samples(tmp_path)
        with pytest.raises(GivenNotImportedError) as excinfo:
            _plan(tmp_path, lib)
        message = str(excinfo.value)
        assert "metasmith data import" in message
        assert "GivenLibrary" in message

    def test_a_value_written_here_is_refused(self, tmp_path):
        # `AddValue` writes the file and then reads the mtime it just created,
        # so byte-identical content gets a fresh identity on every call. It is
        # the same defect as an unstattable path wearing different clothes.
        types_path = _build_type_lib(tmp_path / "types.yml")
        lib = DataInstanceLibrary(tmp_path / "values.xgdb")
        lib.AddTypeLibrary(types_path, namespace="mock")
        lib.AddValue("sample_00.txt", ">s\nACGT\n", "mock::assembly")

        with pytest.raises(GivenNotImportedError) as excinfo:
            _plan(tmp_path, lib)
        assert "sample_00.txt" in str(excinfo.value)

    def test_an_imported_given_plans(self, tmp_path):
        lib = pool_backed(_samples(tmp_path))
        plan = _plan(tmp_path, lib)
        assert plan.given, "the plan took no givens"
        assert all(inst.origin == "imported" for inst in plan.given)

    def test_a_library_that_is_a_record_rather_than_a_mint_plans(self, tmp_path):
        # The deliberate limit of this refusal. It asks whether THIS process
        # invented the identity, not whether a pool holds it, because a shipped
        # template and the env and container libraries all carry recorded leaf
        # ids and none of them is data a pool should hold. A library saved and
        # loaded again is a record by the same test.
        lib = _samples(tmp_path)
        lib.Save()
        reloaded = DataInstanceLibrary.Load(lib.location)
        plan = _plan(tmp_path, reloaded)
        assert plan.given

    def test_every_offender_is_named_once(self, tmp_path):
        types_path = _build_type_lib(tmp_path / "types.yml")
        lib = DataInstanceLibrary(tmp_path / "many.xgdb")
        lib.AddTypeLibrary(types_path, namespace="mock")
        for i in range(3):
            sdir = lib.location / f"sample_{i:02d}"
            sdir.mkdir(parents=True, exist_ok=True)
            (sdir / "assembly.txt").write_text(f">s{i}\nACGT\n", encoding="utf-8")
            lib.AddItem(Path(f"sample_{i:02d}/assembly.txt"), "mock::assembly")

        with pytest.raises(GivenNotImportedError) as excinfo:
            _plan(tmp_path, lib)
        message = str(excinfo.value)
        assert "3 of these are not" in message
        for i in range(3):
            assert message.count(f"sample_{i:02d}/assembly.txt") == 1


class TestWhatTheRefusalMustNotReach:
    def test_a_transforms_own_registrations_still_work(self, tmp_path, virtual_runtime):
        # The registration sites inside transforms are product slots and the
        # markers a transform writes for its own outputs. They are mints by
        # construction and they are not givens, so a run that makes them must
        # be untouched by a rule about what a plan may be given.
        bp = build_linear_plan(tmp_path, n_steps=2)
        _task, lib = run_and_load(virtual_runtime, bp)
        assert lib._trace.events, "the run registered nothing"
        assert all(e.status == "promoted" for e in lib._trace.events)

    def test_a_deferred_given_is_not_refused_here(self, tmp_path):
        # There is nothing at a deferred path yet, so there is nothing to
        # import. `StageWorkflow.RefuseIfDeferred` is what catches one that
        # never got a source, and it names the row and the path when it does.
        lib = pool_backed(_samples(tmp_path))
        lib.AddItem(DEFERRED, "mock::reads")
        plan = _plan(tmp_path, lib)
        assert plan.given
