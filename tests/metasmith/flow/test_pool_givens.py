"""A plan built from pool references keys the same every time.

This is the defect the pool exists to fix. A given's identity used to be the
file's path and mtime, and an input the client could not stat fell back to a
fresh uuid4 -- so two identical submissions minutes apart planned to different
keys, the run directory is named after the key, and nextflow's `-resume` found
nothing to resume. Measured on a live campaign: `daqL9cFU` then `WwhBaN3k` from
back-to-back dry runs of one driver.

A pool reference cannot do that. The identity was assigned when the data was
imported and is a record from then on, so the only way the key moves is if the
plan's shape moves.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents.pool import _PoolAccess
from metasmith.models.libraries import DataInstanceLibraryView, DataTypeLibrary
from metasmith.models.remote import Source
from metasmith.models.workflow import WorkflowPlan
from metasmith.ops import data as op_data
from metasmith.testing import mock_transforms as mt

from .conftest import (
    _MOCK_TYPE_PROPERTIES,
    _build_transform_lib,
    _build_type_lib,
    _make_target_model,
)


class _LocalAgent(_PoolAccess):
    def __init__(self, home: Path):
        self.home = Source.FromLocal(home)
        self.setup_commands = []

    def _is_ssh(self):
        return False


@pytest.fixture
def rig(tmp_path):
    """An agent whose pool holds one imported assembly, and a one-step library."""
    types_path = _build_type_lib(tmp_path / "types.yml")
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path,
        mt.identity_transform("mock::assembly", "mock::bam"),
    )
    home = tmp_path / "agent_home"
    home.mkdir()
    data = tmp_path / "data"
    data.mkdir()
    reads = data / "sample.txt"
    reads.write_text(">s\nACGT\n")
    imported = op_data.import_item(
        str(reads), "mock::assembly", agent_home=str(home), name="batch1/assembly",
    )
    return _LocalAgent(home), types_path, tr_lib, imported, tmp_path


def _plan(agent, types_path, tr_lib, location) -> WorkflowPlan:
    given = agent.GivenLibrary(
        ["batch1/assembly"], location=location,
        types={"mock": DataTypeLibrary.Load(types_path)},
    )
    plan = WorkflowPlan.Generate(
        given=[[DataInstanceLibraryView(given)]],
        transforms=[tr_lib],
        target_names=["bam"],
        target_model=_make_target_model([_MOCK_TYPE_PROPERTIES["bam"]]),
    )
    assert isinstance(plan, WorkflowPlan), f"planner did not converge: {plan!r}"
    return plan


def test_the_given_carries_the_pools_identity(rig):
    agent, types_path, tr_lib, imported, tmp_path = rig
    plan = _plan(agent, types_path, tr_lib, tmp_path / "given")
    assert [i.instance_id for i in plan.given] == [imported["instance_id"]]


def test_two_builds_of_one_reference_plan_to_one_key(rig):
    agent, types_path, tr_lib, _imported, tmp_path = rig
    first = _plan(agent, types_path, tr_lib, tmp_path / "given_a")
    second = _plan(agent, types_path, tr_lib, tmp_path / "given_b")
    assert first._key == second._key


def test_touching_the_data_does_not_move_the_key(rig):
    # The identity is a record of an import, not a reading of the file, so the
    # thing that used to cold-start a campaign now changes nothing.
    import os

    agent, types_path, tr_lib, _imported, tmp_path = rig
    first = _plan(agent, types_path, tr_lib, tmp_path / "given_a")
    os.utime(tmp_path / "data" / "sample.txt", (1, 1))
    second = _plan(agent, types_path, tr_lib, tmp_path / "given_b")
    assert first._key == second._key


def test_a_second_import_is_a_second_given_and_a_different_key(rig):
    # And the other direction: re-importing is how a caller says this is a new
    # thing, so the key SHOULD move when they do it.
    agent, types_path, tr_lib, imported, tmp_path = rig
    first = _plan(agent, types_path, tr_lib, tmp_path / "given_a")
    again = op_data.import_item(
        str(tmp_path / "data" / "sample.txt"), "mock::assembly",
        agent_home=str(agent.home.GetPath()), name="batch2/assembly",
    )
    assert again["instance_id"] != imported["instance_id"]

    given = agent.GivenLibrary(
        ["batch2/assembly"], location=tmp_path / "given_c",
        types={"mock": DataTypeLibrary.Load(types_path)},
    )
    second = WorkflowPlan.Generate(
        given=[[DataInstanceLibraryView(given)]],
        transforms=[tr_lib],
        target_names=["bam"],
        target_model=_make_target_model([_MOCK_TYPE_PROPERTIES["bam"]]),
    )
    assert first._key != second._key


class TestTheDriverFacingForm:
    """`PoolGivens`, which is what a driver writes instead of `AddItem`."""

    def _declared(self, agent, tmp_path, types_path, location, *, ensure=True):
        givens = agent.PoolGivens()
        meta = givens.Value(
            "run1/sample/metadata",
            {"parity": "paired"},
            "mock::sample_metadata",
        )
        givens.Add(
            tmp_path / "data" / "sample.txt", "mock::assembly",
            name="run1/sample/assembly", parents=[meta], tags=["run1"],
        )
        return givens.Build(
            location,
            types={"mock": DataTypeLibrary.Load(types_path)},
            ensure=ensure,
        )

    def test_a_declaration_plans_and_keys_the_same_twice(self, rig):
        agent, types_path, tr_lib, _imported, tmp_path = rig
        first = self._declared(agent, tmp_path, types_path, tmp_path / "g1")
        second = self._declared(agent, tmp_path, types_path, tmp_path / "g2")
        assert {str(p) for p in first.manifest} == {str(p) for p in second.manifest}
        assert [first.Get(p).instance_id for p in sorted(first.manifest)] == [
            second.Get(p).instance_id for p in sorted(second.manifest)
        ]

    def test_an_authored_value_lands_on_the_agent_not_beside_the_driver(self, rig):
        agent, types_path, _tr_lib, _imported, tmp_path = rig
        lib = self._declared(agent, tmp_path, types_path, tmp_path / "g1")
        authored = [
            p for p in lib.manifest if "metadata" in str(p)
        ]
        assert authored, "the authored document is not in the library"
        assert Path(authored[0]).is_relative_to(agent.home.GetPath())

    def test_citing_without_importing_refuses_by_name(self, rig):
        agent, types_path, _tr_lib, _imported, tmp_path = rig
        with pytest.raises(ValueError) as excinfo:
            self._declared(
                agent, tmp_path, types_path, tmp_path / "g1", ensure=False,
            )
        assert "metasmith data import" in str(excinfo.value)

    def test_the_plan_takes_it(self, rig):
        agent, types_path, tr_lib, _imported, tmp_path = rig
        given = self._declared(agent, tmp_path, types_path, tmp_path / "g1")
        plan = WorkflowPlan.Generate(
            given=[[DataInstanceLibraryView(given)]],
            transforms=[tr_lib],
            target_names=["bam"],
            target_model=_make_target_model([_MOCK_TYPE_PROPERTIES["bam"]]),
        )
        assert isinstance(plan, WorkflowPlan)
        assert all(inst.origin == "imported" for inst in plan.given)
