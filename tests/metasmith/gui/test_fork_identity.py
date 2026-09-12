from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.testing.mock_transforms import identity_transform

from metasmith.ops import (
    data as op_data,
    workflow as op_workflow,
    workspace as op_workspace,
)

from tests.metasmith.e2e.docker.conftest import create_transform_library

pytestmark = pytest.mark.gui


@pytest.fixture
def types_path(tmp_path) -> Path:
    types = DataTypeLibrary()
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    types["bam"] = Endpoint(properties={"bam"})
    p = tmp_path / "mock_types.yml"
    types.Save(p)
    return p


def _build_lib(location: Path, types_path: Path) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(location)
    lib.AddTypeLibrary(types_path, namespace="mock")
    for i in range(2):
        sid = f"sample_{i:02d}"
        d = lib.location / sid
        d.mkdir(parents=True, exist_ok=True)
        (d / "reads.fq").write_text(f">read_{i}\nACGT\n")
        (d / "assembly.fa").write_text(f">contig_{i}\nACGTACGT\n")
        reads = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads")
        lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[reads])
    lib.Save()
    return lib


@pytest.fixture
def fixed_lib(tmp_path, types_path) -> DataInstanceLibrary:
    return _build_lib(tmp_path / "samples.xgdb", types_path)


def _instance_ids(lib: DataInstanceLibrary) -> set[str]:
    return {lib.Get(p).instance_id for p, _name, _ep in lib.Iterate()}


class TestUnforkedIsUnchanged:
    def test_pack_omits_fork_id(self, fixed_lib):
        assert fixed_lib.fork_id is None
        assert "fork_id" not in fixed_lib.Pack()

    def test_key_is_stable_across_reloads(self, fixed_lib):
        # No golden constant to compare against: a library's key folds in its
        # instance ids, and those are the paths and mtimes of this machine's
        # files. Stability across reloads is the invariant that survives.
        first = DataInstanceLibrary.Load(fixed_lib.location).GetKey()
        assert DataInstanceLibrary.Load(fixed_lib.location).GetKey() == first

    def test_index_yaml_has_no_fork_key(self, fixed_lib):
        index = fixed_lib.location / DataInstanceLibrary._path_to_meta / "index.yml"
        raw = yaml.safe_load(index.read_text())
        assert set(raw.keys()) == {"schema", "manifest"}


class TestForkChangesIdentity:
    # One library, forked in place. Leaf ids are the path and the mtime, so a
    # second library built elsewhere differs for reasons that are not the fork.
    def test_key_and_instance_ids_change(self, tmp_path, types_path):
        lib = _build_lib(tmp_path / "a.xgdb", types_path)
        key_before, ids_before = lib.GetKey(), _instance_ids(lib)

        lib.fork_id = "deadbeef"
        lib._calculate_key()
        assert lib.GetKey() != key_before
        assert _instance_ids(lib).isdisjoint(ids_before)

    def test_task_key_changes(self, tmp_path, types_path):
        transforms = create_transform_library(
            tmp_path / "transforms", types_path,
            identity_transform("mock::assembly", "mock::bam"),
        )
        ws = tmp_path / "workspace"

        def _plan(lib: DataInstanceLibrary) -> str:
            r = op_workflow.plan_workflow(
                data_library=str(lib.location),
                sample_type="mock::assembly",
                target_types=["mock::bam"],
                transform_libraries=[str(transforms.location)],
                workspace=str(ws),
            )
            assert r["success"] is True
            return r["task_key"]

        lib = _build_lib(tmp_path / "a.xgdb", types_path)
        before = _plan(lib)
        assert _plan(lib) == before, "re-planning an untouched library moved the key"

        lib.fork_id = "deadbeef"
        lib.Save()
        assert _plan(lib) != before


class TestForkIdSurvivesRoundTrips:
    def test_save_load_save(self, fixed_lib):
        fixed_lib.fork_id = "abc123"
        fixed_lib.Save()

        reloaded = DataInstanceLibrary.Load(fixed_lib.location)
        assert reloaded.fork_id == "abc123"
        key = reloaded.GetKey()

        reloaded.Save()
        again = DataInstanceLibrary.Load(fixed_lib.location)
        assert again.fork_id == "abc123"
        assert again.GetKey() == key

    def test_survives_consolidate(self, tmp_path, types_path, fixed_lib):
        external = tmp_path / "external.fa"
        external.write_text(">x\nACGT\n")
        fixed_lib.AddItem(external, "mock::assembly")
        fixed_lib.fork_id = "abc123"
        fixed_lib.Save()

        op_data.consolidate(str(fixed_lib.location))
        assert DataInstanceLibrary.Load(fixed_lib.location).fork_id == "abc123"

    def test_prune_types_preserves(self, fixed_lib):
        fixed_lib.fork_id = "abc123"
        fixed_lib.Save()
        op_data.prune_types(str(fixed_lib.location))
        assert DataInstanceLibrary.Load(fixed_lib.location).fork_id == "abc123"


class TestForkLibraryOp:
    def test_fork_stamps_id_and_changes_key(self, tmp_path, fixed_lib):
        dest = tmp_path / "forked.xgdb"
        r = op_data.fork_library(str(fixed_lib.location), str(dest))
        assert r["fork_id"]
        assert r["key"] != fixed_lib.GetKey()

        reloaded = DataInstanceLibrary.Load(dest)
        assert reloaded.fork_id == r["fork_id"]
        assert reloaded.GetKey() == r["key"]
        assert set(reloaded.manifest.keys()) == set(fixed_lib.manifest.keys())

    def test_explicit_fork_id(self, tmp_path, fixed_lib):
        dest = tmp_path / "forked.xgdb"
        r = op_data.fork_library(str(fixed_lib.location), str(dest), fork_id="mine")
        assert r["fork_id"] == "mine"

    def test_two_forks_differ(self, tmp_path, fixed_lib):
        a = op_data.fork_library(str(fixed_lib.location), str(tmp_path / "f1.xgdb"))
        b = op_data.fork_library(str(fixed_lib.location), str(tmp_path / "f2.xgdb"))
        assert a["key"] != b["key"]

    def test_absolute_path_items_are_not_copied(self, tmp_path, fixed_lib):
        external = tmp_path / "big.fa"
        external.write_text(">x\n" + "ACGT" * 100 + "\n")
        fixed_lib.AddItem(external, "mock::assembly")
        fixed_lib.Save()

        dest = tmp_path / "forked.xgdb"
        op_data.fork_library(str(fixed_lib.location), str(dest))
        assert not (dest / "big.fa").exists()
        forked = DataInstanceLibrary.Load(dest)
        assert external in forked.manifest or Path("big.fa") not in forked.manifest

    def test_internal_files_are_hardlinked(self, tmp_path, fixed_lib):
        dest = tmp_path / "forked.xgdb"
        op_data.fork_library(str(fixed_lib.location), str(dest))
        src_file = fixed_lib.location / "sample_00" / "reads.fq"
        dst_file = dest / "sample_00" / "reads.fq"
        assert dst_file.exists()
        assert os.stat(src_file).st_ino == os.stat(dst_file).st_ino

    def test_refuses_nonempty_destination(self, tmp_path, fixed_lib):
        dest = tmp_path / "occupied"
        dest.mkdir()
        (dest / "x").write_text("hi")
        with pytest.raises(AssertionError, match="not empty"):
            op_data.fork_library(str(fixed_lib.location), str(dest))

    def test_refuses_same_path(self, fixed_lib):
        with pytest.raises(AssertionError, match="must differ"):
            op_data.fork_library(str(fixed_lib.location), str(fixed_lib.location))


class TestTaskReferenceResolution:
    @pytest.fixture
    def planned(self, tmp_path, types_path, fixed_lib):
        transforms = create_transform_library(
            tmp_path / "transforms", types_path,
            identity_transform("mock::assembly", "mock::bam"),
        )
        ws = tmp_path / "workspace"
        r = op_workflow.plan_workflow(
            data_library=str(fixed_lib.location),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transforms.location)],
            workspace=str(ws),
        )
        assert r["success"] is True
        return ws, r["task_key"]

    def test_by_key(self, planned):
        ws, key = planned
        task = op_workspace.load_task(str(ws), key)
        assert task.GetKey() == key

    def test_by_directory(self, planned):
        ws, key = planned
        bundle = ws / key
        assert op_workspace.is_task_dir(bundle)
        task = op_workspace.load_task(None, str(bundle))
        assert task.GetKey() == key

    def test_directory_outside_workspace(self, tmp_path, planned):
        import shutil
        ws, key = planned
        elsewhere = tmp_path / "project" / "my-workflow"
        elsewhere.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(ws / key, elsewhere)
        task = op_workspace.load_task(None, str(elsewhere))
        assert task.GetKey() == key

    def test_non_task_dir_is_not_a_reference(self, tmp_path):
        plain = tmp_path / "plain"
        plain.mkdir()
        assert not op_workspace.is_task_dir(plain)
        assert not op_workspace.is_task_dir(tmp_path / "missing")

    def test_stage_returns_task_key_not_the_reference(self, tmp_path, planned):
        from unittest import mock
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        from metasmith.ops import runtime as op_runtime

        ws, key = planned
        agent = Agent(home=Source.FromLocal(tmp_path / "home"))
        agent_path = tmp_path / "smith.yml"
        agent.Save(agent_path)

        with mock.patch.object(op_runtime, "load_agent") as mload:
            mload.return_value = mock.MagicMock()
            r = op_runtime.stage(str(agent_path), str(ws / key), "skip", None)
        assert r["task_key"] == key
        mload.return_value.StageWorkflow.assert_called_once()

    def test_delete_refuses_a_path(self, planned):
        ws, key = planned
        with pytest.raises(AssertionError, match="only accepts a task key"):
            op_workspace.delete_task(str(ws), str(ws / key))
