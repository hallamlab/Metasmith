"""Tests for the metasmith.ops package. Ops are sync, path-driven, and stateless."""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint
from metasmith.testing.mock_transforms import identity_transform

from metasmith.ops import (
    agent as op_agent,
    data as op_data,
    runtime as op_runtime,
    source as op_source,
    transforms as op_transforms,
    types as op_types,
    workflow as op_workflow,
    workspace as op_workspace,
)

from tests.integration.conftest import create_transform_library


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def mock_types(temp_dir) -> Path:
    types = DataTypeLibrary()
    types["sample_metadata"] = Endpoint(properties={"sample_metadata"})
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    types["bam"] = Endpoint(properties={"bam"})
    types["scattered"] = Endpoint(properties={"scattered"})
    p = temp_dir / "mock_types.yml"
    types.Save(p)
    return p


@pytest.fixture
def mock_samples(temp_dir, mock_types) -> DataInstanceLibrary:
    lib_path = temp_dir / "samples.xgdb"
    lib = DataInstanceLibrary(lib_path)
    lib.AddTypeLibrary(mock_types, namespace="mock")
    for i in range(3):
        sid = f"sample_{i:02d}"
        d = lib.location / sid
        d.mkdir(parents=True, exist_ok=True)
        (d / "metadata.json").write_text(f'{{"id": "{sid}"}}')
        (d / "reads.fq").write_text(f">read_{i}\nACGT\n")
        (d / "assembly.fa").write_text(f">contig_{i}\nACGTACGT\n")
        meta = lib.AddItem(Path(f"{sid}/metadata.json"), "mock::sample_metadata")
        reads = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads", parents=[meta])
        lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[reads])
    lib.Save()
    return lib


@pytest.fixture
def transform_lib(temp_dir, mock_types) -> TransformInstanceLibrary:
    transforms = identity_transform("mock::assembly", "mock::bam")
    return create_transform_library(temp_dir / "transforms", mock_types, transforms)


@pytest.fixture
def workspace(tmp_path) -> Path:
    ws = tmp_path / "workspace"
    ws.mkdir()
    return ws


# ---------------------------------------------------------------------------
# Type ops
# ---------------------------------------------------------------------------


class TestTypeOps:
    def test_list_types(self, mock_types):
        result = op_types.list_types(type_paths=[str(mock_types)])
        names = {r["name"] for r in result}
        assert "assembly" in names
        assert "bam" in names

    def test_list_types_namespace_filter(self, mock_types):
        result = op_types.list_types(type_paths=[str(mock_types)], namespace="mock_types")
        assert all(r["namespace"] == "mock_types" for r in result)
        assert op_types.list_types(type_paths=[str(mock_types)], namespace="nope") == []

    def test_get_type(self, mock_types):
        r = op_types.get_type("mock_types::assembly", type_paths=[str(mock_types)])
        assert r["name"] == "assembly"
        assert r["full_name"] == "mock_types::assembly"

    def test_get_type_missing_raises(self, mock_types):
        with pytest.raises(AssertionError, match="type.*not found"):
            op_types.get_type("mock_types::nope", type_paths=[str(mock_types)])

    def test_get_type_bad_format(self, mock_types):
        with pytest.raises(AssertionError, match="expected format"):
            op_types.get_type("no_separator", type_paths=[str(mock_types)])

    def test_check_compatibility_self(self, mock_types):
        r = op_types.check_compatibility(
            "mock_types::assembly", "mock_types::assembly", type_paths=[str(mock_types)],
        )
        assert r["compatible"] is True

    def test_check_compatibility_incompatible(self, mock_types):
        r = op_types.check_compatibility(
            "mock_types::assembly", "mock_types::bam", type_paths=[str(mock_types)],
        )
        assert r["compatible"] is False
        assert "missing" in r["reason"]

    def test_create_and_add_type(self, tmp_path):
        p = tmp_path / "lib.yml"
        op_types.create_type_library(str(p))
        assert p.exists()
        r = op_types.add_type(str(p), "foo", {"a": "b"})
        assert r["name"] == "foo"

    def test_create_existing_raises(self, mock_types):
        with pytest.raises(AssertionError, match="already exists"):
            op_types.create_type_library(str(mock_types))


# ---------------------------------------------------------------------------
# Data ops
# ---------------------------------------------------------------------------


class TestDataOps:
    def test_inspect_library(self, mock_samples):
        r = op_data.inspect_library(str(mock_samples.location))
        assert r["item_count"] == 9
        assert len(r["items"]) == 9

    def test_list_items_filtered(self, mock_samples):
        r = op_data.list_items(str(mock_samples.location), type_filter="mock::assembly")
        assert len(r) == 3
        assert all(item["type_name"] == "mock::assembly" for item in r)

    def test_show_item_lineage(self, mock_samples):
        items = op_data.list_items(str(mock_samples.location), type_filter="mock::assembly")
        r = op_data.show_item_lineage(str(mock_samples.location), items[0]["path"])
        assert r["type_name"] == "mock::assembly"
        assert r["parents"]

    def test_create_and_add(self, tmp_path, mock_types):
        lib_path = tmp_path / "new.xgdb"
        r = op_data.create_library(str(lib_path), type_library_paths=[str(mock_types)])
        assert "mock_types" in r["type_namespaces"]
        f = lib_path / "data.txt"
        f.write_text("hi")
        rec = op_data.add_item(str(lib_path), str(f), "mock_types::assembly")
        assert rec["dtype"] == "mock_types::assembly"


# ---------------------------------------------------------------------------
# Transform ops
# ---------------------------------------------------------------------------


class TestTransformOps:
    def test_list_libraries(self, transform_lib):
        r = op_transforms.list_libraries([str(transform_lib.location)])
        assert r[0]["transform_count"] > 0

    def test_list_transforms(self, transform_lib):
        r = op_transforms.list_transforms([str(transform_lib.location)])
        assert r
        assert "name" in r[0]

    def test_show_contract(self, transform_lib):
        items = op_transforms.list_transforms([str(transform_lib.location)])
        r = op_transforms.show_contract(str(transform_lib.location), items[0]["path"])
        assert "inputs" in r and "outputs" in r and "group_by" in r

    def test_scaffold_creates_file(self, transform_lib):
        r = op_transforms.scaffold_transform(
            str(transform_lib.location), "scaffolded",
            inputs=["mock::assembly"], outputs=["mock::bam"],
            resources={"cpus": 2},
        )
        assert "scaffolded" in r["path"]
        assert "mock::assembly" in r["source"]
        assert "Resources(cpus=2" in r["source"]

    def test_read_write_roundtrip(self, transform_lib):
        op_transforms.scaffold_transform(
            str(transform_lib.location), "rwtest",
            inputs=["mock::assembly"], outputs=["mock::bam"],
        )
        src = op_transforms.read_source(str(transform_lib.location), "rwtest")
        new = src["source"].replace("TODO", "echo done")
        op_transforms.write_transform(str(transform_lib.location), "rwtest", new)
        again = op_transforms.read_source(str(transform_lib.location), "rwtest")
        assert "echo done" in again["source"]


# ---------------------------------------------------------------------------
# Workflow ops
# ---------------------------------------------------------------------------


class TestWorkflowOps:
    def test_plan_success_persists_task(self, mock_samples, transform_lib, workspace):
        r = op_workflow.plan_workflow(
            data_library=str(mock_samples.location),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transform_lib.location)],
            workspace=str(workspace),
        )
        assert r["success"] is True
        assert r["task_key"]
        assert (workspace / r["task_key"]).exists()

        again = op_workflow.get_plan(r["task_key"], workspace=str(workspace))
        assert again["step_count"] == r["step_count"]

        listed = op_workflow.list_tasks(workspace=str(workspace))
        assert r["task_key"] in {t["task_key"] for t in listed}

    def test_plan_no_samples_raises(self, mock_samples, transform_lib, workspace):
        with pytest.raises(AssertionError, match="no samples"):
            op_workflow.plan_workflow(
                data_library=str(mock_samples.location),
                sample_type="mock::bam",  # nothing of this type
                target_types=["mock::assembly"],
                transform_libraries=[str(transform_lib.location)],
                workspace=str(workspace),
            )

    def test_plan_unreachable_target_returns_hints(self, mock_samples, transform_lib, workspace):
        r = op_workflow.plan_workflow(
            data_library=str(mock_samples.location),
            sample_type="mock::assembly",
            target_types=["mock::scattered"],
            transform_libraries=[str(transform_lib.location)],
            workspace=str(workspace),
        )
        assert r["success"] is False

    def test_delete_task(self, mock_samples, transform_lib, workspace):
        r = op_workflow.plan_workflow(
            data_library=str(mock_samples.location),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transform_lib.location)],
            workspace=str(workspace),
        )
        key = r["task_key"]
        op_workflow.delete_task(key, workspace=str(workspace))
        assert not (workspace / key).exists()


# ---------------------------------------------------------------------------
# Agent + runtime ops (mocked)
# ---------------------------------------------------------------------------


class TestAgentOps:
    @pytest.fixture
    def agent_yaml(self, tmp_path):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        agent = Agent(home=Source(address=str(tmp_path / "agent_home")))
        p = tmp_path / "smith.yml"
        agent.Save(p)
        return p

    def test_save_then_info(self, tmp_path):
        p = tmp_path / "alice.yml"
        r = op_agent.save_agent(str(p), home_uri=str(tmp_path / "home"))
        assert r["runtime"] == "DOCKER"
        info = op_agent.info(str(p))
        assert info["runtime"] == "DOCKER"
        assert info["home"] == str(tmp_path / "home")

    def test_list_agents(self, agent_yaml):
        r = op_agent.list_agents([str(agent_yaml)])
        assert r[0]["name"] == "smith"

    def test_deploy_invokes_agent(self, agent_yaml):
        with mock.patch.object(op_agent.Agent, "Deploy") as mdep:
            r = op_agent.deploy(str(agent_yaml))
        assert r["status"] == "deployed"
        mdep.assert_called_once_with(False)


class TestRuntimeOps:
    @pytest.fixture
    def planned(self, tmp_path, mock_samples, transform_lib, workspace):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        agent = Agent(home=Source.FromLocal(tmp_path / "home"))
        agent_path = tmp_path / "smith.yml"
        agent.Save(agent_path)
        plan = op_workflow.plan_workflow(
            data_library=str(mock_samples.location),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transform_lib.location)],
            workspace=str(workspace),
        )
        return agent_path, plan["task_key"], workspace

    def test_stage_calls_agent(self, planned):
        agent_path, key, ws = planned
        with mock.patch.object(op_runtime, "load_agent") as mload:
            mload.return_value = mock.MagicMock()
            r = op_runtime.stage(str(agent_path), key, "skip", str(ws))
        assert r["status"] == "staged"
        mload.return_value.StageWorkflow.assert_called_once()

    def test_run_calls_agent(self, planned):
        agent_path, key, ws = planned
        with mock.patch.object(op_runtime, "load_agent") as mload:
            mload.return_value = mock.MagicMock()
            r = op_runtime.run(str(agent_path), key)
        assert r["status"] == "running"
        mload.return_value.RunWorkflow.assert_called_once()

    def test_result_source(self, planned):
        agent_path, key, _ = planned
        from metasmith.models.remote import Source
        with mock.patch.object(op_runtime, "load_agent") as mload:
            mload.return_value.GetResultSource.return_value = Source(address="/results/x")
            r = op_runtime.result_source(str(agent_path), key)
        assert r["address"] == "/results/x"


# ---------------------------------------------------------------------------
# Source ops
# ---------------------------------------------------------------------------


class TestSourceOps:
    def test_parse_local(self, tmp_path):
        p = tmp_path / "f"
        p.touch()
        r = op_source.parse(str(p))
        assert r["type"] in ("DIRECT", "SYMLINK")
        assert str(p) in r["address"]

    def test_parse_ssh(self):
        r = op_source.parse("ssh://user@host/a/b")
        assert r["type"] == "SSH"

    def test_parse_http(self):
        r = op_source.parse("https://example.com/x.tgz")
        assert r["type"] == "HTTP"

    def test_exists_local(self, tmp_path):
        p = tmp_path / "exists"
        p.touch()
        r = op_source.exists(str(p))
        assert r["exists"] is True
        miss = op_source.exists(str(tmp_path / "missing"))
        assert miss["exists"] is False


# ---------------------------------------------------------------------------
# Workspace resolution
# ---------------------------------------------------------------------------


class TestWorkspace:
    def test_explicit_arg_wins(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METASMITH_WORKSPACE", str(tmp_path / "env"))
        ws = op_workspace.resolve_workspace(str(tmp_path / "explicit"))
        assert ws == tmp_path / "explicit"

    def test_env_fallback(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METASMITH_WORKSPACE", str(tmp_path / "env"))
        ws = op_workspace.resolve_workspace(None)
        assert ws == tmp_path / "env"
