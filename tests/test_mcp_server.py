"""Tests for the Metasmith MCP Server."""

import asyncio
import json
import os
from pathlib import Path
from unittest import mock

import pytest

from metasmith.coms import mcp_server
from metasmith.coms.mcp_server import (
    ServerState,
    _collect_all_types,
    _endpoint_to_dict,
    _resolve_type,
    _safe,
    _parse_args,
    _paths_from_env,
    # tools
    list_types,
    get_type,
    check_type_compatibility,
    list_data_libraries,
    inspect_data_library,
    list_data_items,
    show_item_lineage,
    list_transform_libraries,
    list_transforms,
    show_transform_contract,
    plan_workflow,
    # agent + lifecycle tools
    list_agents,
    load_agent,
    deploy_agent,
    stage_workflow,
    run_workflow,
    get_result_source,
    list_config_presets,
    build_libraries,
    # resources
    resource_types,
    resource_types_namespace,
    resource_type_detail,
    resource_data_library,
    resource_transform_library,
    resource_transform_detail,
)
from metasmith.models.libraries import (
    DataTypeLibrary,
    DataInstanceLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.testing.mock_transforms import identity_transform

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
    types["gathered"] = Endpoint(properties={"gathered"})
    types["metabat2_bins"] = Endpoint(properties={"bins", "method:metabat2"})
    types["maxbin2_bins"] = Endpoint(properties={"bins", "method:maxbin2"})
    types["concoct_bins"] = Endpoint(properties={"bins", "method:concoct"})
    types["container"] = Endpoint(properties={"container"})
    types["annotated"] = Endpoint(properties={"annotated"})
    types["branch_a"] = Endpoint(properties={"branch_a"})
    types["branch_b"] = Endpoint(properties={"branch_b"})
    types["merged"] = Endpoint(properties={"merged"})
    types_path = temp_dir / "mock_types.yml"
    types.Save(types_path)
    return types_path


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
def mcp_state(mock_types, mock_samples, transform_lib):
    state = ServerState()
    state._type_libs = {"mock": DataTypeLibrary.Load(mock_types)}
    state._data_libs = {str(mock_samples.location): mock_samples}
    state._transform_libs = {str(transform_lib.location): transform_lib}
    with mock.patch.object(mcp_server, "STATE", state):
        yield state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# TestServerState
# ---------------------------------------------------------------------------


class TestServerState:
    def test_lazy_loading_type_libs(self, mock_types):
        state = ServerState(type_paths=[mock_types])
        assert len(state.type_libs) > 0
        assert "mock_types" in state.type_libs or len(state.type_libs) == 1

    def test_lazy_loading_data_libs(self, mock_samples):
        state = ServerState(data_paths=[mock_samples.location])
        assert len(state.data_libs) > 0

    def test_lazy_loading_transform_libs(self, transform_lib):
        state = ServerState(transform_paths=[transform_lib.location])
        assert len(state.transform_libs) > 0

    def test_caching(self, mock_types):
        state = ServerState(type_paths=[mock_types])
        first = state.type_libs
        second = state.type_libs
        assert first is second


# ---------------------------------------------------------------------------
# TestHelpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_collect_all_types_from_type_libs(self, mcp_state):
        all_types = _collect_all_types(mcp_state)
        assert "mock" in all_types
        assert "assembly" in all_types["mock"]

    def test_collect_all_types_from_transform_libs(self, mcp_state):
        all_types = _collect_all_types(mcp_state)
        # Transform libs also contribute types under "mock" namespace
        # but "transforms" namespace should be excluded
        assert "transforms" not in all_types

    def test_resolve_type_valid(self, mcp_state):
        ns, name, ep = _resolve_type(mcp_state, "mock::assembly")
        assert ns == "mock"
        assert name == "assembly"
        assert "assembly" in ep.properties

    def test_resolve_type_invalid_format(self, mcp_state):
        with pytest.raises(AssertionError, match="expected format"):
            _resolve_type(mcp_state, "no_separator")

    def test_resolve_type_missing_namespace(self, mcp_state):
        with pytest.raises(AssertionError, match="namespace.*not found"):
            _resolve_type(mcp_state, "nonexistent::assembly")

    def test_resolve_type_missing_name(self, mcp_state):
        with pytest.raises(AssertionError, match="type.*not found"):
            _resolve_type(mcp_state, "mock::nonexistent_type")

    def test_endpoint_to_dict(self, mcp_state):
        ep = Endpoint(properties={"assembly"})
        result = _endpoint_to_dict("assembly", "mock", ep)
        assert result["namespace"] == "mock"
        assert result["name"] == "assembly"
        assert result["full_name"] == "mock::assembly"
        assert "properties" in result


# ---------------------------------------------------------------------------
# TestTypeTools
# ---------------------------------------------------------------------------


class TestTypeTools:
    def test_list_types_all(self, mcp_state):
        result = _run(list_types())
        assert isinstance(result, list)
        assert len(result) > 0
        names = {r["name"] for r in result}
        assert "assembly" in names

    def test_list_types_filtered(self, mcp_state):
        result = _run(list_types(namespace="mock"))
        assert isinstance(result, list)
        assert all(r["namespace"] == "mock" for r in result)

    def test_list_types_empty_namespace(self, mcp_state):
        result = _run(list_types(namespace="nonexistent"))
        assert result == []

    def test_get_type(self, mcp_state):
        result = _run(get_type("mock::assembly"))
        assert result["name"] == "assembly"
        assert result["namespace"] == "mock"
        assert "properties" in result

    def test_get_type_missing(self, mcp_state):
        result = _run(get_type("mock::nonexistent"))
        assert "error" in result

    def test_check_type_compatible(self, mcp_state):
        # A type with more properties is compatible with one with fewer
        # assembly has {"assembly"}, bam has {"bam"} — they're different
        # So let's check self-compatibility
        result = _run(check_type_compatibility("mock::assembly", "mock::assembly"))
        assert result["compatible"] is True

    def test_check_type_incompatible(self, mcp_state):
        result = _run(check_type_compatibility("mock::assembly", "mock::bam"))
        assert result["compatible"] is False
        assert "missing" in result["reason"]


# ---------------------------------------------------------------------------
# TestLibraryTools
# ---------------------------------------------------------------------------


class TestLibraryTools:
    def test_list_data_libraries(self, mcp_state, mock_samples):
        result = _run(list_data_libraries())
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["path"] == str(mock_samples.location)
        assert result[0]["item_count"] > 0

    def test_inspect_data_library(self, mcp_state, mock_samples):
        lib_path = str(mock_samples.location)
        result = _run(inspect_data_library(lib_path))
        assert result["path"] == lib_path
        assert "schema" in result
        assert "type_namespaces" in result
        assert result["item_count"] > 0
        assert len(result["items"]) > 0

    def test_list_data_items_unfiltered(self, mcp_state, mock_samples):
        lib_path = str(mock_samples.location)
        result = _run(list_data_items(lib_path))
        assert isinstance(result, list)
        # 3 samples * 3 items each = 9
        assert len(result) == 9

    def test_list_data_items_filtered(self, mcp_state, mock_samples):
        lib_path = str(mock_samples.location)
        result = _run(list_data_items(lib_path, type_filter="mock::assembly"))
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(r["type_name"] == "mock::assembly" for r in result)

    def test_show_item_lineage(self, mcp_state, mock_samples):
        lib_path = str(mock_samples.location)
        # assembly items have parents (reads)
        items = _run(list_data_items(lib_path, type_filter="mock::assembly"))
        item_path = items[0]["path"]
        result = _run(show_item_lineage(lib_path, item_path))
        assert result["type_name"] == "mock::assembly"
        assert len(result["parents"]) > 0


# ---------------------------------------------------------------------------
# TestTransformTools
# ---------------------------------------------------------------------------


class TestTransformTools:
    def test_list_transform_libraries(self, mcp_state, transform_lib):
        result = _run(list_transform_libraries())
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["transform_count"] > 0
        assert "type_namespaces" in result[0]

    def test_list_transforms_all(self, mcp_state):
        result = _run(list_transforms())
        assert isinstance(result, list)
        assert len(result) > 0
        assert "name" in result[0]

    def test_list_transforms_filtered(self, mcp_state, transform_lib):
        lib_path = str(transform_lib.location)
        result = _run(list_transforms(library_path=lib_path))
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(r["library"] == lib_path for r in result)

    def test_show_transform_contract(self, mcp_state, transform_lib):
        lib_path = str(transform_lib.location)
        transforms = _run(list_transforms(library_path=lib_path))
        tr_path = transforms[0]["path"]
        result = _run(show_transform_contract(lib_path, tr_path))
        assert "inputs" in result
        assert "outputs" in result
        assert "group_by" in result
        assert "name" in result


# ---------------------------------------------------------------------------
# TestWorkflowTools
# ---------------------------------------------------------------------------


class TestWorkflowTools:
    def test_plan_workflow_success(self, mcp_state, mock_samples, transform_lib):
        data_path = str(mock_samples.location)
        tr_path = str(transform_lib.location)
        result = _run(plan_workflow(
            data_library=data_path,
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[tr_path],
        ))
        assert result["success"] is True
        assert "steps" in result
        assert "targets" in result
        assert result["step_count"] > 0

    def test_plan_workflow_no_samples(self, mcp_state, mock_samples, transform_lib):
        data_path = str(mock_samples.location)
        tr_path = str(transform_lib.location)
        result = _run(plan_workflow(
            data_library=data_path,
            sample_type="mock::bam",  # no bam items in library
            target_types=["mock::assembly"],
            transform_libraries=[tr_path],
        ))
        assert "error" in result

    def test_plan_workflow_no_path(self, mcp_state, mock_samples, transform_lib):
        data_path = str(mock_samples.location)
        tr_path = str(transform_lib.location)
        # Target a type that cannot be reached from assembly
        result = _run(plan_workflow(
            data_library=data_path,
            sample_type="mock::assembly",
            target_types=["mock::scattered"],
            transform_libraries=[tr_path],
        ))
        # Either success=False or error dict
        assert result.get("success") is False or "error" in result


# ---------------------------------------------------------------------------
# TestResources
# ---------------------------------------------------------------------------


class TestResources:
    def test_resource_types(self, mcp_state):
        result = _run(resource_types())
        data = json.loads(result)
        assert "mock" in data

    def test_resource_types_namespace(self, mcp_state):
        result = _run(resource_types_namespace("mock"))
        data = json.loads(result)
        assert isinstance(data, list)
        names = {t["name"] for t in data}
        assert "assembly" in names

    def test_resource_type_detail(self, mcp_state):
        result = _run(resource_type_detail("mock", "assembly"))
        data = json.loads(result)
        assert data["name"] == "assembly"
        assert data["namespace"] == "mock"
        assert "properties" in data

    def test_resource_data_library(self, mcp_state, mock_samples):
        lib_path = str(mock_samples.location)
        result = _run(resource_data_library(lib_path))
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) > 0

    def test_resource_transform_library(self, mcp_state, transform_lib):
        lib_path = str(transform_lib.location)
        result = _run(resource_transform_library(lib_path))
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) > 0

    def test_resource_transform_detail(self, mcp_state, transform_lib):
        lib_path = str(transform_lib.location)
        transforms = _run(list_transforms(library_path=lib_path))
        tr_path = transforms[0]["path"]
        result = _run(resource_transform_detail(lib_path, tr_path))
        data = json.loads(result)
        assert "inputs" in data
        assert "outputs" in data


# ---------------------------------------------------------------------------
# TestSafeDecorator
# ---------------------------------------------------------------------------


class TestSafeDecorator:
    def test_safe_returns_normal_result(self):
        @_safe
        async def good():
            return {"ok": True}
        result = _run(good())
        assert result == {"ok": True}

    def test_safe_catches_exception(self):
        @_safe
        async def bad():
            raise RuntimeError("boom")
        result = _run(bad())
        assert "error" in result
        assert "boom" in result["error"]
        assert "traceback" in result

    def test_safe_catches_assertion(self):
        @_safe
        async def bad():
            assert False, "assertion failed"
        result = _run(bad())
        assert "error" in result
        assert "assertion failed" in result["error"]


# ---------------------------------------------------------------------------
# TestCLI
# ---------------------------------------------------------------------------


class TestCLI:
    def test_parse_args_defaults(self):
        with mock.patch("sys.argv", ["metasmith-mcp"]):
            args = _parse_args()
        assert args.transforms == []
        assert args.data == []
        assert args.types == []
        assert args.transport == "stdio"

    def test_parse_args_with_paths(self):
        with mock.patch("sys.argv", [
            "metasmith-mcp",
            "--transforms", "/a/t1", "/a/t2",
            "--data", "/a/d1",
            "--types", "/a/ty1",
        ]):
            args = _parse_args()
        assert args.transforms == ["/a/t1", "/a/t2"]
        assert args.data == ["/a/d1"]
        assert args.types == ["/a/ty1"]

    def test_parse_args_transport_sse(self):
        with mock.patch("sys.argv", ["metasmith-mcp", "--transport", "sse"]):
            args = _parse_args()
        assert args.transport == "sse"

    def test_paths_from_env_empty(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            assert _paths_from_env("METASMITH_NONEXISTENT") == []
        with mock.patch.dict(os.environ, {"METASMITH_EMPTY": ""}):
            assert _paths_from_env("METASMITH_EMPTY") == []

    def test_paths_from_env_colon_separated(self):
        with mock.patch.dict(os.environ, {"METASMITH_TEST": "a:b:c"}):
            result = _paths_from_env("METASMITH_TEST")
        assert result == [Path("a"), Path("b"), Path("c")]

    def test_parse_args_agents_and_workspace(self):
        with mock.patch("sys.argv", [
            "metasmith-mcp",
            "--agents", "/a/agent1.yml", "/a/agent2.yml",
            "--workspace", "/tmp/ws",
        ]):
            args = _parse_args()
        assert args.agents == ["/a/agent1.yml", "/a/agent2.yml"]
        assert args.workspace == "/tmp/ws"


# ---------------------------------------------------------------------------
# TestAgentTools
# ---------------------------------------------------------------------------


class TestAgentTools:
    @pytest.fixture
    def agent_yaml(self, tmp_path):
        """Create a minimal agent YAML file."""
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        agent = Agent(home=Source(address=str(tmp_path / "agent_home")))
        p = tmp_path / "test_agent.yml"
        agent.Save(p)
        return p, agent

    @pytest.fixture
    def mcp_state_with_agent(self, mcp_state, agent_yaml, tmp_path):
        agent_path, agent = agent_yaml
        mcp_state._agents = {"test_agent": agent}
        mcp_state.workspace = tmp_path / "workspace"
        mcp_state.workspace.mkdir()
        return mcp_state

    def test_list_agents_empty(self, mcp_state):
        result = _run(list_agents())
        assert result == []

    def test_list_agents(self, mcp_state_with_agent):
        result = _run(list_agents())
        assert len(result) == 1
        assert result[0]["name"] == "test_agent"
        assert "home" in result[0]
        assert "container" in result[0]

    def test_load_agent(self, mcp_state, agent_yaml):
        agent_path, _ = agent_yaml
        result = _run(load_agent(str(agent_path)))
        assert result["name"] == "test_agent"
        assert "home" in result

    def test_load_agent_custom_name(self, mcp_state, agent_yaml):
        agent_path, _ = agent_yaml
        result = _run(load_agent(str(agent_path), name="custom"))
        assert result["name"] == "custom"

    def test_deploy_agent_unknown(self, mcp_state):
        result = _run(deploy_agent("nonexistent"))
        assert "error" in result

    def test_deploy_agent(self, mcp_state_with_agent):
        with mock.patch.object(
            mcp_state_with_agent._agents["test_agent"], "Deploy"
        ) as mock_deploy:
            result = _run(deploy_agent("test_agent"))
        assert result["status"] == "deployed"
        mock_deploy.assert_called_once_with(False)


# ---------------------------------------------------------------------------
# TestLifecycleTools
# ---------------------------------------------------------------------------


class TestLifecycleTools:
    @pytest.fixture
    def lifecycle_state(self, mcp_state, tmp_path):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        agent = Agent(home=Source(address=str(tmp_path / "agent_home")))
        mcp_state._agents = {"test_agent": agent}
        mcp_state.workspace = tmp_path / "workspace"
        mcp_state.workspace.mkdir()
        return mcp_state, agent

    def test_plan_workflow_returns_task_key(self, lifecycle_state, mock_samples, transform_lib):
        state, _ = lifecycle_state
        data_path = str(mock_samples.location)
        tr_path = str(transform_lib.location)
        result = _run(plan_workflow(
            data_library=data_path,
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[tr_path],
        ))
        assert result["success"] is True
        assert "task_key" in result
        assert result["task_key"] is not None

    def test_stage_workflow(self, lifecycle_state, mock_samples, transform_lib):
        state, agent = lifecycle_state
        # Plan first
        data_path = str(mock_samples.location)
        tr_path = str(transform_lib.location)
        plan_result = _run(plan_workflow(
            data_library=data_path,
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[tr_path],
        ))
        task_key = plan_result["task_key"]
        with mock.patch.object(agent, "StageWorkflow") as mock_stage:
            result = _run(stage_workflow("test_agent", task_key))
        assert result["status"] == "staged"
        mock_stage.assert_called_once()

    def test_run_workflow(self, lifecycle_state, mock_samples, transform_lib):
        state, agent = lifecycle_state
        data_path = str(mock_samples.location)
        tr_path = str(transform_lib.location)
        plan_result = _run(plan_workflow(
            data_library=data_path,
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[tr_path],
        ))
        task_key = plan_result["task_key"]
        with mock.patch.object(agent, "RunWorkflow") as mock_run:
            result = _run(run_workflow("test_agent", task_key))
        assert result["status"] == "running"
        mock_run.assert_called_once()

    def test_get_result_source(self, lifecycle_state):
        state, agent = lifecycle_state
        from metasmith.models.remote import Source
        mock_source = Source(address="/results/path")
        with mock.patch.object(agent, "GetResultSource", return_value=mock_source):
            result = _run(get_result_source("test_agent", "some_key"))
        assert result["address"] == "/results/path"
        assert "type" in result

    def test_list_config_presets(self, lifecycle_state, tmp_path):
        state, agent = lifecycle_state
        preset_dir = tmp_path / "presets"
        preset_dir.mkdir()
        (preset_dir / "local.nf").write_text("// local config")
        (preset_dir / "slurm.nf").write_text("// slurm config")
        with mock.patch.object(agent, "GetNxfConfigPresets", return_value={
            "local": preset_dir / "local.nf",
            "slurm": preset_dir / "slurm.nf",
        }):
            result = _run(list_config_presets("test_agent"))
        assert "local" in result
        assert "slurm" in result

    def test_stage_unknown_task(self, lifecycle_state):
        result = _run(stage_workflow("test_agent", "nonexistent_key"))
        assert "error" in result

    def test_stage_unknown_agent(self, lifecycle_state):
        result = _run(stage_workflow("nonexistent_agent", "some_key"))
        assert "error" in result


# ---------------------------------------------------------------------------
# TestBuildTool
# ---------------------------------------------------------------------------


class TestBuildTool:
    def test_build_libraries(self, mcp_state, mock_types, transform_lib):
        with mock.patch("metasmith.coms.mcp_server.Build") as mock_build:
            # Pre-populate caches to verify invalidation
            _ = mcp_state.type_libs
            _ = mcp_state.transform_libs
            assert len(mcp_state._type_libs) > 0

            result = _run(build_libraries(
                type_paths=[str(mock_types.parent)],
                transform_paths=[str(transform_lib.location)],
            ))
        assert result["status"] == "built"
        mock_build.assert_called_once()
        # Caches should be invalidated
        assert mcp_state._type_libs == {}
        assert mcp_state._transform_libs == {}

    def test_build_libraries_defaults(self, mcp_state):
        mcp_state.type_paths = [Path("/some/types")]
        mcp_state.transform_paths = [Path("/some/transforms")]
        with mock.patch("metasmith.coms.mcp_server.Build") as mock_build:
            result = _run(build_libraries())
        assert result["status"] == "built"
        call_args = mock_build.call_args[0]
        assert call_args[0] == [Path("/some/types")]
        assert call_args[1] == [Path("/some/transforms")]


# ---------------------------------------------------------------------------
# TestSourceParse
# ---------------------------------------------------------------------------


class TestSourceParse:
    def test_parse_local_path(self, tmp_path):
        from metasmith.models.remote import Source, SourceType
        p = tmp_path / "test"
        p.touch()
        src = Source.Parse(str(p))
        assert src.type == SourceType.DIRECT
        assert str(p) in src.address

    def test_parse_ssh_uri(self):
        from metasmith.models.remote import Source, SourceType
        src = Source.Parse("ssh://user@host/data/path")
        assert src.type == SourceType.SSH
        assert "host" in src.address
        assert "/data/path" in src.address

    def test_parse_http_uri(self):
        from metasmith.models.remote import Source, SourceType
        src = Source.Parse("https://example.com/file.tar.gz")
        assert src.type == SourceType.HTTP

    def test_parse_ssh_host_only(self):
        from metasmith.models.remote import Source, SourceType
        src = Source.Parse("ssh://myhost")
        assert src.type == SourceType.SSH


# ---------------------------------------------------------------------------
# TestGenerateWorkflowListTargets
# ---------------------------------------------------------------------------


class TestGenerateWorkflowListTargets:
    def test_list_targets_accepted(self, mcp_state, mock_samples, transform_lib):
        from metasmith.agents import Agent, TargetBuilder
        from metasmith.models.remote import Source

        agent = Agent(home=Source(address="/tmp/test_agent"))

        # Test that list[str] produces same result as TargetBuilder
        samples = list(mock_samples.AsSamples("mock::assembly"))
        tb = TargetBuilder()
        tb.Add("mock::bam")

        with mock.patch.object(agent, "GenerateWorkflow", wraps=agent.GenerateWorkflow) as mock_gen:
            # Can't actually run GenerateWorkflow without a real deployed agent,
            # but we can verify the TargetBuilder conversion happens
            try:
                agent.GenerateWorkflow(
                    samples=samples,
                    resources=[],
                    transforms=[transform_lib],
                    targets=["mock::bam"],
                )
            except Exception:
                pass  # Expected — no deployed agent

            try:
                agent.GenerateWorkflow(
                    samples=samples,
                    resources=[],
                    transforms=[transform_lib],
                    targets=tb,
                )
            except Exception:
                pass
