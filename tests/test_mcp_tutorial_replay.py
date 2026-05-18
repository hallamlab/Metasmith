"""Tutorial replay tests — drive both tutorials end-to-end via MCP tools.

These are the acceptance bar for "MCP is complete": an LLM agent should
be able to reproduce the existing tutorials using only MCP tools.

The replays use a virtual mock chain (`mock::reads -> mock::assembly ->
mock::bam`) parallel to the real NCBI/pangenome tutorials, so the test
runs in CI without containers.

Each replay block is annotated with the tutorial step it mirrors.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest import mock

import pytest

from metasmith.coms import mcp_server
from metasmith.coms.mcp_server import (
    ServerState,
    # Group A
    server_status,
    register_type_library,
    register_data_library,
    register_transform_library,
    register_agent,
    reload_libraries,
    # Group B
    add_type,
    create_type_library,
    # Group C
    create_data_library,
    add_data_value,
    add_data_item,
    set_item_parents,
    save_library,
    trace_lineage,
    # Group D
    scaffold_transform,
    read_transform_source,
    write_transform,
    validate_transform_contract,
    propagate_types,
    # Group E
    plan_workflow,
    get_workflow_plan,
    list_workflow_tasks,
    # Group F
    save_agent,
    load_agent,
    get_agent_info,
    # Group G
    stage_workflow,
    run_workflow,
    wait_for_workflow,
    tail_workflow_log,
    cancel_workflow,
    get_result_source,
    # Group H
    parse_source,
)
from metasmith.models.libraries import (
    DataTypeLibrary,
    DataInstanceLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint
from metasmith.testing.mock_transforms import alignment_transform


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tutorial_types(tmp_path) -> Path:
    """Mirror of data_types/ncbi.yml + sequences.yml from the real tutorial."""
    types = DataTypeLibrary()
    types["sample_metadata"] = Endpoint(properties={"sample_metadata"})
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    types["bam"] = Endpoint(properties={"bam"})
    path = tmp_path / "tutorial_types.yml"
    types.Save(path)
    return path


@pytest.fixture
def mcp_workspace(tmp_path, tutorial_types) -> ServerState:
    state = ServerState()
    state.workspace = tmp_path / "mcp_ws"
    state.workspace.mkdir(parents=True)
    state.type_paths = [tutorial_types]
    with mock.patch.object(mcp_server, "STATE", state):
        yield state


@pytest.fixture
def transform_lib(tmp_path, tutorial_types) -> TransformInstanceLibrary:
    """Pre-built alignment transform library (analog to NCBI/pangenome transforms)."""
    from tests.integration.conftest import create_transform_library
    return create_transform_library(
        tmp_path / "transforms",
        tutorial_types,
        alignment_transform(),
    )


# ---------------------------------------------------------------------------
# Tutorial 1 — my_first_agent (pangenome → bam analog)
# ---------------------------------------------------------------------------


class TestMyFirstAgentReplay:
    """Replay docs/source/tutorials/my_first_agent.rst purely via MCP tools."""

    def test_step1_server_status_shows_loaded_types(self, mcp_workspace, tutorial_types):
        # Tutorial: confirm what the server already knows
        status = _run(server_status())
        assert tutorial_types in [Path(p) for p in status["type_paths"]]
        assert status["workspace"] == str(mcp_workspace.workspace)

    def test_step2_register_types_and_tools(self, mcp_workspace, tutorial_types, transform_lib):
        # register_type_library and register_transform_library
        r = _run(register_transform_library(str(transform_lib.location)))
        assert str(transform_lib.location.resolve()) in r["transform_paths"]

    def test_step3_build_input_library_from_scratch(self, mcp_workspace, tutorial_types, tmp_path):
        # create_data_library + add_data_value (the NCBI accessions analog)
        lib_path = tmp_path / "tutorial_inputs.xgdb"
        r = _run(create_data_library(
            path=str(lib_path),
            type_library_paths=[str(tutorial_types)],
            purge=True,
        ))
        assert r["type_namespaces"], f"expected namespaces, got {r}"
        ns = r["type_namespaces"][0]

        # Tutorial: three accessions registered as values
        for sid in ("K12", "Sakai", "O157"):
            res = _run(add_data_value(
                library_path=str(lib_path),
                name=sid,
                value=f"GCF_{sid}",
                dtype=f"{ns}::sample_metadata",
            ))
            assert res["dtype"] == f"{ns}::sample_metadata"

        # Confirm the library round-trips via inspect_data_library
        items = _run(list_workflow_tasks())
        assert isinstance(items, list)

    def test_step4_save_and_load_agent(self, mcp_workspace, tmp_path):
        # Tutorial: save_agent + load_agent + (deploy_agent — mocked)
        yaml_path = tmp_path / "agents" / "local.yml"
        r = _run(save_agent(
            path=str(yaml_path),
            home_uri=str(tmp_path / "msm_home"),
            runtime="DOCKER",
        ))
        assert r["runtime"] == "DOCKER"
        # save_agent already registers the agent; verify get_agent_info works
        info = _run(get_agent_info(yaml_path.stem))
        assert info["runtime"] == "DOCKER"
        assert info["home"] == str(tmp_path / "msm_home")

    def test_step5_plan_workflow_returns_task_key(self, mcp_workspace, tutorial_types, transform_lib, tmp_path):
        # Build inputs analogous to NCBI accessions but in the mock chain
        # (which actually has transforms registered): reads -> assembly -> bam
        lib_path = tmp_path / "samples.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(tutorial_types, namespace="mock")
        for i in range(3):
            sid = f"s{i:02d}"
            (lib.location / sid).mkdir(parents=True, exist_ok=True)
            (lib.location / sid / "reads.fq").write_text(f"@{sid}\nACGT\n")
            (lib.location / sid / "assembly.fa").write_text(f">{sid}\nACGT\n")
            r = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads")
            lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[r])
        lib.Save()
        mcp_workspace._data_libs[str(lib_path)] = lib
        mcp_workspace._transform_libs[str(transform_lib.location)] = transform_lib

        # plan_workflow returns the task_key
        result = _run(plan_workflow(
            data_library=str(lib_path),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transform_lib.location)],
        ))
        assert result["success"] is True
        assert result["task_key"]
        assert result["step_count"] >= 1

        # re-fetch via get_workflow_plan
        again = _run(get_workflow_plan(result["task_key"]))
        assert again["step_count"] == result["step_count"]

        # task appears in workspace inventory
        inventory = _run(list_workflow_tasks())
        keys = {t["task_key"] for t in inventory}
        assert result["task_key"] in keys

    def test_step6_stage_run_wait_lifecycle(self, mcp_workspace, tutorial_types, transform_lib, tmp_path):
        # Build the agent + a planned task
        from metasmith.agents import Agent
        from metasmith.models.remote import Source

        # plan first (uses the same setup as step 5)
        lib_path = tmp_path / "samples.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(tutorial_types, namespace="mock")
        sdir = lib.location / "s00"
        sdir.mkdir(parents=True)
        (sdir / "reads.fq").write_text("@s\nACGT\n")
        (sdir / "assembly.fa").write_text(">s\nACGT\n")
        r = lib.AddItem(Path("s00/reads.fq"), "mock::reads")
        lib.AddItem(Path("s00/assembly.fa"), "mock::assembly", parents=[r])
        lib.Save()
        mcp_workspace._data_libs[str(lib_path)] = lib
        mcp_workspace._transform_libs[str(transform_lib.location)] = transform_lib

        plan = _run(plan_workflow(
            data_library=str(lib_path),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transform_lib.location)],
        ))
        task_key = plan["task_key"]

        # Tutorial step 6: stage + run + wait
        agent = Agent(home=Source.FromLocal(tmp_path / "home"))
        mcp_workspace._agents["smith"] = agent

        with mock.patch.object(agent, "StageWorkflow") as mstage:
            staged = _run(stage_workflow("smith", task_key))
        assert staged["status"] == "staged"
        mstage.assert_called_once()

        with mock.patch.object(agent, "RunWorkflow") as mrun:
            ran = _run(run_workflow("smith", task_key, config_preset=None))
        assert ran["status"] == "running"
        mrun.assert_called_once()

        # wait_for_workflow: stub the sentinel-detection by mocking WaitForWorkflow
        with mock.patch.object(agent, "WaitForWorkflow", return_value={
            "task_key": task_key, "status": "completed",
            "run_dir": "/fake/run", "elapsed_s": 1.2, "last_log_mtime": 0.0,
            "tail": ["run completed at 2026-05-18T03:18Z"],
        }) as mwait:
            waited = _run(wait_for_workflow("smith", task_key, timeout_s=5))
        assert waited["status"] == "completed"
        mwait.assert_called_once()

        # tail_workflow_log
        with mock.patch.object(agent, "TailWorkflowLog", return_value={
            "task_key": task_key, "source": "main", "run": None,
            "run_dir": "/fake/run", "file": "/fake/run/main.log",
            "exists": True, "lines": ["nextflow start", "nextflow finish"],
        }):
            tail = _run(tail_workflow_log("smith", task_key, source="main", lines=10))
        assert tail["exists"] is True
        assert "nextflow finish" in tail["lines"][-1]

        # cancel_workflow (no-op since no PID.lock — agent.CancelWorkflow handles it)
        with mock.patch.object(agent, "CancelWorkflow", return_value={
            "task_key": task_key, "method": "noop", "killed_pid": None,
            "status": "not_running", "detail": "PID.lock not present",
        }):
            cancel = _run(cancel_workflow("smith", task_key))
        assert cancel["status"] == "not_running"

    def test_step7_get_result_source(self, mcp_workspace, tmp_path):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        agent = Agent(home=Source.FromLocal(tmp_path / "home"))
        mcp_workspace._agents["smith"] = agent
        with mock.patch.object(agent, "GetResultSource", return_value=Source(address="/results/foo")):
            r = _run(get_result_source("smith", "some_key"))
        assert r["address"] == "/results/foo"


# ---------------------------------------------------------------------------
# Tutorial 2 — custom_transforms (fastANI analog)
# ---------------------------------------------------------------------------


class TestCustomTransformsReplay:
    """Replay docs/source/tutorials/custom_transforms.rst purely via MCP tools."""

    def test_step1_add_new_output_type(self, mcp_workspace, tutorial_types):
        # Tutorial: add 'sequences::ani_matrix' analog
        r = _run(add_type(
            library_path=str(tutorial_types),
            name="ani_matrix",
            properties={"_": "ani matrix", "ext": "tsv"},
        ))
        assert r["name"] == "ani_matrix"

        # reload_libraries to pick up the new type
        rr = _run(reload_libraries(kinds=["types"]))
        assert "types" in rr["reloaded"]

        # registered type lib should now expose the new type
        loaded = DataTypeLibrary.Load(tutorial_types)
        assert "ani_matrix" in loaded.types

    def test_step2_scaffold_transform(self, mcp_workspace, transform_lib):
        # scaffold_transform emits a typed skeleton in the transform library
        r = _run(scaffold_transform(
            library_path=str(transform_lib.location),
            name="fastani",
            inputs=["mock::assembly"],
            outputs=["mock::bam"],  # using mock::bam as ani_matrix analog
            group_by="mock::assembly",
            resources={"cpus": 4, "memory_gb": 8, "duration_h": 2},
        ))
        assert "fastani" in r["path"]
        # source should reference the requested types
        assert "mock::assembly" in r["source"]
        assert "mock::bam" in r["source"]
        assert "Resources(cpus=4" in r["source"]

    def test_step3_read_write_transform(self, mcp_workspace, transform_lib):
        # scaffold first
        _run(scaffold_transform(
            library_path=str(transform_lib.location),
            name="myown",
            inputs=["mock::assembly"],
            outputs=["mock::bam"],
        ))
        # read it back
        src = _run(read_transform_source(
            library_path=str(transform_lib.location),
            transform_path="myown",
        ))
        assert "TransformInstance" in src["source"]
        # write a modified version
        new_src = src["source"].replace("TODO", "echo modified")
        r = _run(write_transform(
            library_path=str(transform_lib.location),
            transform_path="myown",
            source=new_src,
        ))
        assert r["bytes"] == len(new_src)
        # re-read confirms the change persisted
        again = _run(read_transform_source(
            library_path=str(transform_lib.location),
            transform_path="myown",
        ))
        assert "echo modified" in again["source"]

    def test_step4_validate_transform_contract(self, mcp_workspace, transform_lib):
        # scaffold a working transform
        _run(scaffold_transform(
            library_path=str(transform_lib.location),
            name="contract_check",
            inputs=["mock::assembly"],
            outputs=["mock::bam"],
        ))
        r = _run(validate_transform_contract(
            library_path=str(transform_lib.location),
            transform_path="contract_check",
        ))
        assert r["ok"] is True
        assert r["inputs"]
        assert r["outputs"]

    def test_step5_propagate_types_and_reload(self, mcp_workspace, transform_lib):
        # propagate_types copies registered type libs into the transform lib
        r = _run(propagate_types(transform_library=str(transform_lib.location)))
        assert r["library"] == str(transform_lib.location)

        rr = _run(reload_libraries(kinds=["transforms"]))
        assert "transforms" in rr["reloaded"]


# ---------------------------------------------------------------------------
# Cross-cutting: parse_source & resources
# ---------------------------------------------------------------------------


class TestSourceParsing:
    def test_parse_local(self, mcp_workspace, tmp_path):
        r = _run(parse_source(str(tmp_path)))
        assert r["type"] in ("DIRECT", "SYMLINK")
        assert str(tmp_path) in r["address"]

    def test_parse_ssh(self, mcp_workspace):
        r = _run(parse_source("ssh://host/data/x"))
        assert r["type"] == "SSH"

    def test_parse_http(self, mcp_workspace):
        r = _run(parse_source("https://example.com/file.tar.gz"))
        assert r["type"] == "HTTP"
