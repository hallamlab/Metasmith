"""Tutorial replay tests — drive both tutorials end-to-end via the ops package.

Replaces test_mcp_tutorial_replay.py. The replays use a virtual mock chain
(`mock::reads -> mock::assembly -> mock::bam`) parallel to the real
NCBI/pangenome tutorials, so the test runs in CI without containers.
"""
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
from metasmith.testing.mock_transforms import alignment_transform

from metasmith.ops import (
    agent as op_agent,
    data as op_data,
    runtime as op_runtime,
    source as op_source,
    transforms as op_transforms,
    types as op_types,
    workflow as op_workflow,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tutorial_types(tmp_path) -> Path:
    types = DataTypeLibrary()
    types["sample_metadata"] = Endpoint(properties={"sample_metadata"})
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    types["bam"] = Endpoint(properties={"bam"})
    p = tmp_path / "mock.yml"
    types.Save(p)
    return p


@pytest.fixture
def workspace(tmp_path) -> str:
    ws = tmp_path / "ws"
    ws.mkdir()
    return str(ws)


@pytest.fixture
def transform_lib(tmp_path, tutorial_types) -> TransformInstanceLibrary:
    from tests.e2e.docker.conftest import create_transform_library
    return create_transform_library(tmp_path / "transforms", tutorial_types, alignment_transform())


@pytest.fixture
def samples_lib(tmp_path, tutorial_types) -> Path:
    lib_path = tmp_path / "samples.xgdb"
    lib = DataInstanceLibrary(lib_path)
    lib.AddTypeLibrary(tutorial_types, namespace="mock")
    for i in range(3):
        sid = f"s{i:02d}"
        (lib.location / sid).mkdir(parents=True)
        (lib.location / sid / "reads.fq").write_text(f"@{sid}\nACGT\n")
        (lib.location / sid / "assembly.fa").write_text(f">{sid}\nACGT\n")
        reads = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads")
        lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[reads])
    lib.Save()
    return lib_path


# ---------------------------------------------------------------------------
# Tutorial 1 — my_first_agent
# ---------------------------------------------------------------------------


class TestMyFirstAgentReplay:
    """Replay docs/source/tutorials/my_first_agent.rst purely via ops."""

    def test_build_input_library_from_scratch(self, tutorial_types, tmp_path):
        lib_path = tmp_path / "tutorial_inputs.xgdb"
        r = op_data.create_library(str(lib_path), type_library_paths=[str(tutorial_types)], purge=True)
        ns = r["type_namespaces"][0]
        for sid in ("K12", "Sakai", "O157"):
            res = op_data.add_value(
                str(lib_path), name=sid, value=f"GCF_{sid}",
                dtype=f"{ns}::sample_metadata",
            )
            assert res["dtype"] == f"{ns}::sample_metadata"

    def test_save_and_load_agent(self, tmp_path):
        yaml_path = tmp_path / "agents" / "local.yml"
        r = op_agent.save_agent(str(yaml_path), home_uri=str(tmp_path / "msm_home"), runtime="DOCKER")
        assert r["runtime"] == "DOCKER"
        info = op_agent.info(str(yaml_path))
        assert info["runtime"] == "DOCKER"
        assert info["home"] == str(tmp_path / "msm_home")

    def test_plan_workflow_returns_task_key(
        self, samples_lib, transform_lib, workspace,
    ):
        r = op_workflow.plan_workflow(
            data_library=str(samples_lib),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transform_lib.location)],
            workspace=workspace,
        )
        assert r["success"] is True
        assert r["task_key"]
        assert r["step_count"] >= 1

        again = op_workflow.get_plan(r["task_key"], workspace=workspace)
        assert again["step_count"] == r["step_count"]

        inventory = op_workflow.list_tasks(workspace=workspace)
        assert r["task_key"] in {t["task_key"] for t in inventory}

    def test_stage_run_wait_lifecycle(
        self, tmp_path, samples_lib, transform_lib, workspace,
    ):
        plan = op_workflow.plan_workflow(
            data_library=str(samples_lib),
            sample_type="mock::assembly",
            target_types=["mock::bam"],
            transform_libraries=[str(transform_lib.location)],
            workspace=workspace,
        )
        task_key = plan["task_key"]

        agent_path = tmp_path / "smith.yml"
        op_agent.save_agent(str(agent_path), home_uri=str(tmp_path / "home"))

        with mock.patch.object(op_runtime, "load_agent") as mload:
            fake = mock.MagicMock()
            fake.WaitForWorkflow.return_value = {
                "task_key": task_key, "status": "completed",
                "run_dir": "/fake/run", "elapsed_s": 1.2, "last_log_mtime": 0.0,
                "tail": ["run completed at 2026-05-18T03:18Z"],
            }
            fake.TailWorkflowLog.return_value = {
                "task_key": task_key, "source": "main", "run": None,
                "run_dir": "/fake/run", "file": "/fake/run/main.log",
                "exists": True, "lines": ["nextflow start", "nextflow finish"],
            }
            fake.CancelWorkflow.return_value = {
                "task_key": task_key, "method": "noop", "killed_pid": None,
                "status": "not_running", "detail": "PID.lock not present",
            }
            mload.return_value = fake

            staged = op_runtime.stage(str(agent_path), task_key, workspace=workspace)
            assert staged["status"] == "staged"
            fake.StageWorkflow.assert_called_once()

            ran = op_runtime.run(str(agent_path), task_key)
            assert ran["status"] == "running"
            fake.RunWorkflow.assert_called_once()

            waited = op_runtime.wait(str(agent_path), task_key, timeout_s=5)
            assert waited["status"] == "completed"

            tail = op_runtime.tail(str(agent_path), task_key, source="main", lines=10)
            assert tail["exists"] is True
            assert "nextflow finish" in tail["lines"][-1]

            cancel = op_runtime.cancel(str(agent_path), task_key)
            assert cancel["status"] == "not_running"

    def test_get_result_source(self, tmp_path):
        from metasmith.models.remote import Source
        agent_path = tmp_path / "smith.yml"
        op_agent.save_agent(str(agent_path), home_uri=str(tmp_path / "home"))
        with mock.patch.object(op_runtime, "load_agent") as mload:
            mload.return_value.GetResultSource.return_value = Source(address="/results/foo")
            r = op_runtime.result_source(str(agent_path), "some_key")
        assert r["address"] == "/results/foo"


# ---------------------------------------------------------------------------
# Tutorial 2 — custom_transforms
# ---------------------------------------------------------------------------


class TestCustomTransformsReplay:
    def test_add_new_output_type(self, tutorial_types):
        r = op_types.add_type(
            str(tutorial_types), name="ani_matrix",
            properties={"_": "ani matrix", "ext": "tsv"},
        )
        assert r["name"] == "ani_matrix"
        loaded = DataTypeLibrary.Load(tutorial_types)
        assert "ani_matrix" in loaded.types

    def test_scaffold_transform(self, transform_lib):
        r = op_transforms.scaffold_transform(
            str(transform_lib.location), name="fastani",
            inputs=["mock::assembly"], outputs=["mock::bam"],
            group_by="mock::assembly",
            resources={"cpus": 4, "memory_gb": 8, "duration_h": 2},
        )
        assert "fastani" in r["path"]
        assert "mock::assembly" in r["source"]
        assert "mock::bam" in r["source"]
        assert "Resources(cpus=4" in r["source"]

    def test_read_write_transform(self, transform_lib):
        op_transforms.scaffold_transform(
            str(transform_lib.location), name="myown",
            inputs=["mock::assembly"], outputs=["mock::bam"],
        )
        src = op_transforms.read_source(str(transform_lib.location), "myown")
        new = src["source"].replace("TODO", "echo modified")
        r = op_transforms.write_transform(str(transform_lib.location), "myown", new)
        assert r["bytes"] == len(new)
        again = op_transforms.read_source(str(transform_lib.location), "myown")
        assert "echo modified" in again["source"]

    def test_propagate_types(self, transform_lib, tutorial_types):
        r = op_transforms.propagate_types(str(transform_lib.location), [str(tutorial_types)])
        assert r["library"] == str(transform_lib.location)


# ---------------------------------------------------------------------------
# Source parsing
# ---------------------------------------------------------------------------


class TestSourceParsing:
    def test_parse_local(self, tmp_path):
        r = op_source.parse(str(tmp_path))
        assert r["type"] in ("DIRECT", "SYMLINK")
        assert str(tmp_path) in r["address"]

    def test_parse_ssh(self):
        r = op_source.parse("ssh://host/data/x")
        assert r["type"] == "SSH"

    def test_parse_http(self):
        r = op_source.parse("https://example.com/file.tar.gz")
        assert r["type"] == "HTTP"
