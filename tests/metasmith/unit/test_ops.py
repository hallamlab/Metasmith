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

from tests.metasmith.e2e.docker.conftest import create_transform_library


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
        import json
        assert r["format"] == "json"
        assert r["rendered"] is not None
        tree = json.loads(r["rendered"])
        assert tree["dtype_key"] == "mock::assembly"
        assert tree["produced_by"]["kind"] == "leaf"

    def test_show_item_lineage_keeps_manifest_parents_and_properties(self, mock_samples):
        items = op_data.list_items(str(mock_samples.location), type_filter="mock::assembly")
        r = op_data.show_item_lineage(str(mock_samples.location), items[0]["path"])

        assert r["properties"], "dtype properties dropped from the result"
        assert {p["type_name"] for p in r["parents"]} == {
            "mock::reads",
            "mock::sample_metadata",
        }

    def test_show_item_lineage_render_false_skips_the_trace_walk(self, mock_samples):
        items = op_data.list_items(str(mock_samples.location), type_filter="mock::assembly")
        path = items[0]["path"]

        with mock.patch.object(
            DataInstanceLibrary, "get_lineage_of", side_effect=AssertionError("walked")
        ):
            r = op_data.show_item_lineage(str(mock_samples.location), path, render=False)

        assert r["rendered"] is None
        assert r["type_name"] == "mock::assembly"
        assert {p["type_name"] for p in r["parents"]} == {
            "mock::reads",
            "mock::sample_metadata",
        }

    def test_create_and_add(self, tmp_path, mock_types):
        lib_path = tmp_path / "new.xgdb"
        r = op_data.create_library(str(lib_path), type_library_paths=[str(mock_types)])
        assert "mock_types" in r["type_namespaces"]
        f = lib_path / "data.txt"
        f.write_text("hi")
        rec = op_data.add_item(str(lib_path), str(f), "mock_types::assembly")
        assert rec["dtype"] == "mock_types::assembly"

    def test_resync_picks_up_a_type_added_to_an_existing_namespace(self, tmp_path, mock_types):
        lib_path = tmp_path / "new.xgdb"
        op_data.create_library(str(lib_path), type_library_paths=[str(mock_types)])
        lib = DataInstanceLibrary.Load(lib_path)
        with pytest.raises((AssertionError, ValueError, KeyError)):
            lib.GetType("mock_types::genome_name")

        types = DataTypeLibrary.Load(mock_types)
        types["genome_name"] = Endpoint(properties={"genome_name"})
        types.Save(mock_types)

        r = op_data.resync_type_libraries(str(lib_path), [str(mock_types)])
        assert "mock_types" in r["type_namespaces"]
        lib = DataInstanceLibrary.Load(lib_path)
        lib.GetType("mock_types::genome_name")


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
                sample_type="mock::bam",
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
        mdep.assert_called_once_with(False, on_phase=None)


class TestDefaultPreset:
    def test_the_shipped_presets_are_listed(self):
        assert "local" in op_agent.config_presets()

    def test_a_saved_preset_comes_back_on_info(self, tmp_path):
        p = tmp_path / "alice.yml"
        op_agent.save_agent(str(p), home_uri=str(tmp_path / "home"),
                            default_preset="slurm")
        assert op_agent.info(str(p))["default_preset"] == "slurm"

    def test_saving_without_one_clears_it(self, tmp_path):
        p = tmp_path / "alice.yml"
        op_agent.save_agent(str(p), home_uri=str(tmp_path / "home"),
                            default_preset="slurm")
        op_agent.save_agent(str(p), home_uri=str(tmp_path / "home"))
        assert op_agent.info(str(p))["default_preset"] is None

    def test_a_preset_that_no_longer_exists_says_which(self, tmp_path):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        agent = Agent(home=Source(address=str(tmp_path / "home")),
                      default_preset="no-such-preset")
        with pytest.raises(AssertionError, match="no-such-preset"):
            agent.RunWorkflow("some-task-key")


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

    def _run_kwargs(self, agent_path, key, **kwargs):
        with mock.patch.object(op_runtime, "load_agent") as mload:
            mload.return_value = mock.MagicMock()
            op_runtime.run(str(agent_path), key, **kwargs)
        return mload.return_value.RunWorkflow.call_args

    def test_the_dry_run_delay_reaches_the_delay_argument(self, planned):
        agent_path, key, _ = planned
        call = self._run_kwargs(agent_path, key, stub_delay=2.5)
        assert call.kwargs["stub_delay"] == 2.5
        assert "gpus" not in call.kwargs
        assert call.args == (key,)

    def test_a_numeric_override_key_addresses_one_step(self, planned):
        agent_path, key, _ = planned
        call = self._run_kwargs(
            agent_path, key,
            resource_overrides={"3": {"cpus": 8}, "sort_bam": {"cpus": 2}, "all": {"cpus": 1}},
        )
        ro = call.kwargs["resource_overrides"]
        assert 3 in ro and ro[3].cpus == 8
        assert "sort_bam" in ro
        assert "all" in ro


class TestAgentDefaultParams:
    def test_they_round_trip_as_a_mapping(self, tmp_path):
        p = tmp_path / "alice.yml"
        op_agent.save_agent(
            str(p), home_uri=str(tmp_path / "home"),
            default_params={"slurmAccount": "st-you-1", "process_tries": 3},
        )
        got = op_agent.info(str(p))["default_params"]
        assert got == {"slurmAccount": "st-you-1", "process_tries": 3}

    def test_saving_without_them_clears_them(self, tmp_path):
        p = tmp_path / "alice.yml"
        op_agent.save_agent(str(p), home_uri=str(tmp_path / "home"),
                            default_params={"a": 1})
        op_agent.save_agent(str(p), home_uri=str(tmp_path / "home"))
        assert op_agent.info(str(p))["default_params"] == {}

    def _agent(self, tmp_path, **kwargs):
        from metasmith.agents import Agent
        from metasmith.models.remote import Source
        return Agent(home=Source.FromLocal(tmp_path / "home"), **kwargs)

    def test_a_runs_params_win_per_key(self, tmp_path):
        agent = self._agent(tmp_path, default_params={"acct": "st-you-1", "keepMe": "yes"})
        assert agent._resolve_params({"acct": "other"}) == {
            "acct": "other", "keepMe": "yes",
        }

    def test_a_run_that_names_none_inherits_them(self, tmp_path):
        agent = self._agent(tmp_path, default_params={"acct": "st-you-1"})
        assert agent._resolve_params(None) == {"acct": "st-you-1"}

    def test_an_agent_with_none_changes_nothing(self, tmp_path):
        agent = self._agent(tmp_path)
        assert agent._resolve_params(None) is None
        assert agent._resolve_params({"a": 1}) == {"a": 1}

    def test_a_params_file_wins_whole(self, tmp_path):
        agent = self._agent(tmp_path, default_params={"acct": "st-you-1"})
        given = Path("/somewhere/params.yml")
        assert agent._resolve_params(given) is given


class TestCollect:
    def _library(self, tmp_path):
        work = tmp_path / "work"
        (work / "aa").mkdir(parents=True)
        (work / "aa" / "out.bam").write_text("bam bytes")
        logs = tmp_path / "logs.20260726"
        logs.mkdir()
        (logs / "agent.log").write_text("a log line")

        results = tmp_path / "results"
        (results / "_metadata").mkdir(parents=True)
        (results / "out.bam").symlink_to(work / "aa" / "out.bam")
        (results / "_metadata" / "index.yml").write_text("out.bam: {}\n")
        (results / "_metadata" / logs.name).symlink_to(logs)
        (results / "_metadata" / "logs.latest").symlink_to(logs)
        return results

    def _collect(self, tmp_path, results, dest):
        from metasmith.models.remote import Source
        agent = mock.MagicMock()
        agent.GetResultSource.return_value = Source.FromLocal(results)
        with mock.patch.object(op_runtime, "load_agent", return_value=agent):
            return op_runtime.collect(str(tmp_path / "a.yml"), "key", str(dest))

    def test_outputs_arrive_as_real_files(self, tmp_path):
        results = self._library(tmp_path)
        dest = tmp_path / "collected"
        out = self._collect(tmp_path, results, dest)
        assert out["errors"] == [], out["errors"]
        assert not (dest / "out.bam").is_symlink()
        assert (dest / "out.bam").read_text() == "bam bytes"

    def test_the_logs_come_too_and_the_alias_resolves_locally(self, tmp_path):
        results = self._library(tmp_path)
        dest = tmp_path / "collected"
        self._collect(tmp_path, results, dest)
        meta = dest / "_metadata"
        dirs = sorted(p.name for p in meta.glob("logs.*") if p.is_dir() and not p.is_symlink())
        assert dirs == ["logs.20260726"]
        assert (meta / "logs.20260726" / "agent.log").read_text() == "a log line"
        alias = meta / "logs.latest"
        assert alias.is_symlink()
        assert alias.resolve() == (meta / "logs.20260726").resolve()

    def test_a_broken_source_link_is_reported(self, tmp_path):
        results = self._library(tmp_path)
        (tmp_path / "work" / "aa" / "out.bam").unlink()
        dest = tmp_path / "collected"
        out = self._collect(tmp_path, results, dest)
        assert any("out.bam" in d for d in out["dangling"]), out
        assert not (dest / "out.bam").exists()


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


class TestWorkspace:
    def test_explicit_arg_wins(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METASMITH_WORKSPACE", str(tmp_path / "env"))
        ws = op_workspace.resolve_workspace(str(tmp_path / "explicit"))
        assert ws == tmp_path / "explicit"

    def test_env_fallback(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METASMITH_WORKSPACE", str(tmp_path / "env"))
        ws = op_workspace.resolve_workspace(None)
        assert ws == tmp_path / "env"
