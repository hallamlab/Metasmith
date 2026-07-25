"""Route-level tests for the GUI backend, driven against a temporary project.

These go through the HTTP surface rather than calling the store directly,
because the contract that matters is the one the page sees.
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.testing.mock_transforms import identity_transform

from metasmith.gui.app import create_app
from metasmith.gui.store import Project
from metasmith.ops import workspace as op_workspace

from tests.integration.conftest import create_transform_library


@pytest.fixture
def project_root(tmp_path) -> Path:
    """A project with a stand-in standard library already in place.

    The GUI clones the real one; here it is fabricated so the tests never touch
    the network.
    """
    root = tmp_path / "project"
    mlib = root / "MetasmithLibraries"
    (mlib / "data_types").mkdir(parents=True)

    types = DataTypeLibrary()
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    types["bam"] = Endpoint(properties={"bam"})
    types["unreachable"] = Endpoint(properties={"unreachable"})
    types_path = mlib / "data_types" / "mock.yml"
    types.Save(types_path)

    # writes <dir>/transforms.xgdb, which is what discover() picks up
    create_transform_library(
        mlib / "transforms", types_path,
        identity_transform("mock::assembly", "mock::bam"),
    )
    (mlib / "resources").mkdir()
    return root


@pytest.fixture
def client(project_root, tmp_path):
    app = create_app(project_root, ssh_config_path=tmp_path / "ssh_config", watch=False)
    app.config["TESTING"] = True
    with app.test_client() as c:
        c.application = app
        yield c


def _finish(client, job_summary, timeout=120) -> dict:
    """Block on a background job and return its final summary."""
    job = client.application.config["MSM_JOBS"].get(job_summary["id"])
    assert job.wait(timeout), f"job [{job_summary['id']}] did not finish"
    assert job.status == "done", f"job failed: {job.error}\n" + "\n".join(job.lines()[-20:])
    return job.result


def _seed_inputs(client, workflow: str, count: int = 2, prefix: str = "sample"):
    project = client.application.config["MSM_PROJECT"]
    lib_path = project.input_library_path(workflow)
    for i in range(count):
        f = lib_path / f"{prefix}_{i}.fa"
        f.write_text(f">contig_{i}\nACGT\n")
        r = client.post(f"/api/workflows/{workflow}/inputs/items", json={
            "path": str(f), "dtype": "mock::assembly",
        })
        assert r.status_code == 201, r.get_json()


def _make_workflow(client, name=None, sample="mock::assembly", targets=("mock::bam",)) -> str:
    r = client.post("/api/workflows", json={
        "name": name, "sample_type": sample, "target_types": list(targets),
    })
    assert r.status_code == 201, r.get_json()
    return r.get_json()["name"]


# ---------------------------------------------------------------------------
# project + agents
# ---------------------------------------------------------------------------


class TestProject:
    def test_project_reports_stdlib(self, client):
        body = client.get("/api/project").get_json()
        assert body["stdlib"]["present"] is True
        assert len(body["stdlib"]["transform_libraries"]) == 1

    def test_types_are_listed(self, client):
        names = {t["full_name"] for t in client.get("/api/project/types").get_json()}
        assert {"mock::assembly", "mock::bam"} <= names


class TestAgents:
    def test_create_list_get(self, client, tmp_path):
        r = client.post("/api/agents", json={
            "name": "smith", "home": str(tmp_path / "home"), "runtime": "DOCKER",
        })
        assert r.status_code == 201, r.get_json()
        listed = client.get("/api/agents").get_json()
        assert [a["name"] for a in listed] == ["smith"]
        assert client.get("/api/agents/smith").get_json()["runtime"] == "DOCKER"

    def test_duplicate_refused(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        assert r.status_code == 409
        assert "already exists" in r.get_json()["error"]

    def test_delete(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.delete("/api/agents/smith")
        assert r.get_json()["action"] == "deleted"
        assert client.get("/api/agents").get_json() == []

    def test_deploy_is_a_job(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        with mock.patch("metasmith.ops.agent.deploy") as m:
            m.return_value = {"status": "deployed"}
            r = client.post("/api/agents/smith/deploy", json={})
            assert r.status_code == 202
            assert _finish(client, r.get_json())["status"] == "deployed"
        m.assert_called_once()

    def test_archive_hides_from_list(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        client.post("/api/agents/smith/archive", json={"archived": True})
        assert client.get("/api/agents").get_json() == []
        assert len(client.get("/api/agents?archived=1").get_json()) == 1


# ---------------------------------------------------------------------------
# workflows
# ---------------------------------------------------------------------------


class TestWorkflows:
    def test_generated_name_is_readable(self, client):
        name = _make_workflow(client)
        assert "-" in name and name.islower()

    def test_creates_an_editable_input_library(self, client):
        name = _make_workflow(client)
        body = client.get(f"/api/workflows/{name}").get_json()
        assert body["input_library"]["exists"] is True
        assert Path(body["input_library"]["path"]).name == "input.xgdb"

    def test_add_and_remove_inputs(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, 2)
        items = client.get(f"/api/workflows/{name}/inputs").get_json()["items"]
        assert len(items) == 2
        r = client.delete(
            f"/api/workflows/{name}/inputs/items", query_string={"path": items[0]["path"]},
        )
        assert r.status_code == 200
        assert len(client.get(f"/api/workflows/{name}/inputs").get_json()["items"]) == 1

    def test_generate_success(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name)
        r = client.post(f"/api/workflows/{name}/generate", json={})
        assert r.status_code == 202
        result = _finish(client, r.get_json())
        assert result["success"] is True

        body = client.get(f"/api/workflows/{name}").get_json()
        assert body["success"] is True
        assert body["task_key"] == result["task_key"]
        assert body["step_count"] >= 1

    def test_generate_writes_the_bundle_at_the_workflow_root(self, client):
        """The workflow directory must itself be a task reference.

        This is what `metasmith workflow stage AGENT workflows/<name>` addresses.
        """
        name = _make_workflow(client)
        _seed_inputs(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())

        wf_dir = Path(client.get(f"/api/workflows/{name}").get_json()["path"])
        assert (wf_dir / "task.yml").is_file()
        assert (wf_dir / "data").is_dir()
        assert op_workspace.is_task_dir(wf_dir)

        task = op_workspace.load_task(None, str(wf_dir))
        assert task.GetKey() == client.get(f"/api/workflows/{name}").get_json()["task_key"]

    def test_no_stray_task_key_directory_is_left_behind(self, client, project_root):
        name = _make_workflow(client)
        _seed_inputs(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert [p.name for p in (project_root / "workflows").iterdir()] == [name]
        wf_dir = project_root / "workflows" / name
        assert not (wf_dir / ".staging").exists()

    def test_two_workflows_with_identical_inputs_can_generate_at_once(self, client):
        """Two generates in flight at once must both land.

        Two things would otherwise break. Identical inputs deliberately produce
        the same task key, and the planner always writes to
        <workspace>/<task_key> -- a shared workspace would have them fighting
        over one directory. And planning is not reentrant: transforms are
        imported by bare module name through process-global state, so the two
        plans have to be serialised.
        """
        project = client.application.config["MSM_PROJECT"]
        shared = project.root / "shared.fa"
        shared.write_text(">x\nACGT\n")
        names = [_make_workflow(client) for _ in range(2)]
        for n in names:
            client.post(f"/api/workflows/{n}/inputs/items", json={
                "path": str(shared), "dtype": "mock::assembly",
            })

        # generate returns 202 immediately, so both jobs are in flight at once
        jobs = {n: client.post(f"/api/workflows/{n}/generate", json={}).get_json() for n in names}
        keys = {n: _finish(client, jobs[n])["task_key"] for n in names}
        assert len(set(keys.values())) == 1, "same inputs should give the same key"
        for n in names:
            body = client.get(f"/api/workflows/{n}").get_json()
            assert body["success"] is True
            assert (Path(body["path"]) / "task.yml").is_file()
            assert op_workspace.load_task(None, body["path"]).GetKey() == keys[n]

    def test_regenerating_replaces_the_bundle(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        first = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        _seed_inputs(client, name, 2, prefix="more")
        second = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert first["task_key"] != second["task_key"]

        wf_dir = Path(client.get(f"/api/workflows/{name}").get_json()["path"])
        assert op_workspace.load_task(None, str(wf_dir)).GetKey() == second["task_key"]

    def test_generate_failure_persists_hints(self, client):
        name = _make_workflow(client, targets=["mock::unreachable"])
        _seed_inputs(client, name)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"] is False
        assert result["hints"]

        # the failure state must survive a reload -- a failed plan has no bundle,
        # so without the result record it would simply be lost
        body = client.get(f"/api/workflows/{name}").get_json()
        assert body["planned"] is True
        assert body["success"] is False
        assert body["result"]["hints"]
        assert body["request"]["target_types"] == ["mock::unreachable"]

    def test_generate_requires_a_sample_type(self, client):
        r = client.post("/api/workflows", json={"target_types": ["mock::bam"]})
        name = r.get_json()["name"]
        assert client.post(f"/api/workflows/{name}/generate", json={}).status_code == 400

    def test_fork_changes_the_task_key(self, client):
        """The whole point: same paths, same bytes, different identity."""
        name = _make_workflow(client)
        _seed_inputs(client, name)
        original = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())

        forked = client.post(f"/api/workflows/{name}/fork", json={}).get_json()["name"]
        assert client.get(f"/api/workflows/{forked}").get_json()["forked_from"] == name

        after = _finish(client, client.post(f"/api/workflows/{forked}/generate", json={}).get_json())
        assert after["success"] is True
        assert after["task_key"] != original["task_key"]

    def test_unforked_copy_would_collide(self, client):
        """The behaviour the fork exists to work around, pinned deliberately."""
        a = _make_workflow(client)
        b = _make_workflow(client)
        project = client.application.config["MSM_PROJECT"]
        for name in (a, b):
            lib = project.input_library_path(name)
            f = lib / "same.fa"
            f.write_text(">x\nACGT\n")
            client.post(f"/api/workflows/{name}/inputs/items", json={
                "path": str(f.resolve()), "dtype": "mock::assembly",
            })
        # different files at different paths -> different keys; now point both at one path
        shared = project.root / "shared.fa"
        shared.write_text(">x\nACGT\n")
        for name in (a, b):
            lib = project.input_library_path(name)
            items = client.get(f"/api/workflows/{name}/inputs").get_json()["items"]
            client.delete(f"/api/workflows/{name}/inputs/items",
                          query_string={"path": items[0]["path"]})
            client.post(f"/api/workflows/{name}/inputs/items", json={
                "path": str(shared), "dtype": "mock::assembly",
            })
        ka = _finish(client, client.post(f"/api/workflows/{a}/generate", json={}).get_json())
        kb = _finish(client, client.post(f"/api/workflows/{b}/generate", json={}).get_json())
        assert ka["task_key"] == kb["task_key"]

    def test_delete_without_runs(self, client):
        name = _make_workflow(client)
        assert client.delete(f"/api/workflows/{name}").get_json()["action"] == "deleted"
        assert client.get("/api/workflows").get_json() == []


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------


@pytest.fixture
def runnable(client, tmp_path):
    """A workflow with a successful plan and an agent to run it on."""
    client.post("/api/agents", json={
        "name": "smith", "home": str(tmp_path / "home"), "runtime": "DOCKER",
    })
    name = _make_workflow(client)
    _seed_inputs(client, name)
    _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
    return name


class TestRuns:
    def _launch(self, client, workflow) -> dict:
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.StageWorkflow.return_value = None
            mload.return_value.ListWorkflowRuns.return_value = [{"run": 1}]
            r = client.post("/api/runs", json={"workflow": workflow, "agent": "smith"})
            assert r.status_code == 202, r.get_json()
            body = r.get_json()
            _finish(client, body["job"])
        return body["run"]

    def test_launch_records_agent_and_key(self, client, runnable):
        run = self._launch(client, runnable)
        body = client.get(f"/api/runs/{runnable}/{run['name']}").get_json()
        assert body["agent"] == "smith"
        assert body["state"] == "running"
        assert body["task_key"] == client.get(
            f"/api/workflows/{runnable}").get_json()["task_key"]

    def test_run_name_extends_the_workflow_name(self, client, runnable):
        run = self._launch(client, runnable)
        assert run["name"].startswith(f"{runnable}-")
        assert len(run["name"]) == len(runnable) + 6

    def test_two_runs_of_one_workflow_are_distinct(self, client, runnable):
        a = self._launch(client, runnable)
        b = self._launch(client, runnable)
        assert a["name"] != b["name"]
        # ... but the task key is deliberately the same: identity is the plan
        assert a["task_key"] == b["task_key"]

    def test_runs_list_is_newest_first(self, client, runnable):
        self._launch(client, runnable)
        self._launch(client, runnable)
        listed = client.get("/api/runs").get_json()
        assert len(listed) == 2
        assert listed[0]["created_at"] >= listed[1]["created_at"]

    def test_refuses_to_run_an_unplanned_workflow(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        name = _make_workflow(client)
        r = client.post("/api/runs", json={"workflow": name, "agent": "smith"})
        assert r.status_code == 409
        assert "no successful plan" in r.get_json()["error"]

    def test_delete_refused_while_live(self, client, runnable):
        run = self._launch(client, runnable)
        r = client.delete(f"/api/runs/{runnable}/{run['name']}")
        assert r.status_code == 409
        assert "cancel it before deleting" in r.get_json()["error"]

    def test_cancel_then_delete(self, client, runnable):
        run = self._launch(client, runnable)
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.CancelWorkflow.return_value = {"status": "cancelled"}
            assert client.post(
                f"/api/runs/{runnable}/{run['name']}/cancel", json={}).status_code == 200
        assert client.delete(
            f"/api/runs/{runnable}/{run['name']}").get_json()["action"] == "deleted"

    def test_workflow_with_runs_archives_instead_of_deleting(self, client, runnable):
        self._launch(client, runnable)
        r = client.delete(f"/api/workflows/{runnable}").get_json()
        assert r["action"] == "archived"
        assert "run(s)" in r["reason"]
        assert client.get("/api/workflows").get_json() == []
        assert len(client.get("/api/workflows?archived=1").get_json()) == 1

    def test_agent_with_runs_archives_instead_of_deleting(self, client, runnable):
        self._launch(client, runnable)
        r = client.delete("/api/agents/smith").get_json()
        assert r["action"] == "archived"
        assert r["dependents"]

    def test_collect_writes_into_the_runs_own_outputs(self, client, runnable):
        run = self._launch(client, runnable)
        seen = {}

        def _collect(agent_path, task_key, dest_uri, allow_globus=True):
            seen["dest"] = dest_uri
            seen["globus"] = allow_globus
            Path(dest_uri).mkdir(parents=True, exist_ok=True)
            return {"completed": [], "errors": []}

        with mock.patch("metasmith.ops.runtime.collect", side_effect=_collect):
            r = client.post(f"/api/runs/{runnable}/{run['name']}/collect", json={})
            _finish(client, r.get_json())
        # a fast transfer logs nothing on its own; an empty job log reads as
        # "nothing happened" rather than "already done"
        lines = client.application.config["MSM_JOBS"].get(r.get_json()["id"]).lines()
        assert any("collecting results" in ln for ln in lines)
        assert seen["dest"].endswith(f"{run['name']}/outputs")
        # everything goes over ssh; globus is deliberately off
        assert seen["globus"] is False
        assert client.get(
            f"/api/runs/{runnable}/{run['name']}").get_json()["collected_at"]

    def test_results_leads_with_the_uncollected_state(self, client, runnable):
        run = self._launch(client, runnable)
        body = client.get(f"/api/runs/{runnable}/{run['name']}/results").get_json()
        assert body["collected"] is False
        assert body["items"] == []

    def test_missing_agent_renders_as_a_named_absence(self, client, runnable):
        """Anything the GUI stores can be deleted from a shell."""
        run = self._launch(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        project.agent_path("smith").unlink()
        body = client.get(f"/api/runs/{runnable}/{run['name']}/log").get_json()
        assert "smith" in body["error"]


class TestResultsFiltering:
    def test_absolute_paths_are_treated_as_inputs(self, client, runnable, tmp_path):
        run = self._name(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        outputs = project.outputs_path(runnable, run)

        external = tmp_path / "input.fa"
        external.write_text(">x\nACGT\n")
        types = DataTypeLibrary()
        types["assembly"] = Endpoint(properties={"assembly"})
        types["bam"] = Endpoint(properties={"bam"})
        tp = tmp_path / "t.yml"
        types.Save(tp)

        lib = DataInstanceLibrary(outputs)
        lib.AddTypeLibrary(tp, namespace="mock")
        (outputs / "out.bam").write_text("bam")
        lib.AddItem(Path("out.bam"), "mock::bam")
        lib.AddItem(external, "mock::assembly")
        lib.Save()

        body = client.get(f"/api/runs/{runnable}/{run}/results").get_json()
        assert body["collected"] is True
        assert [i["path"] for i in body["items"]] == ["out.bam"]

    @staticmethod
    def _name(client, workflow) -> str:
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.ListWorkflowRuns.return_value = [{"run": 1}]
            body = client.post(
                "/api/runs", json={"workflow": workflow, "agent": "smith"}).get_json()
            _finish(client, body["job"])
        return body["run"]["name"]


class TestJobs:
    def test_log_lines_are_captured(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        from metasmith.logging import Log

        def _deploy(path, assertive=False):
            Log.Info("a distinctive line")
            return {"status": "deployed"}

        with mock.patch("metasmith.ops.agent.deploy", side_effect=_deploy):
            r = client.post("/api/agents/smith/deploy", json={})
            _finish(client, r.get_json())
        body = client.get(f"/api/jobs/{r.get_json()['id']}").get_json()
        assert any("a distinctive line" in ln for ln in body["lines"])

    def test_failure_is_reported_not_raised(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        with mock.patch("metasmith.ops.agent.deploy", side_effect=RuntimeError("boom")):
            r = client.post("/api/agents/smith/deploy", json={})
            job = client.application.config["MSM_JOBS"].get(r.get_json()["id"])
            assert job.wait(30)
        body = client.get(f"/api/jobs/{r.get_json()['id']}").get_json()
        assert body["status"] == "failed"
        assert body["error"] == "boom"

    def test_jobs_can_be_filtered_by_subject(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name)
        r = client.post(f"/api/workflows/{name}/generate", json={})
        _finish(client, r.get_json())
        listed = client.get("/api/jobs", query_string={"workflow": name}).get_json()
        assert [j["kind"] for j in listed] == ["generate"]
