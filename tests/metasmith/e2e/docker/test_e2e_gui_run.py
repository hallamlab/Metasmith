from __future__ import annotations

import re
import shutil
import time
from pathlib import Path

import pytest
import yaml

from metasmith.gui.app import create_app
from metasmith.gui.store import Project

pytestmark = pytest.mark.docker

REPO_ROOT = Path(__file__).resolve().parents[4]
EXAMPLES = REPO_ROOT / "src" / "metasmith" / "examples"

JOB_TIMEOUT_S = 600
RUN_TIMEOUT_S = 600

_RELAY = REPO_ROOT / "src/bash_relay/target/x86_64-unknown-linux-musl/release/msm_relay"


def _real_relay() -> bool:
    if not _RELAY.is_file():
        return False
    with open(_RELAY, "rb") as f:
        return f.read(4) == b"\x7fELF"


def _library(project_root: Path, image: str) -> None:
    lib = project_root / "MetasmithLibraries"
    shutil.copytree(EXAMPLES, lib / "transforms" / "examples")
    (lib / "data_types").mkdir(parents=True)
    for name in ("containers.yml", "examples.yml"):
        shutil.copy(EXAMPLES / "data_types" / name, lib / "data_types" / name)

    index_path = lib / "transforms" / "examples" / "_metadata" / "index.yml"
    index = yaml.safe_load(index_path.read_text())
    index.pop("remote_src", None)
    index_path.write_text(yaml.dump(index, sort_keys=False))

    from metasmith.models.libraries import DataInstanceLibrary

    resources = DataInstanceLibrary(lib / "resources" / "containers.xgdb")
    resources.AddTypeLibrary(lib / "data_types" / "containers.yml")
    declaration = (EXAMPLES / "metasmith.env").read_text()
    declaration = re.sub(r"^container:.*$", f"container: docker://{image}",
                         declaration, flags=re.M)
    (resources.location / "metasmith.env").write_text(declaration)
    resources.AddItem(Path("metasmith.env"), "containers::metasmith.env")
    resources.Save()


@pytest.fixture
def gui(tmp_path, docker_image):
    project_root = tmp_path / "project"
    project_root.mkdir()
    _library(project_root, docker_image)
    app = create_app(project_root, ssh_config_path=tmp_path / "ssh_config", watch=True)
    app.config["TESTING"] = True
    with app.test_client() as client:
        client.application = app
        yield client
    app.config["MSM_WATCHER"].stop()


def _await_job(client, summary: dict, timeout: float = JOB_TIMEOUT_S) -> dict:
    job = client.application.config["MSM_JOBS"].get(summary["id"])
    assert job.wait(timeout), f"job [{summary['id']}] did not finish in {timeout}s"
    assert job.status == "done", (
        f"job [{summary['id']}] failed: {job.error}\n" + "\n".join(job.lines()[-30:])
    )
    return job.result


def _await_state(client, workflow: str, run: str, timeout: float = RUN_TIMEOUT_S) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/api/runs/{workflow}/{run}").get_json()
        if not body["live"]:
            return body
        time.sleep(2)
    pytest.fail(f"run [{run}] was still {body['state']} after {timeout}s")


def _ready_to_launch(gui, tmp_path, docker_image) -> tuple[str, str]:
    name_file = tmp_path / "name.txt"
    name_file.write_text("world\n")

    agent = gui.post("/api/agents", json={}).get_json()
    assert agent["name"].endswith("-local"), agent["name"]
    assert agent["deployed"] is False
    agent_name = agent["name"]

    saved = gui.put(f"/api/agents/{agent_name}", json={
        "name": agent_name,
        "runtime": "DOCKER",
        "container": f"docker://{docker_image}",
    }).get_json()
    assert saved["container"] == f"docker://{docker_image}"

    wf_name = gui.post("/api/workflows", json={}).get_json()["name"]
    gui.put(f"/api/workflows/{wf_name}", json={
        "input_drafts": [{
            "id": "name", "mode": "file", "path": str(name_file), "name": "", "value": "",
            "dtype": "examples::name", "parents": [],
        }],
        "target_types": [{"type": "examples::greeting", "parents": []}],
    })
    result = _await_job(
        gui, gui.post(f"/api/workflows/{wf_name}/generate", json={}).get_json()
    )
    assert result["success"] is True, result
    assert result["step_count"] == 1

    refused = gui.post("/api/runs", json={"workflow": wf_name, "agent": agent_name})
    assert refused.status_code == 409
    assert "deployed" in refused.get_json()["error"], refused.get_json()

    _await_job(gui, gui.post(f"/api/agents/{agent_name}/deploy", json={}).get_json())
    after = gui.get(f"/api/agents/{agent_name}").get_json()
    assert after["deployed"] is True, after
    assert after["real_path"], "a deploy resolves the home and records it"
    return wf_name, agent_name


class TestGuiDrivenRun:
    def test_the_page_can_get_an_agent_ready_to_run_on(self, gui, tmp_path, docker_image):
        _ready_to_launch(gui, tmp_path, docker_image)

    @pytest.mark.skipif(
        not _real_relay(),
        reason="needs a real msm_relay; this suite's image builds a stub "
               "(./dev.sh -brc && ./dev.sh -br builds the real one)",
    )
    def test_a_run_started_from_the_page_reaches_completed(self, gui, tmp_path, docker_image):
        wf_name, agent_name = _ready_to_launch(gui, tmp_path, docker_image)

        launched = gui.post("/api/runs", json={"workflow": wf_name, "agent": agent_name})
        assert launched.status_code == 202, launched.get_json()
        body = launched.get_json()
        run_name = body["run"]["name"]
        _await_job(gui, body["job"])

        started = gui.get(f"/api/runs/{wf_name}/{run_name}").get_json()
        assert started["run_number"] == 1, started

        final = _await_state(gui, wf_name, run_name)
        assert final["state"] == "completed", final

        project: Project = gui.application.config["MSM_PROJECT"]
        home = Path(gui.get(f"/api/agents/{agent_name}").get_json()["real_path"])
        key = gui.get(f"/api/workflows/{wf_name}").get_json()["task_key"]
        produced = sorted((home / "runs" / key / "results").rglob("*.txt"))
        assert produced, f"nothing under [{home / 'runs' / key / 'results'}]"
        assert any("hello world" in p.resolve().read_text() for p in produced), (
            [str(p) for p in produced]
        )
        assert project.read_run(wf_name, run_name).state == "completed"

    def test_an_orphaned_launch_is_resolved_rather_than_stranded(self, gui):
        project: Project = gui.application.config["MSM_PROJECT"]
        wf = project.create_workflow(name="orphaned", request={})
        rec = project.create_run(wf.name, {
            "agent": "gone", "task_key": "k",
            "launched_by": "a-server-that-has-since-exited",
        })
        watcher = gui.application.config["MSM_WATCHER"]
        assert watcher.poll_once() == [{"run": rec.name, "state": "failed"}]
        body = gui.get(f"/api/runs/{wf.name}/{rec.name}").get_json()
        assert body["state"] == "failed"
        assert "is gone" in body["error"]
