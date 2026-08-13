"""The web GUI's run path, driven for real: agent -> deploy -> solve -> launch.

Every other test that touches `POST /runs` patches `ops.runtime.load_agent` to a
`MagicMock`, so staging, launching and listing runs had never once executed from
the GUI -- only the record-keeping around them had. That is why a deploy that
hung forever on its own default setup block, a `run_number` read from a key
`ListWorkflowRuns` has never returned, and a launch accepted onto an agent with
nothing installed on it all went unnoticed together.

This is the answer to that, and it is deliberately excluded from both fast
suites: it wants a docker daemon and about a minute. Run it on purpose.

    pytest -m docker tests/e2e/docker/test_e2e_gui_run.py

The library is the repository's own `examples/`: one transform that turns a name
into a greeting with a single `echo`, whose tool environment is the metasmith
image itself. Small enough to run in seconds, real enough that nothing about the
path is simulated.
"""
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

# The greeting run is one `echo`; the wall clock is docker start-up and
# nextflow's own launch. Generous rather than tight -- a flaky timeout in a
# suite that is run deliberately teaches nothing.
JOB_TIMEOUT_S = 600
RUN_TIMEOUT_S = 600

# The image this suite builds carries a *stub* relay -- a shell script that
# echoes and exits -- because the real one is a Rust binary and the test
# infrastructure has never assumed a Rust toolchain. Staging connects to the
# relay's socket, so everything up to the launch is provable with the stub and
# the tool step is not. Rather than quietly not testing the run, the one test
# that needs a real relay says so and skips.
_RELAY = REPO_ROOT / "src/bash_relay/target/x86_64-unknown-linux-musl/release/msm_relay"


def _real_relay() -> bool:
    if not _RELAY.is_file():
        return False
    with open(_RELAY, "rb") as f:
        return f.read(4) == b"\x7fELF"


def _library(project_root: Path, image: str) -> None:
    """Lay out `examples/` as a standard-library clone the project can plan from.

    `examples/` is not a valid `MetasmithLibraries` root on its own -- it is one
    transform library -- so the three pieces are placed where `stdlib.discover`
    looks for them. Writing it here also short-circuits the real clone, which
    returns early when the directory already exists.
    """
    lib = project_root / "MetasmithLibraries"
    shutil.copytree(EXAMPLES, lib / "transforms" / "examples")
    (lib / "data_types").mkdir(parents=True)
    for name in ("containers.yml", "examples.yml"):
        shutil.copy(EXAMPLES / "data_types" / name, lib / "data_types" / name)

    # The shipped index names a copy of this library on sockeye. A local run
    # short-circuits on the integrity check, but a *failed* check turns into an
    # ssh attempt to a host a test host has no business reaching.
    index_path = lib / "transforms" / "examples" / "_metadata" / "index.yml"
    index = yaml.safe_load(index_path.read_text())
    index.pop("remote_src", None)
    index_path.write_text(yaml.dump(index, sort_keys=False))

    # The transform asks for a `containers::metasmith.env`, and nothing produces
    # one -- it is a resource, given rather than made. Pointed at the image this
    # suite builds, so the tool runs from the same metasmith the test is of.
    from metasmith.models.libraries import DataInstanceLibrary

    resources = DataInstanceLibrary(lib / "resources" / "containers.xgdb")
    resources.AddTypeLibrary(lib / "data_types" / "containers.yml")
    declaration = (EXAMPLES / "metasmith.env").read_text()
    declaration = re.sub(r"^container:.*$", f"container: docker://{image}",
                         declaration, flags=re.M)
    # written inside the library so its manifest entry stays relative
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
    """Let the *watcher* be what moves the run, which is the claim being tested.

    Nothing here patches the record: the run reaches a terminal state because
    the sentinel appeared in the agent's log and a probe saw it, exactly as it
    would for a browser left open on the page.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/api/runs/{workflow}/{run}").get_json()
        if not body["live"]:
            return body
        time.sleep(2)
    pytest.fail(f"run [{run}] was still {body['state']} after {timeout}s")


def _ready_to_launch(gui, tmp_path, docker_image) -> tuple[str, str]:
    """Everything the page does before the launch button: agent, deploy, solve.

    Returns (workflow, agent). Split out because the two tests below want the
    same setup and differ only in how far the run itself can be taken.
    """
    name_file = tmp_path / "name.txt"
    name_file.write_text("world\n")

    # -- an agent, made the way `+ agent` makes one ------------------------
    agent = gui.post("/api/agents", json={}).get_json()
    assert agent["name"].endswith("-local"), agent["name"]
    assert agent["deployed"] is False
    agent_name = agent["name"]

    saved = gui.put(f"/api/agents/{agent_name}", json={
        "name": agent_name,
        "runtime": "DOCKER",
        # the default is built from this tree's version *and build hash*, so in
        # a working copy it names an image nobody published. This is the box the
        # page now draws for exactly that reason.
        "container": f"docker://{docker_image}",
    }).get_json()
    assert saved["container"] == f"docker://{docker_image}"

    # -- the recipe -------------------------------------------------------
    wf_name = gui.post("/api/workflows", json={}).get_json()["name"]
    # the recipe *is* the input rows; the library is built from them by the
    # generate below, which is the only thing that writes one
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

    # -- a run refused before anything is installed -----------------------
    # asserted here rather than earlier only because the launch route checks
    # for a plan first, and a workflow has none until now
    refused = gui.post("/api/runs", json={"workflow": wf_name, "agent": agent_name})
    assert refused.status_code == 409
    assert "deployed" in refused.get_json()["error"], refused.get_json()

    # -- deploy -----------------------------------------------------------
    # This is the step that used to hang forever with one line of log: the
    # setup block the page defaults to is `#!/bin/bash`, a comment, which made
    # LiveShell's brace wrapper empty and killed bash on a syntax error.
    _await_job(gui, gui.post(f"/api/agents/{agent_name}/deploy", json={}).get_json())
    after = gui.get(f"/api/agents/{agent_name}").get_json()
    assert after["deployed"] is True, after
    assert after["real_path"], "a deploy resolves the home and records it"
    return wf_name, agent_name


class TestGuiDrivenRun:
    def test_the_page_can_get_an_agent_ready_to_run_on(self, gui, tmp_path, docker_image):
        """Everything up to the launch, which is what the stub relay allows.

        Small-sounding, and it is the test that would have caught the freeze:
        `deploy` runs for real here, over the same default setup block the page
        puts in the box.
        """
        _ready_to_launch(gui, tmp_path, docker_image)

    @pytest.mark.skipif(
        not _real_relay(),
        reason="needs a real msm_relay; this suite's image builds a stub "
               "(./dev.sh -brc && ./dev.sh -br builds the real one)",
    )
    def test_a_run_started_from_the_page_reaches_completed(self, gui, tmp_path, docker_image):
        """The whole path a new user takes, with nothing mocked out."""
        wf_name, agent_name = _ready_to_launch(gui, tmp_path, docker_image)

        # -- launch, and let the watcher carry it -------------------------
        launched = gui.post("/api/runs", json={"workflow": wf_name, "agent": agent_name})
        assert launched.status_code == 202, launched.get_json()
        body = launched.get_json()
        run_name = body["run"]["name"]
        _await_job(gui, body["job"])

        started = gui.get(f"/api/runs/{wf_name}/{run_name}").get_json()
        # `index`, not `run`. Read wrong, this is always None and every probe
        # follows `logs.latest`, which on a re-run is the other run's directory.
        assert started["run_number"] == 1, started

        final = _await_state(gui, wf_name, run_name)
        assert final["state"] == "completed", final

        # -- and the greeting actually exists -----------------------------
        project: Project = gui.application.config["MSM_PROJECT"]
        home = Path(gui.get(f"/api/agents/{agent_name}").get_json()["real_path"])
        key = gui.get(f"/api/workflows/{wf_name}").get_json()["task_key"]
        produced = sorted((home / "runs" / key / "results").rglob("*.txt"))
        assert produced, f"nothing under [{home / 'runs' / key / 'results'}]"
        # the results library links into nextflow's work dir, so read through it
        assert any("hello world" in p.resolve().read_text() for p in produced), (
            [str(p) for p in produced]
        )
        assert project.read_run(wf_name, run_name).state == "completed"

    def test_an_orphaned_launch_is_resolved_rather_than_stranded(self, gui):
        """`staging` is owned by a thread and by nothing on disk.

        A server that is restarted mid-stage leaves the run claiming to be
        staged by something that no longer exists, and nothing on the agent can
        move it -- there is no staged task yet to probe.
        """
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
