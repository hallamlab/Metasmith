# One verb, two mechanisms, chosen by the agent rather than by the person asking.
#
# `setup environment` is pressed on the workflow page next to the agent that will
# run it, and what it has to do depends entirely on that agent: fill the image
# store for a container runtime, build the conda envs the steps name for a mamba
# or native one. These drive a real `Agent` against a fake shell, so what is
# pinned is the dispatch and the report -- the fetching and building themselves
# are pinned in the bootstrap axis, against the same free functions.

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

import metasmith.agents.conda as _conda
import metasmith.agents.workflow_ops as _agents
from metasmith.agents import Agent
from metasmith.constants import AgentPaths
from metasmith.coms.terminals import ShellResult
from metasmith.env import Runtime
from metasmith.models.remote import Source

TASK_KEY = "testtask01"
KRAKEN = "docker://quay.io/biocontainers/kraken2:2.1.3--h43eeafb_0"


def _step(order, transform, envs):
    return {f"p{order:02}__{transform}": {
        "step": order, "transform": transform, "process": f"p{order:02}__{transform}",
        "arms": ["ifVirtualEnvDo"], "envs": envs,
    }}


class FakeShell:
    def __init__(self, doc: dict, frontend: str | None = "mamba", present=()):
        self.doc = doc
        self.frontend = frontend
        self.present = set(present)
        self.calls: list[str] = []

    def Exec(self, cmd, timeout=None, history=False, quiet=False, **_) -> ShellResult:
        self.calls.append(cmd)
        if "workspace exists" in cmd:
            return ShellResult(out=["workspace exists"], err=[])
        if AgentPaths.ENV_MANIFEST in cmd and "cat" in cmd:
            return ShellResult(out=[json.dumps(self.doc)], err=[])
        if "command -v" in cmd:
            if self.frontend is None: return ShellResult(out=[], err=[])
            return ShellResult(out=[f"MSM_FRONTEND={self.frontend}"], err=[])
        if " run -n " in cmd:
            name = cmd.split(" run -n ")[1].split()[0]
            return ShellResult(out=["env-ready"] if name in self.present else [], err=[])
        if "env create" in cmd:
            self.present.add(cmd.split("-n ")[1].split()[0])
            return ShellResult(out=[], err=[])
        if "image-ready" in cmd:
            return ShellResult(out=["image-ready"], err=[])
        return ShellResult(out=[], err=[])

    def created(self) -> list[str]:
        return [c.split("-n ")[1].split()[0] for c in self.calls if "env create" in c]

    def pushed(self) -> dict[str, str]:
        out = {}
        for c in self.calls:
            if "base64 -d" not in c: continue
            out[c.rsplit("> ", 1)[1].strip('"')] = base64.b64decode(
                c.split('echo "')[1].split('"')[0]
            ).decode()
        return out


@pytest.fixture
def home(tmp_path) -> Path:
    root = tmp_path/"msm_home"
    (root/AgentPaths.STAGED/TASK_KEY/AgentPaths.INTERNALS/AgentPaths.TASK).mkdir(parents=True)
    return root


@pytest.fixture
def library(tmp_path) -> Path:
    root = tmp_path/"MetasmithLibraries"
    (root/"envs"/"tools").mkdir(parents=True)
    (root/"envs"/"tools"/"blast.yml").write_text("name: blast\ndependencies:\n  - blast=2.16.0\n")
    return root


def _setup(monkeypatch, home, doc, runtime=Runtime.MAMBA, library=None, **kw) -> tuple[dict, FakeShell]:
    shell = FakeShell(doc, **kw)

    class _AgentShell:
        def __init__(self, _agent): pass
        def __enter__(self): return shell
        def __exit__(self, *a): return False

    monkeypatch.setattr(_agents, "AgentShell", _AgentShell)
    # `_recipe_roots` searches the planning library AND the installed package.
    # These tests are about what the agent does with the library it was handed,
    # so the second root is shut off: with it live, the answer depends on
    # whether `dev/libraries.sh --stage-envs` has ever run in this checkout --
    # a staged src/metasmith_libraries/envs/tools/ supplies recipes the tmp_path
    # library deliberately does not have, and "no recipe for gtdbtk" stops being
    # true for anyone who followed AGENTS.md.
    monkeypatch.setattr(_conda, "_installed_library_root", lambda: None)
    agent = Agent(home=Source.FromLocal(home), runtime=runtime)
    return agent.SetupEnvironment(TASK_KEY, library=library), shell


class TestAMambaAgentGetsEnvironments:
    def test_it_builds_what_the_steps_name_from_the_librarys_recipe(
        self, monkeypatch, home, library,
    ):
        doc = {"schema": 2, "steps": _step(1, "blastp", {"b.env": {"conda": "blast"}})}
        report, shell = _setup(monkeypatch, home, doc, library=library)
        assert report["mode"] == "conda"
        assert report["created"] == 1
        assert shell.created() == ["blast"]
        assert "blast=2.16.0" in "".join(shell.pushed().values())

    def test_a_second_press_does_nothing(self, monkeypatch, home, library):
        doc = {"schema": 2, "steps": _step(1, "blastp", {"b.env": {"conda": "blast"}})}
        report, shell = _setup(monkeypatch, home, doc, library=library, present=["blast"])
        assert (report["created"], report["already_present"]) == (0, 1)
        assert shell.created() == []

    def test_an_env_the_library_has_no_recipe_for_is_named(self, monkeypatch, home, library):
        doc = {"schema": 2, "steps": _step(1, "gtdbtk", {"g.env": {"conda": "gtdbtk"}})}
        report, _ = _setup(monkeypatch, home, doc, library=library)
        assert report["no_recipe"] == ["gtdbtk"]

    def test_a_step_with_no_conda_entry_is_reported_with_its_container(
        self, monkeypatch, home, library,
    ):
        doc = {"schema": 2, "steps": _step(1, "kraken2", {"k.env": {"container": KRAKEN}})}
        report, shell = _setup(monkeypatch, home, doc, library=library)
        assert report["no_conda"] == [{
            "transform": "kraken2", "step": 1, "resource": "k.env", "container": KRAKEN,
        }]
        assert shell.created() == []

    def test_a_host_with_no_conda_says_so_rather_than_failing_every_env(
        self, monkeypatch, home, library,
    ):
        doc = {"schema": 2, "steps": _step(1, "blastp", {"b.env": {"conda": "blast"}})}
        report, shell = _setup(monkeypatch, home, doc, library=library, frontend=None)
        assert report["frontend"] is None
        assert report["needed"] == ["blast"]
        assert shell.created() == []


class TestAContainerAgentGetsImages:
    def test_it_fills_the_image_store_instead(self, monkeypatch, home, library):
        doc = {"schema": 2, "steps": _step(1, "kraken2", {"k.env": {"container": KRAKEN}})}
        report, shell = _setup(
            monkeypatch, home, doc, runtime=Runtime.APPTAINER, library=library,
        )
        assert report["mode"] == "container"
        assert [r["image"] for r in report["images"]] == [KRAKEN]
        assert shell.created() == []
