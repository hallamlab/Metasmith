"""Route-level tests for the GUI backend, driven against a temporary project.

These go through the HTTP surface rather than calling the store directly,
because the contract that matters is the one the page sees.
"""
from __future__ import annotations

import io
import threading
from pathlib import Path
from unittest import mock

import pytest
import yaml

from metasmith.agents import Spec, Template
from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.paths import DEFERRED
from metasmith.models.solver import Endpoint
from metasmith.testing.mock_transforms import identity_transform

from metasmith.gui import share, stdlib
from metasmith.gui.app import bind_project, create_app
from metasmith.gui.store import Project
from metasmith.ops import agent as op_agent
from metasmith.ops import inputs as op_inputs
from metasmith.ops import workspace as op_workspace

from tests.e2e.docker.conftest import create_transform_library

# tests/gui/ IS the GUI's own suite -- conftest stamps `gui` (and `fast`) on
# everything under it, and `dev.sh -tg` runs the directory. This line is kept
# as a local reminder of what the file is for; it is no longer what selects it.
pytestmark = pytest.mark.gui

def _fabricate_project(root: Path, mlib_target: Path | None = None) -> Path:
    """A project with a stand-in standard library already in place.

    The GUI clones the real one; here it is fabricated so the tests never touch
    the network. A function as well as a fixture because sharing needs two
    projects at once -- an export is only worth anything somewhere else.

    `mlib_target`, when given, makes `MetasmithLibraries` a symlink to a
    checkout elsewhere rather than a directory inside the project -- the shape
    a real project can have (a shared dev checkout) and the one that once made
    `save_as_template` fail: resolving the symlink lands outside the project
    root.
    """
    mlib = root / "MetasmithLibraries"
    if mlib_target is not None:
        mlib_target.mkdir(parents=True, exist_ok=True)
        root.mkdir(parents=True, exist_ok=True)
        mlib.symlink_to(mlib_target)
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
def project_root(tmp_path) -> Path:
    return _fabricate_project(tmp_path / "project")


@pytest.fixture(scope="session")
def _app(tmp_path_factory):
    """One app for the whole file, re-pointed per test.

    The routes are the expensive half of `create_app` -- werkzeug compiles a
    builder per rule -- and they hold nothing a test could leak through. What a
    test *does* own is the project behind them, so that half is rebuilt for each
    one by `bind_project`, exactly as a fresh app would have.
    """
    scratch = tmp_path_factory.mktemp("app")
    app = create_app(scratch, ssh_config_path=scratch / "ssh_config", watch=False)
    app.config["TESTING"] = True
    return app


def _client_on(app, project_root, ssh_config_path):
    bind_project(app, project_root, ssh_config_path=ssh_config_path, watch=False)
    with app.test_client() as c:
        c.application = app
        yield c


@pytest.fixture
def client(_app, project_root, tmp_path):
    yield from _client_on(_app, project_root, tmp_path / "ssh_config")


def _deployed(client, name: str):
    """Stamp an agent as deployed, which is what a launch now requires.

    `real_path` is what `Agent.Deploy` resolves on the host and what
    re-pointing the home clears, so it is the record's answer to "has anything
    been installed there". A test cannot deploy for real -- that needs a
    container and a host -- so it writes the one mark a deploy leaves.
    """
    project: Project = client.application.config["MSM_PROJECT"]
    path = project.agent_path(name)
    agent = op_agent.load_agent(str(path))
    agent.real_path = Path(agent.home.GetPath())
    agent.Save(path)


def _finish(client, job_summary, timeout=120) -> dict:
    """Block on a background job and return its final summary."""
    job = client.application.config["MSM_JOBS"].get(job_summary["id"])
    assert job.wait(timeout), f"job [{job_summary['id']}] did not finish"
    assert job.status == "done", f"job failed: {job.error}\n" + "\n".join(job.lines()[-20:])
    return job.result


def _row(rid, path="", dtype="mock::assembly", parents=None, **over):
    """One input row of the recipe, in the shape the browser saves."""
    return {
        "id": rid, "mode": "file", "path": str(path), "name": "", "value": "",
        "dtype": dtype, "parents": list(parents or []),
    } | over


def _put_rows(client, workflow: str, rows: list[dict]):
    """Save the recipe's rows, and build the library from them.

    The rows are what a workflow *has*; the library is what a solve makes of
    them. A test that wants the library without paying for a solve calls the
    same sync the generate calls -- there is deliberately no route that does it
    on its own, because there is no gesture in the page that would.
    """
    r = client.put(f"/api/workflows/{workflow}", json={"input_drafts": rows})
    assert r.status_code == 200, r.get_json()
    project = client.application.config["MSM_PROJECT"]
    op_inputs.sync(str(project.input_library_path(workflow)), rows)
    return rows


def _rows_of(client, workflow: str) -> list[dict]:
    return list(
        client.get(f"/api/workflows/{workflow}").get_json()["request"].get("input_drafts") or []
    )


def _seed_inputs(client, workflow: str, count: int = 2, prefix: str = "sample"):
    project = client.application.config["MSM_PROJECT"]
    lib_path = project.input_library_path(workflow)
    rows = _rows_of(client, workflow)
    for i in range(count):
        f = lib_path / f"{prefix}_{i}.fa"
        f.write_text(f">contig_{i}\nACGT\n")
        rows.append(_row(f"{prefix}{i}", f))
    return _put_rows(client, workflow, rows)


def _make_workflow(client, name=None, sample="mock::assembly", targets=("mock::bam",)) -> str:
    r = client.post("/api/workflows", json={
        "name": name, "sample_type": sample, "target_types": list(targets),
    })
    assert r.status_code == 201, r.get_json()
    return r.get_json()["name"]


# ---------------------------------------------------------------------------
# project + agents
# ---------------------------------------------------------------------------


class TestHealth:
    """What the header's dot polls."""

    def test_health_answers(self, client):
        res = client.get("/api/health")
        assert res.status_code == 200
        assert res.get_json() == {"ok": True}

    def test_health_touches_nothing(self, client):
        # it runs on a timer for as long as the page is open, so it must not do
        # the stdlib walk `/project` does -- patching that walk to explode is
        # the cheapest way to state "this route does not go near it"
        with mock.patch.object(stdlib, "discover", side_effect=AssertionError("walked")):
            assert client.get("/api/health").status_code == 200


class TestProject:
    def test_project_reports_stdlib(self, client):
        body = client.get("/api/project").get_json()
        assert body["stdlib"]["present"] is True
        assert len(body["stdlib"]["transform_libraries"]) == 1

    def test_types_are_listed(self, client):
        names = {t["full_name"] for t in client.get("/api/project/types").get_json()}
        assert {"mock::assembly", "mock::bam"} <= names


class TestTypeIndex:
    """The map the builder consults while a type is being chosen."""

    def test_both_sides_of_a_type_are_indexed(self, client):
        body = client.get("/api/project/type-index").get_json()
        names = [t["name"] for t in body["transforms"]]
        assert names, "the mock library has one transform; it should be listed"

        # mock::assembly -> mock::bam, so each type appears on one side only
        consumed = body["by_type"]["mock::assembly"]
        produced = body["by_type"]["mock::bam"]
        assert consumed["consumed_by"] and not consumed["produced_by"]
        assert produced["produced_by"] and not produced["consumed_by"]

        # entries address the shared list rather than repeating the transform
        entry = consumed["consumed_by"][0]
        assert entry["match"] == "exact" and entry["as"] == "mock::assembly"
        tr = body["transforms"][entry["i"]]
        assert tr["inputs"] == ["mock::assembly"]
        assert tr["outputs"] == ["mock::bam"]
        assert tr["library"] == body["libraries"][0]["path"]

    def test_every_named_type_is_a_key(self, client):
        """Including one no transform mentions.

        The builder offers what it finds here, and asks it for counts before
        anything has been registered -- a type with no transforms either side is
        a different answer to a type it has never heard of.
        """
        body = client.get("/api/project/type-index").get_json()
        assert "mock::unreachable" in body["by_type"]
        assert body["by_type"]["mock::unreachable"] == {"produced_by": [], "consumed_by": []}

    def test_libraries_carry_their_counts(self, client):
        body = client.get("/api/project/type-index").get_json()
        assert [l["transform_count"] for l in body["libraries"]] == [1]

    def test_an_unreadable_library_does_not_blank_the_index(self, client, project_root):
        """One broken library must cost only itself.

        The panel is furniture: it has to keep answering for the libraries that
        do load, and name the one that did not.
        """
        (project_root / "MetasmithLibraries" / "transforms" / "broken.xgdb").mkdir()
        body = client.get("/api/project/type-index").get_json()
        broken = [l for l in body["libraries"] if l["name"] == "broken.xgdb"]
        assert len(broken) == 1 and broken[0]["error"]
        assert body["by_type"]["mock::bam"]["produced_by"], "the good library still indexed"


@pytest.fixture
def poly_root(tmp_path) -> Path:
    """A library where one type extends another, and a name repeats across files.

    The stand-in in `project_root` is deliberately flat, so every match in it is
    an exact one -- which is the shape that hid the property matching the solver
    actually does.
    """
    root = tmp_path / "poly"
    mlib = root / "MetasmithLibraries"
    (mlib / "data_types").mkdir(parents=True)

    types = DataTypeLibrary()
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    # an assembly, plus the method that made it: strictly more specific
    types["flye_assembly"] = Endpoint(properties={"assembly", "flye"})
    types["stats"] = Endpoint(properties={"stats"})
    types_path = mlib / "data_types" / "mock.yml"
    types.Save(types_path)

    # the same properties, named again in a second namespace
    alias = DataTypeLibrary()
    alias["assembly"] = Endpoint(properties={"assembly"})
    alias.Save(mlib / "data_types" / "other.yml")

    create_transform_library(
        mlib / "transforms", types_path,
        # makes the narrow one; takes the broad one; takes the narrow one
        identity_transform("mock::reads", "mock::flye_assembly")
        | identity_transform("mock::assembly", "mock::stats")
        | identity_transform("mock::flye_assembly", "mock::reads"),
    )
    (mlib / "resources").mkdir()
    return root


@pytest.fixture
def poly_client(_app, poly_root, tmp_path):
    yield from _client_on(_app, poly_root, tmp_path / "ssh_config")


class TestTypeIndexIsA:
    """Matching is `Endpoint.IsA`, never name equality.

    Keying the index on names alone told the user "nothing can make this" about
    a type five transforms produce a subtype of, and "nothing takes this" about
    an input a dozen transforms accept. Both readings drive a decision -- whether
    to register a file, whether a target is reachable -- so both have to be the
    same relation the solver will apply.
    """

    def _index(self, client):
        return client.get("/api/project/type-index").get_json()

    def _names(self, body, type_name, side):
        return {
            (body["transforms"][e["i"]]["name"], e["match"], e["as"])
            for e in body["by_type"][type_name][side]
        }

    def test_a_narrower_product_satisfies_a_broader_want(self, poly_client):
        body = self._index(poly_client)
        assert ("identity_flye_assembly", "narrower", "mock::flye_assembly") in self._names(
            body, "mock::assembly", "produced_by"
        )

    def test_a_broader_requirement_accepts_a_narrower_input(self, poly_client):
        body = self._index(poly_client)
        assert ("identity_stats", "broader", "mock::assembly") in self._names(
            body, "mock::flye_assembly", "consumed_by"
        )

    def test_the_relation_is_not_symmetric(self, poly_client):
        """A supertype cannot stand in for a subtype -- in either direction.

        This is the half that must stay refused: showing it would promise a plan
        the solver will not find.
        """
        body = self._index(poly_client)
        consumers = {n for n, _, _ in self._names(body, "mock::assembly", "consumed_by")}
        assert "identity_reads" not in consumers, "wants a flye_assembly specifically"
        producers = {n for n, _, _ in self._names(body, "mock::flye_assembly", "produced_by")}
        assert producers == {"identity_flye_assembly"}, "only the narrow product makes it"

    def test_the_same_properties_under_another_name_is_an_alias(self, poly_client):
        body = self._index(poly_client)
        assert ("identity_stats", "alias", "mock::assembly") in self._names(
            body, "other::assembly", "consumed_by"
        )

    def test_direct_matches_come_first(self, poly_client):
        """The list is read top-down, so what named this type leads it."""
        body = self._index(poly_client)
        rank = {"exact": 0, "alias": 1}
        for spec in body["by_type"].values():
            for side in ("produced_by", "consumed_by"):
                ranks = [rank.get(e["match"], 2) for e in spec[side]]
                assert ranks == sorted(ranks)

    def test_a_transform_is_listed_once_per_side(self, poly_client):
        """Under its closest relation, even when several of its deps match."""
        body = self._index(poly_client)
        for spec in body["by_type"].values():
            for side in ("produced_by", "consumed_by"):
                seen = [e["i"] for e in spec[side]]
                assert len(seen) == len(set(seen))


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

    def test_delete_archives_first(self, client, tmp_path):
        """Deleting is archiving; deleting again is the removal.

        Nothing here is recoverable from anywhere else, and the gesture is one
        double-click on a list of near-identical names -- so the first press
        tombstones and the second one means it.
        """
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        assert client.delete("/api/agents/smith").get_json()["action"] == "archived"
        assert client.get("/api/agents").get_json() == []
        assert len(client.get("/api/agents?archived=1").get_json()) == 1
        # and it comes back
        client.post("/api/agents/smith/archive", json={"archived": False})
        assert len(client.get("/api/agents").get_json()) == 1
        client.delete("/api/agents/smith")
        assert client.delete("/api/agents/smith").get_json()["action"] == "deleted"
        assert client.get("/api/agents?archived=1").get_json() == []

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

    def test_created_from_nothing(self, client):
        """`+ agent` posts an empty body -- there is no form in front of it."""
        r = client.post("/api/agents", json={})
        assert r.status_code == 201, r.get_json()
        body = r.get_json()
        assert body["name"]
        # named after its stable id, not its display name, so a rename can
        # never relocate it and a host with three agents has three directories
        assert body["id"]
        assert body["home"].endswith(f"msm.{body['id']}")
        assert body["setup_commands"] == ["#!/bin/bash"]
        assert body["valid"] is True
        # complete, but nothing is installed on that host yet. Kept out of
        # `problems` -- the deploy button reads those, and a deploy that
        # disabled itself for want of a deploy is a deadlock.
        assert body["deployed"] is False

    def test_defaults_offer_every_runtime(self, client):
        d = client.get("/api/defaults/agent").get_json()
        # read off env.Runtime rather than written out again
        assert set(d["runtimes"]) >= {"APPTAINER", "DOCKER", "MAMBA"}
        # a preview only -- the real agent this becomes gets its own id, and
        # the real home is built from that, not from this name
        assert d["home"].startswith("~/msm.")
        assert not d["home"].endswith(f"msm.{d['name']}")

    def test_mamba_is_a_runtime(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/smith", json={"name": "smith", "runtime": "MAMBA"})
        assert r.status_code == 200, r.get_json()
        assert r.get_json()["runtime"] == "MAMBA"

    def test_unknown_runtime_refused(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/smith", json={"name": "smith", "runtime": "PODMAN"})
        assert r.status_code == 400
        assert "PODMAN" in r.get_json()["error"]


class TestAgentNaming:
    """A made-up name says which machine it is; a typed one is left alone."""

    def _prefix(self, project_root, name) -> str:
        return Project(project_root).agent_naming(name)["prefix"]

    def test_a_made_up_name_carries_its_host(self, client):
        body = client.post("/api/agents", json={}).get_json()
        # a new agent is local until it is pointed somewhere
        assert body["name"].endswith("-local")
        prefix = body["name"][: -len("-local")]
        # one word, not the adjective-noun slug: 'blazing-local', never
        # 'blazing-ape-local'
        assert "-" not in prefix
        assert body["sort_name"] == f"local{prefix}"
        assert body["auto_named"] is True

    def test_a_typed_name_gets_no_sort_key(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.get("/api/agents/smith").get_json()
        assert body["sort_name"] is None
        assert body["auto_named"] is False

    def test_pointing_it_at_a_host_renames_it(self, client, project_root):
        created = client.post("/api/agents", json={}).get_json()
        name, agent_id = created["name"], created["id"]
        prefix = self._prefix(project_root, name)
        # the client's home-path box was left blank, so it recomputes the
        # default home from the agent's (unchanging) id, on whatever host the
        # form now names
        body = client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://sockeye:~/msm.{agent_id}",
        }).get_json()
        assert body["name"] == f"{prefix}-sockeye"
        assert body["sort_name"] == f"sockeye{prefix}"
        # the default home is made out of the id, so it moved with the host
        assert body["home"] == f"ssh://sockeye:~/msm.{agent_id}"
        assert body["id"] == agent_id
        assert body["notes"]

    def test_a_home_someone_wrote_out_does_not_move(self, client, project_root):
        name = client.post("/api/agents", json={}).get_json()["name"]
        prefix = self._prefix(project_root, name)
        body = client.put(f"/api/agents/{name}", json={
            "name": name, "home": "ssh://sockeye:/scratch/tony/here",
        }).get_json()
        assert body["name"] == f"{prefix}-sockeye"
        assert body["home"] == "ssh://sockeye:/scratch/tony/here"

    def test_a_remote_agent_with_no_host_yet_keeps_its_name(self, client):
        """Half-pointed is not a machine to be named after."""
        name = client.post("/api/agents", json={}).get_json()["name"]
        body = client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://:~/msm.{name}",
        }).get_json()
        assert body["name"] == name
        assert body["auto_named"] is True

    def test_regenerating_a_name_re_arms_it(self, client):
        """The ↻ beside the name is the undo for having typed one.

        A typed name makes an agent manually-named for good, which is right --
        but then there is no way back to a name that follows its host, and the
        button that offers one has to be able to say "this came from you, not
        from a keyboard". That is what the `naming` record on the save is.
        """
        name = client.post("/api/agents", json={}).get_json()["name"]
        client.put(f"/api/agents/{name}", json={"name": "bertha"})
        assert client.get("/api/agents/bertha").get_json()["auto_named"] is False

        suggestion = client.get(
            "/api/defaults/agent/name", query_string={"host": "sockeye"}).get_json()
        assert suggestion["name"].endswith("-sockeye")
        # nothing is renamed by asking
        assert client.get("/api/agents/bertha").status_code == 200

        body = client.put("/api/agents/bertha", json={
            "name": suggestion["name"],
            "home": f"ssh://sockeye:~/msm.{suggestion['name']}",
            "naming": {"prefix": suggestion["prefix"], "sort_name": suggestion["sort_name"]},
        }).get_json()
        assert body["name"] == suggestion["name"]
        assert body["auto_named"] is True
        # and it follows its host again
        after = client.put(f"/api/agents/{body['name']}", json={
            "name": body["name"], "home": f"ssh://mira:~/msm.{body['name']}",
        }).get_json()
        assert after["name"] == f"{suggestion['prefix']}-mira"

    def test_typing_a_name_stops_it_following(self, client):
        name = client.post("/api/agents", json={}).get_json()["name"]
        body = client.put(f"/api/agents/{name}", json={"name": "bertha"}).get_json()
        assert body["name"] == "bertha"
        assert body["auto_named"] is False
        after = client.put("/api/agents/bertha", json={
            "name": "bertha", "home": "ssh://sockeye:~/msm.bertha",
        }).get_json()
        assert after["name"] == "bertha"
        assert after["sort_name"] is None

    def test_a_taken_name_keeps_the_old_one_and_says_so(self, client, project_root):
        name = client.post("/api/agents", json={}).get_json()["name"]
        prefix = self._prefix(project_root, name)
        client.post("/api/agents", json={"name": f"{prefix}-sockeye"})
        body = client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://sockeye:~/msm.{name}",
        }).get_json()
        # nothing lost and nothing overwritten -- only the following stops
        assert body["name"] == name
        assert body["auto_named"] is False
        assert any("already taken" in n for n in body["notes"])
        assert client.get(f"/api/agents/{prefix}-sockeye").status_code == 200

    def test_an_alias_rename_carries_the_names_on_it(self, client, project_root):
        client.post("/api/ssh/hosts", json={"alias": "old", "hostname": "old.example"})
        created = client.post("/api/agents", json={}).get_json()
        name, agent_id = created["name"], created["id"]
        prefix = self._prefix(project_root, name)
        client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://old:~/msm.{agent_id}",
        })
        r = client.put("/api/ssh/hosts/old", json={"alias": "new", "hostname": "old.example"})
        body = r.get_json()
        assert body["agents_repointed"] == [f"{prefix}-old → {prefix}-new"]
        agent = client.get(f"/api/agents/{prefix}-new").get_json()
        assert agent["sort_name"] == f"new{prefix}"
        assert agent["id"] == agent_id
        # the directory on that machine did not move, so neither did the home
        assert agent["home"] == f"ssh://new:~/msm.{agent_id}"

    def test_the_list_groups_by_host(self, client, tmp_path):
        for host in ("sockeye", "chamois", "sockeye"):
            name = client.post("/api/agents", json={}).get_json()["name"]
            client.put(f"/api/agents/{name}", json={
                "name": name, "home": f"ssh://{host}:~/msm.{name}",
            })
        # a typed name sorts as it was typed, among them
        client.post("/api/agents", json={"name": "middling", "home": str(tmp_path / "h")})
        listed = [a["name"] for a in client.get("/api/agents").get_json()]
        hosts = [n.rsplit("-", 1)[-1] for n in listed]
        assert hosts == ["chamois", "middling", "sockeye", "sockeye"]


class TestAgentDefaultPreset:
    """`(agent default)` on the launch pane now has something to point at."""

    def test_the_page_is_told_the_options_and_the_choice(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.get("/api/agents/smith").get_json()
        assert "local" in body["config_presets"]
        assert body["default_preset"] is None

    def test_it_saves_and_survives_an_unrelated_edit(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.put("/api/agents/smith", json={
            "name": "smith", "default_preset": "slurm",
        }).get_json()
        assert body["default_preset"] == "slurm"
        # a PUT that does not mention it keeps what is on disk, like the image
        after = client.put("/api/agents/smith", json={
            "name": "smith", "runtime": "DOCKER",
        }).get_json()
        assert after["default_preset"] == "slurm"

    def test_clearing_it_means_the_built_in_local(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        client.put("/api/agents/smith", json={"name": "smith", "default_preset": "slurm"})
        body = client.put("/api/agents/smith", json={
            "name": "smith", "default_preset": None,
        }).get_json()
        assert body["default_preset"] is None

    def test_an_unknown_preset_is_refused(self, client, tmp_path):
        """A fixed list the page picks from, so a value outside it is a bad request."""
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/smith", json={
            "name": "smith", "default_preset": "wishful",
        })
        assert r.status_code == 400
        assert "wishful" in r.get_json()["error"]


class TestAgentDefaultParams:
    """The other half of a scheduler preset: where its account comes from."""

    def test_they_save_and_come_back_as_values(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.put("/api/agents/smith", json={
            "name": "smith",
            "default_params": {"slurmAccount": "st-you-1", "process_tries": "3"},
        }).get_json()
        # typed in a text box, stored as what it looks like: one rule, applied
        # here so the CLI and the notebook get the same answer
        assert body["default_params"] == {"slurmAccount": "st-you-1", "process_tries": 3}

    def test_a_quoted_number_stays_a_string(self, client, tmp_path):
        """The escape hatch for the one case the rule gets wrong."""
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {"version": '"50"'},
        }).get_json()
        assert body["default_params"] == {"version": "50"}

    def test_a_row_with_no_name_is_dropped(self, client, tmp_path):
        """A row still being typed is not a param called empty-string."""
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {"": "orphan", "  ": "also", "a": "1"},
        }).get_json()
        assert body["default_params"] == {"a": 1}

    def test_a_put_that_does_not_mention_them_keeps_them(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {"acct": "x"},
        })
        after = client.put("/api/agents/smith", json={
            "name": "smith", "runtime": "DOCKER",
        }).get_json()
        assert after["default_params"] == {"acct": "x"}

    def test_sending_an_empty_mapping_clears_them(self, client, tmp_path):
        """Whole-object save: what it does not say, it does not have."""
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {"acct": "x"},
        })
        after = client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {},
        }).get_json()
        assert after["default_params"] == {}


class TestAgentUpdateConvention:
    """One PUT carrying the whole object, identity field included."""

    def test_rename_moves_the_file(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/smith", json={"name": "wesson"})
        assert r.status_code == 200, r.get_json()
        assert r.get_json()["name"] == "wesson"
        assert client.get("/api/agents/smith").status_code == 409
        assert client.get("/api/agents/wesson").status_code == 200
        assert [a["name"] for a in client.get("/api/agents").get_json()] == ["wesson"]

    def test_rename_keeps_the_other_fields(self, client, tmp_path):
        client.post("/api/agents", json={
            "name": "smith", "home": str(tmp_path / "h"), "runtime": "DOCKER",
            "setup_commands": ["module load gcc"],
        })
        body = client.put("/api/agents/smith", json={"name": "wesson"}).get_json()
        assert body["runtime"] == "DOCKER"
        assert body["setup_commands"] == ["module load gcc"]

    def test_rename_onto_a_taken_name_is_refused(self, client, tmp_path):
        client.post("/api/agents", json={"name": "a", "home": str(tmp_path / "h")})
        client.post("/api/agents", json={"name": "b", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/a", json={"name": "b"})
        assert r.status_code == 409
        # and neither one lost its file to the attempt
        assert {x["name"] for x in client.get("/api/agents").get_json()} == {"a", "b"}

    def test_rename_takes_its_runs_with_it(self, client, project_root, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        p = Project(project_root)
        wf = p.create_workflow(name="wf", request={})
        p.create_run(wf.name, {"agent": "smith", "task_key": "k"})
        client.put("/api/agents/smith", json={"name": "wesson"})
        # a run whose agent has vanished cannot be tailed, cancelled or collected
        assert [r.record["agent"] for r in p.list_runs()] == ["wesson"]
        assert client.get("/api/agents/wesson").get_json()["runs"][0]["workflow"] == "wf"

    def test_archive_mark_moves_with_it(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        client.post("/api/agents/smith/archive", json={"archived": True})
        client.put("/api/agents/smith", json={"name": "wesson"})
        assert client.get("/api/agents").get_json() == []
        listed = client.get("/api/agents?archived=1").get_json()
        assert [a["name"] for a in listed] == ["wesson"]
        assert listed[0]["archived_at"]

    def test_a_refused_save_does_not_half_rename(self, client, tmp_path):
        """The rename lands after the fields are checked, never before."""
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/smith", json={"name": "wesson", "runtime": "PODMAN"})
        assert r.status_code == 400
        assert client.get("/api/agents/smith").status_code == 200
        assert client.get("/api/agents/wesson").status_code == 409

    def test_fields_the_page_does_not_draw_survive(self, client, tmp_path):
        """The image is a dev field and is not rendered; a save must not eat it."""
        client.post("/api/agents", json={
            "name": "smith", "home": str(tmp_path / "h"), "container": "docker://pinned:1",
        })
        body = client.put("/api/agents/smith", json={
            "name": "smith", "home": str(tmp_path / "h"), "runtime": "DOCKER",
        }).get_json()
        assert body["container"] == "docker://pinned:1"


class TestAgentValidity:
    """Saveable while it is being filled in; not launchable until it is."""

    def test_a_remote_agent_with_no_host_saves_and_says_so(self, client):
        client.post("/api/agents", json={"name": "smith"})
        r = client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://:~/msm.smith"})
        assert r.status_code == 200, r.get_json()
        body = r.get_json()
        assert body["valid"] is False
        assert "no host chosen" in body["problems"]
        # and it round-trips as remote rather than reverting to local
        assert client.get("/api/agents/smith").get_json()["home"] == "ssh://:~/msm.smith"

    def test_a_host_that_is_not_in_the_config_is_a_problem(self, client):
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://nowhere:~/x"})
        body = client.get("/api/agents/smith").get_json()
        assert body["valid"] is False
        assert any("nowhere" in p for p in body["problems"])

    def test_a_known_host_is_valid(self, client):
        client.post("/api/ssh/hosts", json={"alias": "sockeye", "hostname": "sockeye.example"})
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://sockeye:~/x"})
        _deployed(client, "smith")
        body = client.get("/api/agents/smith").get_json()
        assert body["valid"] is True, body["problems"]

    def test_a_blank_home_is_refused(self, client):
        client.post("/api/agents", json={"name": "smith"})
        r = client.put("/api/agents/smith", json={"name": "smith", "home": " "})
        # blank is not a state worth saving: the field has a default, and an
        # empty Source.Parse silently resolves to the cwd
        assert r.status_code == 400

    def test_an_incomplete_agent_cannot_be_launched_on(self, client, project_root):
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://:~/x"})
        p = Project(project_root)
        wf = p.create_workflow(name="wf", request={})
        p.write_result(wf.name, {"success": True, "task_key": "k"})
        r = client.post("/api/runs", json={"workflow": "wf", "agent": "smith"})
        assert r.status_code == 409
        assert "no host chosen" in r.get_json()["error"]

    def test_a_wildcard_pattern_counts_as_knowing_the_host(self, client, tmp_path):
        """`Host *.cluster.edu` makes every name under it reachable."""
        (tmp_path / "ssh_config").write_text("Host *.cluster.edu\n    User tony\n")
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://n1.cluster.edu:~/x"})
        _deployed(client, "smith")
        assert client.get("/api/agents/smith").get_json()["valid"] is True


class TestDefaultHome:
    """`home_is_default` decides whether the page draws the home field empty.

    An empty field saves as the default, so a wrong True replaces a path
    somebody chose with a different one on their next save. Both spellings of
    the default count -- the literal `~/msm.<id>` and the expansion a local
    save leaves behind -- and nothing else does. The id, not the name, is
    what the default is built from, so renaming an agent must never change
    the answer.
    """

    def test_a_fresh_local_agent_is_default(self, client):
        client.post("/api/agents", json={"name": "smith"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is True

    def test_the_unexpanded_remote_spelling_is_default(self, client):
        agent_id = client.post("/api/agents", json={"name": "smith"}).get_json()["id"]
        client.put("/api/agents/smith", json={"name": "smith", "home": f"ssh://h:~/msm.{agent_id}"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is True

    def test_another_directory_ending_in_the_same_id_is_not(self, client):
        """The trap a suffix test walks into: `/scratch/you/msm.<id>` is not it."""
        agent_id = client.post("/api/agents", json={"name": "smith"}).get_json()["id"]
        client.put("/api/agents/smith", json={"name": "smith", "home": f"ssh://h:/scratch/msm.{agent_id}"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is False

    def test_a_renamed_agent_stays_default(self, client):
        """The whole point: a rename must never relocate the default home."""
        agent_id = client.post("/api/agents", json={"name": "smith"}).get_json()["id"]
        client.put("/api/agents/smith", json={"name": "smith", "home": f"ssh://h:~/msm.{agent_id}"})
        client.put("/api/agents/smith", json={"name": "jones", "home": f"ssh://h:~/msm.{agent_id}"})
        after = client.get("/api/agents/jones").get_json()
        assert after["home_is_default"] is True
        assert after["id"] == agent_id

    def test_a_name_shaped_home_is_not_default(self, client):
        """A path that merely looks like the old name-based scheme is just a path."""
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://h:~/msm.smith"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is False


class TestSshUpdateConvention:
    def test_rename_repoints_the_agents_on_that_host(self, client):
        client.post("/api/ssh/hosts", json={"alias": "old", "hostname": "old.example"})
        client.post("/api/agents", json={"name": "smith", "home": "ssh://old:~/msm.smith"})
        _deployed(client, "smith")
        r = client.put("/api/ssh/hosts/old", json={"alias": "new", "hostname": "old.example"})
        assert r.status_code == 200, r.get_json()
        body = r.get_json()
        assert body["host"]["alias"] == "new"
        assert body["agents_repointed"] == ["smith"]
        # the agent followed the host rather than being left naming nothing
        agent = client.get("/api/agents/smith").get_json()
        assert agent["home"] == "ssh://new:~/msm.smith"
        assert agent["valid"] is True

    def test_rename_keeps_what_the_host_resolved_to(self, client, project_root):
        """Same machine, same directory -- only the alias moved."""
        client.post("/api/ssh/hosts", json={"alias": "old", "hostname": "old.example"})
        client.post("/api/agents", json={"name": "smith", "home": "ssh://old:~/msm.smith"})
        p = Project(project_root)
        agent = op_agent.load_agent(str(p.agent_path("smith")))
        agent.real_path = Path("/scratch/tony/msm.smith")
        agent.Save(p.agent_path("smith"))
        client.put("/api/ssh/hosts/old", json={"alias": "new", "hostname": "old.example"})
        # cleared, this would make a cosmetic rename cost a redeploy
        assert client.get("/api/agents/smith").get_json()["real_path"] == "/scratch/tony/msm.smith"

    def test_a_repoint_of_the_home_itself_does_clear_it(self, client, project_root):
        client.post("/api/agents", json={"name": "smith", "home": "ssh://old:~/msm.smith"})
        p = Project(project_root)
        agent = op_agent.load_agent(str(p.agent_path("smith")))
        agent.real_path = Path("/scratch/tony/msm.smith")
        agent.Save(p.agent_path("smith"))
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://old:~/elsewhere"})
        assert client.get("/api/agents/smith").get_json()["real_path"] is None

    def test_a_rename_through_patch_is_ignored(self, client):
        """PATCH cannot rename: it would move the host and strand the agents."""
        client.post("/api/ssh/hosts", json={"alias": "one", "hostname": "one.example"})
        body = client.patch("/api/ssh/hosts/one", json={"alias": "two"}).get_json()
        assert body["host"]["alias"] == "one"

    def test_an_edit_without_an_alias_is_not_a_rename(self, client):
        client.post("/api/ssh/hosts", json={"alias": "one", "hostname": "one.example"})
        body = client.put("/api/ssh/hosts/one", json={"hostname": "two.example"}).get_json()
        assert body["host"]["alias"] == "one"
        assert body["renamed_from"] is None


# ---------------------------------------------------------------------------
# workflows
# ---------------------------------------------------------------------------


class TestWorkflows:
    def test_generated_name_is_readable(self, client):
        name = _make_workflow(client)
        assert "-" in name and name.islower()

    def test_created_empty_and_named_for_you(self, client):
        """No form precedes the workflow, so create takes nothing.

        The page it lands on is where the recipe is built, and the name it was
        given is editable there -- see `TestWorkflowRename`.
        """
        r = client.post("/api/workflows", json={})
        assert r.status_code == 201
        body = r.get_json()
        assert body["planned"] is False
        detail = client.get(f"/api/workflows/{body['name']}").get_json()
        assert detail["request"]["target_types"] == []
        assert detail["request"]["sample_type"] is None
        assert detail["input_library"]["exists"] is True

    def test_creates_an_editable_input_library(self, client):
        name = _make_workflow(client)
        body = client.get(f"/api/workflows/{name}").get_json()
        assert body["input_library"]["exists"] is True
        assert Path(body["input_library"]["path"]).name == "input.xgdb"

    def test_add_and_remove_inputs(self, client):
        """A row is the input, and removing one is removing the row.

        Nothing calls the library either way: it is built from the rows, so what
        a row registered goes when the row does, at the next solve.
        """
        name = _make_workflow(client)
        rows = _seed_inputs(client, name, 2)
        assert len(client.get(f"/api/workflows/{name}/inputs").get_json()["items"]) == 2
        _put_rows(client, name, rows[:1])
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
            _put_rows(client, n, [_row("a", shared)])

        # generate returns 202 immediately, so both jobs are in flight at once
        jobs = {n: client.post(f"/api/workflows/{n}/generate", json={}).get_json() for n in names}
        keys = {n: _finish(client, jobs[n])["task_key"] for n in names}
        assert len(set(keys.values())) == 1, "same inputs should give the same key"
        for n in names:
            body = client.get(f"/api/workflows/{n}").get_json()
            assert body["success"] is True
            assert (Path(body["path"]) / "task.yml").is_file()
            assert op_workspace.load_task(None, body["path"]).GetKey() == keys[n]

    def test_reading_a_bundle_back_is_serialised_too(self, client):
        """Not just the plan: everything that imports a transform.

        Transforms are imported by bare module name through a process-global
        class attribute, so two of anything doing it at once can hand one of
        them a transform that loaded as None. The plan is the obvious one and
        was locked; reading the bundle back for the step summary, and drawing
        the DAG, do it too, and were not. This asserts the *route* holds the
        lock, since the race it guards against is one in ~6 runs by wall clock.
        """
        from metasmith.gui import api

        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        wf_dir = Path(client.get(f"/api/workflows/{name}").get_json()["path"])

        real = op_workspace.load_task
        held = []
        def _spy(*args, **kwargs):
            held.append(api._plan_lock.locked())
            return real(*args, **kwargs)

        with mock.patch.object(op_workspace, "load_task", _spy):
            assert api._load_task(wf_dir).GetKey()
        assert held == [True], "the task load ran without the planner's lock"


class TestWorkflowDag:
    """The drawing the page shows, and the two files it caches to."""

    def _drawn(self, client, name, **query):
        r = client.get(f"/api/workflows/{name}/dag", query_string=query)
        assert r.status_code == 200, r.get_json()
        assert r.mimetype == "image/svg+xml"
        return r.get_data(as_text=True)

    def test_the_theme_is_honoured_and_cached_per_theme(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        wf_dir = Path(client.get(f"/api/workflows/{name}").get_json()["path"])

        light = self._drawn(client, name)
        dark = self._drawn(client, name, theme="dark")
        assert light != dark
        assert 'fill="#FFFFFF"' in light and 'fill="#FFFFFF"' not in dark

        # two files, not one repainted: the light name is the historical one,
        # since the CLI stages that exact file into the bundle
        assert (wf_dir / "plan.dag.svg").read_text() == light
        assert (wf_dir / "plan.dag.dark.svg").read_text() == dark

    def test_an_unknown_theme_draws_the_default_rather_than_failing(self, client):
        # the value comes off a url; a malformed one must not turn the diagram
        # into an error card
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert self._drawn(client, name, theme="twilight") == self._drawn(client, name)

    def test_a_resolve_drops_every_cached_theme(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        wf_dir = Path(client.get(f"/api/workflows/{name}").get_json()["path"])
        self._drawn(client, name)
        self._drawn(client, name, theme="dark")

        _seed_inputs(client, name, 2, prefix="more")
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        # a stale drawing outliving its plan is the bug; one theme swept and the
        # other left is the same bug wearing the other hat
        for f in ("plan.dag.svg", "plan.dag.dark.svg"):
            assert not (wf_dir / f).exists(), f

    def test_a_workflow_with_no_plan_is_refused(self, client):
        name = _make_workflow(client)
        assert client.get(f"/api/workflows/{name}/dag").status_code == 409


def _make_template(project_root: Path) -> str:
    """Write one template into the stand-in standard library: deferred assembly in.

    Written the way the libraries repository writes its own -- a `Spec` with
    `DEFERRED` inputs, saved with references relative to the repository root --
    because that is the file the GUI has to read.
    """
    mlib = project_root / "MetasmithLibraries"
    lib = DataInstanceLibrary(mlib / "templates" / "assembly_to_bam" / "inputs.xgdb")
    lib.AddTypeLibrary(mlib / "data_types" / "mock.yml")
    lib.AddItem(DEFERRED, "mock::assembly")
    lib.Save()
    Template(
        name="assembly_to_bam",
        description="an assembly in, a bam out",
        spec=Spec(
            input_library=lib,
            target_types=["mock::bam"],
            transform_libraries=[mlib / "transforms" / "transforms.xgdb"],
        ),
    ).Save(mlib)
    return "assembly_to_bam"


@pytest.fixture
def template(project_root) -> str:
    return _make_template(project_root)


class TestTemplates:
    """The starting points the new-workflow modal offers."""

    def test_a_fresh_start_warms_every_template_s_dag(self, _app, project_root, tmp_path):
        """`warm_template_dags` draws every template before anyone asks.

        Unlike the `client` fixture, the template must exist *before*
        `bind_project` starts the warm thread -- that ordering is the whole
        point of the test, since a template written after the warm pass has
        already run would never be picked up by it.
        """
        import time

        name = _make_template(project_root)
        for client in _client_on(_app, project_root, tmp_path / "ssh_config"):
            deadline = time.monotonic() + 10
            entry = None
            while time.monotonic() < deadline:
                (entry,) = client.get("/api/templates").get_json()
                if entry["dag_ready"]:
                    break
                time.sleep(0.1)
            assert entry is not None and entry["dag_ready"], (
                "warm_template_dags did not draw the template in time"
            )
            # never asked the route to draw it -- only the warm thread could have
            assert client.get(f"/api/templates/{name}/dag").status_code == 200

    def test_listing_reads_yaml_and_never_solves(self, client, template):
        # the modal opens on every `+ workflow`; if listing planned anything it
        # would be as slow as a generate and would hold the planner's lock
        with mock.patch.object(Spec, "Solve", side_effect=AssertionError("solved")):
            body = client.get("/api/templates").get_json()
        (entry,) = body
        assert entry["name"] == template
        assert entry["description"] == "an assembly in, a bam out"
        assert entry["target_types"] == ["mock::bam"]
        assert entry["dag_ready"] is False

    def test_a_project_without_templates_lists_none(self, client):
        assert client.get("/api/templates").get_json() == []

    def test_drawing_is_a_job_and_is_then_served_from_cache(self, client, template):
        r = client.post(f"/api/templates/{template}/dag")
        assert r.status_code == 202, r.get_json()
        result = _finish(client, r.get_json())
        assert result["step_count"] >= 1

        drawn = client.get(f"/api/templates/{template}/dag")
        assert drawn.status_code == 200 and drawn.mimetype == "image/svg+xml"
        assert "<svg" in drawn.get_data(as_text=True)

        # the second ask costs nothing: no job, no solve
        again = client.post(f"/api/templates/{template}/dag")
        assert again.status_code == 200
        assert again.get_json()["cached"] is True
        assert client.get("/api/templates").get_json()[0]["dag_ready"] is True

    def test_each_theme_is_drawn_and_cached_separately(self, client, template):
        _finish(client, client.post(f"/api/templates/{template}/dag").get_json())
        r = client.post(f"/api/templates/{template}/dag", query_string={"theme": "dark"})
        assert r.status_code == 202, "a theme drawn once is not a theme drawn"
        _finish(client, r.get_json())
        light = client.get(f"/api/templates/{template}/dag").get_data(as_text=True)
        dark = client.get(
            f"/api/templates/{template}/dag", query_string={"theme": "dark"}
        ).get_data(as_text=True)
        assert light != dark

    def test_a_library_pull_invalidates_the_drawing(self, client, template):
        """The cache keys on the stdlib commit, not just the name.

        A template names transforms; pulling the library can change what it
        solves to. Without the commit in the key the modal would keep showing
        the previous graph, with nothing on screen to say so.
        """
        with mock.patch.object(stdlib, "stdlib_commit", return_value="a" * 40):
            _finish(client, client.post(f"/api/templates/{template}/dag").get_json())
            assert client.post(f"/api/templates/{template}/dag").status_code == 200
        with mock.patch.object(stdlib, "stdlib_commit", return_value="b" * 40):
            assert client.post(f"/api/templates/{template}/dag").status_code == 202

    def test_an_undrawn_template_is_refused_rather_than_drawn_inline(self, client, template):
        assert client.get(f"/api/templates/{template}/dag").status_code == 409

    def test_an_unknown_template_is_refused(self, client, template):
        assert client.post("/api/templates/nope/dag").status_code == 409
        assert client.post("/api/workflows", json={"template": "nope"}).status_code == 409

    def test_creating_from_a_template_takes_its_spec_and_its_rows(self, client, template):
        r = client.post("/api/workflows", json={"template": template})
        assert r.status_code == 201, r.get_json()
        name = r.get_json()["name"]

        detail = client.get(f"/api/workflows/{name}").get_json()
        assert detail["request"]["target_types"] == ["mock::bam"]
        assert [Path(p).name for p in detail["request"]["transform_libraries"]] == [
            "transforms.xgdb"
        ]

        items = client.get(f"/api/workflows/{name}/inputs").get_json()["items"]
        assert [i["type_name"] for i in items] == ["mock::assembly"]

    def test_the_copied_rows_keep_the_template_s_identity(self, client, template, project_root):
        """A deferred path is minted once; re-minting would move the task key.

        The template's build asserted a solve at one key. If creating from it
        re-added the rows, the workflow would plan to a different one and the
        two could not be said to be the same workflow.
        """
        source = DataInstanceLibrary.Load(
            project_root / "MetasmithLibraries" / "templates" / template / "inputs.xgdb"
        )
        name = client.post("/api/workflows", json={"template": template}).get_json()["name"]
        copied = DataInstanceLibrary.Load(
            Path(client.get(f"/api/workflows/{name}").get_json()["input_library"]["path"])
        )
        assert sorted(str(p) for p in copied.manifest) == sorted(
            str(p) for p in source.manifest
        )
        assert copied.GetKey() == source.GetKey()

    def test_a_created_workflow_can_still_be_typed_beyond_the_template(self, client, template):
        """The copy carries only the namespaces the template used; the editor
        needs every one the library offers, or a row cannot be retyped."""
        name = client.post("/api/workflows", json={"template": template}).get_json()["name"]
        info = client.get(f"/api/workflows/{name}/inputs").get_json()
        assert "mock" in info["type_namespaces"]

    def test_creating_without_a_template_is_untouched(self, client, template):
        """The blank path is the default and must stay exactly what it was."""
        name = client.post("/api/workflows", json={}).get_json()["name"]
        detail = client.get(f"/api/workflows/{name}").get_json()
        assert detail["request"]["target_types"] == []
        assert client.get(f"/api/workflows/{name}/inputs").get_json()["items"] == []


class TestSaveAsTemplate:
    """A workflow's recipe, saved as a starting point -- inputs stripped."""

    def _seeded(self, client, count=2) -> str:
        """A workflow with a plan: the precondition for saving one as a template.

        A template is never solved when it is saved or when it is loaded, so
        the guarantee that it can be solved at all is this one -- the recipe
        being saved is one that already worked.
        """
        name = _make_workflow(client)
        _seed_inputs(client, name, count)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        return name

    def _drawn(self, client, tmpl: str) -> str:
        r = client.post(f"/api/templates/{tmpl}/dag")
        assert r.status_code == 202, r.get_json()
        _finish(client, r.get_json())
        svg = client.get(f"/api/templates/{tmpl}/dag")
        assert svg.status_code == 200, svg.get_json()
        return svg.get_data(as_text=True)

    def test_saves_under_user_templates_with_blank_inputs(self, client, project_root):
        name = self._seeded(client, count=2)
        r = client.post(f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"})
        assert r.status_code == 201, r.get_json()
        body = r.get_json()
        assert body["source"] == "user"
        assert body["target_types"] == ["mock::bam"]

        spec_path = project_root / "user_templates" / "my-tpl" / "spec.yml"
        assert spec_path.is_file()
        raw = yaml.safe_load(spec_path.read_text())
        manifest = raw["input_library"]["manifest"]
        # same shape (one item per seeded input, same type) but none of the
        # source workflow's own file paths made it into the saved template
        assert len(manifest) == 2
        assert all(v["type"] == "mock::assembly" for v in manifest.values())
        seeded_names = {f"sample_{i}.fa" for i in range(2)}
        assert not any(n in str(k) for k in manifest for n in seeded_names)
        # names, not locations: nothing in the file points into this project
        assert "types" not in raw["input_library"]
        assert not [
            v for v in raw["transform_libraries"] + raw["resource_libraries"]
            if str(project_root) in v or "/" in v
        ]

    def test_its_dag_can_be_drawn_like_any_other_template_s(self, client):
        """The reported bug, from the outside.

        Opening `+ workflow` draws every template it lists, a user's included,
        by solving it -- so a saved template that only names its types has to
        arrive at that solve with them resolved, or the modal fails on
        `namespace [...] not found`.
        """
        name = self._seeded(client, count=1)
        client.post(f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"})
        assert "<svg" in self._drawn(client, "my-tpl")
        assert client.get("/api/templates").get_json()[0]["problems"] == []

    def test_a_name_this_project_cannot_account_for_is_listed_then_refused(
        self, client, project_root
    ):
        """Incompleteness is reported until it is used, same as everywhere else.

        A template written against libraries this project does not have is
        still a template: it lists, with what is missing said plainly. The
        refusal happens at the door that needs it resolved.
        """
        name = self._seeded(client, count=1)
        client.post(f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"})
        spec_path = project_root / "user_templates" / "my-tpl" / "spec.yml"
        raw = yaml.safe_load(spec_path.read_text())
        raw["transform_libraries"] = ["not_here"]
        spec_path.write_text(yaml.safe_dump(raw))

        (entry,) = client.get("/api/templates").get_json()
        assert entry["problems"] == ["transform library [not_here]"]
        r = client.post("/api/workflows", json={"template": "my-tpl"})
        assert r.status_code == 400 and "not_here" in r.get_json()["error"]

    def test_listed_alongside_library_templates(self, client, template):
        name = self._seeded(client)
        client.post(f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"})
        body = client.get("/api/templates").get_json()
        sources = {t["name"]: t["source"] for t in body}
        assert sources == {template: "library", "my-tpl": "user"}

    def test_creating_from_it_reproduces_the_shape(self, client):
        name = self._seeded(client, count=2)
        client.post(f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"})
        made = client.post("/api/workflows", json={"template": "my-tpl"}).get_json()["name"]
        items = client.get(f"/api/workflows/{made}/inputs").get_json()["items"]
        assert len(items) == 2
        assert all(i["type_name"] == "mock::assembly" for i in items)

    def test_a_workflow_that_has_not_solved_is_refused(self, client):
        """No plan, no template: the recipe has never been shown to work, and
        nothing downstream solves one before offering it."""
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        r = client.post(f"/api/workflows/{name}/save_as_template", json={"name": "unsolved"})
        assert r.status_code == 400
        assert "no successful plan" in r.get_json()["error"]

    def test_a_workflow_whose_plan_failed_is_refused(self, client):
        """`planned` is not `ok`: a solve that drops its target leaves a result."""
        name = _make_workflow(client, targets=("mock::unreachable",))
        _seed_inputs(client, name, 1)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        detail = client.get(f"/api/workflows/{name}").get_json()
        assert detail["planned"] and detail["success"] is False
        r = client.post(f"/api/workflows/{name}/save_as_template", json={"name": "failed"})
        assert r.status_code == 400

    def test_name_collision_is_refused(self, client):
        name = self._seeded(client)
        assert client.post(
            f"/api/workflows/{name}/save_as_template", json={"name": "dup"}
        ).status_code == 201
        second = self._seeded(client)
        assert client.post(
            f"/api/workflows/{second}/save_as_template", json={"name": "dup"}
        ).status_code == 409

    def test_user_template_can_be_deleted_but_library_one_cannot(self, client, template):
        name = self._seeded(client)
        client.post(f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"})
        assert client.delete("/api/templates/my-tpl").status_code == 200
        assert "my-tpl" not in {t["name"] for t in client.get("/api/templates").get_json()}
        assert client.delete(f"/api/templates/{template}").status_code == 409
        assert client.delete("/api/templates/nope").status_code == 409

    def test_saves_and_recreates_when_the_stdlib_is_a_symlinked_checkout(self, _app, tmp_path):
        """The reported bug: `MetasmithLibraries` a symlink to a shared checkout.

        `stdlib.discover` resolves it -- deliberately, so a workflow and the
        library it loaded from agree -- and that used to leak a real,
        symlink-crossing path into the saved template, which then fell outside
        the project root and tripped `Template.Save`'s portability check. A
        template only ever names what it needs now (namespace names, and
        whatever the workflow's own request already held), so there is nothing
        left for that check to catch.
        """
        root = _fabricate_project(
            tmp_path / "project", mlib_target=tmp_path / "shared_stdlib_checkout",
        )
        for client in _client_on(_app, root, tmp_path / "ssh_config"):
            name = self._seeded(client, count=1)
            r = client.post(
                f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"},
            )
            assert r.status_code == 201, r.get_json()

            made = client.post("/api/workflows", json={"template": "my-tpl"})
            assert made.status_code == 201, made.get_json()
            items = client.get(
                f"/api/workflows/{made.get_json()['name']}/inputs"
            ).get_json()["items"]
            assert len(items) == 1 and items[0]["type_name"] == "mock::assembly"

    def test_saves_when_transform_libraries_were_narrowed_under_a_symlinked_stdlib(
        self, _app, tmp_path
    ):
        """Narrowing which libraries a workflow draws from writes their real,
        resolved paths into its own request -- the same paths a symlinked
        stdlib resolves to outside the project. Saving that as a template
        must not freeze those in either: only the library names travel, same
        as a type namespace.
        """
        root = _fabricate_project(
            tmp_path / "project", mlib_target=tmp_path / "shared_stdlib_checkout",
        )
        for client in _client_on(_app, root, tmp_path / "ssh_config"):
            name = self._seeded(client, count=1)
            available = stdlib.discover(root)["transform_libraries"]
            assert client.patch(
                f"/api/workflows/{name}", json={"transform_libraries": available},
            ).status_code == 200

            r = client.post(
                f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"},
            )
            assert r.status_code == 201, r.get_json()
            assert r.get_json()["problems"] == []
            # the second unresolved reference, and it fails in its own frame:
            # a narrowed library is a name too, and the DAG is drawn by solving
            assert "<svg" in self._drawn(client, "my-tpl")

            made = client.post("/api/workflows", json={"template": "my-tpl"})
            assert made.status_code == 201, made.get_json()
            detail = client.get(f"/api/workflows/{made.get_json()['name']}").get_json()
            assert sorted(Path(p).name for p in detail["request"]["transform_libraries"]) == (
                sorted(Path(p).name for p in available)
            )


class TestTypeResync:
    """A workflow's input library must not stay pinned to the stdlib as it
    stood the day the workflow was created."""

    def test_restart_picks_up_a_type_added_to_an_existing_namespace(
        self, _app, project_root, tmp_path
    ):
        """`resync_workflow_types` runs at `bind_project` time, alongside the
        other warm-up passes -- this is that thread's own version of
        `test_a_fresh_start_warms_every_template_s_dag`.

        The workflow is created *before* the type is added and the app is
        rebound (simulating a restart) *after* -- that ordering is the whole
        point: a live process never re-reads `mock.yml` on its own, only a
        fresh `bind_project` does.
        """
        import time

        mock_yml = project_root / "MetasmithLibraries" / "data_types" / "mock.yml"

        for client in _client_on(_app, project_root, tmp_path / "ssh_config"):
            name = _make_workflow(client)
            lib_path = Path(
                client.get(f"/api/workflows/{name}").get_json()["input_library"]["path"]
            )

        lib = DataInstanceLibrary.Load(lib_path)
        with pytest.raises((AssertionError, ValueError, KeyError)):
            lib.GetType("mock::genome_name")

        types = DataTypeLibrary.Load(mock_yml)
        types["genome_name"] = Endpoint(properties={"genome_name"})
        types.Save(mock_yml)

        for _client in _client_on(_app, project_root, tmp_path / "ssh_config"):
            deadline = time.monotonic() + 10
            ok = False
            while time.monotonic() < deadline:
                try:
                    DataInstanceLibrary.Load(lib_path).GetType("mock::genome_name")
                    ok = True
                    break
                except (AssertionError, ValueError, KeyError):
                    time.sleep(0.1)
            assert ok, "resync_workflow_types did not refresh the workflow's library in time"


class TestDagLayoutRoute:
    """Geometry for the graph the info panel draws itself.

    The panel's nodes are buttons, so it cannot show the rendered SVG -- and it
    used to lay its graph out with an engine of its own, which is how it came to
    disagree with the plan diagram about the shape of the same graph. The page
    still builds the graph; only the placement is here.
    """

    def _lay(self, client, nodes, edges, **body):
        r = client.post("/api/dag/layout", json={"nodes": nodes, "edges": edges, **body})
        assert r.status_code == 200, r.get_json()
        return r.get_json()

    def test_a_chain_is_placed_top_down_with_a_path_per_edge(self, client):
        geo = self._lay(
            client,
            [
                {"id": "t:sequences::gbk", "kind": "type", "label": "sequences::gbk"},
                {"id": "x:0", "kind": "transform", "label": "ppanggolin"},
                {"id": "t:pangenome::heatmap", "kind": "type", "label": "pangenome::heatmap"},
            ],
            [
                {"from": "t:sequences::gbk", "to": "x:0"},
                {"from": "x:0", "to": "t:pangenome::heatmap"},
            ],
        )
        rows = {n["id"]: n["row"] for n in geo["nodes"]}
        assert rows["t:sequences::gbk"] < rows["x:0"] < rows["t:pangenome::heatmap"]
        assert geo["width"] > 0 and geo["height"] > 0
        assert all(e["d"].startswith("M ") for e in geo["edges"])

    def test_the_label_is_split_the_way_the_svg_splits_it(self, client):
        geo = self._lay(
            client, [{"id": "a", "kind": "type", "label": "sequences::gbk"}], [],
        )
        n = geo["nodes"][0]
        assert (n["namespace"], n["label"], n["full"]) == ("sequences", "gbk", "sequences::gbk")

    def test_ids_are_the_callers_and_come_back_untouched(self, client):
        """`t:<type>` and `x:<index>` are how the panel knows what was clicked."""
        geo = self._lay(
            client,
            [{"id": "x:12", "kind": "transform", "label": "t"}, {"id": "t:a::b", "kind": "type"}],
            [{"from": "x:12", "to": "t:a::b"}],
        )
        assert {n["id"] for n in geo["nodes"]} == {"x:12", "t:a::b"}
        assert geo["edges"][0]["from"] == "x:12"

    def test_an_edge_naming_a_node_that_is_not_there_is_dropped(self, client):
        # the panel trims a graph to what fits and leaves the `+N more` stub
        # behind; an edge into what was trimmed must not invent a node
        geo = self._lay(
            client, [{"id": "a", "kind": "type"}], [{"from": "a", "to": "gone"}],
        )
        assert [n["id"] for n in geo["nodes"]] == ["a"]
        assert geo["edges"] == []

    def test_an_empty_graph_is_an_empty_canvas_not_an_error(self, client):
        geo = self._lay(client, [], [])
        assert geo["nodes"] == [] and geo["edges"] == []

    def test_a_cycle_is_reported_without_a_path_to_draw(self, client):
        """One tool consuming and producing the same type is legal input."""
        geo = self._lay(
            client,
            [{"id": "a", "kind": "type"}, {"id": "b", "kind": "transform"}],
            [{"from": "a", "to": "b"}, {"from": "b", "to": "a"}],
        )
        back = [e for e in geo["edges"] if e["back"]]
        assert len(back) == 1 and back[0]["d"] == ""

    def test_nodes_and_edges_must_be_lists(self, client):
        r = client.post("/api/dag/layout", json={"nodes": {"a": 1}, "edges": []})
        assert r.status_code == 400


class TestDagTheme:
    """The ink the page draws in, served rather than restated in a stylesheet.

    `dag_renderer.DARK` is `LIGHT` with only its colours replaced, so a marker's
    shape, its scale and its stroke weight cannot drift between them. A CSS copy
    of any of that gives the guarantee away, and a locally-drawn plan would stop
    being the same drawing as the exported one.
    """

    def test_both_themes_arrive_at_once(self, client):
        # the toggle must not cost a round trip
        ink = client.get("/api/dag/theme").get_json()
        assert set(ink) == {"light", "dark"}
        for theme in ink.values():
            assert set(theme["styles"]) == {"transform", "data", "target"}
            assert theme["plate"]["background"] and theme["plate"]["edge"]

    def test_a_style_carries_what_a_browser_has_to_draw_with(self, client):
        ink = client.get("/api/dag/theme").get_json()
        st = ink["light"]["styles"]
        assert st["transform"]["shape"] == "triangle_down"
        assert st["data"]["shape"] == "circle" and not st["data"]["solid"]
        # the requested output is the same circle drawn solid, heavier
        assert st["target"]["solid"] and st["target"]["stroke_width"] > st["data"]["stroke_width"]

    def test_only_the_colours_differ_between_the_two(self, client):
        ink = client.get("/api/dag/theme").get_json()
        for kind in ink["light"]["styles"]:
            light, dark = ink["light"]["styles"][kind], ink["dark"]["styles"][kind]
            for field in ("shape", "marker_scale", "stroke_width", "rx", "solid"):
                assert light[field] == dark[field], (kind, field)


class TestWorkflowGenerateMore:
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

    def test_a_result_echoes_what_it_planned_from(self, client):
        """The server says what it was given, in its own words.

        A plan fails almost always because a type is not what the person
        thought it was, and the recipe on the page is the browser's belief
        about that -- so a failure that only lists hints leaves the one fact
        worth checking unstated.
        """
        name = _make_workflow(client, targets=["mock::unreachable"])
        _seed_inputs(client, name, 2)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"] is False
        assert [g["type"] for g in result["given"]] == ["mock::assembly"] * 2
        assert all(g["path"] for g in result["given"])
        assert result["targets"] == [{"type": "mock::unreachable", "parents": []}]
        # and it survives the reload, like the hints beside it
        assert client.get(f"/api/workflows/{name}").get_json()["result"]["given"]

    def test_targets_may_carry_lineage(self, client):
        """A target is either a bare name or a name plus the targets it comes off.

        Both spellings reach disk and both plan; the dict form is what lets two
        targets of one type be distinct requests rather than a duplicate.
        """
        name = _make_workflow(client, targets=[{"type": "mock::bam", "parents": []}])
        _seed_inputs(client, name)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"] is True

        stored = client.get(f"/api/workflows/{name}").get_json()["request"]["target_types"]
        assert stored == [{"type": "mock::bam", "parents": []}]

    def test_a_target_cannot_descend_from_a_later_one(self, client):
        """Parents are indices into the targets declared *before* this one.

        A forward reference would silently link to the wrong target once the
        list is renumbered, so it is refused with the position named.

        Named 1-based, like the target it is refusing. Positions are stored
        0-based, and a sentence that says "target #1 names parent #1" about
        index 1 reads as an off-by-one in whichever half you trust less.
        """
        from metasmith.agents import TargetBuilder

        with pytest.raises(AssertionError, match=r"target #1 \[mock::bam\] names parent #2"):
            TargetBuilder().AddAll([{"type": "mock::bam", "parents": [1]}])

    def test_generate_requires_a_target(self, client):
        r = client.post("/api/workflows", json={})
        name = r.get_json()["name"]
        assert client.post(f"/api/workflows/{name}/generate", json={}).status_code == 400

    def test_generate_without_a_sample_type_plans_the_whole_library(self, client):
        """No sample type is not an incomplete request -- it is one sample.

        Sampling splits a library into one run per item of a type; a plan does
        not need it, and the page does not offer it. What comes back has to be
        the same plan the sampled form produces for a library holding one item
        of that type.
        """
        name = _make_workflow(client, sample=None)
        _seed_inputs(client, name, count=1)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"], result
        assert result["step_count"] > 0

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
            f = project.input_library_path(name) / "same.fa"
            f.write_text(">x\nACGT\n")
            _put_rows(client, name, [_row("a", f.resolve())])
        # different files at different paths -> different keys; now point both at one path
        shared = project.root / "shared.fa"
        shared.write_text(">x\nACGT\n")
        for name in (a, b):
            _put_rows(client, name, [_row("a", shared)])
        ka = _finish(client, client.post(f"/api/workflows/{a}/generate", json={}).get_json())
        kb = _finish(client, client.post(f"/api/workflows/{b}/generate", json={}).get_json())
        assert ka["task_key"] == kb["task_key"]

    def test_delete_without_runs_archives_first(self, client):
        project: Project = client.application.config["MSM_PROJECT"]
        name = _make_workflow(client)
        assert client.delete(f"/api/workflows/{name}").get_json()["action"] == "archived"
        # the directory is still there -- that is what makes it recoverable
        assert project.workflow_path(name).is_dir()
        assert client.delete(f"/api/workflows/{name}").get_json()["action"] == "deleted"
        assert not project.workflow_path(name).exists()


class TestWorkflowRename:
    """Correcting the made-up name, while nothing is keyed to it yet.

    The name is the directory, and once a plan lands in that directory the
    directory *is* the task bundle a run stages from -- so this is deliberately
    only open before a generate. `fork` is the way to get a new name afterwards,
    and it says out loud that it discards cache reuse.
    """

    def test_renames_the_directory_and_the_record(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        r = client.post(f"/api/workflows/{name}/rename", json={"name": "chosen-name"})
        assert r.status_code == 200, r.get_json()
        assert r.get_json()["name"] == "chosen-name"

        project = client.application.config["MSM_PROJECT"]
        assert not project.workflow_path(name).exists()
        assert project.workflow_path("chosen-name").is_dir()
        body = client.get("/api/workflows/chosen-name").get_json()
        assert body["request"]["name"] == "chosen-name"
        assert client.get(f"/api/workflows/{name}").status_code == 409
        # the input library moved with it, still live and still holding its item
        assert len(client.get("/api/workflows/chosen-name/inputs").get_json()["items"]) == 1

    def test_typed_text_is_slugified(self, client):
        name = _make_workflow(client)
        r = client.post(f"/api/workflows/{name}/rename", json={"name": "My Assembly Run"})
        assert r.get_json()["name"] == "my-assembly-run"

    def test_the_put_renames_it_too(self, client):
        """The same convention the agent and the host are saved by."""
        name = _make_workflow(client)
        r = client.put(f"/api/workflows/{name}", json={"name": "chosen-name"})
        assert r.status_code == 200, r.get_json()
        assert r.get_json()["name"] == "chosen-name"
        assert client.get(f"/api/workflows/{name}").status_code == 409

    def test_the_put_saves_the_recipe_and_the_name_at_once(self, client):
        name = _make_workflow(client)
        r = client.put(f"/api/workflows/{name}", json={
            "name": "chosen-name", "target_types": ["mock::bam"],
        })
        assert r.status_code == 200, r.get_json()
        body = client.get("/api/workflows/chosen-name").get_json()
        assert body["request"]["target_types"] == ["mock::bam"]
        assert body["request"]["name"] == "chosen-name"

    def test_a_put_without_a_name_only_saves_the_recipe(self, client):
        name = _make_workflow(client)
        r = client.put(f"/api/workflows/{name}", json={"target_types": ["mock::bam"]})
        assert r.status_code == 200 and r.get_json()["name"] == name

    def test_an_empty_name_is_refused(self, client):
        name = _make_workflow(client)
        assert client.post(f"/api/workflows/{name}/rename", json={"name": "  "}).status_code == 400

    def test_a_taken_name_is_refused(self, client):
        a = _make_workflow(client)
        b = _make_workflow(client)
        r = client.post(f"/api/workflows/{a}/rename", json={"name": b})
        assert r.status_code == 409
        assert "already exists" in r.get_json()["error"]

    def test_renaming_to_itself_is_a_no_op(self, client):
        name = _make_workflow(client)
        r = client.post(f"/api/workflows/{name}/rename", json={"name": name})
        assert r.status_code == 200 and r.get_json()["name"] == name

    def test_refused_once_generated(self, client):
        """The bundle is keyed to this directory; moving it would strand it."""
        name = _make_workflow(client)
        _seed_inputs(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        r = client.post(f"/api/workflows/{name}/rename", json={"name": "too-late"})
        assert r.status_code == 409
        assert "fork" in r.get_json()["error"].lower()
        assert client.application.config["MSM_PROJECT"].workflow_path(name).is_dir()

    def test_a_repeated_rename_is_refused_not_raised(self, client):
        """The page can send this twice: Enter closes the field, which blurs it.

        The second request reads the workflow under a name the first one has
        already moved -- once it is gone that is a plain 409, but if it slips in
        while the move is still happening the rename itself fails. Either way it
        must reach the user as a refusal.
        """
        name = _make_workflow(client)
        assert client.post(f"/api/workflows/{name}/rename", json={"name": "once"}).status_code == 200
        again = client.post(f"/api/workflows/{name}/rename", json={"name": "once"})
        assert again.status_code == 409
        assert again.get_json()["kind"] == "refused"

    def test_a_failed_move_is_refused_not_raised(self, client):
        """The existence check is not a lock, so the move itself can still fail.

        `Path.rename` raises on a non-empty target, which is exactly what the
        loser of a race meets. That is a refusal, not a server fault.
        """
        name = _make_workflow(client)
        boom = OSError(39, "Directory not empty")
        with mock.patch("pathlib.Path.rename", side_effect=boom):
            r = client.post(f"/api/workflows/{name}/rename", json={"name": "wanted"})
        assert r.status_code == 409
        assert "could not rename" in r.get_json()["error"]
        project = client.application.config["MSM_PROJECT"]
        assert project.workflow_path(name).is_dir(), "the original is left alone"

    def test_the_archive_mark_moves_with_it(self, client):
        name = _make_workflow(client)
        client.post(f"/api/workflows/{name}/archive", json={"archived": True})
        r = client.post(f"/api/workflows/{name}/rename", json={"name": "put-away"})
        assert r.status_code == 200
        assert r.get_json()["archived_at"]
        assert client.get("/api/workflows").get_json() == []
        listed = client.get("/api/workflows?archived=1").get_json()
        assert [w["name"] for w in listed] == ["put-away"]
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
    _deployed(client, "smith")
    name = _make_workflow(client)
    _seed_inputs(client, name)
    _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
    return name


class TestRuns:
    def _launch(self, client, workflow) -> dict:
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.StageWorkflow.return_value = None
            # `index` is the key ListWorkflowRuns actually returns. This said
            # `run` -- a key it has never had -- which is what kept a
            # permanently-None run_number invisible.
            mload.return_value.ListWorkflowRuns.return_value = [
                {"index": 1, "path": "/x/logs.1", "timestamp": "t"},
            ]
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

    def test_the_run_number_is_the_run_index(self, client, runnable):
        """Without it every probe follows `logs.latest`, which on a re-run is
        the *other* run's directory."""
        run = self._launch(client, runnable)
        body = client.get(f"/api/runs/{runnable}/{run['name']}").get_json()
        assert body["run_number"] == 1

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

    def test_refuses_an_agent_that_was_never_deployed(self, client, runnable, tmp_path):
        """Deploy is its own button, so it is entirely skippable.

        Before this, a launch onto a fresh agent was accepted and then failed
        somewhere inside staging, minutes later, if it reported at all.
        """
        client.post("/api/agents", json={"name": "fresh", "home": str(tmp_path / "fresh")})
        r = client.post("/api/runs", json={"workflow": runnable, "agent": "fresh"})
        assert r.status_code == 409
        assert "has not been deployed yet" in r.get_json()["error"]

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
            f"/api/runs/{runnable}/{run['name']}").get_json()["action"] == "archived"
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

    def test_a_collect_that_lands_broken_links_fails_and_says_which(self, client, runnable):
        """The folder looks collected and holds nothing.

        A results library links into nextflow's work directory; if what it
        named is gone on the agent, following the links produces a destination
        full of pointers at a disk this machine does not have. It used to be
        stamped collected and reported as a success.
        """
        run = self._launch(client, runnable)

        def _collect(agent_path, task_key, dest_uri, allow_globus=True):
            Path(dest_uri).mkdir(parents=True, exist_ok=True)
            return {"completed": [], "errors": [], "dangling": ["out.bam"]}

        with mock.patch("metasmith.ops.runtime.collect", side_effect=_collect):
            r = client.post(f"/api/runs/{runnable}/{run['name']}/collect", json={})
            job = client.application.config["MSM_JOBS"].get(r.get_json()["id"])
            assert job.wait(120)
        assert job.status == "failed", job.status
        lines = job.lines()
        assert any("out.bam" in ln for ln in lines), lines
        assert not client.get(
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


class TestLaunchParams:
    """What the launch panel sends, and where it ends up."""

    def _launch(self, client, workflow, **body) -> tuple:
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            agent = mock.MagicMock()
            mload.return_value = agent
            agent.ListWorkflowRuns.return_value = [{"index": 1}]
            r = client.post("/api/runs", json={
                "workflow": workflow, "agent": "smith", **body,
            })
            assert r.status_code == 202, r.get_json()
            _finish(client, r.get_json()["job"])
            return r.get_json()["run"], agent.RunWorkflow.call_args

    def test_params_reach_the_agent_and_the_record(self, client, runnable):
        run, call = self._launch(client, runnable, params={"acct": "st-you-1", "n": "4"})
        assert call.kwargs["params"] == {"acct": "st-you-1", "n": 4}
        body = client.get(f"/api/runs/{runnable}/{run['name']}").get_json()
        assert body["params"] == {"acct": "st-you-1", "n": 4}

    def test_a_step_keyed_override_becomes_a_per_step_selector(self, client, runnable):
        """The whole point of keying by position.

        A JSON object has string keys, and a string key is read as a transform
        *name* -- which matches every step running it, or nothing. Only an int
        addresses one step.
        """
        run, call = self._launch(client, runnable, resource_overrides={
            "1": {"cpus": "8", "memory_gb": "16", "duration_h": ""},
        })
        ro = call.kwargs["resource_overrides"]
        assert list(ro) == [1]
        assert ro[1].cpus == 8
        assert ro[1].memory.value_gb == 16
        assert ro[1].duration is None
        body = client.get(f"/api/runs/{runnable}/{run['name']}").get_json()
        assert body["resource_overrides"] == {"1": {"cpus": 8, "memory_gb": 16.0}}

    def test_a_step_with_every_box_empty_is_not_sent(self, client, runnable):
        """The page draws one row per step; only the filled ones are overrides."""
        _, call = self._launch(client, runnable, resource_overrides={
            "1": {"cpus": "", "memory_gb": "", "duration_h": ""},
            "2": {"cpus": "4"},
        })
        assert list(call.kwargs["resource_overrides"]) == [2]

    def test_unlimited_time_is_not_the_same_as_an_empty_box(self, client, runnable):
        """The two things an empty time box could not both mean.

        Nothing in the box is "whatever the transform declared"; the infinity
        button sends a token, and it has to survive the pass that drops empties.
        """
        run, call = self._launch(client, runnable, resource_overrides={
            "1": {"duration_h": "unlimited"},
            "2": {"duration_h": "3"},
        })
        ro = call.kwargs["resource_overrides"]
        assert ro[1].duration.unlimited
        assert ro[1].duration.AsNextflowFormat() == "null"
        assert not ro[2].duration.unlimited
        body = client.get(f"/api/runs/{runnable}/{run['name']}").get_json()
        assert body["resource_overrides"]["1"] == {"duration_h": "unlimited"}

    def test_only_the_time_can_be_unlimited(self, client, runnable):
        r = client.post("/api/runs", json={
            "workflow": runnable, "agent": "smith",
            "resource_overrides": {"1": {"memory_gb": "unlimited"}},
        })
        assert r.status_code == 400
        assert "memory_gb" in r.get_json()["error"]

    def test_a_non_numeric_override_is_refused(self, client, runnable):
        r = client.post("/api/runs", json={
            "workflow": runnable, "agent": "smith",
            "resource_overrides": {"1": {"cpus": "lots"}},
        })
        assert r.status_code == 400
        assert "cpus" in r.get_json()["error"]

    def test_the_dry_run_delay_does_not_land_in_the_gpu_slot(self, client, runnable):
        """It used to: the ops-level call was one positional argument short."""
        _, call = self._launch(client, runnable)
        assert call.kwargs.get("stub_delay") == 0
        assert "gpus" not in call.kwargs


class TestStepSelectors:
    """The name a step's resource selector has to address."""

    def test_the_summary_names_the_nextflow_process(self, client, runnable):
        """The file name a person recognises is not necessarily that name.

        The process is minted from the transform's own `name` with a position
        prefix; the summary used to carry only the `.py` file it came from, so
        a page could not build a correct selector from what it had.
        """
        steps = client.get(f"/api/workflows/{runnable}").get_json()["result"]["step_display"]
        assert steps
        for s in steps:
            assert s["process"].startswith(f"p{s['order']:02}__")
            assert "declared_resources" in s

    def test_the_result_carries_the_whole_drawing(self, client, runnable):
        """The page draws the plan itself, from this, and lays its step rows
        out against the same numbers.

        Stored rather than computed per request: laying a plan out is the most
        expensive thing metasmith does with one, and it cannot change without a
        re-solve. Nothing here may be missing -- the block that builds it is
        caught broadly, and a silent failure leaves the diagram blank.
        """
        result = client.get(f"/api/workflows/{runnable}").get_json()["result"]
        graph = result["plan_graph"]
        for key in ("v", "width", "height", "row_pitch", "lane_pitch", "anchor"):
            assert key in graph, key
        # an edge is its baked path and nothing else -- the grid it came from
        # used to travel with it, for a browser that re-baked it itself
        assert all(e["back"] or e["d"] for e in graph["edges"])
        # every step is a node of it, and every node is inside the plate
        by_step = {n["step"]: n for n in graph["nodes"] if n.get("step") is not None}
        assert {s["order"] for s in result["step_display"]} == set(by_step)
        for n in graph["nodes"]:
            assert 0 < n["cy"] < graph["height"]

    def test_a_step_node_points_at_the_transform_it_runs(self, client, runnable):
        """What a click on a plan node has to end up as.

        Resolved server-side, against the same index the panel is addressed by:
        the plan was solved against a staged clone of the library, so its own
        paths are not the indexed clone's and the browser has nothing to match
        on. A step whose library is not the indexed one stays unclickable, so
        the assertion is on the shape rather than on every step resolving.
        """
        result = client.get(f"/api/workflows/{runnable}").get_json()["result"]
        graph = result["plan_graph"]
        index = client.get("/api/project/type-index").get_json()["transforms"]
        steps = [n for n in graph["nodes"] if n.get("step") is not None]
        assert steps
        for n in steps:
            i = n["transform_index"]
            if i is None:
                continue
            assert 0 <= i < len(index)
        # a data node is addressed by the type it stands for, which is its id
        for n in graph["nodes"]:
            if n["kind"] != "transform":
                assert n["type"] == n["id"]

    def test_the_drawing_is_backfilled_onto_a_result_without_one(
        self, client, runnable, project_root,
    ):
        """A result planned by the CLI, or stored against an older shape of the
        drawing, is revisited rather than left to draw nothing.

        Tested by the payload's own version rather than by presence, which is
        why the stored one is replaced rather than deleted: a result carrying an
        older shape of the block would otherwise never be looked at again.
        """
        import yaml

        from metasmith.ops.workflow import GEOMETRY_VERSION

        path = project_root / "workflows" / runnable / "result.yml"
        stored = yaml.safe_load(path.read_text())
        assert stored["plan_graph"]["v"] == GEOMETRY_VERSION
        stored["plan_graph"] = {"v": GEOMETRY_VERSION - 1, "width": 1, "height": 1}
        path.write_text(yaml.dump(stored))
        again = client.get(f"/api/workflows/{runnable}").get_json()["result"]
        assert (again.get("plan_graph") or {}).get("v") == GEOMETRY_VERSION

    def test_a_position_selector_matches_the_process_that_position_gets(self):
        """The two halves that have to agree, pinned against each other.

        `RunWorkflow` renders an int override key as `p03__.*`; the compiler
        names the process from the transform with the same prefix. They live in
        different files, and a page addressing a step is trusting them to match.
        """
        import re
        from metasmith.models.workflow import NextflowProcessName
        assert re.fullmatch("p03__.*", NextflowProcessName(3, "sort/bam"))
        # a transform whose name has a slash cannot be a nextflow process name
        assert "/" not in NextflowProcessName(3, "sort/bam")

    def test_a_name_selector_matches_every_step_of_that_transform(self):
        """Which is the distinction the int key exists to avoid."""
        import re
        from metasmith.models.workflow import NextflowProcessName
        for order in (1, 7):
            assert re.fullmatch(".*__map_reads", NextflowProcessName(order, "map_reads"))


class TestOrphanedLaunches:
    """`staging` and `launching` are owned by a thread, not by anything on disk.

    So a server that is restarted, Ctrl-C'd, or whose launch thread died on a
    hung ssh leaves runs claiming to be staged by something that no longer
    exists. Nothing on the agent can move them -- there is no staged task to
    probe -- and before this they sat there for good.
    """

    def _staging_run(self, client, runnable, **record) -> tuple:
        project: Project = client.application.config["MSM_PROJECT"]
        rec = project.create_run(runnable, {
            "agent": "smith", "task_key": "k",
            "launched_by": client.application.config["MSM_INSTANCE"],
            **record,
        })
        return project, rec

    def _watcher(self, client):
        return client.application.config["MSM_WATCHER"]

    def test_a_run_from_a_dead_server_is_resolved(self, client, runnable):
        project, rec = self._staging_run(client, runnable, launched_by="some-other-server")
        assert self._watcher(client).poll_once() == [{"run": rec.name, "state": "failed"}]
        body = client.get(f"/api/runs/{runnable}/{rec.name}").get_json()
        assert body["state"] == "failed"
        assert "is gone" in body["error"]

    def test_a_launch_this_server_never_started_is_resolved(self, client, runnable):
        """Mine, but with no job running for it, and old enough to mean it."""
        project, rec = self._staging_run(client, runnable, created_at="2020-01-01T00:00:00+00:00")
        self._watcher(client).poll_once()
        assert client.get(f"/api/runs/{runnable}/{rec.name}").get_json()["state"] == "failed"

    def test_a_launch_submitted_a_moment_ago_is_left_alone(self, client, runnable):
        """The window between writing the record and the job appearing is real."""
        project, rec = self._staging_run(client, runnable)
        assert self._watcher(client).poll_once() == []
        assert client.get(f"/api/runs/{runnable}/{rec.name}").get_json()["state"] == "staging"

    def test_a_live_job_owns_its_run(self, client, runnable):
        project, rec = self._staging_run(client, runnable, created_at="2020-01-01T00:00:00+00:00")
        jobs = client.application.config["MSM_JOBS"]
        held = threading.Event()
        job = jobs.submit("run", "held", lambda j: held.wait(10),
                          subject={"workflow": runnable, "run": rec.name})
        try:
            assert self._watcher(client).poll_once() == []
            assert client.get(
                f"/api/runs/{runnable}/{rec.name}").get_json()["state"] == "staging"
        finally:
            held.set()
            job.wait(10)


TRACE_TSV = "\n".join([
    "task_id\thash\tnative_id\tname\tstatus\texit\tsubmit\tduration\tpeak_rss",
    "1\te9/18a9b7\t297940\tp01__getNcbiAssembly (1)\tCOMPLETED\t0\t2026-07-28 02:10:01\t19.6s\t8.5 MB",
    "2\te4/015949\t301869\tp02__ppanggolin (1)\tFAILED\t1\t2026-07-28 02:10:21\t55.6s\t2 GB",
]) + "\n"


class TestTrace:
    """The per-task record, and the one thing it exists to make visible.

    A step failing under `errorStrategy 'ignore'` leaves the run reported as
    completed, so the trace is the only place the loss is written down.
    """

    def test_a_collected_run_reads_its_own_trace(self, client, runnable):
        run = TestResultsFiltering._name(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        logs = project.outputs_path(runnable, run) / "_metadata" / "logs.2026-01-01"
        logs.mkdir(parents=True)
        (logs / "nxf_trace.tsv").write_text(TRACE_TSV)

        body = client.get(f"/api/runs/{runnable}/{run}/trace").get_json()
        assert body["source"] == "local"
        assert body["done"] == 1 and body["failed"] == 1
        assert [t["state"] for t in body["tasks"]] == ["done", "failed"]
        assert body["tasks"][1]["exit"] == 1

    def test_the_alias_is_never_the_log_directory(self, client, runnable):
        """`logs.latest` names one of its own siblings, so taking it would read
        the same run twice and, after a re-collect, could read a stale one."""
        run = TestResultsFiltering._name(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        meta = project.outputs_path(runnable, run) / "_metadata"
        real = meta / "logs.2026-01-01"
        real.mkdir(parents=True)
        (real / "nxf_trace.tsv").write_text(TRACE_TSV)
        (meta / "logs.latest").symlink_to(real.name)

        body = client.get(f"/api/runs/{runnable}/{run}/trace").get_json()
        assert body["file"].endswith("logs.2026-01-01/nxf_trace.tsv")

    def test_no_trace_is_an_empty_answer_not_an_error(self, client, runnable):
        run = TestResultsFiltering._name(client, runnable)
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.ReadWorkflowTrace.return_value = {
                "exists": False, "lines": [], "file": "/x", "run_dir": "/x",
            }
            body = client.get(f"/api/runs/{runnable}/{run}/trace").get_json()
        assert body["tasks"] == [] and body["failed"] == 0

    def test_a_nonzero_exit_is_failed_whatever_the_status_says(self):
        """Nextflow can call a task COMPLETED and still hand back an exit code;
        a caller asking which steps died should not have to know that."""
        from metasmith.ops import runtime as op_runtime
        rows = op_runtime.parse_trace(
            "name\tstatus\texit\nx (1)\tCOMPLETED\t137\n"
        )
        assert rows[0]["state"] == "failed"


class TestResultTree:
    """Walking the collected folder, and refusing to walk out of it."""

    @staticmethod
    def _collected(client, workflow, tmp_path) -> tuple[str, Path]:
        run = TestResultsFiltering._name(client, workflow)
        project: Project = client.application.config["MSM_PROJECT"]
        outputs = project.outputs_path(workflow, run)
        (outputs / "1_mock-bam").mkdir(parents=True)
        (outputs / "1_mock-bam" / "a.bam").write_text("x" * 4096)
        logs = outputs / "_metadata" / "logs.2026-01-01"
        logs.mkdir(parents=True)
        (logs / "agent.log").write_text("hello\nworld\n")
        (outputs / "_metadata" / "logs.latest").symlink_to("logs.2026-01-01")
        return run, outputs

    def _flat(self, node, out=None):
        out = {} if out is None else out
        for c in node.get("children") or []:
            out[c["path"]] = c
            self._flat(c, out)
        return out

    def test_the_logs_are_in_the_tree(self, client, runnable, tmp_path):
        """They are not manifest entries, so a tree built from the manifest
        could not show them -- and they are what a failed step is read from."""
        run, _ = self._collected(client, runnable, tmp_path)
        body = client.get(f"/api/runs/{runnable}/{run}/tree").get_json()
        flat = self._flat(body["root"])
        assert flat["_metadata/logs.2026-01-01/agent.log"]["role"] == "log"
        assert flat["1_mock-bam/a.bam"]["size"] == 4096

    def test_the_alias_is_listed_but_not_descended(self, client, runnable, tmp_path):
        """`logs.latest` names its own sibling; walking into it would carry the
        whole log tree across twice under two names."""
        run, _ = self._collected(client, runnable, tmp_path)
        flat = self._flat(client.get(f"/api/runs/{runnable}/{run}/tree").get_json()["root"])
        alias = flat["_metadata/logs.latest"]
        assert alias["symlink"] is True
        assert not alias["children"]
        assert "_metadata/logs.latest/agent.log" not in flat

    def test_products_lead_and_metadata_trails(self, client, runnable, tmp_path):
        run, _ = self._collected(client, runnable, tmp_path)
        top = [
            n["name"]
            for n in client.get(f"/api/runs/{runnable}/{run}/tree").get_json()["root"]["children"]
        ]
        assert top[-1] == "_metadata"

    @pytest.mark.parametrize("bad", ["../../../etc/passwd", "/etc/passwd", "", "nope"])
    def test_a_path_outside_the_run_is_refused(self, client, runnable, tmp_path, bad):
        run, _ = self._collected(client, runnable, tmp_path)
        r = client.get(f"/api/runs/{runnable}/{run}/file", query_string={"path": bad})
        assert r.status_code >= 400

    def test_a_symlink_pointing_out_is_refused(self, client, runnable, tmp_path):
        """Containment is asserted against the link's *target*, so planting one
        is not a way around it."""
        run, outputs = self._collected(client, runnable, tmp_path)
        secret = tmp_path / "secret.txt"
        secret.write_text("nope")
        (outputs / "escape.txt").symlink_to(secret)
        r = client.get(
            f"/api/runs/{runnable}/{run}/file", query_string={"path": "escape.txt"})
        assert r.status_code >= 400

    def test_the_alias_resolves_for_reading(self, client, runnable, tmp_path):
        """Following links is the point -- `logs.latest/agent.log` is a real
        thing to want -- it is only leaving the root that is refused."""
        run, _ = self._collected(client, runnable, tmp_path)
        body = client.get(
            f"/api/runs/{runnable}/{run}/file",
            query_string={"path": "_metadata/logs.latest/agent.log"},
        ).get_json()
        assert body["text"] == "hello\nworld\n" and body["eof"] is True

    def test_a_window_pages_by_byte_offset(self, client, runnable, tmp_path):
        """Line counts cannot resume a window: the ends are trimmed at line
        boundaries for looks, so bytes shown and lines shown differ."""
        run, outputs = self._collected(client, runnable, tmp_path)
        (outputs / "big.txt").write_text("".join(f"line {i}\n" for i in range(4000)))
        q = {"path": "big.txt", "limit": 200}
        first = client.get(f"/api/runs/{runnable}/{run}/file", query_string=q).get_json()
        assert first["eof"] is False and first["offset"] == 0
        second = client.get(
            f"/api/runs/{runnable}/{run}/file",
            query_string=q | {"offset": first["offset"] + first["length"]},
        ).get_json()
        assert second["offset"] == first["length"]
        # the partial line the first window ended on is where the second starts
        assert second["dropped_head_bytes"] >= 0

    def test_tail_reaches_the_end(self, client, runnable, tmp_path):
        run, outputs = self._collected(client, runnable, tmp_path)
        (outputs / "big.txt").write_text("".join(f"line {i}\n" for i in range(4000)))
        body = client.get(
            f"/api/runs/{runnable}/{run}/file",
            query_string={"path": "big.txt", "mode": "tail", "limit": 200},
        ).get_json()
        assert body["eof"] is True and body["text"].endswith("line 3999\n")

    def test_a_binary_file_is_named_not_decoded(self, client, runnable, tmp_path):
        run, outputs = self._collected(client, runnable, tmp_path)
        (outputs / "x.bin").write_bytes(b"\x00\x01\x02" * 100)
        body = client.get(
            f"/api/runs/{runnable}/{run}/file", query_string={"path": "x.bin"}).get_json()
        assert body["encoding"] == "binary" and body["text"] is None

    def test_a_download_is_an_attachment_unless_it_is_an_image(
        self, client, runnable, tmp_path,
    ):
        run, outputs = self._collected(client, runnable, tmp_path)
        (outputs / "heat.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 32)
        img = client.get(f"/api/runs/{runnable}/{run}/download",
                         query_string={"path": "heat.png"})
        assert img.headers["Content-Type"].startswith("image/png")
        assert "attachment" not in img.headers.get("Content-Disposition", "")
        assert img.headers["X-Content-Type-Options"] == "nosniff"

        rep = client.get(f"/api/runs/{runnable}/{run}/download",
                         query_string={"path": "1_mock-bam/a.bam"})
        assert rep.headers["Content-Type"] == "application/octet-stream"
        assert "attachment" in rep.headers["Content-Disposition"]


class TestPublishedPaths:
    def test_a_produced_file_is_recorded_where_it_landed(self, tmp_path):
        """The trace records a file by its cache-shard path; nextflow publishes
        it under a folder named for the step. Writing the shard path made a
        manifest of names that were not in the directory."""
        from metasmith.agents import _published_index, _published_path

        out = tmp_path / "results"
        (out / "1_seq-gbk").mkdir(parents=True)
        (out / "1_seq-gbk" / "a.gbk").write_text("x")
        index = _published_index(out)
        assert _published_path(out / "out" / "a.gbk", out, index) == Path("1_seq-gbk/a.gbk")

    def test_a_path_that_is_already_right_is_left_alone(self, tmp_path):
        from metasmith.agents import _published_index, _published_path

        out = tmp_path / "results"
        (out / "out").mkdir(parents=True)
        (out / "out" / "a.gbk").write_text("x")
        index = _published_index(out)
        assert _published_path(out / "out" / "a.gbk", out, index) == Path("out/a.gbk")

    def test_a_name_that_matches_nothing_is_not_guessed_at(self, tmp_path):
        from metasmith.agents import _published_index, _published_path

        out = tmp_path / "results"
        out.mkdir()
        assert _published_path(out / "out" / "gone.gbk", out, {}) == Path("out/gone.gbk")


class TestDeliveredTargets:
    def test_a_requested_type_that_never_arrived_is_named(self, client, runnable, tmp_path):
        run = TestResultsFiltering._name(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        outputs = project.outputs_path(runnable, run)

        types = DataTypeLibrary()
        types["assembly"] = Endpoint(properties={"assembly"})
        types["bam"] = Endpoint(properties={"bam"})
        tp = tmp_path / "t.yml"
        types.Save(tp)
        lib = DataInstanceLibrary(outputs)
        lib.AddTypeLibrary(tp, namespace="mock")
        (outputs / "mid.fa").write_text("acgt")
        lib.AddItem(Path("mid.fa"), "mock::assembly")
        lib.Save()

        body = client.get(f"/api/runs/{runnable}/{run}/results").get_json()
        # the workflow asks for mock::bam; only an intermediate came back
        assert body["targets"] == [{"type": "mock::bam", "count": 0, "paths": []}]

    def test_a_delivered_target_counts_its_files(self, client, runnable, tmp_path):
        run = TestResultsFiltering._name(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        outputs = project.outputs_path(runnable, run)

        types = DataTypeLibrary()
        types["bam"] = Endpoint(properties={"bam"})
        tp = tmp_path / "t.yml"
        types.Save(tp)
        lib = DataInstanceLibrary(outputs)
        lib.AddTypeLibrary(tp, namespace="mock")
        for n in ("a.bam", "b.bam"):
            (outputs / n).write_text("bam")
            lib.AddItem(Path(n), "mock::bam")
        lib.Save()

        [target] = client.get(
            f"/api/runs/{runnable}/{run}/results").get_json()["targets"]
        assert target["type"] == "mock::bam" and target["count"] == 2


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
            # `index` is the key ListWorkflowRuns actually returns. This said
            # `run` -- a key it has never had -- which is what kept a
            # permanently-None run_number invisible.
            mload.return_value.ListWorkflowRuns.return_value = [
                {"index": 1, "path": "/x/logs.1", "timestamp": "t"},
            ]
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


# ---------------------------------------------------------------------------
# sample tables
# ---------------------------------------------------------------------------


SHEET = b"sample,asm\nS1,/data/a.fa\nS2,/data/b.fa\n"

# the recipe's input rows as the browser holds them. With the sheet attached
# every row is a sample array -- a value row with nothing above it, and a file
# row descending from it -- and each field names the column it binds. The `path`
# and `value` beside them are the other half of the state machine: what the
# fields hold when no sheet is attached, kept rather than overwritten.
ARRAY_ROWS = [
    {"id": "idx", "mode": "value", "values": [{"key": "", "value": "", "column": "sample"}],
     "dtype": "mock::reads", "parents": []},
    {"id": "asm", "mode": "file", "path": "", "column": "asm",
     "dtype": "mock::assembly", "parents": ["#idx"]},
]


def _attach(client, name, sheet=SHEET, rows=ARRAY_ROWS):
    r = client.post(f"/api/workflows/{name}/table",
                    json={"text": sheet.decode(), "filename": "sheet.csv"})
    assert r.status_code == 201, r.get_json()
    if rows is not None:
        client.put(f"/api/workflows/{name}", json={"input_drafts": rows})
    return r.get_json()


class TestSampleTable:
    """A sheet you already have, one declared row per kind of input."""

    def test_pasted_text_is_read_as_a_table(self, client):
        name = _make_workflow(client)
        body = _attach(client, name, rows=None)
        assert body["columns"] == ["sample", "asm"]
        assert body["row_count"] == 2

    def test_an_upload_is_stored_verbatim(self, client):
        name = _make_workflow(client)
        r = client.post(
            f"/api/workflows/{name}/table",
            data={"file": (io.BytesIO(SHEET), "sheet.csv")},
            content_type="multipart/form-data",
        )
        assert r.status_code == 201, r.get_json()
        stored = Path(r.get_json()["path"])
        assert stored.read_bytes() == SHEET

    def test_the_table_reports_nothing_wrong_for_an_ordinary_dag(self, client):
        name = _make_workflow(client)
        _attach(client, name)
        body = client.get(f"/api/workflows/{name}/table").get_json()
        assert body["attached"] is True
        assert body["problems"] == []

    def test_solving_registers_and_attributes_every_item(self, client):
        # there is no standalone expand any more: `generate` is what turns an
        # array row into real items now, every time, so this is what a solve
        # does as a side effect rather than a step of its own to call first
        name = _make_workflow(client)
        _attach(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())

        inputs = client.get(f"/api/workflows/{name}/inputs").get_json()
        assert inputs["expansion"]["counts"] == {"idx": 2, "asm": 2}
        assert inputs["item_count"] == 4
        # the recipe shows a count against the array row, never the rows it
        # made -- which it can only do if the server says which row made what
        by_array = {}
        for item in inputs["items"]:
            by_array.setdefault(item["array_id"], []).append(item["path"])
        assert sorted(by_array) == ["asm", "idx"]

    def test_solving_again_replaces_the_previous_generation(self, client):
        name = _make_workflow(client)
        _attach(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        _attach(client, name, sheet=b"sample,asm\nS9,/data/z.fa\n")
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        paths = {i["path"] for i in client.get(f"/api/workflows/{name}/inputs").get_json()["items"]}
        # the value row's path is minted, so what is asserted is that the
        # previous generation is gone and exactly one of each remains
        assert "/data/z.fa" in paths and len(paths) == 2
        assert not any(p.endswith(".id") for p in paths)

    def test_detach_leaves_what_was_registered(self, client):
        name = _make_workflow(client)
        _attach(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert client.delete(f"/api/workflows/{name}/table").status_code == 200
        assert client.get(f"/api/workflows/{name}/table").get_json() == {"attached": False}
        assert client.get(f"/api/workflows/{name}/inputs").get_json()["item_count"] == 4

    def test_solving_with_no_table_left_clears_what_was_registered(self, client):
        # the symmetric case: nothing to call "unregister" on any more either --
        # a solve with no table (or no array row left) clears a past
        # generation's items the same way a solve with one replaces them
        name = _make_workflow(client)
        _attach(client, name)
        # a plain input beside the array rows, so the library is not left
        # completely empty once they are cleared -- solving *that* is its own
        # question and not this test's; this one only cares whether a past
        # generation's items outlive the row that made them
        _seed_inputs(client, name, count=1, prefix="plain")
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert client.delete(f"/api/workflows/{name}/table").status_code == 200
        # the browser always resends `sample_type` fresh off the current recipe,
        # null once there is no table to index against -- done by hand here,
        # since this client posts the body directly rather than through it
        client.put(f"/api/workflows/{name}", json={"sample_type": None})
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        items = client.get(f"/api/workflows/{name}/inputs").get_json()["items"]
        # nothing the sheet minted survives the sheet. The three rows are back
        # to what they hold with no sheet -- the two former array rows to their
        # own (empty) text, the plain one to its file -- which is the other half
        # of the switch and not a leftover.
        assert not any(it.get("array_id") for it in items)
        assert len(items) == 3

    def test_a_sample_table_solves_under_its_index_type(self, client):
        """The whole point: a sheet in, a sampled plan out.

        One step, not two: the planner folds structurally identical samples into
        one case and the runtime fans that case back out per sample. That the
        library really splits per row is pinned in `tests/unit/test_sample_tables`.
        """
        name = _make_workflow(client, sample="mock::reads")
        _attach(client, name)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"], result
        assert result["step_count"] > 0


class TestSharedInputs:
    """A reference the whole study uses, under a sampled plan.

    A sample's mask is one index item's lineage, so anything beside it is
    invisible to the planner -- and making it an *ancestor* of the index instead
    collapses every sample into one view. Hence a third way in.
    """

    ROWS = [ARRAY_ROWS[0]]  # the index only; the assembly is shared, not per-sample

    def _shared_setup(self, client):
        name = _make_workflow(client, sample="mock::reads")
        project = client.application.config["MSM_PROJECT"]
        f = project.input_library_path(name) / "shared.fa"
        f.write_text(">contig\nACGT\n")
        # With a sheet attached every row binds a column, so "the same file for
        # every sample" is a column repeating that path -- which costs a column
        # there and nothing in the library, since identical cells group onto one
        # instance. Every row registers as a side effect of the `generate` each
        # test method below calls; nothing here pre-registers them.
        sheet = f"sample,asm,ref\nS1,/data/a.fa,{f}\nS2,/data/b.fa,{f}\n".encode()
        _attach(client, name, sheet=sheet,
                rows=self.ROWS + [_row("shared", column="ref")])
        return name, "#shared"

    def test_an_unshared_neighbour_is_invisible_to_every_sample(self, client):
        name, _ = self._shared_setup(client)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert not result["success"]

    def test_marking_it_shared_puts_it_in_every_sample(self, client):
        """Named by the row, not by the path.

        A row may not have a path yet -- the normal state of a fresh recipe --
        so the request says which row, and the generate turns it into a path
        between building the library and solving from it.
        """
        name, key = self._shared_setup(client)
        client.put(f"/api/workflows/{name}", json={"shared_input_paths": [key]})
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"], result

    def test_a_shared_path_that_is_not_registered_is_refused(self, client):
        name, _ = self._shared_setup(client)
        client.put(f"/api/workflows/{name}", json={"shared_input_paths": ["/nope.fa"]})
        job = client.application.config["MSM_JOBS"].get(
            client.post(f"/api/workflows/{name}/generate", json={}).get_json()["id"])
        assert job.wait(120)
        assert job.status == "failed"
        assert "not in" in job.error


# ---------------------------------------------------------------------------
# sharing
# ---------------------------------------------------------------------------


@pytest.fixture
def elsewhere(_app, tmp_path):
    """A second project on the same app -- where an import has to land.

    Sharing is only worth testing across the seam: a payload that imports into
    the project it came from proves nothing about names resolving.
    """
    from contextlib import contextmanager

    @contextmanager
    def _open():
        root = _fabricate_project(tmp_path / "other")
        bind_project(_app, root, ssh_config_path=tmp_path / "other_ssh", watch=False)
        with _app.test_client() as c:
            c.application = _app
            yield c, root

    return _open


def _payload(client, kind, name, bound=False):
    r = client.post("/api/share/export", json={"kind": kind, "name": name, "bound": bound})
    assert r.status_code == 200, r.get_json()
    return r.get_json()


class TestShareEnvelope:
    """What the string itself has to promise."""

    def test_a_payload_round_trips(self, client):
        client.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "big.example"})
        out = _payload(client, "ssh_host", "big-iron")
        assert out["payload"].startswith("msm1:")
        kind, body = share.decode(out["payload"])
        assert kind == "ssh_host"
        assert body["hostname"] == "big.example"

    def test_the_body_is_shown_beside_the_payload(self, client):
        """Exports carry paths and accounts; the page shows what is in one
        before anyone copies it."""
        client.post("/api/agents", json={"name": "smith", "home": "ssh://box:~/msm.smith"})
        out = _payload(client, "agent", "smith")
        assert out["body"]["home"] == "ssh://box:~/msm.smith"

    def test_a_truncated_payload_is_refused_not_half_read(self, client):
        client.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "big.example"})
        text = _payload(client, "ssh_host", "big-iron")["payload"]
        r = client.post("/api/share/preview", json={"payload": text[:-8]})
        assert r.status_code == 409
        assert "whole" in r.get_json()["error"] or "damaged" in r.get_json()["error"]

    def test_a_later_format_is_refused_by_name(self, client):
        r = client.post("/api/share/preview", json={"payload": "msm9:abc:ZZZZ"})
        assert r.status_code == 409
        assert "msm9" in r.get_json()["error"]

    def test_something_that_is_not_a_payload_says_so(self, client):
        r = client.post("/api/share/preview", json={"payload": "hello there"})
        assert r.status_code == 409
        assert "not a metasmith share string" in r.get_json()["error"]

    def test_wrapped_whitespace_survives(self, client):
        """Mail clients wrap long strings; the wrap is not damage."""
        client.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "big.example"})
        text = _payload(client, "ssh_host", "big-iron")["payload"]
        wrapped = "\n".join(text[i:i + 20] for i in range(0, len(text), 20))
        assert share.decode(wrapped)[1]["hostname"] == "big.example"


class TestShareHosts:
    def test_a_host_arrives_in_another_config(self, client, elsewhere):
        client.post("/api/ssh/hosts", json={
            "alias": "big-iron", "hostname": "big.example", "user": "tony", "port": "2222",
        })
        text = _payload(client, "ssh_host", "big-iron")["payload"]
        with elsewhere() as (other, _):
            prev = other.post("/api/share/preview", json={"payload": text}).get_json()
            assert prev["kind"] == "ssh_host" and prev["blocked"] is False
            assert other.post("/api/share/import", json={"payload": text}).status_code == 201
            hosts = other.get("/api/ssh/hosts").get_json()["hosts"]
            (host,) = [h for h in hosts if h["alias"] == "big-iron"]
            assert host["hostname"] == "big.example"
            assert host["user"] == "tony"

    def test_the_private_key_does_not_travel(self, client):
        """A shared host names a machine, never a key on the sender's disk."""
        client.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "big.example"})
        client.post("/api/ssh/keys", json={"alias": "big-iron"})
        body = _payload(client, "ssh_host", "big-iron")["body"]
        assert "identity_file" not in body
        assert "id_" not in yaml.safe_dump(body)

    def test_an_alias_already_here_is_named_before_it_is_tried(self, client, elsewhere):
        client.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "big.example"})
        text = _payload(client, "ssh_host", "big-iron")["payload"]
        with elsewhere() as (other, _):
            other.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "mine.example"})
            prev = other.post("/api/share/preview", json={"payload": text}).get_json()
            assert prev["blocked"] is True
            assert "already in your ssh config" in " ".join(prev["notes"])
            # and the attempt refuses rather than overwriting the user's own host
            assert other.post("/api/share/import", json={"payload": text}).status_code == 409
            assert other.get("/api/ssh/hosts").get_json()["hosts"][0]["hostname"] == "mine.example"


class TestShareAgents:
    def test_an_agent_arrives_with_its_params(self, client, elsewhere):
        """The `Agent.Pack` trap: params outside the stringifying block.

        A mapping written through it reloads as a quoted Python literal -- still
        truthy, so nothing complains, and the agent runs with no params at all.
        """
        client.post("/api/agents", json={
            "name": "smith", "home": "ssh://box:~/msm.smith", "runtime": "APPTAINER",
            "setup_commands": ["#!/bin/bash", "module load gcc"],
            "default_preset": "slurm", "default_params": {"account": "st-x-1", "cpus": 8},
        })
        text = _payload(client, "agent", "smith")["payload"]
        with elsewhere() as (other, _):
            assert other.post("/api/share/import", json={"payload": text}).status_code == 201
            got = other.get("/api/agents/smith").get_json()
            assert got["default_params"] == {"account": "st-x-1", "cpus": 8}
            assert got["default_preset"] == "slurm"
            assert got["setup_commands"] == ["#!/bin/bash", "module load gcc"]

    def test_host_facts_no_editor_draws_still_travel(self, client, elsewhere):
        client.post("/api/agents", json={"name": "smith", "home": "~/msm.smith"})
        project = client.application.config["MSM_PROJECT"]
        agent = op_agent.load_agent(str(project.agent_path("smith")))
        agent.gpu_args = ["--bind", "/usr/lib/wsl:/usr/lib/wsl"]
        agent.native = True
        agent.Save(project.agent_path("smith"))
        text = _payload(client, "agent", "smith")["payload"]
        with elsewhere() as (other, root):
            other.post("/api/share/import", json={"payload": text})
            got = op_agent.load_agent(str(Project(root).agent_path("smith")))
            assert got.gpu_args == ["--bind", "/usr/lib/wsl:/usr/lib/wsl"]
            assert got.native is True

    def test_where_it_was_deployed_does_not_travel(self, client, elsewhere):
        """`real_path` is the sender's host. A copy claiming it would skip its
        own deploy and stage into a directory nothing installed."""
        client.post("/api/agents", json={"name": "smith", "home": "ssh://box:~/msm.smith"})
        _deployed(client, "smith")
        text = _payload(client, "agent", "smith")["payload"]
        with elsewhere() as (other, _):
            other.post("/api/share/import", json={"payload": text})
            assert other.get("/api/agents/smith").get_json()["deployed"] is False

    def test_an_unknown_host_is_created_red_rather_than_refused(self, client, elsewhere):
        client.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "big.example"})
        client.post("/api/agents", json={"name": "smith", "home": "ssh://big-iron:~/msm.smith"})
        text = _payload(client, "agent", "smith")["payload"]
        with elsewhere() as (other, _):
            prev = other.post("/api/share/preview", json={"payload": text}).get_json()
            assert "not in your ssh config" in " ".join(prev["notes"])
            assert other.post("/api/share/import", json={"payload": text}).status_code == 201
            got = other.get("/api/agents/smith").get_json()
            assert got["valid"] is False
            assert any("big-iron" in p for p in got["problems"])

    def test_a_name_already_taken_is_moved_aside_and_said(self, client, elsewhere):
        client.post("/api/agents", json={"name": "smith", "home": "~/msm.smith"})
        text = _payload(client, "agent", "smith")["payload"]
        with elsewhere() as (other, _):
            other.post("/api/agents", json={"name": "smith", "home": "~/mine"})
            prev = other.post("/api/share/preview", json={"payload": text}).get_json()
            assert prev["name"] == "smith-2"
            other.post("/api/share/import", json={"payload": text})
            assert other.get("/api/agents/smith").get_json()["home"].endswith("mine")
            assert other.get("/api/agents/smith-2").get_json()["home"].endswith("msm.smith")


class TestShareWorkflows:
    def _recipe(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, count=2)
        return name

    def test_the_spec_travels_and_the_bookkeeping_does_not(self, client):
        name = self._recipe(client)
        body = _payload(client, "workflow", name)["body"]
        assert body["spec"]["target_types"] == ["mock::bam"]
        assert body["spec"]["sample_type"] == "mock::assembly"
        for k in ("created_at", "forked_from", "schema"):
            assert k not in body and k not in body["spec"]

    def test_libraries_travel_as_names_not_as_paths(self, client, project_root):
        name = self._recipe(client)
        client.put(f"/api/workflows/{name}", json={
            "transform_libraries": [str(project_root / "MetasmithLibraries" / "transforms")],
        })
        body = _payload(client, "workflow", name)["body"]
        assert body["spec"]["transform_libraries"] == ["transforms"]

    def test_unbound_is_the_recipe_and_bound_is_the_files(self, client):
        """The recipe is the rows, and a path is what an unbound share drops."""
        name = self._recipe(client)
        unbound = _payload(client, "workflow", name)["body"]
        assert [r["path"] for r in unbound["drafts"]] == ["", ""]
        bound = _payload(client, "workflow", name, bound=True)["body"]
        assert all(r["path"].endswith(".fa") for r in bound["drafts"])

    def test_an_unbound_workflow_lands_and_still_plans(self, client, elsewhere):
        name = self._recipe(client)
        text = _payload(client, "workflow", name)["payload"]
        with elsewhere() as (other, root):
            prev = other.post("/api/share/preview", json={"payload": text}).get_json()
            assert prev["creates"]["input_count"] == 2
            assert "fill them in" in " ".join(prev["notes"])
            got = other.post("/api/share/import", json={"payload": text}).get_json()
            rows = _rows_of(other, got["name"])
            assert len(rows) == 2
            assert all(r["dtype"] == "mock::assembly" for r in rows)
            assert all(not r["path"] for r in rows), "an unbound share carries no paths"

            # and the workflow it landed in is the ordinary kind: solving builds
            # the library from those rows, and a row with no path yet is a
            # deferred input -- minted *here*, so they are this project's paths
            result = _finish(other, other.post(
                f"/api/workflows/{got['name']}/generate", json={}).get_json())
            assert result["success"], result
            items = other.get(f"/api/workflows/{got['name']}/inputs").get_json()["items"]
            assert len(items) == 2
            assert all(r["path"].startswith("/msm_deferred/") for r in items)
            assert len({r["path"] for r in items}) == 2

    def test_lineage_survives_the_crossing(self, client, elsewhere):
        name = _make_workflow(client)
        project = client.application.config["MSM_PROJECT"]
        parent = project.input_library_path(name) / "parent.fa"
        parent.write_text(">p\nACGT\n")
        child = project.input_library_path(name) / "child.fa"
        child.write_text(">c\nACGT\n")
        _put_rows(client, name, [
            _row("p", parent),
            _row("c", child, dtype="mock::bam", parents=["#p"]),
        ])
        text = _payload(client, "workflow", name, bound=True)["payload"]
        with elsewhere() as (other, _):
            got = other.post("/api/share/import", json={"payload": text}).get_json()
            rows = {r["dtype"]: r for r in _rows_of(other, got["name"])}
            # stated in row ids, not paths: unbound there is no path to state it in
            assert rows["mock::bam"]["parents"] == [f"#{rows['mock::assembly']['id']}"]
            assert rows["mock::assembly"]["path"] == str(parent)

    def test_a_missing_library_is_dropped_and_named(self, client, elsewhere):
        name = self._recipe(client)
        client.put(f"/api/workflows/{name}", json={"transform_libraries": ["/nowhere/special"]})
        text = _payload(client, "workflow", name)["payload"]
        with elsewhere() as (other, _):
            prev = other.post("/api/share/preview", json={"payload": text}).get_json()
            assert "not in your standard library" in " ".join(prev["notes"])
            got = other.post("/api/share/import", json={"payload": text}).get_json()
            wf = other.get(f"/api/workflows/{got['name']}").get_json()
            # dropped rather than kept: a path that resolves to nothing here
            # fails a solve from inside the library loader, saying only that
            assert wf["request"]["transform_libraries"] == []

    def test_an_unknown_type_arrives_placed_and_red(self, client, elsewhere):
        """The row is not lost and not dropped: it lands carrying the type it
        came with, which is what the recipe already draws red."""
        name = _make_workflow(client)
        client.put(f"/api/workflows/{name}", json={
            "input_drafts": [_row("odd", "/data/odd.dat", dtype="exotic::exotic")],
        })
        text = _payload(client, "workflow", name)["payload"]
        with elsewhere() as (other, _):
            prev = other.post("/api/share/preview", json={"payload": text}).get_json()
            assert "exotic::exotic" in " ".join(prev["notes"])
            got = other.post("/api/share/import", json={"payload": text}).get_json()
            assert [d["dtype"] for d in _rows_of(other, got["name"])] == ["exotic::exotic"]
            # ...and nothing was registered for it: the library refuses a type it
            # does not have, and a solve is where that is said
            items = other.get(f"/api/workflows/{got['name']}/inputs").get_json()["items"]
            assert items == []

    def test_a_binding_travels_but_a_typed_path_does_not(self, client, elsewhere):
        """A binding is a rule about a sheet, not a file on this machine: it is
        the substance of a sample-array recipe and means the same thing
        anywhere. The path beside it is the row's other state, and that is
        exactly what an unbound export is for withholding."""
        name = _make_workflow(client)
        client.put(f"/api/workflows/{name}", json={"input_drafts": [
            {"id": "a", "mode": "file", "path": "/data/mine.fa", "column": "asm",
             "dtype": "mock::assembly", "parents": []},
            {"id": "b", "mode": "file", "path": "/home/me/one_off.fa", "column": "",
             "dtype": "mock::assembly", "parents": []},
        ]})
        body = _payload(client, "workflow", name)["body"]
        assert [d["path"] for d in body["drafts"]] == ["", ""]
        assert [d["column"] for d in body["drafts"]] == ["asm", ""]
        bound = _payload(client, "workflow", name, bound=True)["body"]
        assert [d["path"] for d in bound["drafts"]] == ["/data/mine.fa", "/home/me/one_off.fa"]

    def test_a_typed_in_value_travels_whole(self, client, elsewhere):
        """A value row *is* its contents: a few lines someone typed.

        The recipe refers to it by row id, not by a filename -- the library
        names its own file and the sender's name for it is not the receiver's
        business, so what has to survive the trip is the text.
        """
        name = _make_workflow(client)
        _put_rows(client, name, [_row("v", mode="value", value="left,right\n")])
        body = _payload(client, "workflow", name)["body"]
        (row,) = body["drafts"]
        assert row["value"] == "left,right\n"
        text = _payload(client, "workflow", name)["payload"]
        with elsewhere() as (other, root):
            got = other.post("/api/share/import", json={"payload": text}).get_json()
            (landed,) = _rows_of(other, got["name"])
            assert landed["value"] == "left,right\n"
            # ...and the file it stands for is written when the library is built
            _finish(other, other.post(
                f"/api/workflows/{got['name']}/generate", json={}).get_json())
            lib = Project(root).input_library_path(got["name"])
            (f,) = [x for x in lib.iterdir() if x.is_file() and len(x.name) == 32]
            assert f.read_text() == "left,right\n"

    def test_two_deferred_rows_stay_two_rows_across_the_wire(self, client, elsewhere):
        """Unbound throws every path away, so lineage cannot be stated in paths:
        a child naming a pathless parent would have no way to say which one."""
        name = _make_workflow(client)
        client.put(f"/api/workflows/{name}", json={"input_drafts": [
            _row("a", dtype="mock::assembly"),
            _row("b", dtype="mock::reads"),
            _row("c", dtype="mock::bam", parents=["#a"]),
        ]})
        text = _payload(client, "workflow", name)["payload"]
        with elsewhere() as (other, _):
            got = other.post("/api/share/import", json={"payload": text}).get_json()
            rows = _rows_of(other, got["name"])
            assert len(rows) == 3 and len({r["id"] for r in rows}) == 3
            by_type = {r["dtype"]: r for r in rows}
            assert by_type["mock::bam"]["parents"] == [f"#{by_type['mock::assembly']['id']}"]


class TestValueRowFields:
    """A value row holds a list of keyed fields, and a run refuses the blanks."""

    def _kv(self, *pairs):
        return [{"key": k, "value": v} for k, v in pairs]

    def test_fields_round_trip_through_the_request(self, client):
        name = _make_workflow(client)
        rows = [_row("v", mode="value", dtype="mock::reads",
                     values=self._kv(("insert", "300"), ("paired", "true")))]
        _put_rows(client, name, rows)
        (back,) = _rows_of(client, name)
        assert back["values"] == self._kv(("insert", "300"), ("paired", "true"))

        project = client.application.config["MSM_PROJECT"]
        lib = project.input_library_path(name)
        (written,) = [p for p in lib.iterdir() if len(p.name) == 32]
        assert written.read_text() == '{"insert": 300, "paired": true}'

    def test_a_run_refuses_a_recipe_with_a_blank_key(self, client, tmp_path):
        """Solving stays permissive; launching does not.

        The verdict belongs to the solve that produced the bundle, so a fix has
        to be re-solved before it counts -- which is the same solve that would
        put it into the bundle a run stages.
        """
        client.post("/api/agents", json={
            "name": "smith", "home": str(tmp_path / "home"), "runtime": "DOCKER",
        })
        _deployed(client, "smith")
        name = _make_workflow(client)
        rows = _seed_inputs(client, name)
        rows = rows + [_row("v", mode="value", dtype="mock::reads",
                            values=self._kv(("insert", "300"), ("", "true")))]
        _put_rows(client, name, rows)

        result = _finish(client, client.post(
            f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"] is True, "an unfinished recipe still solves"
        assert result["recipe_problems"]

        r = client.post("/api/runs", json={"workflow": name, "agent": "smith"})
        assert r.status_code == 409
        assert "unfinished recipe" in r.get_json()["error"]

        rows[-1]["values"] = self._kv(("insert", "300"), ("paired", "true"))
        _put_rows(client, name, rows)
        # still refused until it is solved again: the bundle a run would stage
        # is the one with the blank in it
        assert client.post(
            "/api/runs", json={"workflow": name, "agent": "smith"}).status_code == 409
        result = _finish(client, client.post(
            f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["recipe_problems"] == []

        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.StageWorkflow.return_value = None
            mload.return_value.ListWorkflowRuns.return_value = []
            r = client.post("/api/runs", json={"workflow": name, "agent": "smith"})
        assert r.status_code == 202, r.get_json()

    def test_a_result_from_before_this_existed_is_launchable(self, client, tmp_path):
        """Absent means no problems -- or every workflow planned before this
        change becomes permanently unlaunchable."""
        client.post("/api/agents", json={
            "name": "smith", "home": str(tmp_path / "home"), "runtime": "DOCKER",
        })
        _deployed(client, "smith")
        name = _make_workflow(client)
        _seed_inputs(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())

        project = client.application.config["MSM_PROJECT"]
        wf = project.read_workflow(name)
        project.write_result(name, {
            k: v for k, v in wf.result.items() if k != "recipe_problems"
        })
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.StageWorkflow.return_value = None
            mload.return_value.ListWorkflowRuns.return_value = []
            r = client.post("/api/runs", json={"workflow": name, "agent": "smith"})
        assert r.status_code == 202, r.get_json()
