"""Route-level tests for the GUI backend, driven against a temporary project.

These go through the HTTP surface rather than calling the store directly,
because the contract that matters is the one the page sees.
"""
from __future__ import annotations

import threading
from pathlib import Path
from unittest import mock

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.testing.mock_transforms import identity_transform

from metasmith.gui import stdlib
from metasmith.gui.app import bind_project, create_app
from metasmith.gui.store import Project
from metasmith.ops import agent as op_agent
from metasmith.ops import workspace as op_workspace

from tests.e2e.docker.conftest import create_transform_library

# the GUI's own suite: `dev.sh -tg` runs exactly the files carrying this,
# and it is the inner loop while working on the page -- keep it fast.
pytestmark = pytest.mark.gui

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

    def test_created_from_nothing(self, client):
        """`+ agent` posts an empty body -- there is no form in front of it."""
        r = client.post("/api/agents", json={})
        assert r.status_code == 201, r.get_json()
        body = r.get_json()
        assert body["name"]
        # named after itself, so a host with three agents has three directories
        assert body["home"].endswith(f"msm.{body['name']}")
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
        assert d["home"] == f"~/msm.{d['name']}"

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
        name = client.post("/api/agents", json={}).get_json()["name"]
        prefix = self._prefix(project_root, name)
        body = client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://sockeye:~/msm.{name}",
        }).get_json()
        assert body["name"] == f"{prefix}-sockeye"
        assert body["sort_name"] == f"sockeye{prefix}"
        # the default home is made out of the name, so it moved with it
        assert body["home"] == f"ssh://sockeye:~/msm.{prefix}-sockeye"
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
        name = client.post("/api/agents", json={}).get_json()["name"]
        prefix = self._prefix(project_root, name)
        client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://old:~/msm.{name}",
        })
        r = client.put("/api/ssh/hosts/old", json={"alias": "new", "hostname": "old.example"})
        body = r.get_json()
        assert body["agents_repointed"] == [f"{prefix}-old → {prefix}-new"]
        agent = client.get(f"/api/agents/{prefix}-new").get_json()
        assert agent["sort_name"] == f"new{prefix}"
        # the directory on that machine did not move, so neither did the home
        assert agent["home"] == f"ssh://new:~/msm.{prefix}-old"

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
    the default count -- the literal `~/msm.<name>` and the expansion a local
    save leaves behind -- and nothing else does.
    """

    def test_a_fresh_local_agent_is_default(self, client):
        client.post("/api/agents", json={"name": "smith"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is True

    def test_the_unexpanded_remote_spelling_is_default(self, client):
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://h:~/msm.smith"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is True

    def test_another_directory_ending_in_the_same_name_is_not(self, client):
        """The trap a suffix test walks into: `/scratch/you/msm.smith` is not it."""
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://h:/scratch/msm.smith"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is False

    def test_a_renamed_agent_stops_being_default(self, client):
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://h:~/msm.smith"})
        client.put("/api/agents/smith", json={"name": "jones", "home": "ssh://h:~/msm.smith"})
        assert client.get("/api/agents/jones").get_json()["home_is_default"] is False


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
        from metasmith.ops.workflow import _add_targets

        with pytest.raises(AssertionError, match=r"target #1 \[mock::bam\] names parent #2"):
            _add_targets(TargetBuilder(), [{"type": "mock::bam", "parents": [1]}])

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


class TestInputRowEdits:
    """Correcting a row that is already registered, in place.

    Both are a manifest edit and neither is a filesystem operation: an input
    path is a *pointer* at the user's file, so re-pointing one moves nothing --
    the library's own `Rename` would, which is why these do not route through it.
    """

    def _one(self, client, name, path, dtype="mock::assembly", parents=None):
        r = client.post(f"/api/workflows/{name}/inputs/items", json={
            "path": str(path), "dtype": dtype, "parents": parents,
        })
        assert r.status_code == 201, r.get_json()
        return r.get_json()["path"]

    def _items(self, client, name):
        return {
            it["path"]: it
            for it in client.get(f"/api/workflows/{name}/inputs").get_json()["items"]
        }

    def test_the_type_changes_in_place(self, client, tmp_path):
        name = _make_workflow(client)
        parent = tmp_path / "reads.fa"
        parent.write_text(">x\nACGT\n")
        child = tmp_path / "reads.bam"
        child.write_text("bam")
        p = self._one(client, name, parent)
        c = self._one(client, name, child, dtype="mock::bam", parents=[p])

        r = client.put(f"/api/workflows/{name}/inputs/items/type", json={
            "path": p, "dtype": "mock::reads",
        })
        assert r.status_code == 200, r.get_json()
        items = self._items(client, name)
        assert items[p]["type_name"] == "mock::reads"
        # ...and the row is still the same row: it was not removed and re-added,
        # so what descends from it still does -- under the *new* type name, since
        # a parent record carries its parent's type as well as its path
        assert [(x["path"], x["type_name"]) for x in items[c]["parents"]] == [
            (p, "mock::reads"),
        ]
        assert parent.is_file() and child.is_file(), (
            "changing a type must not touch the filesystem"
        )

    def test_an_unknown_type_is_refused(self, client, tmp_path):
        name = _make_workflow(client)
        f = tmp_path / "reads.fa"
        f.write_text(">x\nACGT\n")
        p = self._one(client, name, f)
        r = client.put(f"/api/workflows/{name}/inputs/items/type", json={
            "path": p, "dtype": "mock::nonesuch",
        })
        assert r.status_code >= 400
        assert self._items(client, name)[p]["type_name"] == "mock::assembly"

    def test_the_path_changes_and_a_descendant_follows(self, client, tmp_path):
        name = _make_workflow(client)
        parent = tmp_path / "run_a.fa"
        parent.write_text(">x\nACGT\n")
        child = tmp_path / "run_a.bam"
        child.write_text("bam")
        moved_to = tmp_path / "run_b.fa"
        moved_to.write_text(">y\nTTTT\n")

        p = self._one(client, name, parent)
        c = self._one(client, name, child, dtype="mock::bam", parents=[p])

        r = client.put(f"/api/workflows/{name}/inputs/items/path", json={
            "path": p, "new_path": str(moved_to),
        })
        assert r.status_code == 200, r.get_json()
        assert r.get_json()["moved"] is False
        assert r.get_json()["relinked"] == 1

        items = self._items(client, name)
        assert set(items) == {str(moved_to), c}
        # the child descends from the row, not from the string it used to hold
        assert [x["path"] for x in items[c]["parents"]] == [str(moved_to)]
        # ...and neither file went anywhere
        assert parent.is_file() and moved_to.is_file()
        assert parent.read_text().startswith(">x")
        assert moved_to.read_text().startswith(">y")

    def test_a_collision_is_refused_and_the_row_is_left_alone(self, client, tmp_path):
        name = _make_workflow(client)
        a, b = tmp_path / "a.fa", tmp_path / "b.fa"
        for f in (a, b):
            f.write_text(">x\nACGT\n")
        pa = self._one(client, name, a)
        pb = self._one(client, name, b)
        r = client.put(f"/api/workflows/{name}/inputs/items/path", json={
            "path": pa, "new_path": pb,
        })
        assert r.status_code >= 400
        assert "already registered" in r.get_json()["error"]
        assert set(self._items(client, name)) == {pa, pb}

    def test_a_library_owned_value_is_moved_inside_the_library(self, client):
        """The one case where the file *is* the library's, so a move is right."""
        name = _make_workflow(client)
        project = client.application.config["MSM_PROJECT"]
        lib = project.input_library_path(name)
        r = client.post(f"/api/workflows/{name}/inputs/items", json={
            "name": "K12", "value": "GCF_000005845.2", "dtype": "mock::assembly",
        })
        assert r.status_code == 201, r.get_json()
        assert (lib / "K12").is_file()

        r = client.put(f"/api/workflows/{name}/inputs/items/path", json={
            "path": "K12", "new_path": "K12_MG1655",
        })
        assert r.status_code == 200, r.get_json()
        assert r.get_json()["moved"] is True
        assert not (lib / "K12").exists()
        assert (lib / "K12_MG1655").read_text() == "GCF_000005845.2"
        assert list(self._items(client, name)) == ["K12_MG1655"]

    def test_a_pointer_cannot_become_library_owned(self, client, tmp_path):
        """Relative means the library owns the file; absolute means it does not.

        Flipping between them silently changes what the entry claims, so it is
        refused rather than guessed at.
        """
        name = _make_workflow(client)
        f = tmp_path / "reads.fa"
        f.write_text(">x\nACGT\n")
        p = self._one(client, name, f)
        r = client.put(f"/api/workflows/{name}/inputs/items/path", json={
            "path": p, "new_path": "reads.fa",
        })
        assert r.status_code >= 400
        assert list(self._items(client, name)) == [p]

    def test_a_lineage_loop_is_refused(self, client, tmp_path):
        """A row cannot descend from something that descends from it.

        The browser filters these out of the menu it offers, but this route is
        reachable without it, and nothing downstream is defined over a cycle:
        `AsSamples` walks up to the ancestors and then back down to their
        descendants, so a loop makes every branch the whole library.
        """
        name = _make_workflow(client)
        parent = tmp_path / "reads.fa"
        parent.write_text(">x\nACGT\n")
        child = tmp_path / "reads.bam"
        child.write_text("bam")
        p = self._one(client, name, parent)
        c = self._one(client, name, child, dtype="mock::bam", parents=[p])

        # the direct loop, and the one that closes through a third row
        r = client.put(f"/api/workflows/{name}/inputs/items/parents", json={
            "path": p, "parents": [c],
        })
        assert r.status_code >= 400
        r = client.put(f"/api/workflows/{name}/inputs/items/parents", json={
            "path": p, "parents": [p],
        })
        assert r.status_code >= 400

        # ...and the lineage that was there is untouched by the refusal
        items = self._items(client, name)
        assert [x["path"] for x in items[c]["parents"]] == [p]
        assert items[p]["parents"] == []

    def test_a_grandparent_link_still_works(self, client, tmp_path):
        """The guard is about loops, not about depth: a chain is still a chain."""
        name = _make_workflow(client)
        a, b, c = (tmp_path / f"{n}.fa" for n in ("a", "b", "c"))
        for f in (a, b, c):
            f.write_text(">x\nACGT\n")
        pa = self._one(client, name, a)
        pb = self._one(client, name, b, parents=[pa])
        pc = self._one(client, name, c, parents=[pb])

        r = client.put(f"/api/workflows/{name}/inputs/items/parents", json={
            "path": pc, "parents": [pb, pa],
        })
        assert r.status_code == 200, r.get_json()
        assert {x["path"] for x in self._items(client, name)[pc]["parents"]} == {pa, pb}


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
