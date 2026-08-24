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

from tests.metasmith.e2e.docker.conftest import create_transform_library

pytestmark = pytest.mark.gui

def _fabricate_project(root: Path, mlib_target: Path | None = None) -> Path:
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
    project: Project = client.application.config["MSM_PROJECT"]
    path = project.agent_path(name)
    agent = op_agent.load_agent(str(path))
    agent.real_path = Path(agent.home.GetPath())
    agent.Save(path)


def _finish(client, job_summary, timeout=120) -> dict:
    job = client.application.config["MSM_JOBS"].get(job_summary["id"])
    assert job.wait(timeout), f"job [{job_summary['id']}] did not finish"
    assert job.status == "done", f"job failed: {job.error}\n" + "\n".join(job.lines()[-20:])
    return job.result


def _row(rid, path="", dtype="mock::assembly", parents=None, **over):
    return {
        "id": rid, "mode": "file", "path": str(path), "name": "", "value": "",
        "dtype": dtype, "parents": list(parents or []),
    } | over


def _put_rows(client, workflow: str, rows: list[dict]):
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


class TestHealth:
    def test_health_answers(self, client):
        res = client.get("/api/health")
        assert res.status_code == 200
        assert res.get_json() == {"ok": True}

    def test_health_touches_nothing(self, client):
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


class TestGzip:
    def test_json_is_gzipped_when_accepted(self, client):
        import gzip

        res = client.get("/api/project/type-index", headers={"Accept-Encoding": "gzip"})
        assert res.headers.get("Content-Encoding") == "gzip"
        assert gzip.decompress(res.data).startswith(b"{")

    def test_json_is_plain_without_accept_encoding(self, client):
        res = client.get("/api/project/type-index")
        assert "Content-Encoding" not in res.headers
        assert res.get_json() is not None

    def test_the_job_stream_is_never_gzipped(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        with mock.patch("metasmith.ops.agent.deploy", return_value={"status": "deployed"}):
            r = client.post("/api/agents/smith/deploy", json={})
            _finish(client, r.get_json())
        res = client.get(f"/api/jobs/{r.get_json()['id']}/stream", headers={"Accept-Encoding": "gzip"})
        assert "Content-Encoding" not in res.headers
        assert res.mimetype == "text/event-stream"


class TestTypeIndex:
    def test_both_sides_of_a_type_are_indexed(self, client):
        body = client.get("/api/project/type-index").get_json()
        names = [t["name"] for t in body["transforms"]]
        assert names, "the mock library has one transform; it should be listed"

        consumed = body["by_type"]["mock::assembly"]
        produced = body["by_type"]["mock::bam"]
        assert consumed["consumed_by"] and not consumed["produced_by"]
        assert produced["produced_by"] and not produced["consumed_by"]

        entry = consumed["consumed_by"][0]
        assert entry["match"] == "exact" and entry["as"] == "mock::assembly"
        tr = body["transforms"][entry["i"]]
        assert tr["inputs"] == ["mock::assembly"]
        assert tr["outputs"] == ["mock::bam"]
        assert tr["library"] == body["libraries"][0]["path"]

    def test_every_named_type_is_a_key(self, client):
        body = client.get("/api/project/type-index").get_json()
        assert "mock::unreachable" in body["by_type"]
        assert body["by_type"]["mock::unreachable"] == {"produced_by": [], "consumed_by": []}

    def test_libraries_carry_their_counts(self, client):
        body = client.get("/api/project/type-index").get_json()
        assert [l["transform_count"] for l in body["libraries"]] == [1]

    def test_an_unreadable_library_does_not_blank_the_index(self, client, project_root):
        (project_root / "MetasmithLibraries" / "transforms" / "broken.xgdb").mkdir()
        body = client.get("/api/project/type-index").get_json()
        broken = [l for l in body["libraries"] if l["name"] == "broken.xgdb"]
        assert len(broken) == 1 and broken[0]["error"]
        assert body["by_type"]["mock::bam"]["produced_by"], "the good library still indexed"


@pytest.fixture
def poly_root(tmp_path) -> Path:
    root = tmp_path / "poly"
    mlib = root / "MetasmithLibraries"
    (mlib / "data_types").mkdir(parents=True)

    types = DataTypeLibrary()
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    types["flye_assembly"] = Endpoint(properties={"assembly", "flye"})
    types["stats"] = Endpoint(properties={"stats"})
    types_path = mlib / "data_types" / "mock.yml"
    types.Save(types_path)

    alias = DataTypeLibrary()
    alias["assembly"] = Endpoint(properties={"assembly"})
    alias.Save(mlib / "data_types" / "other.yml")

    create_transform_library(
        mlib / "transforms", types_path,
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
        body = self._index(poly_client)
        rank = {"exact": 0, "alias": 1}
        for spec in body["by_type"].values():
            for side in ("produced_by", "consumed_by"):
                ranks = [rank.get(e["match"], 2) for e in spec[side]]
                assert ranks == sorted(ranks)

    def test_a_transform_is_listed_once_per_side(self, poly_client):
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
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        assert client.delete("/api/agents/smith").get_json()["action"] == "archived"
        assert client.get("/api/agents").get_json() == []
        assert len(client.get("/api/agents?archived=1").get_json()) == 1
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
        r = client.post("/api/agents", json={})
        assert r.status_code == 201, r.get_json()
        body = r.get_json()
        assert body["name"]
        assert body["id"]
        assert body["home"].endswith(f"msm.{body['id']}")
        assert body["setup_commands"] == ["#!/bin/bash"]
        assert body["valid"] is True
        assert body["deployed"] is False

    def test_defaults_offer_every_runtime(self, client):
        d = client.get("/api/defaults/agent").get_json()
        assert set(d["runtimes"]) >= {"APPTAINER", "DOCKER", "MAMBA"}
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
    def _prefix(self, project_root, name) -> str:
        return Project(project_root).agent_naming(name)["prefix"]

    def test_a_made_up_name_carries_its_host(self, client):
        body = client.post("/api/agents", json={}).get_json()
        assert body["name"].endswith("-local")
        prefix = body["name"][: -len("-local")]
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
        body = client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://sockeye:~/msm.{agent_id}",
        }).get_json()
        assert body["name"] == f"{prefix}-sockeye"
        assert body["sort_name"] == f"sockeye{prefix}"
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
        name = client.post("/api/agents", json={}).get_json()["name"]
        body = client.put(f"/api/agents/{name}", json={
            "name": name, "home": f"ssh://:~/msm.{name}",
        }).get_json()
        assert body["name"] == name
        assert body["auto_named"] is True

    def test_regenerating_a_name_re_arms_it(self, client):
        name = client.post("/api/agents", json={}).get_json()["name"]
        client.put(f"/api/agents/{name}", json={"name": "bertha"})
        assert client.get("/api/agents/bertha").get_json()["auto_named"] is False

        suggestion = client.get(
            "/api/defaults/agent/name", query_string={"host": "sockeye"}).get_json()
        assert suggestion["name"].endswith("-sockeye")
        assert client.get("/api/agents/bertha").status_code == 200

        body = client.put("/api/agents/bertha", json={
            "name": suggestion["name"],
            "home": f"ssh://sockeye:~/msm.{suggestion['name']}",
            "naming": {"prefix": suggestion["prefix"], "sort_name": suggestion["sort_name"]},
        }).get_json()
        assert body["name"] == suggestion["name"]
        assert body["auto_named"] is True
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
        assert agent["home"] == f"ssh://new:~/msm.{agent_id}"

    def test_the_list_groups_by_host(self, client, tmp_path):
        for host in ("sockeye", "chamois", "sockeye"):
            name = client.post("/api/agents", json={}).get_json()["name"]
            client.put(f"/api/agents/{name}", json={
                "name": name, "home": f"ssh://{host}:~/msm.{name}",
            })
        client.post("/api/agents", json={"name": "middling", "home": str(tmp_path / "h")})
        listed = [a["name"] for a in client.get("/api/agents").get_json()]
        hosts = [n.rsplit("-", 1)[-1] for n in listed]
        assert hosts == ["chamois", "middling", "sockeye", "sockeye"]


class TestAgentDefaultPreset:
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
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/smith", json={
            "name": "smith", "default_preset": "wishful",
        })
        assert r.status_code == 400
        assert "wishful" in r.get_json()["error"]


class TestAgentDefaultParams:
    def test_they_save_and_come_back_as_values(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.put("/api/agents/smith", json={
            "name": "smith",
            "default_params": {"slurmAccount": "st-you-1", "process_tries": "3"},
        }).get_json()
        assert body["default_params"] == {"slurmAccount": "st-you-1", "process_tries": 3}

    def test_a_quoted_number_stays_a_string(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        body = client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {"version": '"50"'},
        }).get_json()
        assert body["default_params"] == {"version": "50"}

    def test_a_row_with_no_name_is_dropped(self, client, tmp_path):
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
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {"acct": "x"},
        })
        after = client.put("/api/agents/smith", json={
            "name": "smith", "default_params": {},
        }).get_json()
        assert after["default_params"] == {}


class TestAgentUpdateConvention:
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
        assert {x["name"] for x in client.get("/api/agents").get_json()} == {"a", "b"}

    def test_rename_takes_its_runs_with_it(self, client, project_root, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        p = Project(project_root)
        wf = p.create_workflow(name="wf", request={})
        p.create_run(wf.name, {"agent": "smith", "task_key": "k"})
        client.put("/api/agents/smith", json={"name": "wesson"})
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
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path / "h")})
        r = client.put("/api/agents/smith", json={"name": "wesson", "runtime": "PODMAN"})
        assert r.status_code == 400
        assert client.get("/api/agents/smith").status_code == 200
        assert client.get("/api/agents/wesson").status_code == 409

    def test_fields_the_page_does_not_draw_survive(self, client, tmp_path):
        client.post("/api/agents", json={
            "name": "smith", "home": str(tmp_path / "h"), "container": "docker://pinned:1",
        })
        body = client.put("/api/agents/smith", json={
            "name": "smith", "home": str(tmp_path / "h"), "runtime": "DOCKER",
        }).get_json()
        assert body["container"] == "docker://pinned:1"


class TestAgentValidity:
    def test_a_remote_agent_with_no_host_saves_and_says_so(self, client):
        client.post("/api/agents", json={"name": "smith"})
        r = client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://:~/msm.smith"})
        assert r.status_code == 200, r.get_json()
        body = r.get_json()
        assert body["valid"] is False
        assert "no host chosen" in body["problems"]
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
        (tmp_path / "ssh_config").write_text("Host *.cluster.edu\n    User tony\n")
        client.post("/api/agents", json={"name": "smith"})
        client.put("/api/agents/smith", json={"name": "smith", "home": "ssh://n1.cluster.edu:~/x"})
        _deployed(client, "smith")
        assert client.get("/api/agents/smith").get_json()["valid"] is True


class TestDefaultHome:
    def test_a_fresh_local_agent_is_default(self, client):
        client.post("/api/agents", json={"name": "smith"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is True

    def test_the_unexpanded_remote_spelling_is_default(self, client):
        agent_id = client.post("/api/agents", json={"name": "smith"}).get_json()["id"]
        client.put("/api/agents/smith", json={"name": "smith", "home": f"ssh://h:~/msm.{agent_id}"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is True

    def test_another_directory_ending_in_the_same_id_is_not(self, client):
        agent_id = client.post("/api/agents", json={"name": "smith"}).get_json()["id"]
        client.put("/api/agents/smith", json={"name": "smith", "home": f"ssh://h:/scratch/msm.{agent_id}"})
        assert client.get("/api/agents/smith").get_json()["home_is_default"] is False

    def test_a_renamed_agent_stays_default(self, client):
        agent_id = client.post("/api/agents", json={"name": "smith"}).get_json()["id"]
        client.put("/api/agents/smith", json={"name": "smith", "home": f"ssh://h:~/msm.{agent_id}"})
        client.put("/api/agents/smith", json={"name": "jones", "home": f"ssh://h:~/msm.{agent_id}"})
        after = client.get("/api/agents/jones").get_json()
        assert after["home_is_default"] is True
        assert after["id"] == agent_id

    def test_a_name_shaped_home_is_not_default(self, client):
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
        agent = client.get("/api/agents/smith").get_json()
        assert agent["home"] == "ssh://new:~/msm.smith"
        assert agent["valid"] is True

    def test_rename_keeps_what_the_host_resolved_to(self, client, project_root):
        client.post("/api/ssh/hosts", json={"alias": "old", "hostname": "old.example"})
        client.post("/api/agents", json={"name": "smith", "home": "ssh://old:~/msm.smith"})
        p = Project(project_root)
        agent = op_agent.load_agent(str(p.agent_path("smith")))
        agent.real_path = Path("/scratch/tony/msm.smith")
        agent.Save(p.agent_path("smith"))
        client.put("/api/ssh/hosts/old", json={"alias": "new", "hostname": "old.example"})
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
        client.post("/api/ssh/hosts", json={"alias": "one", "hostname": "one.example"})
        body = client.patch("/api/ssh/hosts/one", json={"alias": "two"}).get_json()
        assert body["host"]["alias"] == "one"

    def test_an_edit_without_an_alias_is_not_a_rename(self, client):
        client.post("/api/ssh/hosts", json={"alias": "one", "hostname": "one.example"})
        body = client.put("/api/ssh/hosts/one", json={"hostname": "two.example"}).get_json()
        assert body["host"]["alias"] == "one"
        assert body["renamed_from"] is None


class TestWorkflows:
    def test_generated_name_is_readable(self, client):
        name = _make_workflow(client)
        assert "-" in name and name.islower()

    def test_created_empty_and_named_for_you(self, client):
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
        project = client.application.config["MSM_PROJECT"]
        shared = project.root / "shared.fa"
        shared.write_text(">x\nACGT\n")
        names = [_make_workflow(client) for _ in range(2)]
        for n in names:
            _put_rows(client, n, [_row("a", shared)])

        jobs = {n: client.post(f"/api/workflows/{n}/generate", json={}).get_json() for n in names}
        keys = {n: _finish(client, jobs[n])["task_key"] for n in names}
        assert len(set(keys.values())) == 1, "same inputs should give the same key"
        for n in names:
            body = client.get(f"/api/workflows/{n}").get_json()
            assert body["success"] is True
            assert (Path(body["path"]) / "task.yml").is_file()
            assert op_workspace.load_task(None, body["path"]).GetKey() == keys[n]

    def test_reading_a_bundle_back_is_serialised_too(self, client):
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

        assert (wf_dir / "plan.dag.svg").read_text() == light
        assert (wf_dir / "plan.dag.dark.svg").read_text() == dark

    def test_the_background_is_off_by_default_and_cached_apart(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        wf_dir = Path(client.get(f"/api/workflows/{name}").get_json()["path"])

        bare = self._drawn(client, name)
        filled = self._drawn(client, name, background=1)
        assert bare != filled
        assert filled.count("<rect") == bare.count("<rect") + 1

        assert (wf_dir / "plan.dag.svg").read_text() == bare
        assert (wf_dir / "plan.dag.filled.svg").read_text() == filled
        assert self._drawn(client, name, theme="dark", background=1) == (
            wf_dir / "plan.dag.dark.filled.svg"
        ).read_text()

    def test_an_unknown_background_reads_as_off(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert self._drawn(client, name, background="please") == self._drawn(client, name)

    def test_an_unknown_theme_draws_the_default_rather_than_failing(self, client):
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
        self._drawn(client, name, background=1)
        self._drawn(client, name, theme="dark", background=1)

        _seed_inputs(client, name, 2, prefix="more")
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        for f in (
            "plan.dag.svg", "plan.dag.dark.svg",
            "plan.dag.filled.svg", "plan.dag.dark.filled.svg",
        ):
            assert not (wf_dir / f).exists(), f

    def test_a_workflow_with_no_plan_is_refused(self, client):
        name = _make_workflow(client)
        assert client.get(f"/api/workflows/{name}/dag").status_code == 409


def _make_template(project_root: Path) -> str:
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
    def test_a_fresh_start_warms_every_template_s_dag(self, _app, project_root, tmp_path):
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
            assert client.get(f"/api/templates/{name}/dag").status_code == 200

    def test_listing_reads_yaml_and_never_solves(self, client, template):
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
        name = client.post("/api/workflows", json={"template": template}).get_json()["name"]
        info = client.get(f"/api/workflows/{name}/inputs").get_json()
        assert "mock" in info["type_namespaces"]

    def test_creating_without_a_template_is_untouched(self, client, template):
        name = client.post("/api/workflows", json={}).get_json()["name"]
        detail = client.get(f"/api/workflows/{name}").get_json()
        assert detail["request"]["target_types"] == []
        assert client.get(f"/api/workflows/{name}/inputs").get_json()["items"] == []


class TestSaveAsTemplate:
    def _seeded(self, client, count=2) -> str:
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
        assert len(manifest) == 2
        assert all(v["type"] == "mock::assembly" for v in manifest.values())
        seeded_names = {f"sample_{i}.fa" for i in range(2)}
        assert not any(n in str(k) for k in manifest for n in seeded_names)
        assert "types" not in raw["input_library"]
        assert not [
            v for v in raw["transform_libraries"] + raw["resource_libraries"]
            if str(project_root) in v or "/" in v
        ]

    def test_its_dag_can_be_drawn_like_any_other_template_s(self, client):
        name = self._seeded(client, count=1)
        client.post(f"/api/workflows/{name}/save_as_template", json={"name": "my-tpl"})
        assert "<svg" in self._drawn(client, "my-tpl")
        assert client.get("/api/templates").get_json()[0]["problems"] == []

    def test_a_name_this_project_cannot_account_for_is_listed_then_refused(
        self, client, project_root
    ):
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
        name = _make_workflow(client)
        _seed_inputs(client, name, 1)
        r = client.post(f"/api/workflows/{name}/save_as_template", json={"name": "unsolved"})
        assert r.status_code == 400
        assert "no successful plan" in r.get_json()["error"]

    def test_a_workflow_whose_plan_failed_is_refused(self, client):
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
            assert "<svg" in self._drawn(client, "my-tpl")

            made = client.post("/api/workflows", json={"template": "my-tpl"})
            assert made.status_code == 201, made.get_json()
            detail = client.get(f"/api/workflows/{made.get_json()['name']}").get_json()
            assert sorted(Path(p).name for p in detail["request"]["transform_libraries"]) == (
                sorted(Path(p).name for p in available)
            )


class TestTypeResync:
    def test_restart_picks_up_a_type_added_to_an_existing_namespace(
        self, _app, project_root, tmp_path
    ):
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
        geo = self._lay(
            client,
            [{"id": "x:12", "kind": "transform", "label": "t"}, {"id": "t:a::b", "kind": "type"}],
            [{"from": "x:12", "to": "t:a::b"}],
        )
        assert {n["id"] for n in geo["nodes"]} == {"x:12", "t:a::b"}
        assert geo["edges"][0]["from"] == "x:12"

    def test_an_edge_naming_a_node_that_is_not_there_is_dropped(self, client):
        geo = self._lay(
            client, [{"id": "a", "kind": "type"}], [{"from": "a", "to": "gone"}],
        )
        assert [n["id"] for n in geo["nodes"]] == ["a"]
        assert geo["edges"] == []

    def test_an_empty_graph_is_an_empty_canvas_not_an_error(self, client):
        geo = self._lay(client, [], [])
        assert geo["nodes"] == [] and geo["edges"] == []

    def test_a_cycle_is_reported_without_a_path_to_draw(self, client):
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
    def test_both_themes_arrive_at_once(self, client):
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

        body = client.get(f"/api/workflows/{name}").get_json()
        assert body["planned"] is True
        assert body["success"] is False
        assert body["result"]["hints"]
        assert body["request"]["target_types"] == ["mock::unreachable"]

    def test_a_result_echoes_what_it_planned_from(self, client):
        name = _make_workflow(client, targets=["mock::unreachable"])
        _seed_inputs(client, name, 2)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"] is False
        assert [g["type"] for g in result["given"]] == ["mock::assembly"] * 2
        assert all(g["path"] for g in result["given"])
        assert result["targets"] == [{"type": "mock::unreachable", "parents": []}]
        assert client.get(f"/api/workflows/{name}").get_json()["result"]["given"]

    def test_a_result_carries_the_fingerprint_of_the_recipe_it_solved(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name)
        result = _finish(client, client.post(
            f"/api/workflows/{name}/generate", json={"recipe_fingerprint": "kf3n1qz"},
        ).get_json())
        assert result["recipe_fingerprint"] == "kf3n1qz"

        body = client.get(f"/api/workflows/{name}").get_json()
        assert body["result"]["recipe_fingerprint"] == "kf3n1qz"
        assert "recipe_fingerprint" not in body["request"]

        bare = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert bare["recipe_fingerprint"] is None

    def test_targets_may_carry_lineage(self, client):
        name = _make_workflow(client, targets=[{"type": "mock::bam", "parents": []}])
        _seed_inputs(client, name)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"] is True

        stored = client.get(f"/api/workflows/{name}").get_json()["request"]["target_types"]
        assert stored == [{"type": "mock::bam", "parents": []}]

    def test_a_target_cannot_descend_from_a_later_one(self, client):
        from metasmith.agents import TargetBuilder

        with pytest.raises(AssertionError, match=r"target #1 \[mock::bam\] names parent #2"):
            TargetBuilder().AddAll([{"type": "mock::bam", "parents": [1]}])

    def test_generate_requires_a_target(self, client):
        r = client.post("/api/workflows", json={})
        name = r.get_json()["name"]
        assert client.post(f"/api/workflows/{name}/generate", json={}).status_code == 400

    def test_generate_without_a_sample_type_plans_the_whole_library(self, client):
        name = _make_workflow(client, sample=None)
        _seed_inputs(client, name, count=1)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"], result
        assert result["step_count"] > 0

    def test_fork_changes_the_task_key(self, client):
        name = _make_workflow(client)
        _seed_inputs(client, name)
        original = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())

        forked = client.post(f"/api/workflows/{name}/fork", json={}).get_json()["name"]
        assert client.get(f"/api/workflows/{forked}").get_json()["forked_from"] == name

        after = _finish(client, client.post(f"/api/workflows/{forked}/generate", json={}).get_json())
        assert after["success"] is True
        assert after["task_key"] != original["task_key"]

    def test_unforked_copy_would_collide(self, client):
        a = _make_workflow(client)
        b = _make_workflow(client)
        project = client.application.config["MSM_PROJECT"]
        for name in (a, b):
            f = project.input_library_path(name) / "same.fa"
            f.write_text(">x\nACGT\n")
            _put_rows(client, name, [_row("a", f.resolve())])
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
        assert project.workflow_path(name).is_dir()
        assert client.delete(f"/api/workflows/{name}").get_json()["action"] == "deleted"
        assert not project.workflow_path(name).exists()


class TestWorkflowRename:
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
        assert len(client.get("/api/workflows/chosen-name/inputs").get_json()["items"]) == 1

    def test_typed_text_is_slugified(self, client):
        name = _make_workflow(client)
        r = client.post(f"/api/workflows/{name}/rename", json={"name": "My Assembly Run"})
        assert r.get_json()["name"] == "my-assembly-run"

    def test_the_put_renames_it_too(self, client):
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
        name = _make_workflow(client)
        _seed_inputs(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        r = client.post(f"/api/workflows/{name}/rename", json={"name": "too-late"})
        assert r.status_code == 409
        assert "fork" in r.get_json()["error"].lower()
        assert client.application.config["MSM_PROJECT"].workflow_path(name).is_dir()

    def test_a_repeated_rename_is_refused_not_raised(self, client):
        name = _make_workflow(client)
        assert client.post(f"/api/workflows/{name}/rename", json={"name": "once"}).status_code == 200
        again = client.post(f"/api/workflows/{name}/rename", json={"name": "once"})
        assert again.status_code == 409
        assert again.get_json()["kind"] == "refused"

    def test_a_failed_move_is_refused_not_raised(self, client):
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


@pytest.fixture
def runnable(client, tmp_path):
    client.post("/api/agents", json={
        "name": "smith", "home": str(tmp_path / "home"), "runtime": "DOCKER",
    })
    _deployed(client, "smith")
    name = _make_workflow(client)
    _seed_inputs(client, name)
    _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
    return name


class TestSetupEnvironment:
    # Preparing the agent, without running on it.
    #
    # Staging first is the endpoint's job rather than the caller's: the manifest
    # that says which images and envs a workflow needs is written by staging, so
    # there is nothing to read before it. It writes no run record -- this is
    # preparation, and a run that never happened should not appear in the list.

    def _setup(self, client, workflow, report, **body):
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            agent = mock.MagicMock()
            agent.StageWorkflow.return_value = None
            agent.SetupEnvironment.return_value = report
            mload.return_value = agent
            r = client.post(
                f"/api/workflows/{workflow}/environment",
                json={"agent": "smith"} | body,
            )
            assert r.status_code == 202, r.get_json()
            result = _finish(client, r.get_json())
        return agent, result

    def test_it_stages_then_prepares_and_hands_back_the_report(self, client, runnable):
        report = {"mode": "container", "fetched": 2, "already_present": 1}
        agent, result = self._setup(client, runnable, report)
        assert result == report
        agent.StageWorkflow.assert_called_once()
        assert agent.StageWorkflow.call_args[0][1] == "update"
        key = client.get(f"/api/workflows/{runnable}").get_json()["task_key"]
        assert agent.SetupEnvironment.call_args[0][0] == key

    def test_the_recipes_come_from_the_workflows_own_library(self, client, runnable):
        agent, _ = self._setup(client, runnable, {"mode": "conda"})
        project: Project = client.application.config["MSM_PROJECT"]
        assert agent.SetupEnvironment.call_args.kwargs["library"] == str(
            project.root/"MetasmithLibraries"
        )

    def test_force_travels(self, client, runnable):
        agent, _ = self._setup(client, runnable, {"mode": "conda"}, force=True)
        assert agent.SetupEnvironment.call_args.kwargs["force"] is True

    def test_it_writes_no_run(self, client, runnable):
        self._setup(client, runnable, {"mode": "conda"})
        assert client.get("/api/runs").get_json() == []

    def test_it_refuses_an_agent_that_was_never_deployed(self, client, runnable, tmp_path):
        client.post("/api/agents", json={"name": "fresh", "home": str(tmp_path/"fresh")})
        r = client.post(f"/api/workflows/{runnable}/environment", json={"agent": "fresh"})
        assert r.status_code == 409
        assert "has not been deployed yet" in r.get_json()["error"]

    def test_it_refuses_an_unplanned_workflow(self, client, tmp_path):
        client.post("/api/agents", json={"name": "smith", "home": str(tmp_path/"h")})
        name = _make_workflow(client)
        r = client.post(f"/api/workflows/{name}/environment", json={"agent": "smith"})
        assert r.status_code == 409
        assert "no successful plan" in r.get_json()["error"]


class TestRuns:
    def _launch(self, client, workflow) -> dict:
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.StageWorkflow.return_value = None
            mload.return_value.ListWorkflowRuns.return_value = [
                {"index": 1, "path": "/x/logs.1", "timestamp": "t"},
            ]
            r = client.post("/api/runs", json={"workflow": workflow, "agent": "smith"})
            assert r.status_code == 202, r.get_json()
            body = r.get_json()
            _finish(client, body["job"])
        return body["run"]

    def _cancel(self, client, workflow, run) -> None:
        # A workflow may only have one live run, so a second launch has to
        # follow the first one being stopped.
        with mock.patch("metasmith.ops.runtime.load_agent") as mload:
            mload.return_value = mock.MagicMock()
            mload.return_value.CancelWorkflow.return_value = {"status": "cancelled"}
            assert client.post(
                f"/api/runs/{workflow}/{run}/cancel", json={}).status_code == 200

    def test_launch_records_agent_and_key(self, client, runnable):
        run = self._launch(client, runnable)
        body = client.get(f"/api/runs/{runnable}/{run['name']}").get_json()
        assert body["agent"] == "smith"
        assert body["state"] == "running"
        assert body["task_key"] == client.get(
            f"/api/workflows/{runnable}").get_json()["task_key"]

    def test_the_run_number_is_the_run_index(self, client, runnable):
        run = self._launch(client, runnable)
        body = client.get(f"/api/runs/{runnable}/{run['name']}").get_json()
        assert body["run_number"] == 1

    def test_run_name_extends_the_workflow_name(self, client, runnable):
        run = self._launch(client, runnable)
        assert run["name"].startswith(f"{runnable}-")
        assert len(run["name"]) == len(runnable) + 6

    def test_two_runs_of_one_workflow_are_distinct(self, client, runnable):
        a = self._launch(client, runnable)
        self._cancel(client, runnable, a["name"])
        b = self._launch(client, runnable)
        assert a["name"] != b["name"]
        assert a["task_key"] == b["task_key"]

    def test_runs_list_is_newest_first(self, client, runnable):
        first = self._launch(client, runnable)
        self._cancel(client, runnable, first["name"])
        self._launch(client, runnable)
        listed = client.get("/api/runs").get_json()
        assert len(listed) == 2
        assert listed[0]["created_at"] >= listed[1]["created_at"]

    def test_refuses_an_agent_that_was_never_deployed(self, client, runnable, tmp_path):
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
        lines = client.application.config["MSM_JOBS"].get(r.get_json()["id"]).lines()
        assert any("collecting results" in ln for ln in lines)
        assert seen["dest"].endswith(f"{run['name']}/outputs")
        assert seen["globus"] is False
        assert client.get(
            f"/api/runs/{runnable}/{run['name']}").get_json()["collected_at"]

    def test_a_collect_that_lands_broken_links_fails_and_says_which(self, client, runnable):
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
        run = self._launch(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        project.agent_path("smith").unlink()
        body = client.get(f"/api/runs/{runnable}/{run['name']}/log").get_json()
        assert "smith" in body["error"]


class TestLaunchParams:
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
        _, call = self._launch(client, runnable, resource_overrides={
            "1": {"cpus": "", "memory_gb": "", "duration_h": ""},
            "2": {"cpus": "4"},
        })
        assert list(call.kwargs["resource_overrides"]) == [2]

    def test_unlimited_time_is_not_the_same_as_an_empty_box(self, client, runnable):
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
        _, call = self._launch(client, runnable)
        assert call.kwargs.get("stub_delay") == 0
        assert "gpus" not in call.kwargs


class TestStepSelectors:
    def test_the_summary_names_the_nextflow_process(self, client, runnable):
        steps = client.get(f"/api/workflows/{runnable}").get_json()["result"]["step_display"]
        assert steps
        for s in steps:
            assert s["process"].startswith(f"p{s['order']:02}__")
            assert "declared_resources" in s

    def test_the_result_carries_the_whole_drawing(self, client, runnable):
        result = client.get(f"/api/workflows/{runnable}").get_json()["result"]
        graph = result["plan_graph"]
        for key in ("v", "width", "height", "row_pitch", "lane_pitch", "anchor"):
            assert key in graph, key
        assert all(e["back"] or e["d"] for e in graph["edges"])
        by_step = {n["step"]: n for n in graph["nodes"] if n.get("step") is not None}
        assert {s["order"] for s in result["step_display"]} == set(by_step)
        for n in graph["nodes"]:
            assert 0 < n["cy"] < graph["height"]

    def test_a_step_node_points_at_the_transform_it_runs(self, client, runnable):
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
        for n in graph["nodes"]:
            if n["kind"] != "transform":
                assert n["type"] == n["id"]

    def test_the_drawing_is_backfilled_onto_a_result_without_one(
        self, client, runnable, project_root,
    ):
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
        import re
        from metasmith.models.workflow import NextflowProcessName
        assert re.fullmatch("p03__.*", NextflowProcessName(3, "sort/bam"))
        assert "/" not in NextflowProcessName(3, "sort/bam")

    def test_a_name_selector_matches_every_step_of_that_transform(self):
        import re
        from metasmith.models.workflow import NextflowProcessName
        for order in (1, 7):
            assert re.fullmatch(".*__map_reads", NextflowProcessName(order, "map_reads"))


class TestOrphanedLaunches:
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
        project, rec = self._staging_run(client, runnable, created_at="2020-01-01T00:00:00+00:00")
        self._watcher(client).poll_once()
        assert client.get(f"/api/runs/{runnable}/{rec.name}").get_json()["state"] == "failed"

    def test_a_launch_submitted_a_moment_ago_is_left_alone(self, client, runnable):
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
        from metasmith.ops import runtime as op_runtime
        rows = op_runtime.parse_trace(
            "name\tstatus\texit\nx (1)\tCOMPLETED\t137\n"
        )
        assert rows[0]["state"] == "failed"


class TestResultTree:
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
        run, _ = self._collected(client, runnable, tmp_path)
        body = client.get(f"/api/runs/{runnable}/{run}/tree").get_json()
        flat = self._flat(body["root"])
        assert flat["_metadata/logs.2026-01-01/agent.log"]["role"] == "log"
        assert flat["1_mock-bam/a.bam"]["size"] == 4096

    def test_the_alias_is_listed_but_not_descended(self, client, runnable, tmp_path):
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
        run, outputs = self._collected(client, runnable, tmp_path)
        secret = tmp_path / "secret.txt"
        secret.write_text("nope")
        (outputs / "escape.txt").symlink_to(secret)
        r = client.get(
            f"/api/runs/{runnable}/{run}/file", query_string={"path": "escape.txt"})
        assert r.status_code >= 400

    def test_a_node_carries_its_whole_ancestry(self, client, runnable, tmp_path):
        run = TestResultsFiltering._name(client, runnable)
        project: Project = client.application.config["MSM_PROJECT"]
        outputs = project.outputs_path(runnable, run)
        outputs.mkdir(parents=True, exist_ok=True)

        external = tmp_path / "reads.fq"
        external.write_text("@x\nACGT\n+\n!!!!\n")
        types = DataTypeLibrary()
        for name in ("reads", "assembly", "bam"):
            types[name] = Endpoint(properties={name})
        tp = tmp_path / "t.yml"
        types.Save(tp)

        lib = DataInstanceLibrary(outputs)
        lib.AddTypeLibrary(tp, namespace="mock")
        lib.AddItem(external, "mock::reads")
        (outputs / "asm.fa").write_text("asm")
        lib.AddItem(Path("asm.fa"), "mock::assembly", parents=[external])
        (outputs / "out.bam").write_text("bam")
        lib.AddItem(Path("out.bam"), "mock::bam", parents=[Path("asm.fa")])
        lib.Save()

        flat = self._flat(client.get(f"/api/runs/{runnable}/{run}/tree").get_json()["root"])
        # The grandparent is there because a saved index is read back expanded,
        # not because the tree walks anything.
        parents = flat["out.bam"]["parents"]
        assert {p["type_name"] for p in parents} == {"mock::assembly", "mock::reads"}
        by_type = {p["type_name"]: p for p in parents}
        assert by_type["mock::assembly"]["node"] == "asm.fa"
        assert by_type["mock::reads"]["node"] is None
        assert flat["asm.fa"]["parents"] == [
            {"path": str(external), "type_name": "mock::reads", "node": None},
        ]

    def test_the_alias_resolves_for_reading(self, client, runnable, tmp_path):
        run, _ = self._collected(client, runnable, tmp_path)
        body = client.get(
            f"/api/runs/{runnable}/{run}/file",
            query_string={"path": "_metadata/logs.latest/agent.log"},
        ).get_json()
        assert body["text"] == "hello\nworld\n" and body["eof"] is True

    def test_a_window_pages_by_byte_offset(self, client, runnable, tmp_path):
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

        def _deploy(path, assertive=False, on_phase=None):
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


SHEET = b"sample,asm\nS1,/data/a.fa\nS2,/data/b.fa\n"

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
        name = _make_workflow(client)
        _attach(client, name)
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())

        inputs = client.get(f"/api/workflows/{name}/inputs").get_json()
        assert inputs["expansion"]["counts"] == {"idx": 2, "asm": 2}
        assert inputs["item_count"] == 4
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
        name = _make_workflow(client)
        _attach(client, name)
        _seed_inputs(client, name, count=1, prefix="plain")
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert client.delete(f"/api/workflows/{name}/table").status_code == 200
        client.put(f"/api/workflows/{name}", json={"sample_type": None})
        _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        items = client.get(f"/api/workflows/{name}/inputs").get_json()["items"]
        assert not any(it.get("array_id") for it in items)
        assert len(items) == 3

    def test_a_sample_table_solves_under_its_index_type(self, client):
        name = _make_workflow(client, sample="mock::reads")
        _attach(client, name)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert result["success"], result
        assert result["step_count"] > 0


class TestSharedInputs:
    ROWS = [ARRAY_ROWS[0]]

    def _shared_setup(self, client):
        name = _make_workflow(client, sample="mock::reads")
        project = client.application.config["MSM_PROJECT"]
        f = project.input_library_path(name) / "shared.fa"
        f.write_text(">contig\nACGT\n")
        sheet = f"sample,asm,ref\nS1,/data/a.fa,{f}\nS2,/data/b.fa,{f}\n".encode()
        _attach(client, name, sheet=sheet,
                rows=self.ROWS + [_row("shared", column="ref")])
        return name, "#shared"

    def test_an_unshared_neighbour_is_invisible_to_every_sample(self, client):
        name, _ = self._shared_setup(client)
        result = _finish(client, client.post(f"/api/workflows/{name}/generate", json={}).get_json())
        assert not result["success"]

    def test_marking_it_shared_puts_it_in_every_sample(self, client):
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


@pytest.fixture
def elsewhere(_app, tmp_path):
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
    def test_a_payload_round_trips(self, client):
        client.post("/api/ssh/hosts", json={"alias": "big-iron", "hostname": "big.example"})
        out = _payload(client, "ssh_host", "big-iron")
        assert out["payload"].startswith("msm1:")
        kind, body = share.decode(out["payload"])
        assert kind == "ssh_host"
        assert body["hostname"] == "big.example"

    def test_the_body_is_shown_beside_the_payload(self, client):
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
            assert other.post("/api/share/import", json={"payload": text}).status_code == 409
            assert other.get("/api/ssh/hosts").get_json()["hosts"][0]["hostname"] == "mine.example"


class TestShareAgents:
    def test_an_agent_arrives_with_its_params(self, client, elsewhere):
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
            assert wf["request"]["transform_libraries"] == []

    def test_an_unknown_type_arrives_placed_and_red(self, client, elsewhere):
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
            items = other.get(f"/api/workflows/{got['name']}/inputs").get_json()["items"]
            assert items == []

    def test_a_binding_travels_but_a_typed_path_does_not(self, client, elsewhere):
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
            _finish(other, other.post(
                f"/api/workflows/{got['name']}/generate", json={}).get_json())
            lib = Project(root).input_library_path(got["name"])
            (f,) = [x for x in lib.iterdir() if x.is_file() and len(x.name) == 32]
            assert f.read_text() == "left,right\n"

    def test_two_deferred_rows_stay_two_rows_across_the_wire(self, client, elsewhere):
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
