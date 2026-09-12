"""The client reads an agent's pool without reaching the agent's filesystem.

A campaign's pool lives in an agent home on a cluster. The client that builds
the plan has no mount there, cannot stat a path in it, and must still name a
given. That works only because a given is a record -- a name, a type and an
assigned identity -- rather than something derived from the bytes.

Every path these tests report is deliberately one that does not exist here. A
read that starts working by accident because the path happened to resolve is
the failure this file is watching for.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from metasmith.agents.pool import _PoolAccess, first_json
from metasmith.models.remote import Source, SourceType


REMOTE_HOME = "/scratch/nowhere/msm_home"
REMOTE_DATA = "/scratch/nowhere/reads"


def _row(name, iid, dtype="cami::reads", path=None):
    return {
        "key": iid, "name": name, "origin": "imported", "run": "", "tags": [],
        "transform_key": "", "step_name": "", "size_bytes": 4,
        "created_at": 1, "last_hit_at": 1, "hit_count": 0,
        "tombstoned_at": None, "instance_id": iid,
        "path": path or f"{REMOTE_DATA}/{name}", "dtype": dtype,
    }


class _SshAgent(_PoolAccess):
    """An agent whose home is on a host this process cannot reach."""

    def __init__(self, rows, setup_commands=()):
        self.home = Source(
            address=f"ssh://cluster:{REMOTE_HOME}", type=SourceType.SSH,
        )
        self.setup_commands = list(setup_commands)
        self.rows = rows
        self.sent: list[str] = []

    def _is_ssh(self) -> bool:
        return True

    def _remote_oneshot(self, cmd, timeout=30):
        self.sent.append(cmd)
        body = json.dumps(
            {"cache_root": f"{REMOTE_HOME}/task_cache", "entries": self.rows},
            indent=2,
        )
        # What a real agent sends back: the wrapper's own chatter, the answer,
        # and whatever the container runtime says on the way out.
        lines = ["including dev binds", "binds [-B /scratch:/scratch]"]
        lines += body.splitlines()
        lines += ["INFO:    Cleaning up image..."]
        return type("R", (), {"out": lines, "err": []})()


class TestTheAnswerSurvivesTheNoise:
    def test_the_json_is_found_between_the_chatter(self):
        agent = _SshAgent([_row("batch1/reads", "aa" * 34)])
        out = agent.ReadPool()
        assert out["cache_root"] == f"{REMOTE_HOME}/task_cache"
        assert [e["name"] for e in out["entries"]] == ["batch1/reads"]

    def test_no_json_at_all_says_what_came_back_instead(self):
        with pytest.raises(ValueError) as e:
            first_json(["module: command not found", "bash: ./msm: No such file"])
        assert "No such file" in str(e.value)

    def test_a_failed_read_names_the_pool_and_the_host(self):
        agent = _SshAgent([])
        agent._remote_oneshot = lambda cmd, timeout=30: type(
            "R", (), {"out": ["Permission denied (publickey)."], "err": []},
        )()
        with pytest.raises(ValueError) as e:
            agent.ReadPool()
        assert REMOTE_HOME in str(e.value) and "cluster" in str(e.value)


class TestTheCommandTheClientSends:
    def test_it_reads_the_pool_in_the_agents_own_home(self):
        agent = _SshAgent([])
        agent.ReadPool()
        cmd = agent.sent[0]
        assert f"{REMOTE_HOME}/task_cache" in cmd
        assert "./msm --json cache list" in cmd

    def test_the_setup_commands_come_first(self):
        # A cluster reaches apptainer through a module load, and a non-login
        # ssh command inherits none of the login shell that would have done it.
        agent = _SshAgent([], setup_commands=["module load apptainer"])
        agent.ReadPool()
        cmd = agent.sent[0]
        assert cmd.index("module load apptainer") < cmd.index("./msm")

    def test_a_filter_is_passed_through(self):
        agent = _SshAgent([])
        agent.ReadPool(origin="imported", dtype="cami::reads")
        assert "--origin imported" in agent.sent[0]
        assert "--dtype cami::reads" in agent.sent[0]


class TestResolvingAReference:
    def test_a_name_resolves_to_an_identity(self):
        iid = "aa" * 34
        agent = _SshAgent([_row("batch1/reads", iid)])
        (entry,) = agent.ResolvePoolRefs(["batch1/reads"])
        assert entry["instance_id"] == iid
        assert not Path(entry["path"]).exists(), (
            "the fixture's whole point is a path this host cannot see"
        )

    def test_an_identity_resolves_to_itself(self):
        iid = "bb" * 34
        agent = _SshAgent([_row("batch1/reads", iid)])
        (entry,) = agent.ResolvePoolRefs([iid])
        assert entry["name"] == "batch1/reads"

    def test_the_order_asked_is_the_order_returned(self):
        rows = [_row("b", "bb" * 34), _row("a", "aa" * 34), _row("c", "cc" * 34)]
        agent = _SshAgent(rows)
        assert [e["name"] for e in agent.ResolvePoolRefs(["c", "a", "b"])] == [
            "c", "a", "b",
        ]

    def test_an_unknown_name_names_the_import_call(self):
        agent = _SshAgent([_row("batch1/reads", "aa" * 34)])
        with pytest.raises(ValueError) as e:
            agent.ResolvePoolRefs(["batch2/reads"])
        msg = str(e.value)
        assert "metasmith data import" in msg
        assert "batch2/reads" in msg
        assert "nothing this end can mint" in msg

    def test_one_name_on_two_imports_is_refused_with_both_ids(self):
        # Re-importing under a name is how a caller says the second is a
        # different thing, so the pool holding both is expected. Choosing
        # between them is the caller's call, not this end's.
        a, b = "aa" * 34, "bb" * 34
        agent = _SshAgent([_row("batch1/reads", a), _row("batch1/reads", b)])
        with pytest.raises(ValueError) as e:
            agent.ResolvePoolRefs(["batch1/reads"])
        assert a in str(e.value) and b in str(e.value)

    def test_resolving_reads_the_pool_once_for_many_refs(self):
        rows = [_row(n, f"{i:02x}" * 34) for i, n in enumerate("abcd")]
        agent = _SshAgent(rows)
        agent.ResolvePoolRefs(list("abcd"))
        assert len(agent.sent) == 1


class TestALocalHomeIsReadInProcess:
    def test_nothing_is_shelled_out_for_a_home_on_this_host(self, tmp_path):
        from metasmith.ops import data as op_data

        class _Local(_PoolAccess):
            def __init__(self, home):
                self.home = Source.FromLocal(home)
                self.setup_commands = []

            def _is_ssh(self):
                return False

            def _remote_oneshot(self, cmd, timeout=30):
                raise AssertionError("a home on this host needs no ssh")

        home = tmp_path / "home"
        home.mkdir()
        f = tmp_path / "reads.fq"
        f.write_text("acgt\n")
        res = op_data.import_item(
            str(f), "cami::reads", agent_home=str(home), name="batch1/reads",
        )
        entries = _Local(home).ReadPool()["entries"]
        assert [e["instance_id"] for e in entries] == [res["instance_id"]]
        assert entries[0]["name"] == "batch1/reads"


class TestAGivenLibraryFromPoolReferences:
    """The plan key is what this is for: a reference keys the same every time."""

    @pytest.fixture
    def types(self, tmp_path):
        from tests.metasmith.cache._cache_harness import build_types_library

        return build_types_library(tmp_path / "types", ("reads", "meta"))

    def _agent(self, rows):
        return _SshAgent(rows)

    def test_the_instances_carry_the_pools_identities(self, tmp_path, types):
        from metasmith.models.libraries import DataTypeLibrary

        iid = "aa" * 34
        agent = self._agent([_row("batch1/reads", iid, dtype="cf::reads")])
        lib = agent.GivenLibrary(
            ["batch1/reads"], location=tmp_path / "given",
            types={"cf": DataTypeLibrary.Load(types)},
        )
        path = Path(f"{REMOTE_DATA}/batch1/reads")
        assert list(lib.manifest) == [path]
        assert lib.Get(path).instance_id == iid

    def test_two_builds_of_one_reference_key_the_same(self, tmp_path, types):
        from metasmith.models.libraries import DataTypeLibrary

        rows = [_row("batch1/reads", "aa" * 34, dtype="cf::reads")]
        ids = []
        for i in (1, 2):
            agent = self._agent(rows)
            lib = agent.GivenLibrary(
                ["batch1/reads"], location=tmp_path / f"given{i}",
                types={"cf": DataTypeLibrary.Load(types)},
            )
            ids.append(lib.Get(Path(f"{REMOTE_DATA}/batch1/reads")).instance_id)
        assert ids[0] == ids[1]

    def test_recorded_ancestry_becomes_a_parent_edge(self, tmp_path, types):
        from metasmith.models.libraries import DataTypeLibrary

        parent = _row("batch1/meta", "bb" * 34, dtype="cf::meta")
        child = _row("batch1/reads", "aa" * 34, dtype="cf::reads")
        child["parents"] = [parent["instance_id"]]
        agent = self._agent([parent, child])
        lib = agent.GivenLibrary(
            ["batch1/meta", "batch1/reads"], location=tmp_path / "given",
            types={"cf": DataTypeLibrary.Load(types)},
        )
        kid = Path(f"{REMOTE_DATA}/batch1/reads")
        assert [pm.name for pm in lib.parents.get(kid, [])] == ["cf::meta"]

    def test_an_edge_to_something_unreferenced_is_dropped(self, tmp_path, types):
        # Unpack walks a parent by path, so an edge to an entry the library has
        # no item for is a walk into nothing.
        from metasmith.models.libraries import DataTypeLibrary

        parent = _row("batch1/meta", "bb" * 34, dtype="cf::meta")
        child = _row("batch1/reads", "aa" * 34, dtype="cf::reads")
        child["parents"] = [parent["instance_id"]]
        agent = self._agent([parent, child])
        lib = agent.GivenLibrary(
            ["batch1/reads"], location=tmp_path / "given",
            types={"cf": DataTypeLibrary.Load(types)},
        )
        assert lib.parents == {}

    def test_an_unknown_reference_refuses_before_a_library_exists(
        self, tmp_path, types,
    ):
        from metasmith.models.libraries import DataTypeLibrary

        agent = self._agent([_row("batch1/reads", "aa" * 34, dtype="cf::reads")])
        with pytest.raises(ValueError) as e:
            agent.GivenLibrary(
                ["batch9/reads"], location=tmp_path / "given",
                types={"cf": DataTypeLibrary.Load(types)},
            )
        assert "metasmith data import" in str(e.value)


class _ImportingAgent(_SshAgent):
    """An agent that records what it was asked to import, and grows a pool."""

    def _remote_oneshot(self, cmd, timeout=30):
        self.sent.append(cmd)
        if "data import" not in cmd:
            return super()._remote_oneshot(cmd, timeout=timeout)
        flags = cmd.rsplit("data import ", 1)[1].split()
        path = flags[0]
        name = flags[flags.index("--name") + 1] if "--name" in flags else path
        dtype = flags[flags.index("--dtype") + 1]
        iid = f"{len(self.rows):02x}" * 34
        self.rows.append(_row(name, iid, dtype=dtype, path=path))
        body = json.dumps({"name": name, "instance_id": iid, "path": path})
        return type("R", (), {"out": ["binds [...]", body], "err": []})()


class TestImportingIntoARemotePool:
    def test_the_command_names_the_pool_and_the_declaration(self):
        agent = _ImportingAgent([])
        agent.ImportToPool(
            f"{REMOTE_DATA}/s1.fq", "cami::reads",
            name="batch1/s1/reads", tags=["batch1"],
        )
        sent = agent.sent[-1]
        assert f"{REMOTE_HOME}/task_cache" in sent
        assert "--dtype cami::reads" in sent
        assert "--name batch1/s1/reads" in sent
        assert "--tag batch1" in sent

    def test_the_setup_commands_come_first(self):
        agent = _ImportingAgent([], setup_commands=["module load apptainer"])
        agent.ImportToPool(f"{REMOTE_DATA}/s1.fq", "cami::reads")
        assert agent.sent[-1].startswith("module load apptainer ; ")

    def test_a_name_the_pool_holds_is_referenced_rather_than_imported(self):
        agent = _ImportingAgent([_row("batch1/s1/reads", "aa" * 34)])
        found = agent.EnsurePoolEntries([
            {"path": f"{REMOTE_DATA}/s1.fq", "dtype": "cami::reads",
             "name": "batch1/s1/reads"},
        ])
        assert found == {"batch1/s1/reads": "aa" * 34}
        assert not any("data import" in c for c in agent.sent)

    def test_setup_run_twice_gives_the_same_identities(self):
        items = [
            {"path": f"{REMOTE_DATA}/s1.fq", "dtype": "cami::reads",
             "name": "batch1/s1/reads"},
            {"path": f"{REMOTE_DATA}/s2.fq", "dtype": "cami::reads",
             "name": "batch1/s2/reads"},
        ]
        agent = _ImportingAgent([])
        first = agent.EnsurePoolEntries(items)
        second = agent.EnsurePoolEntries(items)
        assert first == second
        assert len(first) == 2
        assert sum("data import" in c for c in agent.sent) == 2

    def test_a_parent_named_earlier_in_the_list_resolves_to_its_identity(self):
        agent = _ImportingAgent([])
        found = agent.EnsurePoolEntries([
            {"path": f"{REMOTE_DATA}/meta.json", "dtype": "cami::meta",
             "name": "batch1/s1/meta"},
            {"path": f"{REMOTE_DATA}/s1.fq", "dtype": "cami::reads",
             "name": "batch1/s1/reads", "parents": ["batch1/s1/meta"]},
        ])
        reads_cmd = [c for c in agent.sent if "s1.fq" in c][-1]
        assert f"--parent {found['batch1/s1/meta']}" in reads_cmd

    def test_nothing_here_touches_the_agents_filesystem(self):
        agent = _ImportingAgent([])
        agent.EnsurePoolEntries([
            {"path": f"{REMOTE_DATA}/s1.fq", "dtype": "cami::reads",
             "name": "batch1/s1/reads"},
        ])
        assert not Path(REMOTE_DATA).exists()


class _WritingAgent(_ImportingAgent):
    """An agent whose home is a real directory, so an authored file lands."""

    def __init__(self, home: Path, rows=None):
        super().__init__(list(rows or []))
        self.home = Source(address=str(home), type=SourceType.DIRECT)
        self.written: list[str] = []

    def _is_ssh(self) -> bool:
        return False


class TestAnAuthoredFileIsWrittenWhereThePoolCanNameIt:
    def test_it_lands_under_the_agents_home(self, tmp_path):
        agent = _WritingAgent(tmp_path / "home")
        out = agent.WriteImportable("cami/s1/meta.json", '{"parity": "paired"}')
        assert out.read_text() == '{"parity": "paired"}'
        assert out.is_relative_to(tmp_path / "home")

    def test_writing_the_same_content_again_changes_nothing(self, tmp_path):
        agent = _WritingAgent(tmp_path / "home")
        out = agent.WriteImportable("meta.json", "same")
        before = out.stat().st_mtime_ns
        agent.WriteImportable("meta.json", "same")
        assert out.stat().st_mtime_ns == before

    def test_the_remote_form_never_pastes_the_content_into_the_command(self, tmp_path):
        agent = _ImportingAgent([])
        agent.WriteImportable("meta.json", "parity: 'paired' && rm -rf /")
        sent = agent.sent[-1]
        assert "rm -rf /" not in sent
        assert "base64 -d" in sent


class TestDeclaringGivensThroughTheCollector:
    def test_a_declaration_becomes_a_pool_entry_and_a_library_item(self, tmp_path):
        agent = _WritingAgent(tmp_path / "home")
        givens = agent.PoolGivens()
        meta = givens.Value(
            "cami/s1/meta.json", {"parity": "paired"}, "cami::meta",
        )
        givens.Add(f"{REMOTE_DATA}/s1.fq", "cami::reads",
                   name="cami/s1/reads", parents=[meta])

        assert [i["name"] for i in givens.items] == [
            "cami/s1/meta.json", "cami/s1/reads",
        ]
        found = agent.EnsurePoolEntries(givens.items)
        assert len(found) == 2
        assert len(set(found.values())) == 2

    def test_declaring_twice_cites_the_same_identities(self, tmp_path):
        agent = _WritingAgent(tmp_path / "home")
        first = agent.PoolGivens()
        first.Add(f"{REMOTE_DATA}/s1.fq", "cami::reads", name="cami/s1/reads")
        a = agent.EnsurePoolEntries(first.items)

        second = agent.PoolGivens()
        second.Add(f"{REMOTE_DATA}/s1.fq", "cami::reads", name="cami/s1/reads")
        b = agent.EnsurePoolEntries(second.items)
        assert a == b
