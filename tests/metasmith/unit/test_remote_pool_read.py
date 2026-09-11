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
