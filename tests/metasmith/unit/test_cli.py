from __future__ import annotations

import json
import subprocess
import sys

import pytest

from metasmith.coms.cli import _main


@pytest.fixture
def cli(capsys, monkeypatch):
    monkeypatch.setattr(_main._log_handler, "stream", _main._log_handler.stream)

    def run(*argv):
        try:
            rc = _main.main(list(argv))
        except SystemExit as e:
            rc = e.code if isinstance(e.code, int) else 1
        return rc, capsys.readouterr().out

    return run


class TestCLISmoke:
    def test_help_lists_subcommands(self):
        r = subprocess.run([sys.executable, "-m", "metasmith", "--help"], capture_output=True, text=True)
        assert r.returncode == 0
        for cmd in ("type", "data", "transform", "plan", "workflow", "agent", "source", "task", "build"):
            assert cmd in r.stdout

    def test_version_flag(self, cli):
        rc, out = cli("--version")
        assert rc == 0
        assert "metasmith" in out.lower()

    def test_type_list_json_is_valid(self, cli, tmp_path):
        ty = tmp_path / "t.yml"
        assert cli("type", "create", str(ty), "--types", '{"foo": {"properties": ["bar"]}}')[0] == 0
        rc, out = cli("--json", "type", "list", "-t", str(ty))
        assert rc == 0
        assert any(item["name"] == "foo" for item in json.loads(out))

    def test_unknown_type_exits_nonzero(self, cli):
        assert cli("type", "show", "nonexistent::nope", "-t", "/dev/null")[0] != 0

    def test_build_types_subcommand_runs(self, cli, tmp_path):
        ty = tmp_path / "ty.yml"
        assert cli("type", "create", str(ty), "--types", '{"foo": {"properties": ["bar"]}}')[0] == 0
        rc1, out1 = cli("--json", "build", "types", "-t", str(tmp_path))
        rc2, out2 = cli("--json", "build", "-t", str(tmp_path), "types")
        assert rc1 == 0 and rc2 == 0
        assert json.loads(out1) == json.loads(out2)

    def test_no_subcommand_prints_help(self, cli):
        rc, out = cli()
        assert rc == 0
        assert "COMMAND" in out
