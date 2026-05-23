"""Smoke tests for the CLI dispatch layer (argparse, output formatting, error exit)."""
from __future__ import annotations

import json
import subprocess
import sys

import pytest


METASMITH = [sys.executable, "-m", "metasmith"]


def _run(*argv, **kwargs):
    return subprocess.run(METASMITH + list(argv), capture_output=True, text=True, **kwargs)


class TestCLISmoke:
    def test_help_lists_subcommands(self):
        r = _run("--help")
        assert r.returncode == 0
        for cmd in ("type", "data", "transform", "plan", "workflow", "agent", "source", "task", "build"):
            assert cmd in r.stdout

    def test_version_flag(self):
        r = _run("--version")
        assert r.returncode == 0
        assert "metasmith" in r.stdout.lower()

    def test_type_list_json_is_valid(self, tmp_path):
        ty = tmp_path / "t.yml"
        _run("type", "create", str(ty),
             "--types", '{"foo": {"properties": ["bar"]}}', check=True)
        r = _run("--json", "type", "list", "-t", str(ty))
        assert r.returncode == 0
        data = json.loads(r.stdout)
        assert any(item["name"] == "foo" for item in data)

    def test_unknown_type_exits_nonzero(self):
        r = _run("type", "show", "nonexistent::nope", "-t", "/dev/null")
        assert r.returncode != 0

    def test_build_types_subcommand_runs(self, tmp_path):
        ty = tmp_path / "ty.yml"
        _run("type", "create", str(ty),
             "--types", '{"foo": {"properties": ["bar"]}}', check=True)
        # Both invocation orders should work.
        r1 = _run("--json", "build", "types", "-t", str(tmp_path))
        r2 = _run("--json", "build", "-t", str(tmp_path), "types")
        assert r1.returncode == 0 and r2.returncode == 0
        assert json.loads(r1.stdout) == json.loads(r2.stdout)

    def test_no_subcommand_prints_help(self):
        r = _run()
        assert r.returncode == 0
        assert "COMMAND" in r.stdout
