"""`msm gui`, and the project bootstrap it shares with `msm lab`."""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from metasmith.coms.cli._main import _build_parser
from metasmith.gui import stdlib

# tests/gui/ IS the GUI's own suite -- conftest stamps `gui` (and `fast`) on
# everything under it, and `dev.sh -tg` runs the directory. This line is kept
# as a local reminder of what the file is for; it is no longer what selects it.
pytestmark = pytest.mark.gui

class TestCommandSurface:
    def test_gui_is_registered(self):
        args = _build_parser().parse_args(["gui", "--port", "9999", "--no-browser"])
        assert args.port == 9999
        assert args.no_browser is True

    def test_gui_binds_loopback_by_default(self):
        """A localhost tool with no authentication must not default to 0.0.0.0."""
        assert _build_parser().parse_args(["gui"]).host == "127.0.0.1"

    def test_lab_still_parses(self):
        args = _build_parser().parse_args(["lab", "--port", "8080"])
        assert args.port == 8080


class TestBootstrap:
    def test_clones_once(self, tmp_path):
        calls = []

        def _fake_git(cmd, **kwargs):
            calls.append(cmd)
            Path(cmd[-1]).mkdir(parents=True)
            return mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(stdlib.subprocess, "run", side_effect=_fake_git):
            first = stdlib.clone_stdlib(tmp_path)
            second = stdlib.clone_stdlib(tmp_path)
        assert first["cloned"] is True
        assert second["cloned"] is False
        assert len(calls) == 1

    def test_a_failed_clone_is_reported_not_raised(self, tmp_path):
        """No network should still leave you with a usable page, not a traceback."""
        with mock.patch.object(stdlib.subprocess, "run") as m:
            m.return_value = mock.Mock(returncode=128, stdout="", stderr="could not resolve host")
            out = stdlib.clone_stdlib(tmp_path)
        assert out["cloned"] is False
        assert "could not resolve host" in out["error"]

    def test_lab_and_gui_share_it(self):
        """The bootstrap lives in one place so the two front ends cannot drift."""
        from metasmith.coms.cli import legacy

        source = Path(legacy.__file__).read_text()
        assert "bootstrap_project" in source
        assert "MetasmithLibraries.git" not in source, (
            "the repository url should come from constants, not be re-hardcoded here"
        )


class TestDiscovery:
    def test_finds_the_three_shapes(self, tmp_path):
        lib = tmp_path / "MetasmithLibraries"
        (lib / "data_types").mkdir(parents=True)
        (lib / "data_types" / "seq.yml").write_text("schema: v1\ntypes: {}\n")
        (lib / "transforms" / "logistics").mkdir(parents=True)
        (lib / "resources" / "containers").mkdir(parents=True)

        found = stdlib.discover(tmp_path)
        assert found["present"] is True
        assert [Path(p).name for p in found["data_types"]] == ["seq.yml"]
        assert [Path(p).name for p in found["transform_libraries"]] == ["logistics"]
        assert [Path(p).name for p in found["resource_libraries"]] == ["containers"]

    def test_absent_library_is_stated_not_raised(self, tmp_path):
        found = stdlib.discover(tmp_path)
        assert found["present"] is False
        assert found["transform_libraries"] == []


@pytest.mark.parametrize("module", [
    "metasmith.gui.store",
    "metasmith.gui.names",
    "metasmith.gui.sshconfig",
    "metasmith.gui.jobs",
    "metasmith.gui.watcher",
])
def test_gui_modules_import_without_flask(module):
    """Only app.py and api.py need flask, so the rest stays testable without it."""
    import importlib

    importlib.import_module(module)
