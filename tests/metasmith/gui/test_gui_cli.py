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
    def test_prefers_the_vendored_bundle(self, tmp_path):
        """The default path: no network involved at all when a bundle shipped."""
        bundle = tmp_path / "bundle"
        (bundle / "data_types").mkdir(parents=True)

        with mock.patch.object(stdlib, "_vendor_bundle_dir", return_value=bundle), \
             mock.patch.object(stdlib.subprocess, "run") as m:
            first = stdlib.clone_stdlib(tmp_path)
            second = stdlib.clone_stdlib(tmp_path)
        assert first["cloned"] is True
        assert first["source"] == "vendor"
        assert (tmp_path / stdlib.STDLIB_NAME / "data_types").is_dir()
        assert second["cloned"] is False
        m.assert_not_called()

    def test_no_bundle_and_no_opt_in_fails_without_touching_the_network(self, tmp_path, monkeypatch):
        monkeypatch.delenv("METASMITH_STDLIB_LIVE", raising=False)
        with mock.patch.object(stdlib, "_vendor_bundle_dir", return_value=tmp_path / "absent"), \
             mock.patch.object(stdlib.subprocess, "run") as m:
            out = stdlib.clone_stdlib(tmp_path)
        assert out["cloned"] is False
        assert "opt-in" in out["error"]
        assert "METASMITH_STDLIB_LIVE" in out["error"]
        m.assert_not_called()

    def test_opt_in_live_fetch_sparse_checks_out_and_copies(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METASMITH_STDLIB_LIVE", "1")
        calls = []

        def _fake_git(cmd, **kwargs):
            calls.append(cmd)
            if cmd[1] == "clone":
                clone_dir = Path(cmd[-1])
                (clone_dir / stdlib.STDLIB_SPARSE_PATH).mkdir(parents=True)
                (clone_dir / stdlib.STDLIB_SPARSE_PATH / "data_types").mkdir()
            return mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(stdlib, "_vendor_bundle_dir", return_value=tmp_path / "absent"), \
             mock.patch.object(stdlib.subprocess, "run", side_effect=_fake_git):
            out = stdlib.clone_stdlib(tmp_path)
        assert out["cloned"] is True
        assert out["source"] == "live"
        assert (tmp_path / stdlib.STDLIB_NAME / "data_types").is_dir()
        # clone --no-checkout, sparse-checkout init, sparse-checkout set, checkout
        assert len(calls) == 4
        assert calls[0][1] == "clone"
        assert stdlib.STDLIB_SPARSE_PATH in calls[2]

    def test_a_failed_live_fetch_is_reported_not_raised(self, tmp_path, monkeypatch):
        """No network should still leave you with a usable page, not a traceback."""
        monkeypatch.setenv("METASMITH_STDLIB_LIVE", "1")
        with mock.patch.object(stdlib, "_vendor_bundle_dir", return_value=tmp_path / "absent"), \
             mock.patch.object(stdlib.subprocess, "run") as m:
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
