"""`msm gui`, and the project bootstrap it shares with `msm lab`."""
from __future__ import annotations

import os
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
    """What `clone_stdlib` guarantees now that the library is an installed module.

    The install directory is read-only in the cases that matter -- conda
    hardlinks its `pkgs` files in that way -- and the copy has to be compiled,
    so these pin the three properties that follow from that and nothing else.
    """

    def _fake_library(self, root: Path) -> Path:
        """A directory shaped enough like a library for `library_module_root`."""
        lib = root / "pkg" / "metasmith_libraries"
        (lib / "data_types").mkdir(parents=True)
        (lib / "version.txt").write_text("9.9.9")
        return lib

    def test_copies_from_the_installed_module_and_compiles_it(self, tmp_path):
        """No network involved at all, and the copy is the thing that gets built."""
        lib = self._fake_library(tmp_path)
        built = []
        with mock.patch.object(stdlib, "library_module_root", return_value=lib), \
             mock.patch.object(stdlib, "compile_library", side_effect=built.append), \
             mock.patch.object(stdlib.subprocess, "run") as m:
            first = stdlib.clone_stdlib(tmp_path)
            second = stdlib.clone_stdlib(tmp_path)
        assert first["cloned"] is True
        assert first["source"] == str(lib)
        assert (tmp_path / stdlib.STDLIB_NAME / "data_types").is_dir()
        # compiled once, against the COPY -- never against the install
        assert [Path(b).name for b in built] == [stdlib.STDLIB_NAME + ".partial"]
        assert second["cloned"] is False
        m.assert_not_called()

    def test_a_read_only_install_still_yields_a_writable_copy(self, tmp_path):
        """`copytree` inherits the source's mode bits; the compile needs to write."""
        lib = self._fake_library(tmp_path)
        for p in (lib, lib / "data_types", lib / "version.txt"):
            p.chmod(0o500 if p.is_dir() else 0o400)
        try:
            with mock.patch.object(stdlib, "library_module_root", return_value=lib), \
                 mock.patch.object(stdlib, "compile_library"):
                out = stdlib.clone_stdlib(tmp_path)
            assert out["cloned"] is True
            copy = tmp_path / stdlib.STDLIB_NAME
            assert os.access(copy / "data_types", os.W_OK)
            assert os.access(copy / "version.txt", os.W_OK)
        finally:
            for p in (lib / "version.txt", lib / "data_types", lib):
                p.chmod(0o700)

    def test_a_failed_compile_leaves_nothing_behind(self, tmp_path):
        """Materialisation is atomic: a directory that exists is one that resolves.

        Otherwise a half-built copy persists across restarts and every later
        bootstrap short-circuits on it, silently, with no metadata in it.
        """
        lib = self._fake_library(tmp_path)
        with mock.patch.object(stdlib, "library_module_root", return_value=lib), \
             mock.patch.object(stdlib, "compile_library",
                               side_effect=RuntimeError("no transforms resolved")):
            out = stdlib.clone_stdlib(tmp_path)
        assert out["cloned"] is False
        assert "no transforms resolved" in out["error"]
        assert not (tmp_path / stdlib.STDLIB_NAME).exists()
        assert not (tmp_path / (stdlib.STDLIB_NAME + ".partial")).exists()

    def test_a_missing_package_is_reported_not_raised(self, tmp_path):
        """No library should still leave you with a usable page, not a traceback."""
        with mock.patch.object(stdlib, "library_module_root", return_value=None):
            out = stdlib.clone_stdlib(tmp_path)
        assert out["cloned"] is False
        assert "metasmith_libraries" in out["error"]

    def test_the_stamp_is_what_callers_key_caches_on(self, tmp_path):
        """It stands in for the git commit, so it must move when the library does."""
        lib = self._fake_library(tmp_path)
        with mock.patch.object(stdlib, "library_module_root", return_value=lib), \
             mock.patch.object(stdlib, "compile_library"):
            stdlib.clone_stdlib(tmp_path)
        stamp = stdlib.stdlib_commit(tmp_path)
        assert stamp and stamp.startswith("9.9.9+")
        assert stdlib.discover(tmp_path)["commit"] == stamp

    def test_lab_and_gui_share_it(self):
        """The bootstrap lives in one place so the two front ends cannot drift."""
        from metasmith.coms.cli import legacy

        source = Path(legacy.__file__).read_text()
        assert "bootstrap_project" in source
        assert "clone" not in source, (
            "the library is copied from the installed module; nothing here clones"
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
