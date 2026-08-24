from __future__ import annotations

import os
from pathlib import Path
from unittest import mock

import pytest

from metasmith.coms.cli._main import _build_parser
from metasmith.gui import stdlib

pytestmark = pytest.mark.gui

class TestCommandSurface:
    def test_gui_is_registered(self):
        args = _build_parser().parse_args(["gui", "--port", "9999", "--no-browser"])
        assert args.port == 9999
        assert args.no_browser is True

    def test_gui_binds_loopback_by_default(self):
        assert _build_parser().parse_args(["gui"]).host == "127.0.0.1"

    def test_lab_still_parses(self):
        args = _build_parser().parse_args(["lab", "--port", "8080"])
        assert args.port == 8080


class TestBootstrap:
    def _fake_library(self, root: Path) -> Path:
        lib = root / "pkg" / "metasmith_libraries"
        (lib / "data_types").mkdir(parents=True)
        (lib / "version.txt").write_text("9.9.9")
        return lib

    def test_copies_from_the_installed_module_and_compiles_it(self, tmp_path):
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
        assert [Path(b).name for b in built] == [stdlib.STDLIB_NAME + ".partial"]
        assert second["cloned"] is False
        m.assert_not_called()

    def test_a_read_only_install_still_yields_a_writable_copy(self, tmp_path):
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
        with mock.patch.object(stdlib, "library_module_root", return_value=None):
            out = stdlib.clone_stdlib(tmp_path)
        assert out["cloned"] is False
        assert "metasmith_libraries" in out["error"]

    def test_the_stamp_is_what_callers_key_caches_on(self, tmp_path):
        lib = self._fake_library(tmp_path)
        with mock.patch.object(stdlib, "library_module_root", return_value=lib), \
             mock.patch.object(stdlib, "compile_library"):
            stdlib.clone_stdlib(tmp_path)
        stamp = stdlib.stdlib_commit(tmp_path)
        assert stamp and stamp.startswith("9.9.9+")
        assert stdlib.discover(tmp_path)["commit"] == stamp

    def test_lab_and_gui_share_it(self):
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
    import importlib

    importlib.import_module(module)


class TestServing:
    # What a start prints, and what it must not.
    #
    # `app.run()` printed Flask's banner and werkzeug's "development server"
    # warning on top of metasmith's own three lines, and neither could be turned
    # off. The warning is about a public multi-user server; bound to loopback the
    # only client is the person who started it. What replaces it fires on the one
    # case where the advice is real -- a bind address other machines can reach --
    # and says the thing that is actually true of this API.

    def _serve(self, host="127.0.0.1"):
        from metasmith.gui import app as gui_app

        server = mock.MagicMock()
        server.server_port = 8090
        server.serve_forever.side_effect = KeyboardInterrupt
        app = mock.MagicMock()
        with mock.patch.object(gui_app, "create_app", return_value=app), \
             mock.patch("werkzeug.serving.make_server", return_value=server):
            code = gui_app.serve(host=host, port=8090, open_browser=False)
        return code, server, app

    def test_it_says_where_it_is_serving_and_nothing_more(self, caplog):
        with caplog.at_level("INFO"):
            code, _, app = self._serve()
        assert code == 0
        # The banners are printed by `app.run()` and nothing below it, so the
        # thing to pin is that this does not go through it.
        app.run.assert_not_called()
        text = caplog.text
        assert "serving at [http://127.0.0.1:8090]" in text
        for banner in ("development server", "Running on", "Serving Flask app", "CTRL+C"):
            assert banner not in text

    def test_ctrl_c_closes_the_socket(self):
        code, server, _ = self._serve()
        assert code == 0
        server.server_close.assert_called_once()

    def test_a_loopback_bind_warns_about_nothing(self, caplog):
        with caplog.at_level("INFO"):
            self._serve()
        assert "reachable from other machines" not in caplog.text

    def test_a_reachable_bind_says_what_that_costs(self, caplog):
        with caplog.at_level("INFO"):
            self._serve(host="0.0.0.0")
        said = [r for r in caplog.records if "reachable from other machines" in r.message]
        assert len(said) == 1
        assert "no authentication" in said[0].message

    @pytest.mark.parametrize("host", ["127.0.0.1", "127.0.1.5", "localhost", "::1"])
    def test_loopback_hosts(self, host):
        from metasmith.gui.app import _is_loopback

        assert _is_loopback(host)

    @pytest.mark.parametrize("host", ["0.0.0.0", "", "::", "10.0.0.4", "some-node"])
    def test_reachable_hosts(self, host):
        from metasmith.gui.app import _is_loopback

        assert not _is_loopback(host)
