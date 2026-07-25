"""The Flask application: a localhost server for a single user.

There is no authentication and no multi-tenancy here by design -- this binds to
the loopback interface and drives the machine it runs on with the privileges of
the person who started it.
"""
from __future__ import annotations

import webbrowser
from pathlib import Path

from .api import bp as api_bp
from .jobs import JobRunner, install_log_capture
from .sshconfig import SshConfig
from .store import Project
from .watcher import RunWatcher

STATIC_DIRNAME = "static"
INDEX_FILE = "index.html"

BUILD_INSTRUCTIONS = """\
The GUI bundle has not been built.

It is generated rather than committed, so a fresh checkout has no page to serve
until you build it:

    ./dev.sh --build-gui

An installed release ships the bundle already built; if you are seeing this from
an installed copy, the package was built without that step.
"""


def static_root() -> Path:
    return Path(__file__).resolve().parent / STATIC_DIRNAME


def bundle_exists() -> bool:
    return (static_root() / INDEX_FILE).is_file()


def create_app(
    project_root: Path | str = ".",
    ssh_config_path: Path | str | None = None,
    watch: bool = True,
) -> "Flask":  # noqa: F821
    from flask import Flask, Response, send_from_directory

    project = Project(project_root)
    project.initialize()
    install_log_capture()

    app = Flask(__name__, static_folder=None)
    app.config["MSM_PROJECT"] = project
    app.config["MSM_JOBS"] = JobRunner()
    app.config["MSM_SSH"] = SshConfig(ssh_config_path)
    watcher = RunWatcher(project)
    app.config["MSM_WATCHER"] = watcher
    if watch:
        watcher.start()

    app.register_blueprint(api_bp)

    root = static_root()

    @app.get("/")
    def index():
        if not bundle_exists():
            return Response(BUILD_INSTRUCTIONS, mimetype="text/plain", status=503)
        return send_from_directory(root, INDEX_FILE)

    @app.get("/<path:asset>")
    def assets(asset):
        # a single-page app: unknown paths are routes, not missing files
        target = root / asset
        if target.is_file():
            return send_from_directory(root, asset)
        return index()

    return app


def serve(
    project_root: Path | str = ".",
    host: str = "127.0.0.1",
    port: int = 8090,
    open_browser: bool = True,
    ssh_config_path: Path | str | None = None,
) -> int:
    from ..constants import VERSION
    from ..logging import Log

    app = create_app(project_root, ssh_config_path=ssh_config_path)
    url = f"http://{host}:{port}"
    Log.Info(f"Metasmith {VERSION}")
    Log.Info(f"project [{Path(project_root).resolve()}]")
    if not bundle_exists():
        Log.Error("the GUI bundle is missing; run ./dev.sh --build-gui")
    Log.Info(f"serving at [{url}]")
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    # threaded is not optional: one open log stream would otherwise block every
    # other request and the page would look hung.
    app.run(host=host, port=port, threaded=True, debug=False, use_reloader=False)
    return 0
