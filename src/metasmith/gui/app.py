"""The Flask application: a localhost server for a single user.

There is no authentication and no multi-tenancy here by design -- this binds to
the loopback interface and drives the machine it runs on with the privileges of
the person who started it.
"""
from __future__ import annotations

import threading
import webbrowser
from pathlib import Path
from uuid import uuid4

from .api import bp as api_bp
from .jobs import JobRunner, install_log_capture
from .sshconfig import SshConfig
from .stdlib import resync_workflow_types
from .store import Project
from .watcher import RunWatcher

STATIC_DIRNAME = "static"
INDEX_FILE = "index.html"
MAX_UPLOAD_BYTES = 32 * 1024 * 1024

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


def warm_type_index(project_root: Path) -> None:
    """Build the browser's view of the type system before anyone asks for it.

    Indexing imports every transform in the standard library -- ~15s on a cold
    process -- and the first workflow opened after a start is what pays for it.
    Held off the request path entirely: this runs on its own thread at startup,
    under the same lock the planner uses (transform import is process-global),
    so a request arriving mid-warm waits for the result rather than racing it.
    """
    from ..logging import Log
    from . import stdlib
    from .api import _plan_lock

    try:
        with _plan_lock:
            stdlib.type_index(project_root)
    except Exception as exc:  # a page that has to load it itself is the fallback
        Log.Warn(f"could not pre-build the type index: {exc}")


def warm_template_dags(p: "Project") -> None:  # noqa: F821
    """Draw every template's DAG, in every theme, before anyone opens one.

    Same reasoning as `warm_type_index`, and a separate thread from it: this
    additionally needs the standard library and templates to exist, which
    `warm_type_index` does not, so the two should degrade independently
    rather than one's failure blocking the other. Each template+theme takes
    `_plan_lock` for only its own solve (see `_render_template_dag`), so a
    real request never queues behind the whole warm-up -- at most one solve.
    """
    from ..logging import Log
    from ..models.dag_renderer import THEMES
    from . import stdlib
    from .api import _render_template_dag, _template_dag_path, _templates

    try:
        commit = stdlib.discover(p.root)["commit"]
        for name, tmpl in _templates(p).items():
            for theme in THEMES:
                svg = _template_dag_path(p, name, commit, theme)
                if svg.is_file():
                    continue
                try:
                    _render_template_dag(p, tmpl, name, theme)
                except Exception as exc:
                    Log.Warn(f"could not pre-draw template [{name}] ({theme}): {exc}")
    except Exception as exc:  # a modal that has to draw it itself is the fallback
        Log.Warn(f"could not warm template DAGs: {exc}")


def bind_project(
    app: "Flask",  # noqa: F821
    project_root: Path | str = ".",
    ssh_config_path: Path | str | None = None,
    watch: bool = True,
) -> "Flask":  # noqa: F821
    """Point an app at a project: everything a run of the server is *about*.

    Split out from `create_app` because the routes are the expensive half and
    they hold no state -- werkzeug compiles a builder per rule, which costs more
    than the project side does. Nothing in production rebinds; the GUI's own
    tests do, once per case over one app, and that is the point.
    """
    project = Project(project_root)
    project.initialize()
    install_log_capture()
    threading.Thread(target=warm_type_index, args=(project.root,), daemon=True).start()
    threading.Thread(target=warm_template_dags, args=(project,), daemon=True).start()
    threading.Thread(target=resync_workflow_types, args=(project,), daemon=True).start()

    # Who this run of the server is. Recorded on every run it launches, so a
    # later server can tell "a thread of mine owns this" from "the process that
    # was staging this is gone" -- the difference between leaving a run alone
    # and resolving it. Minted here rather than at import: the GUI's own tests
    # rebind one app per case, and a module-level id would make every one of
    # them the same server.
    instance_id = uuid4().hex
    jobs = JobRunner()
    app.config["MSM_PROJECT"] = project
    app.config["MSM_JOBS"] = jobs
    app.config["MSM_INSTANCE"] = instance_id
    app.config["MSM_SSH"] = SshConfig(ssh_config_path)
    watcher = RunWatcher(project, instance_id=instance_id, jobs=jobs)
    app.config["MSM_WATCHER"] = watcher
    if watch:
        watcher.start()
    return app


def create_app(
    project_root: Path | str = ".",
    ssh_config_path: Path | str | None = None,
    watch: bool = True,
) -> "Flask":  # noqa: F821
    from flask import Flask, Response, jsonify, send_from_directory

    app = Flask(__name__, static_folder=None)
    # A sample sheet is the only thing anyone uploads here, and a sheet that
    # does not fit in this is a mistake rather than a study. Set on the app
    # rather than in `bind_project`, which the GUI's own tests re-run per case.
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
    bind_project(app, project_root, ssh_config_path=ssh_config_path, watch=watch)

    app.register_blueprint(api_bp)

    @app.errorhandler(413)
    def _too_large(_exc):
        # werkzeug raises this while parsing the body, before any blueprint
        # handler is reached, so it needs an answer at the app level or the
        # page gets html where it expects `{error, kind}`
        return jsonify({
            "error": f"that file is larger than {MAX_UPLOAD_BYTES // (1 << 20)} MB",
            "kind": "refused",
        }), 413

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
