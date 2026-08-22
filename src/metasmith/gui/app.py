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
    from ..logging import Log
    from . import stdlib
    from .api import _plan_lock

    try:
        with _plan_lock:
            stdlib.type_index(project_root)
    except Exception as exc:
        Log.Warn(f"could not pre-build the type index: {exc}")


def warm_template_dags(p: "Project") -> None:  # noqa: F821
    from ..logging import Log
    from ..models.dag_renderer import THEMES
    from .api import _all_templates, _render_template_dag, _template_dag_path, _template_version

    try:
        for name, (tmpl, source) in _all_templates(p).items():
            version = _template_version(p, name, source)
            for theme in THEMES:
                svg = _template_dag_path(p, name, version, theme)
                if svg.is_file():
                    continue
                try:
                    _render_template_dag(p, tmpl, name, source, theme)
                except Exception as exc:
                    Log.Warn(f"could not pre-draw template [{name}] ({theme}): {exc}")
    except Exception as exc:
        Log.Warn(f"could not warm template DAGs: {exc}")


def _warm(fn, *args) -> None:
    # A first run has nothing cached, so these three solve and draw every
    # shipped template -- over a hundred planner lines, printed after the
    # "serving at" banner and ending mid-solve. The terminal then reads as a
    # GUI hung on a solve when the server has been up the whole time.
    from ..logging import Log

    with Log.Quiet():
        fn(*args)


def bind_project(
    app: "Flask",  # noqa: F821
    project_root: Path | str = ".",
    ssh_config_path: Path | str | None = None,
    watch: bool = True,
) -> "Flask":  # noqa: F821
    project = Project(project_root)
    project.initialize()
    install_log_capture()
    for fn, arg in (
        (warm_type_index, project.root),
        (warm_template_dags, project),
        (resync_workflow_types, project),
    ):
        threading.Thread(target=_warm, args=(fn, arg), daemon=True).start()

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
    from flask import Flask, Response, jsonify, request, send_from_directory

    app = Flask(__name__, static_folder=None)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
    bind_project(app, project_root, ssh_config_path=ssh_config_path, watch=watch)

    app.register_blueprint(api_bp)

    GZIP_MIMETYPES = {"application/json", "text/plain", "image/svg+xml"}
    GZIP_MIN_BYTES = 512

    @app.after_request
    def _gzip_response(response):
        import gzip as gzip_mod

        if response.direct_passthrough or response.is_streamed:
            return response
        if "gzip" not in (request.headers.get("Accept-Encoding", "") or ""):
            return response
        if response.headers.get("Content-Encoding"):
            return response
        mimetype = (response.mimetype or "").lower()
        if mimetype not in GZIP_MIMETYPES:
            return response
        data = response.get_data()
        if len(data) < GZIP_MIN_BYTES:
            return response
        response.set_data(gzip_mod.compress(data))
        response.headers["Content-Encoding"] = "gzip"
        response.headers["Content-Length"] = str(len(response.get_data()))
        vary = response.headers.get("Vary")
        response.headers["Vary"] = f"{vary}, Accept-Encoding" if vary else "Accept-Encoding"
        return response

    @app.errorhandler(413)
    def _too_large(_exc):
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
        target = root / asset
        if target.is_file():
            return send_from_directory(root, asset)
        return index()

    return app


def _is_loopback(host: str) -> bool:
    # The bind address decides whether this GUI is reachable from off the
    # machine, and that is the only thing the exposure warning is about.
    import ipaddress

    h = (host or "").strip().strip("[]")
    if h in ("localhost", "localhost.localdomain"): return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        # An empty host means every interface; a name is resolved by the OS at
        # bind time, and anything we cannot read as loopback is treated as
        # exposed rather than quietly assumed safe.
        return False


def serve(
    project_root: Path | str = ".",
    host: str = "127.0.0.1",
    port: int = 8090,
    open_browser: bool = True,
    ssh_config_path: Path | str | None = None,
) -> int:
    import logging

    from werkzeug.serving import make_server

    from ..constants import VERSION
    from ..logging import Log

    app = create_app(project_root, ssh_config_path=ssh_config_path)
    # Below app.run(), which prints Flask's banner and werkzeug's production
    # warning and offers no way to turn either off. Per-request access logs
    # (one line per poll) are a separate, silenceable logger -- the frontend
    # polls every few seconds, and at WARNING those lines stop while a real
    # server error (5xx, broken pipe) still surfaces.
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    server = make_server(host, port, app, threaded=True)
    url = f"http://{host}:{server.server_port}"
    Log.Info(f"Metasmith {VERSION}")
    Log.Info(f"project [{Path(project_root).resolve()}]")
    if not bundle_exists():
        Log.Error("the GUI bundle is missing; run ./dev.sh --build-gui")
    Log.Info(f"serving at [{url}]")
    if not _is_loopback(host):
        Log.Warn(
            f"bound to [{host}] -- this GUI is reachable from other machines, and it has no"
            " authentication. Anyone who can reach the port can read files on this host and"
            " stage and run work on every agent it knows. Bind 127.0.0.1 and use an ssh tunnel."
        )
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
