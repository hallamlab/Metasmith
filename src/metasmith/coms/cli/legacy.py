from __future__ import annotations

import argparse
import ipaddress
import os
import shutil
import subprocess
from pathlib import Path

from ...constants import MODULE_PATH, VERSION
from ...logging import Log


def register(subs):
    _register_get(subs)
    _register_lab(subs)
    _register_gui(subs)
    _register_api(subs)


def _register_get(subs):
    p = subs.add_parser("get", help="copy a bundled example resource into cwd")
    p.add_argument("name", type=Path)
    p.set_defaults(func=_cmd_get)


def _cmd_get(args):
    Log.Info(f"Metasmith {VERSION}")
    path = MODULE_PATH / f"example_resources/{args.name}"
    if not path.exists():
        Log.Error(f"the resource doesn't exist [{path}]")
        return None
    if Path(path.name).exists():
        Log.Error(f"[./{path.name}] already exists")
        return None
    shutil.copy(path, path.name)
    Log.Info(f"copied [{path.name}] from [{path}]")
    return None


def _ip_type(val: str) -> str:
    try:
        ipaddress.ip_address(val)
        return val
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid IP address: {val}")


def _register_lab(subs):
    p = subs.add_parser("lab", help="run Jupyter Lab with starter notebooks")
    p.add_argument("--ip", type=_ip_type, default="0.0.0.0")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--tutorial", default=None, help="open the named tutorial notebook")
    p.set_defaults(func=_cmd_lab)


def _cmd_lab(args):
    Log.Info(f"Metasmith {VERSION}")
    from ...gui.stdlib import bootstrap_project

    settings_path = Path("jupyterlab_settings")
    os.environ["JUPYTERLAB_SETTINGS_DIR"] = str(settings_path)

    if not settings_path.exists():
        Log.Info("loading JupyterLab presets...")
        subprocess.run([
            "rsync", "-auP",
            f"{MODULE_PATH}/jupyter_lab/settings/",
            f"{settings_path}",
        ], text=True)
    bootstrap_project(Path("."))

    try:
        Log.Info("starting Jupyter lab...")
        cmds = [
            "jupyter", "lab",
            f"--ip={args.ip}",
            f"--port={args.port}",
            "--allow-root",
            "--no-browser",
            "--ContentsManager.allow_hidden=True",
        ]
        if args.tutorial:
            tutorial = args.tutorial
            if not tutorial.endswith(".ipynb"):
                tutorial += ".ipynb"
            path = MODULE_PATH / f"example_resources/tutorials/{tutorial}"
            if path.exists():
                cmds += [f"--LabApp.default_url='/lab/tree/example_resources/tutorials/{tutorial}'"]
        subprocess.run(cmds, text=True)
    except KeyboardInterrupt:
        pass
    return None


def _register_gui(subs):
    p = subs.add_parser("gui", help="run the web GUI over the current directory")
    p.add_argument("--host", default="127.0.0.1",
                   help="interface to bind (default: loopback only)")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--project", type=Path, default=Path("."),
                   help="project directory (default: cwd)")
    p.add_argument("--no-browser", action="store_true")
    p.add_argument("--no-bootstrap", action="store_true",
                   help="skip copying examples and cloning the standard library")
    p.add_argument("--ssh-config", type=Path, default=None,
                   help="ssh config to manage (default: ~/.ssh/config)")
    p.set_defaults(func=_cmd_gui)


def _cmd_gui(args):
    Log.Info(f"Metasmith {VERSION}")
    try:
        import flask  # noqa: F401
        import coolname  # noqa: F401
    except ImportError as exc:
        Log.Error(
            f"the GUI needs flask and coolname ({exc.name} is missing). "
            f"They ship with the conda package; in a bare environment: "
            f"conda install -c conda-forge flask coolname"
        )
        return None

    from ...gui.app import serve
    from ...gui.stdlib import bootstrap_project

    if not args.no_bootstrap:
        bootstrap_project(args.project)
    serve(
        project_root=args.project,
        host=args.host,
        port=args.port,
        open_browser=not args.no_browser,
        ssh_config_path=args.ssh_config,
    )
    return None


def _register_api(subs):
    p = subs.add_parser(
        "api",
        help="internal agent-to-agent RPC endpoint (not for manual use)",
    )
    p.add_argument("endpoint")
    p.add_argument("--arg", "-a", action="append", default=[], nargs="*", metavar="KEY=VALUE")
    p.set_defaults(func=_cmd_api)


def _cmd_api(args):
    from ..api import HandleRequest
    body: dict = {}
    for alst in args.arg:
        for a in alst:
            if "=" not in a:
                Log.Error(f"invalid argument [{a}]")
                continue
            k, v = a.split("=", 1)
            body[k] = v
    HandleRequest(args.endpoint, body)
    return None
