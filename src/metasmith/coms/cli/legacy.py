"""Preserved subcommands from the original cli.py: get, lab, api.

These don't return structured data; they print logs / run subprocesses directly.
"""
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
    _register_api(subs)


# -- get --------------------------------------------------------------------

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


# -- lab --------------------------------------------------------------------

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
    settings_path = Path("jupyterlab_settings")
    os.environ["JUPYTERLAB_SETTINGS_DIR"] = str(settings_path)
    examples_path = Path("example_resources")
    lib_path = Path("MetasmithLibraries")

    if not settings_path.exists():
        Log.Info("loading JupyterLab presets...")
        subprocess.run([
            "rsync", "-auP",
            f"{MODULE_PATH}/jupyter_lab/settings/",
            f"{settings_path}",
        ], text=True)
    if not examples_path.exists():
        Log.Info("loading tutorials...")
        subprocess.run([
            "rsync", "-auP",
            f"{MODULE_PATH}/example_resources/",
            f"./{examples_path}",
        ], text=True)
    if not lib_path.exists():
        libraries_url = "https://github.com/hallamlab/MetasmithLibraries.git"
        Log.Info(f"downloading standard library from [{libraries_url}]...")
        subprocess.run(["git", "clone", libraries_url], text=True)

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


# -- api --------------------------------------------------------------------

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
