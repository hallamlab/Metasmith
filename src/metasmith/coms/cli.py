import ipaddress
import subprocess
import os, sys
from pathlib import Path
import argparse
import inspect
import base64
import json
import shutil

from ..constants import MODULE_PATH, NAME, VERSION, GIT_URL, ENTRY_POINTS
from ..logging import Log
from ..models.build_libraries import Build

CLI_ENTRY = [e.split("=")[0].strip() for e in ENTRY_POINTS][0]

class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))


class CommandLineInterface:
    def _get_fn_name(self):
        return inspect.stack()[1][3]

    # def deploy(self, raw_args=None):
    #     parser = ArgumentParser(
    #         prog = f'{CLI_ENTRY} {self._get_fn_name()}',
    #         description=f"Deploy an executor agent to a remote machine via ssh"
    #     )

    #     parser.add_argument("--config", required=True, metavar="yaml", help="path to configuration file")

    #     args = parser.parse_args(raw_args)
    #     from ..agents.presets import Deploy
    #     Deploy(args.config)

    # def run(self, raw_args=None):
    #     parser = ArgumentParser(
    #         prog = f'{CLI_ENTRY} {self._get_fn_name()}',
    #         description=f"Register data"
    #     )

    #     parser.add_argument("--request", required=True, metavar="yaml", help="yaml file describing requested data to produce")
    #     parser.add_argument("--work", required=True, metavar="path", help="path to deployed metasmith workspace")
    #     args = parser.parse_args(raw_args)
    #     from ..workflow import Run
    #     home = Path(args.work)
    #     request = Path(args.request)
    #     Run(home, request)

    def api(self, raw_args=None):
        parser = ArgumentParser(
            prog = f'{CLI_ENTRY} {self._get_fn_name()}',
            description=f"Not intended for manual use. This is for communications between agents"
        )

        from .api import HandleRequest
        parser.add_argument("endpoint")
        parser.add_argument("--arg", "-a", required=False, default=[], action='append', nargs='*', metavar="KEY=VALUE")
        args = parser.parse_args(raw_args)
        body = {}
        for alst in args.arg:
            for a in alst:
                if "=" not in a:
                    Log.Error(f"invalid argument [{a}]")
                    continue
                k, v = a.split("=")
                body[k] = v
        HandleRequest(args.endpoint, body)

    def lab(self, raw_args=None):
        Log.Info(f"Metasmith {VERSION}")
        parser = ArgumentParser(
            prog = f'{CLI_ENTRY} {self._get_fn_name()}',
            description=f"run Jupyter Lab with starter notebook"
        )
        def ip_type(val: str) -> str:
            try:
                ipaddress.ip_address(val)
                return val
            except ValueError:
                raise argparse.ArgumentTypeError(f"Invalid IP address: {val}")
        parser.add_argument("--ip", required=False, type=ip_type, default="0.0.0.0", help="IP address to serve the notebook")
        parser.add_argument("--port", required=False, type=int, default=8080, help="Port to serve the notebook")
        parser.add_argument("--tutorial", required=False, type=str, default=None, help="Open the selected tutorial notebook")
        args = parser.parse_args(raw_args)
        
        settings_path = Path("jupyterlab_settings")
        os.environ["JUPYTERLAB_SETTINGS_DIR"] = str(settings_path)
        examples_path = Path("example_resources")
        lib_path = Path("MetasmithLibraries")
        is_first_time = not any(p.exists() for p in [examples_path, settings_path, lib_path])

        if not settings_path.exists():
            Log.Info(f"loading JupyterLab presets...")
            subprocess.run([
                "rsync",
                "-auP",
                f"{MODULE_PATH}/jupyter_lab/settings/",
                f"{settings_path}",
            ], text=True)
        if not examples_path.exists():
            Log.Info(f"loading tutorials...")
            subprocess.run([
                "rsync",
                "-auP",
                f"{MODULE_PATH}/example_resources/",
                f"./{examples_path}",
            ], text=True)
        if not lib_path.exists():
            libraries_url = "https://github.com/hallamlab/MetasmithLibraries.git"
            Log.Info(f"downloading standard library from [{libraries_url}]...")
            subprocess.run([
                "git",
                "clone",
                libraries_url,
            ], text=True)

        try:
            Log.Info(f"starting Jupyter lab...")
            cmds = [
                "jupyter",
                "lab",
                f"--ip={args.ip}",
                f"--port={args.port}",
                "--allow-root",
                "--no-browser",
                "--ContentsManager.allow_hidden=True",
            ]
            tutorial_notebook = str(args.tutorial)
            if tutorial_notebook:
                if not tutorial_notebook.endswith(".ipynb"): tutorial_notebook += ".ipynb"
                path =  MODULE_PATH/f'example_resources/tutorials/{tutorial_notebook}'
                if path.exists():
                    cmds += [
                        f"--LabApp.default_url='/lab/tree/example_resources/tutorials/{tutorial_notebook}'",
                    ]
            subprocess.run(cmds, text=True)
        except KeyboardInterrupt:
            pass

    def build(self, raw_args=None):
        parser = ArgumentParser(
            prog = f'{CLI_ENTRY} {self._get_fn_name()}',
            description=f"build libraries"
        )
        parser.add_argument("-t", "--types", nargs='*', required=False, default=[],
            help="data types")
        parser.add_argument("-r", "--transforms", nargs='*', required=False, default=[],
            help="transforms")
        parser.add_argument("-u", "--uniques", nargs='*', required=False, default=[],
            help="folder where: folder=namespace, file name=type name")
        # parser.add_argument("-d", "--data", nargs='*', required=False, default=[],
        #     help="data")
        args = parser.parse_args(raw_args)
        def parse(arg):
            return [Path(x) for x in arg]
        Build(
            data_type_dirs=parse(args.types),
            transform_dirs=parse(args.transforms),
            unique_dirs=parse(args.uniques),
            # data_dirs=flatten(args.data),
        )

    def help(self, args=None):
        help = [
            f"{NAME} v{VERSION}",
            f"{GIT_URL}",
            f"",
            f"usage: {CLI_ENTRY} COMMAND [OPTIONS]",
            f"",
            f"Where COMMAND is one of:",
        ]+[f"  {k}" for k in COMMANDS]+[
            f"",
            f"for additional help, use:",
            f"{CLI_ENTRY} COMMAND -h/--help",
        ]
        help = "\n".join(help)
        print(help)
COMMANDS = {k:v for k, v in CommandLineInterface.__dict__.items() if k[0]!="_"}

def main():
    cli = CommandLineInterface()
    if len(sys.argv) <= 1:
        cli.help()
        return

    COMMANDS.get(# calls command function with args
        sys.argv[1],
        CommandLineInterface.help # default
    )(cli, sys.argv[2:]) # cli is instance of "self"
