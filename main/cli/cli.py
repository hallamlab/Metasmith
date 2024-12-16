import json
import os, sys
from pathlib import Path
import argparse
import inspect
from dataclasses import dataclass
import importlib

from .constants import NAME, VERSION, GIT_URL

ENTRY_POINTS = [
    "ms=metasmith.cli:main",
    "metasmith=metasmith.cli:main",
]

CLI_ENTRY = [e.split("=")[0].strip() for e in ENTRY_POINTS][0]
    
class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))


class CommandLineInterface:
    def _get_fn_name(self):
        return inspect.stack()[1][3]

    def api(self, raw_args=None):
        parser = ArgumentParser(
            prog = f'{CLI_ENTRY} {self._get_fn_name()}',
            description=f"Snakemake uses this to call the python script for each step"
        )

        parser.add_argument("--step", required=True)
        parser.add_argument("--args", nargs='*', required=False, default=[])
        args = parser.parse_args(raw_args)

        mo = importlib.import_module(name=f".steps.{args.step}", package=NAME)
        try:
            mo.Procedure(args.args)
        except KeyboardInterrupt:
            exit()

    def help(self, args=None):
        help = [
            f"{NAME} v{VERSION}",
            f"{GIT_URL}",
            f"",
            f"Syntax: {CLI_ENTRY} COMMAND [OPTIONS]",
            f"",
            f"Where COMMAND is one of:",
        ]+[f"- {k}" for k in COMMANDS]+[
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
