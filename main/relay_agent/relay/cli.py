import os, sys
from pathlib import Path
import argparse

from .main import RunServer
from .self_test import run

class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))

def main():
    parser = ArgumentParser(
        prog = f'relay',
        description=f"start relay agent to access its terminal environment via named pipes",
    )

    parser.add_argument("--io", required=True, metavar="PATH", type=Path)
    parser.add_argument("--test", action="store_true", help="run self test")
    args = parser.parse_args()

    workspace = Path(args.io)
    if args.test:
        run(workspace/"main.in")
    else:
        RunServer(workspace)
