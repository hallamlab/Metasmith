import os, sys
from pathlib import Path
import argparse

from .main import RunServer, GetStatus, StopServer
from .self_test import run as SelfTest

class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))

SWITCH = {
    "start": RunServer,
    "status": GetStatus,
    "test": SelfTest,
    "stop": StopServer,
}

def main():
    parser = ArgumentParser(
        prog = f'relay',
        description=f"start relay agent to access its terminal environment via named pipes",
    )

    here = Path(sys.orig_argv[0]).parent
    parser.add_argument("--io", default=here/"connections", required=False, metavar="PATH", type=Path)
    parser.add_argument("command", choices=SWITCH.keys())
    args = parser.parse_args()

    workspace = Path(args.io)
    SWITCH.get(args.command, lambda _: print(f"invalid command [{args.command}]"))(workspace)
