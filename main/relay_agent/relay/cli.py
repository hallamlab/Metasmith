import os, sys
from pathlib import Path
import argparse

from .main import RunServer, GetStatus, StopServer
from .self_test import run as SelfTest

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
    parser.add_argument("command")
    args = parser.parse_args()

    workspace = Path(args.io)
    { # switch
        "start": RunServer,
        "status": GetStatus,
        "test": SelfTest,
        "stop": StopServer,
    }.get(args.command, lambda _: print(f"invalid command [{args.command}]"))(workspace)
