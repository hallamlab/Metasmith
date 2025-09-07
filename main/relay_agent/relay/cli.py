from pathlib import Path
import argparse
import inspect
import subprocess
import os, sys
from pathlib import Path
import argparse

CLI_ENTRY = "relay"
    
class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))

def _add_io_arg(parser: ArgumentParser):
    # here = Path(sys.orig_argv[0]).parent
    parser.add_argument("--io", default="./connections", required=False, metavar="PATH", type=Path)
    return parser

def _make_parser(name: str, description: str):
    parser = ArgumentParser(
        prog = f'{CLI_ENTRY} {name}',
        description=description,
    )
    parser = _add_io_arg(parser)
    return parser

class CommandLineInterface:
    def _get_fn_name(self):
        return inspect.stack()[1][3]
    
    def start(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "ensure relay is running")
        parser.add_argument("--connected", "-c", action="store_true", required=False, default=False)
        args = parser.parse_args(raw_args)
        if args.connected:
            args = parser.parse_args(raw_args)
            from .main import RunServer
            RunServer(args.io)
        else:
            cmd = f"nohup {sys.executable} {' '.join(sys.argv)} --connected >/dev/null 2>&1 &"
            os.system(cmd)

    def stop(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "stop relay")
        args = parser.parse_args(raw_args)
        from .main import StopServer
        StopServer(args.io)

    def status(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "get status of connections")
        args = parser.parse_args(raw_args)
        from .main import GetStatus
        GetStatus(args.io)

    def bounce(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "bounce command through relay")
        parser.add_argument("cmd", metavar="STR")
        args = parser.parse_args(raw_args)
        from .main import Bounce
        Bounce(args.io, args.cmd)

    def test(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "run self test")
        args = parser.parse_args(raw_args)
        from .self_test import run as SelfTest
        SelfTest(args.io)

    def help(self, args=None):
        help = [
            f"terminal relay via named pipes",
            f"usage: <executable> COMMAND [OPTIONS]",
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
