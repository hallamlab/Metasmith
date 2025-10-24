from pathlib import Path
import argparse
import inspect
import os, sys
import time
from pathlib import Path
import argparse
import signal
import socket

# may not be strictly used, but needed to tell pyinstaller to pack
import gunicorn.app.base
import gunicorn.glogging
import gunicorn.workers.ggevent
from engineio.async_drivers import gevent # the worker class

from .logging import Log
from .server import app as APP

CLI_ENTRY = "msm_relay"
WS = Path(sys.orig_argv[0]).parent.absolute()
    
class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))

def _add_io_arg(parser: ArgumentParser):
    host = socket.gethostname()
    parser.add_argument("--io", default=WS/host, required=False, metavar="PATH", type=Path)
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
        # parser.add_argument("--channels", "-n", required=False, metavar="INT", type=int, default=8)
        # parser.add_argument("--gunicorn-config", "-c", required=False, metavar="PATH", type=str, default=WS/"gunicorn.conf.py")
        args = parser.parse_args(raw_args)
        workspace = Path(args.io)
        workspace.mkdir(parents=True, exist_ok=True)

        class StandaloneApplication(gunicorn.app.base.BaseApplication):
            def __init__(self, app, options=None):
                self.options = options or {}
                self.application = app
                super().__init__()

            def load_config(self):
                config = {key: value for key, value in self.options.items()
                        if key in self.cfg.settings and value is not None}
                for key, value in config.items():
                    self.cfg.set(key.lower(), value)

            def load(self):
                return self.application
        
        options = {
            # 'bind': f'unix:{workspace}/main.sock:12001',
            'bind': f'localhost:12001',
            'errorlog': f'{workspace}/main.err',
            'accesslog': f'{workspace}/main.log',
            'preload': True,
            'umask': 0o007,
            'workers': 1,
            'worker_class': 'gevent',
            'worker_connections': 1000,
            'timeout': 15, # worker timout, so need constant keepalive ping from client...
        }
        StandaloneApplication(APP, options).run()

    def stop(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "stop relay")
        args = parser.parse_args(raw_args)
        from .main import StopServer
        StopServer(args.io)
        server_channel = Path(args.io)/"main.in"
        try:
            while server_channel.exists():
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass
        if server_channel.exists():
            Log.Error(f"failed")
        else:
            Log.Info(f"success")

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

    def logs(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "print logs")
        args = parser.parse_args(raw_args)
        logs_path = Path(args.io)/"main.log"
        if not logs_path.exists():
            Log.Error(f"no logs at [{logs_path}]")
            return
        with open(logs_path) as f:
            for l in f:
                print(l, end="")

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
