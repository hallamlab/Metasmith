from pathlib import Path
import argparse
import inspect
import os, sys
import time
from pathlib import Path
import argparse
import signal
import socket
import getpass
import shutil

from .logging import Log
from .watcher import RunWatcher, StopWatcher, CheckStatus, Wipe
from .coms.ipc import CurrentTimeMillis, ResetGenerator
from .coms.via_file_watcher import RemoteShell

CLI_ENTRY = "msm_relay"
WS = Path(sys.orig_argv[0]).parent.absolute()
host = socket.gethostname()
default_ws = WS/host

class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(2, '\n%s: error: %s\n' % (self.prog, message))

def _add_io_arg(parser: ArgumentParser):
    parser.add_argument("--io", default=default_ws, required=False, metavar="PATH", type=Path)
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
        parser.add_argument("--local", "-l", action="store_true", required=False, default=False)
        # parser.add_argument("--channels", "-n", required=False, metavar="INT", type=int, default=8)
        # parser.add_argument("--gunicorn-config", "-c", required=False, metavar="PATH", type=str, default=WS/"gunicorn.conf.py")
        args = parser.parse_args(raw_args)
        workspace = Path(args.io)
        # os.system("uvicorn ")
        status = CheckStatus(workspace)
        if status.alive:
            Log.Warn(f"relay server already running at [{workspace}]")
            return
        else:
            if workspace == default_ws and not args.local:
                host = socket.gethostname()
                username = getpass.getuser()
                local = workspace
                tmp = os.environ.get("TMPDIR", "/tmp")
                workspace = Path(tmp)/f"msm_{host}_{username}"
                if local.is_symlink():
                    local.unlink()
                if local.exists():
                    shutil.rmtree(local)
                local.symlink_to(workspace)

            workspace.mkdir(exist_ok=True)
            if args.connected:
                RunWatcher(workspace=workspace)
            else:
                signal.signal(signal.SIGCHLD, signal.SIG_IGN) # no zombie children
                pid = os.fork()
                ResetGenerator()
                if pid != 0: # parent
                    try:
                        while True:
                            _status = CheckStatus(workspace)
                            if _status.alive:
                                Log.Info(f"pid [{_status.pid}]")
                                Log.Info(f"success")
                                return
                            time.sleep(0.1)
                    except KeyboardInterrupt:
                        pass
                    # os._exit(0) # this should keep resources for forked child?
                else: # child
                    RunWatcher(workspace=workspace)

    def stop(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "stop relay")
        args = parser.parse_args(raw_args)
        workspace = Path(args.io)
        status = CheckStatus(workspace)
        if not status.alive:
            Log.Info(f"server not running at [{workspace}]")
            return
        StopWatcher(workspace)
        start = CurrentTimeMillis()
        timeout = 5
        while True:
            _status = CheckStatus(workspace)
            if not _status.alive:
                Log.Info("shutdown success")
                return
            now = CurrentTimeMillis()
            if now-start>=timeout*1000: break
        Wipe(workspace)
        Log.Info("shutdown enforced")

    def status(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "get status of connections")
        args = parser.parse_args(raw_args)
        status = CheckStatus(args.io)
        Log.Info("status:")
        for k, v in status.__dict__.items():
            if k.startswith("_"): continue
            if callable(v): continue
            Log.Info(f"  {k}: {v}")

    def bounce(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "bounce command through relay")
        parser.add_argument("cmd", metavar="STR")
        args = parser.parse_args(raw_args)
        with RemoteShell(args.io) as shell:
            shell.RegisterOnOut(print)
            shell.RegisterOnErr(lambda x: print(x, file=sys.stderr))
            shell.Exec(args.cmd, timeout=None)

    def logs(self, raw_args=None):
        parser = _make_parser(self._get_fn_name(), "print logs")
        args = parser.parse_args(raw_args)

        def _logs(logs_path):
            if not logs_path.exists():
                Log.Error(f"no logs at [{logs_path}]")
                return
            with open(logs_path) as f:
                for l in f:
                    print(l, end="")
        _logs(Path(args.io)/"main.log")

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
