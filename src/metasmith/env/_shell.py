"""The shell contract a tool command runs on, independent of runtime.

Container runtimes launch tools across the container boundary, so they run
commands on a relay shell (`RemoteShell`); mamba/native have no boundary and
run locally (`LiveShell`). `ExecutionContext` holds a `Shell` and never names
either concrete type — the `Environment` decides which one to hand it.
"""

from typing import Callable, Protocol, runtime_checkable

from ..coms.terminals import ShellResult


@runtime_checkable
class Shell(Protocol):
    """Structural type shared by RemoteShell (relay) and LiveShell (local).

    `ExecWithContainer` and transform protocols depend only on this surface,
    so the relay shell and the local shell are interchangeable from their
    point of view.
    """

    def Exec(self, cmd: str, timeout=None, history: bool = False) -> ShellResult: ...
    def ExecAsync(self, cmd: str): ...
    def AwaitDone(self, *args, **kwargs): ...
    def RegisterOnOut(self, callback: Callable[[str], None]): ...
    def RegisterOnErr(self, callback: Callable[[str], None]): ...
    def Dispose(self, *args, **kwargs): ...
    def __enter__(self): ...
    def __exit__(self, *args): ...
