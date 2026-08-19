from __future__ import annotations

import ast
from dataclasses import dataclass, field


CONTAINER_ARM = "ifContainerDo"
VIRTUAL_ENV_ARM = "ifVirtualEnvDo"
ARMS = (CONTAINER_ARM, VIRTUAL_ENV_ARM)
ENTRY = "ExecWithEnv"

_FORBIDDEN_CALLS = ("ExecWithContainer", "_ExecInEnv")


@dataclass
class EnvChain:
    lineno: int
    arms: list[str] = field(default_factory=list)
    envs: list[str|None] = field(default_factory=list)
    cmds: list[str|None] = field(default_factory=list)

    @property
    def duplicate_command(self) -> bool:
        real = [c for c in self.cmds if c is not None]
        return len(self.arms) > 1 and len(real) == len(self.arms) and len(set(real)) == 1


@dataclass
class EnvScan:
    chains: list[EnvChain] = field(default_factory=list)
    forbidden: list[tuple[str, int]] = field(default_factory=list)
    host_shell_calls: list[int] = field(default_factory=list)

    @property
    def arms(self) -> list[str]:
        return sorted({a for c in self.chains for a in c.arms})

    @property
    def declares_env(self) -> bool:
        return len(self.chains) > 0

    def empty_chains(self) -> list[EnvChain]:
        return [c for c in self.chains if not c.arms]


def _kw(call: ast.Call, name: str) -> ast.expr | None:
    for k in call.keywords:
        if k.arg == name:
            return k.value
    return None


def _positional(call: ast.Call, index: int) -> ast.expr | None:
    return call.args[index] if len(call.args) > index else None


def ScanSource(source: str, filename: str = "<transform>") -> EnvScan:
    tree = ast.parse(source, filename=filename)
    scan = EnvScan()

    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]

    def _attr(n) -> str | None:
        return n.func.attr if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) else None

    for n in calls:
        a = _attr(n)
        if a in _FORBIDDEN_CALLS:
            scan.forbidden.append((a, n.lineno))
        elif a == "Exec" and isinstance(n.func.value, ast.Attribute) \
                and n.func.value.attr == "external_shell":
            scan.host_shell_calls.append(n.lineno)

    receivers = {id(n.func.value) for n in calls if _attr(n) in ARMS}
    entries_seen: set[int] = set()

    for tail in calls:
        if _attr(tail) not in ARMS or id(tail) in receivers:
            continue
        arms: list[str] = []
        envs: list[str | None] = []
        cmds: list[str | None] = []
        cur: ast.expr = tail
        while _attr(cur) in ARMS:
            assert isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute)
            arms.append(cur.func.attr)
            e = _kw(cur, "env") or _positional(cur, 0)
            envs.append(e.id if isinstance(e, ast.Name) else None)
            c = _kw(cur, "cmd") or _positional(cur, 1)
            cmds.append(None if c is None else ast.unparse(c))
            cur = cur.func.value
        if _attr(cur) == ENTRY:
            entries_seen.add(id(cur))
        chain = EnvChain(
            lineno=tail.lineno,
            arms=list(reversed(arms)),
            envs=list(reversed(envs)),
            cmds=list(reversed(cmds)),
        )
        scan.chains.append(chain)

    for n in calls:
        if _attr(n) == ENTRY and id(n) not in entries_seen:
            scan.chains.append(EnvChain(lineno=n.lineno))

    scan.chains.sort(key=lambda c: c.lineno)
    return scan


def ScanFile(path) -> EnvScan:
    with open(path) as f:
        return ScanSource(f.read(), filename=str(path))
