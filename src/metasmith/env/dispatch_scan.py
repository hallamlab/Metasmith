from __future__ import annotations

import ast
from dataclasses import dataclass, field


CONTAINER_ARM = "ifContainerDo"
VIRTUAL_ENV_ARM = "ifVirtualEnvDo"
ENTRY = "ExecWithEnv"

# The two arms are here as names to refuse, not as an API. A body that still
# splits its command per runtime is an unmigrated body, and it fails by name
# rather than by running the container half everywhere.
_FORBIDDEN_CALLS = ("ExecWithContainer", "_ExecInEnv", CONTAINER_ARM, VIRTUAL_ENV_ARM)


@dataclass
class EnvRun:
    lineno: int
    env: str|None = None
    cmd: str|None = None
    has_env: bool = False
    has_cmd: bool = False


@dataclass
class EnvScan:
    runs: list[EnvRun] = field(default_factory=list)
    forbidden: list[tuple[str, int]] = field(default_factory=list)
    host_shell_calls: list[int] = field(default_factory=list)

    @property
    def declares_env(self) -> bool:
        return len(self.runs) > 0

    def incomplete(self) -> list[EnvRun]:
        return [r for r in self.runs if not (r.has_env and r.has_cmd)]


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
        elif a == ENTRY:
            e = _kw(n, "env") or _positional(n, 0)
            c = _kw(n, "cmd") or _positional(n, 1)
            scan.runs.append(EnvRun(
                lineno=n.lineno,
                # Only a plain name resolves back to the module-level Dependency
                # the library stages; anything else is recorded as unknown.
                env=e.id if isinstance(e, ast.Name) else None,
                cmd=None if c is None else ast.unparse(c),
                has_env=e is not None,
                has_cmd=c is not None,
            ))

    scan.runs.sort(key=lambda r: r.lineno)
    return scan


def ScanFile(path) -> EnvScan:
    with open(path) as f:
        return ScanSource(f.read(), filename=str(path))
