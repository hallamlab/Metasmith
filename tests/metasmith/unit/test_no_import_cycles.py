from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src"
PKG = SRC / "metasmith"

KNOWN_CYCLES = {
    ("metasmith.coms.cli", "metasmith.coms.cli._main"),
    ("metasmith.gui", "metasmith.gui.api", "metasmith.gui.app"),
}

SPLIT_PACKAGES = (
    "metasmith.models.libraries",
    "metasmith.models.workflow",
    "metasmith.agents",
)


def _modules() -> dict[str, tuple[Path, bool]]:
    found: dict[str, tuple[Path, bool]] = {}
    for path in sorted(PKG.rglob("*.py")):
        parts = list(path.relative_to(SRC).parts)
        if any(p.startswith((".", "__pycache__")) for p in parts):
            continue
        if "bin" in parts:
            continue
        if not all((SRC.joinpath(*parts[:i]) / "__init__.py").is_file()
                   for i in range(1, len(parts))):
            continue
        is_package = parts[-1] == "__init__.py"
        parts = parts[:-1] if is_package else parts[:-1] + [parts[-1][: -len(".py")]]
        found[".".join(parts)] = (path, is_package)
    return found


def _absolute(module: str, level: int, name: str, is_package: bool) -> str:
    if level == 0:
        return name
    base = module.split(".") if is_package else module.split(".")[:-1]
    if level > 1:
        base = base[: len(base) - (level - 1)]
    return ".".join(base + ([name] if name else []))


def _import_graph() -> dict[str, set[str]]:
    modules = _modules()
    graph: dict[str, set[str]] = {}
    for name, (path, is_package) in modules.items():
        targets: set[str] = set()
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if isinstance(node, ast.Import):
                targets |= {a.name for a in node.names if a.name.startswith("metasmith")}
            elif isinstance(node, ast.ImportFrom):
                target = _absolute(name, node.level, node.module or "", is_package)
                if not target.startswith("metasmith"):
                    continue
                targets.add(target)
                targets |= {f"{target}.{a.name}" for a in node.names}
        graph[name] = {t for t in targets if t in modules and t != name}
    return graph


def _cycles(graph: dict[str, set[str]]) -> set[tuple[str, ...]]:
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    counter = 0
    found: set[tuple[str, ...]] = set()

    def visit(root: str) -> None:
        nonlocal counter
        work = [(root, iter(sorted(graph.get(root, ()))))]
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, children = work[-1]
            for child in children:
                if child not in index:
                    index[child] = low[child] = counter
                    counter += 1
                    stack.append(child)
                    on_stack.add(child)
                    work.append((child, iter(sorted(graph.get(child, ())))))
                    break
                if child in on_stack:
                    low[node] = min(low[node], index[child])
            else:
                work.pop()
                if work:
                    low[work[-1][0]] = min(low[work[-1][0]], low[node])
                if low[node] == index[node]:
                    component = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        component.append(member)
                        if member == node:
                            break
                    if len(component) > 1:
                        found.add(tuple(sorted(component)))

    for node in sorted(graph):
        if node not in index:
            visit(node)
    return found


def test_graph_is_not_empty():
    graph = _import_graph()
    assert len(graph) > 50, f"only found {len(graph)} modules under {PKG}"
    assert sum(len(v) for v in graph.values()) > 100, "no intra-package edges found"


def test_no_unexpected_import_cycles():
    found = _cycles(_import_graph())
    new = found - KNOWN_CYCLES
    stale = KNOWN_CYCLES - found
    assert not new, (
        "new import cycle(s) between metasmith modules:\n  "
        + "\n  ".join(" <-> ".join(c) for c in sorted(new))
        + "\n\nA module-scope cycle works only while the import order cooperates, "
        "and the order differs between the test suite, the CLI, and bootstrap "
        "inside the container. Break it, or defer one side to function scope."
    )
    assert not stale, (
        "a known cycle is gone or changed shape -- good, but update KNOWN_CYCLES:\n  "
        + "\n  ".join(" <-> ".join(c) for c in sorted(stale))
    )


def test_split_packages_are_acyclic():
    found = _cycles(_import_graph())
    offenders = sorted(
        c for c in found
        if any(m.startswith(p + ".") or m == p for m in c for p in SPLIT_PACKAGES)
    )
    assert not offenders, (
        "the god-file split introduced a cycle:\n  "
        + "\n  ".join(" <-> ".join(c) for c in offenders)
    )
