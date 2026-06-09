"""Graphviz DAG rendering — abstract node/edge API shared by every metasmith
caller that needs to draw a transform/data graph (currently `WorkflowPlan` and
solver `Solution`).

Consumers declare TRANSFORM and DATA nodes plus directed edges; the class
handles graphviz logger silencing, styling, and the `Source.render` call.
"""
from __future__ import annotations

from enum import Enum, auto
from pathlib import Path


class NodeKind(Enum):
    TRANSFORM = auto()
    DATA      = auto()


class DagRenderer:
    """Build a directed graph of transform/data nodes and render via graphviz."""

    def __init__(self, *, font: str = "Arial", rankdir: str = "TB"):
        self._font    = font
        self._rankdir = rankdir
        self._nodes: dict[str, NodeKind] = {}
        self._edges: list[tuple[str, str]] = []
        self._seen_edges: set[tuple[str, str]] = set()

    def add_node(self, kind: NodeKind, name: str) -> None:
        self._nodes.setdefault(name, kind)

    def add_edge(self, src: str, dst: str) -> None:
        key = (src, dst)
        if key in self._seen_edges:
            return
        self._seen_edges.add(key)
        self._edges.append(key)
        self._nodes.setdefault(src, NodeKind.DATA)
        self._nodes.setdefault(dst, NodeKind.DATA)

    def to_dot(self) -> str:
        lines = ["digraph G {"]
        lines += [
            f'graph [fontname="{self._font}", rankdir="{self._rankdir}"];',
            f'node  [fontname="{self._font}"];',
            f'edge  [fontname="{self._font}"];',
        ]
        for name, kind in self._nodes.items():
            lines.append(self._render_node(kind, name))
        for src, dst in self._edges:
            lines.append(f'    "{src}" -> "{dst}";')
        lines.append("}")
        return "\n".join(lines)

    def render(self, path_base: Path | str, format: str = "svg") -> Path:
        path_base = Path(path_base)
        ext = path_base.suffix
        if ext:
            format    = ext.lstrip(".")
            path_base = path_base.with_suffix("")
        graphviz = _import_graphviz_quietly()
        src = graphviz.Source(self.to_dot(), filename=str(path_base), format=format)
        src.render(cleanup=True, quiet=True)
        return path_base.parent / f"{path_base.name}.{format}"

    @staticmethod
    def _render_node(kind: NodeKind, name: str) -> str:
        if kind is NodeKind.TRANSFORM:
            return f'"{name}" [shape="oval", style="filled", fillcolor="#CCCCCC"]'
        return f'"{name}" [shape="box"]'


def _import_graphviz_quietly():
    """Import graphviz with its loggers muted.

    graphviz's module-scope logger emits noisy lines at import time; both
    historical consumers monkey-patched logging.getLogger around the import to
    suppress them, then walked graphviz.__dict__ two levels deep flipping every
    logger to ERROR. That dance now lives here, once.
    """
    import logging
    _real_getLogger = logging.getLogger

    class _Mute:
        def debug(self, *a, **kw): pass
        def info (self, *a, **kw): pass
        def warn (self, *a, **kw): pass
        def error(self, *a, **kw): pass

    logging.getLogger = lambda *a, **kw: _Mute()
    try:
        import graphviz
    finally:
        logging.getLogger = _real_getLogger

    todo = [(graphviz, 0)]
    while todo:
        m, depth = todo.pop()
        if hasattr(m, "log") and hasattr(m.log, "setLevel"):
            m.log.setLevel(logging.ERROR)
        if depth >= 2:
            continue
        if hasattr(m, "__dict__"):
            todo += [(x, depth + 1) for x in m.__dict__.values()]
    return graphviz
