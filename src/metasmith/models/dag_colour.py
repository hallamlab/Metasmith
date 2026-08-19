from __future__ import annotations

from dataclasses import dataclass, field

from .dag_layout import Layout, dominators, natural_key, repeat_motifs

__all__ = ["Colouring", "SCHEMES", "PALETTE", "UNMATCHED", "colour_layout"]

PALETTE: tuple[str, ...] = (
    "#636EFA",
    "#EF553B",
    "#00CC96",
    "#AB63FA",
    "#FFA15A",
    "#19D3F3",
    "#FF6692",
    "#B6E880",
)
UNMATCHED = "#8A8A8A"

SCHEMES = ("none", "lane", "repeat", "module", "namespace")


@dataclass(frozen=True)
class Colouring:
    nodes: dict[str, str] = field(default_factory=dict)
    edges: dict[tuple[str, str], str] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.nodes or self.edges)

    def node(self, name: str, fallback: str) -> str:
        return self.nodes.get(name, fallback)

    def edge(self, src: str, dst: str, fallback: str) -> str:
        return self.edges.get((src, dst), fallback)


def colour_layout(lay: Layout, scheme: str = "none") -> Colouring:
    if scheme in (None, "", "none"):
        return Colouring()
    try:
        build = _SCHEMES[scheme]
    except KeyError:
        raise ValueError(
            f"unknown colour scheme {scheme!r}; expected one of {', '.join(SCHEMES)}"
        ) from None
    nodes = build(lay)
    edges = {
        (e.src, e.dst): nodes[e.src]
        for e in lay.edges
        if not e.back and e.src in nodes
    }
    return Colouring(nodes=nodes, edges=edges)


def _by_lane(lay: Layout) -> dict[str, str]:
    return {n.name: PALETTE[n.lane % len(PALETTE)] for n in lay.nodes}


def _by_repeat(lay: Layout) -> dict[str, str]:
    out = {n.name: UNMATCHED for n in lay.nodes}
    for i, m in enumerate(repeat_motifs(lay)):
        hue = PALETTE[i % len(PALETTE)]
        for x in m.nodes:
            out[x] = hue
    return out


def _module_owner(lay: Layout) -> dict[str, str | None]:
    names = [n.name for n in lay.nodes]
    parents: dict[str, list[str]] = {n: [] for n in names}
    for e in lay.edges:
        if not e.back:
            parents[e.dst].append(e.src)
    idom = dominators(names, parents)

    size: dict[str, int] = dict.fromkeys(names, 1)
    for n in reversed(names):
        d = idom.get(n)
        if d is not None:
            size[d] += size[n]
    heads = {n for n in names if size[n] >= 3}

    owner: dict[str, str | None] = {}
    for n in names:
        cur = n
        while cur is not None and cur not in heads:
            cur = idom.get(cur)
        owner[n] = cur
    return owner


def _by_module(lay: Layout) -> dict[str, str]:
    names = [n.name for n in lay.nodes]
    owner = _module_owner(lay)
    depth = {n: i for i, n in enumerate(names)}
    order = sorted(
        {h for h in owner.values() if h is not None},
        key=lambda h: (depth[h], natural_key(h)),
    )
    adjacent: dict[str, set[str]] = {h: set() for h in order}
    for e in lay.edges:
        a, b = owner[e.src], owner[e.dst]
        if a is not None and b is not None and a != b:
            adjacent[a].add(b)
            adjacent[b].add(a)

    slot: dict[str, int] = {}
    for h in order:
        taken = {slot[x] for x in adjacent[h] if x in slot}
        slot[h] = next(i for i in range(len(PALETTE) + 1) if i not in taken)
    return {
        n: (UNMATCHED if owner[n] is None else PALETTE[slot[owner[n]] % len(PALETTE)])
        for n in names
    }


def _by_namespace(lay: Layout) -> dict[str, str]:
    spaces = sorted(
        {n.name.split("::", 1)[0] for n in lay.nodes if "::" in n.name}
    )
    slot = {ns: i for i, ns in enumerate(spaces)}
    return {
        n.name: (
            PALETTE[slot[n.name.split("::", 1)[0]] % len(PALETTE)]
            if "::" in n.name
            else UNMATCHED
        )
        for n in lay.nodes
    }


_SCHEMES = {
    "lane": _by_lane,
    "repeat": _by_repeat,
    "module": _by_module,
    "namespace": _by_namespace,
}
