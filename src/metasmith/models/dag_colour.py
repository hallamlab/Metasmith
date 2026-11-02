from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

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


def colour_layout(
    lay: Layout,
    scheme: str = "none",
    *,
    palette: Sequence[str] | None = None,
    overrides: Mapping[str, str] | None = None,
) -> Colouring:
    if scheme in (None, "", "none"):
        return Colouring()
    try:
        build = _SCHEMES[scheme]
    except KeyError:
        raise ValueError(
            f"unknown colour scheme {scheme!r}; expected one of {', '.join(SCHEMES)}"
        ) from None
    nodes = (
        build(lay, palette=palette or PALETTE, overrides=overrides or {})
        if scheme == "module"
        else build(lay)
    )
    edges = {(src, dst): nodes[src] for src, dst in lay.edges if src in nodes}
    return Colouring(nodes=nodes, edges=edges)


def _by_lane(lay: Layout) -> dict[str, str]:
    return {n: PALETTE[c % len(PALETTE)] for n, c in lay.col.items()}


def _by_repeat(lay: Layout) -> dict[str, str]:
    out = {n: UNMATCHED for n in lay.order}
    for i, m in enumerate(repeat_motifs(lay)):
        hue = PALETTE[i % len(PALETTE)]
        for x in m.nodes:
            out[x] = hue
    return out


def _module_owner(lay: Layout) -> dict[str, str | None]:
    names = list(lay.order)
    idom = dominators(names, lay.parents)

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


def _by_module(
    lay: Layout,
    *,
    palette: Sequence[str] = PALETTE,
    overrides: Mapping[str, str] | None = None,
) -> dict[str, str]:
    overrides = overrides or {}
    names = list(lay.order)
    owner = _module_owner(lay)
    depth = {n: i for i, n in enumerate(names)}
    order = sorted(
        {h for h in owner.values() if h is not None},
        key=lambda h: (depth[h], natural_key(h)),
    )
    adjacent: dict[str, set[str]] = {h: set() for h in order}
    for src, dst in lay.edges:
        a, b = owner[src], owner[dst]
        if a is not None and b is not None and a != b:
            adjacent[a].add(b)
            adjacent[b].add(a)

    slot: dict[str, int] = {}
    for h in order:
        taken = {slot[x] for x in adjacent[h] if x in slot}
        slot[h] = next(i for i in range(len(palette) + 1) if i not in taken)

    def hue(h: str) -> str:
        return overrides.get(h, palette[slot[h] % len(palette)])

    return {n: (UNMATCHED if owner[n] is None else hue(owner[n])) for n in names}


def _by_namespace(lay: Layout) -> dict[str, str]:
    spaces = sorted(
        {n.split("::", 1)[0] for n in lay.order if "::" in n}
    )
    slot = {ns: i for i, ns in enumerate(spaces)}
    return {
        n: (
            PALETTE[slot[n.split("::", 1)[0]] % len(PALETTE)]
            if "::" in n
            else UNMATCHED
        )
        for n in lay.order
    }


_SCHEMES = {
    "lane": _by_lane,
    "repeat": _by_repeat,
    "module": _by_module,
    "namespace": _by_namespace,
}
