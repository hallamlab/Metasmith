"""Colour schemes for a drawn `dag_layout.Layout`.

Off by default. A scheme is a pure function from a finished layout to a
node-id -> hex map and an edge -> hex map; the backends in `dag_draw` take one
as an optional argument and fall back to the caller's `Style` where it says
nothing. Nothing here knows what a transform or a datum is, and nothing in the
layout knows colour exists.

Two opposite jobs go under the one word, and they want opposite palettes:

- **Tracing one thing** — following a single rail down the page. This wants
  *adjacent things in different colours*, which is graph colouring, and is what
  GitHub's commit graph does with its lanes. `lane` and `module` are these.
- **Seeing a repetition** — recognising that the plan does one thing three
  times. This wants the reverse: *every instance of a motif in the same
  colour*, so three teal blocks read as three copies of one block. `repeat` is
  this one, and it is the only scheme that helps with what the drawing is
  actually bad at.

Palette is the categorical set from the `dataviz` reference: eight hues in a
fixed order, validated for colour-vision deficiency on adjacent pairs, and
readable on the white plate the SVG draws. Slots are assigned in order and
wrap; a ninth lane gets slot 1 again rather than a generated hue.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .dag_layout import Layout, dominators, natural_key, repeat_motifs

__all__ = ["Colouring", "SCHEMES", "PALETTE", "UNMATCHED", "colour_layout"]

# Plotly's default qualitative colorway (`plotly.colors.qualitative.Plotly`),
# first eight entries.
PALETTE: tuple[str, ...] = (
    "#636EFA",  # blue
    "#EF553B",  # vermillion
    "#00CC96",  # teal green
    "#AB63FA",  # violet
    "#FFA15A",  # orange
    "#19D3F3",  # cyan
    "#FF6692",  # pink
    "#B6E880",  # light green
)
UNMATCHED = "#8A8A8A"  # everything a scheme has nothing to say about

SCHEMES = ("none", "lane", "repeat", "module", "namespace")


@dataclass(frozen=True)
class Colouring:
    """What a scheme decided. Empty means "leave the styles alone"."""
    nodes: dict[str, str] = field(default_factory=dict)
    edges: dict[tuple[str, str], str] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.nodes or self.edges)

    def node(self, name: str, fallback: str) -> str:
        return self.nodes.get(name, fallback)

    def edge(self, src: str, dst: str, fallback: str) -> str:
        return self.edges.get((src, dst), fallback)


def colour_layout(lay: Layout, scheme: str = "none") -> Colouring:
    """A scheme by name. Unknown names raise rather than silently drawing grey."""
    if scheme in (None, "", "none"):
        return Colouring()
    try:
        build = _SCHEMES[scheme]
    except KeyError:
        raise ValueError(
            f"unknown colour scheme {scheme!r}; expected one of {', '.join(SCHEMES)}"
        ) from None
    nodes = build(lay)
    # an edge belongs to a source and a target that may not agree, and the
    # source is the one the reader's eye is already on when the rail starts.
    # A fan-in therefore arrives in as many colours as it has inputs, which is
    # the honest picture of a join and reads as one under `repeat`, where every
    # instance of a motif is the same hue anyway.
    edges = {
        (e.src, e.dst): nodes[e.src]
        for e in lay.edges
        if not e.back and e.src in nodes
    }
    return Colouring(nodes=nodes, edges=edges)


def _by_lane(lay: Layout) -> dict[str, str]:
    """GitHub's: hue by lane index. Traces one rail; says nothing about repeats."""
    return {n.name: PALETTE[n.lane % len(PALETTE)] for n in lay.nodes}


def _by_repeat(lay: Layout) -> dict[str, str]:
    """One hue per repeat class — every instance of a motif the same colour.

    The opposite of graph colouring on purpose: sameness is the signal. Nodes
    in no class stay grey so the classes are the only thing carrying hue.
    """
    out = {n.name: UNMATCHED for n in lay.nodes}
    for i, m in enumerate(repeat_motifs(lay)):
        hue = PALETTE[i % len(PALETTE)]
        for x in m.nodes:
            out[x] = hue
    return out


def _module_owner(lay: Layout) -> dict[str, str | None]:
    """The innermost dominator module each node is in, or None for neither.

    Modules are the same thing `measure` counts: everything a node dominates,
    which is reachable only through it, and only where that is three nodes or
    more — a step and its one product is a module by construction, so counting
    those would make every node its own block. Innermost, so a nested block is
    its own thing and not its parent's.
    """
    names = [n.name for n in lay.nodes]  # row order is a topological order
    parents: dict[str, list[str]] = {n: [] for n in names}
    for e in lay.edges:
        if not e.back:
            parents[e.dst].append(e.src)
    idom = dominators(names, parents)

    size: dict[str, int] = dict.fromkeys(names, 1)
    for n in reversed(names):  # a dominator always precedes what it dominates
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
    """One hue per dominator module, touching modules given different hues.

    The graph-colouring reading, and the one that answers "where does this
    block start and end" rather than "is this the same block as that one".
    """
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
    """Hue by the `namespace::` prefix of a node's id; unprefixed stays grey."""
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
