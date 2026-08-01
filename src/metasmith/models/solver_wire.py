"""Turning a solver problem into something the engine can read, and back.

The Python objects -- `Transform`, `Dependency`, `Endpoint` -- stay the public
API. They are what the standard library, the DAG renderer and `WorkflowPlan`
already speak, and the port is not allowed to change that. So the boundary here
is narrow on purpose: a problem goes out as indices, a plan comes back as
indices, and nothing in between needs to know what a property string looks like.

Three things this module is responsible for, each of which is a way the port
could silently disagree with the solver it replaces:

**Interning.** Every distinct property string gets an index and every distinct
node -- properties plus lineage -- gets an index. Two nodes share an index
exactly when Python's `Node.__eq__` calls them equal, because both are asking
the same question: same properties, same parents. Nothing downstream ever
compares strings again.

**Order.** T5a made iteration order part of the solver's contract, so the two
places where this side has to *choose* an order it chooses deliberately. The
given endpoints within a group are sorted by `Signature()` here rather than in
the engine, because that sort is over Python's base-62 key strings and
reproducing string collation across a language boundary would be a contract
nobody wants to own. The transform list goes out in the caller's own sequence --
the synthesized `given` transform, then `transforms` as passed, then `target` --
which is exactly `_iter_transforms()`, so the engine's arena index *is* the T5a
rank and dependency rank is first appearance walking it.

**The synthesized given transform.** `solve_by_mcts` builds one, and it is not
bookkeeping: it is transform rank 0, its products are ranked before every real
dependency, and it is the root of the loop walk. It is built here, with the same
calls in the same order, rather than reimplemented on the other side.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from .solver import Application, Dependency, Endpoint, Node, Solution, Transform

@dataclass
class EncodedProblem:
    """The payload, plus what is needed to read a reply against it."""
    payload: dict
    #: node index -> the Python node that first defined it. Endpoints created by
    #: the search have no entry; they are rebuilt from properties and parents.
    nodes: list[Node]
    #: transform index -> the Python transform. Index 0 is the synthesized one.
    transforms: list[Transform]
    #: property index -> the string it stands for.
    properties: list[str]
    #: node index -> the `Dependency` that defined it, where one did.
    #:
    #: Not the same as `nodes`. Given endpoints intern first, so a dependency
    #: structurally equal to one lands on an `Endpoint` -- and handing that back
    #: as a plan's slot key would be wrong in a way `Node.__eq__` hides: an
    #: endpoint's parents are endpoints, a dependency's are dependencies, and the
    #: lineage checks read them.
    dep_nodes: dict[int, Dependency]
    given_transform: Transform
    given_application: Application

class _Interner:
    """Property strings and nodes, each to a dense index.

    Assignment order is a walk, not a `set`: the payload has to be the same
    bytes for the same problem, and a set would make it depend on the
    interpreter's hash seed -- the exact defect T5a spent a block removing from
    the solver itself.
    """

    def __init__(self) -> None:
        self.properties: list[str] = []
        self._prop_index: dict[str, int] = {}
        self.nodes: list[Node] = []
        self._node_index: dict[Node, int] = {}
        self.encoded: list[dict] = []

    def property(self, p: str) -> int:
        i = self._prop_index.get(p)
        if i is None:
            i = len(self.properties)
            self._prop_index[p] = i
            self.properties.append(p)
        return i

    def node(self, n: Node) -> int:
        i = self._node_index.get(n)
        if i is not None: return i
        # Parents first, so a node's index is always larger than its ancestors'
        # and a reply can be rebuilt in one forward pass. The recursion cannot
        # run away: a node's key is computed from its parents' keys at
        # construction, so a lineage cycle could never have been built.
        parents = sorted(self.node(p) for p in sorted(n.parents, key=lambda x: x.Signature()))
        props = sorted(self.property(p) for p in sorted(n.properties))
        i = len(self.nodes)
        self._node_index[n] = i
        self.nodes.append(n)
        self.encoded.append({"props": props, "parents": parents})
        return i

def build_given_transform(given: Sequence[set[Endpoint]]) -> tuple[Transform, Application, list[list[Endpoint]]]:
    """The synthesized `given` transform, built with `solve_by_mcts`'s own calls.

    Returned alongside the endpoint order it chose, because that order is the
    product order of transform rank 0 and everything downstream is ranked
    against it.
    """
    assert len(given) > 0, "nothing given"
    given_tr = Transform()
    given_appl = Application(initial_timeline=0, transform=given_tr, used={}, produced=[])
    groups: list[list[Endpoint]] = []
    for i, group in enumerate(given):
        assert len(group) > 0, f"input group [{i}] was empty"
        if i > 0: given_tr.NewProductGroup()
        pgroup: dict[Dependency, Endpoint] = {}
        ordered = sorted(group, key=lambda x: x.Signature())
        for e in ordered:
            pgroup[given_tr.AddProduct(e)] = e
        given_appl.produced.append(pgroup)
        groups.append(ordered)
    return given_tr, given_appl, groups

def encode_problem(
    given: Sequence[set[Endpoint]],
    transforms: Iterable[Transform],
    target: Transform,
    *,
    seed: int,
    max_iter: int,
    max_refine: int,
    wire_version: int,
) -> EncodedProblem:
    given_tr, given_appl, groups = build_given_transform(given)
    caller_transforms = list(transforms)

    # The arena is `_iter_transforms()` deduplicated by *identity*, first
    # occurrence winning -- which is what `_transform_rank`'s `setdefault` does,
    # and why the index is the rank. Identity rather than structure: two
    # duplicate transforms are distinguishable to the solver and must stay so.
    # The caller's own sequence is sent alongside as indices, because a caller
    # that passes its target in `transforms` too would otherwise turn one
    # transform into two and let it consume its own products.
    ordered_transforms: list[Transform] = []
    rank: dict[int, int] = {}
    def _rank(tr: Transform) -> int:
        i = rank.get(id(tr))
        if i is None:
            i = rank[id(tr)] = len(ordered_transforms)
            ordered_transforms.append(tr)
        return i
    given_index = _rank(given_tr)
    caller_indices = [_rank(tr) for tr in caller_transforms]
    target_index = _rank(target)

    it = _Interner()
    # Given endpoints first: they are the roots of every lineage in the problem,
    # and interning them first means their ancestors get the low indices.
    given_ids = [[it.node(e) for e in g] for g in groups]
    dep_nodes: dict[int, Dependency] = {}
    def _dep(d: Dependency) -> int:
        i = it.node(d)
        dep_nodes.setdefault(i, d)
        return i
    encoded_transforms = [
        {
            "requires": [_dep(d) for d in tr.requires],
            "produces": [[_dep(d) for d in pgroup] for pgroup in tr.produces],
        }
        for tr in ordered_transforms
    ]

    return EncodedProblem(
        payload={
            "wire_version": wire_version,
            "seed": seed,
            "max_iter": max_iter,
            "max_refine": max_refine,
            "n_properties": len(it.properties),
            "nodes": it.encoded,
            "transforms": encoded_transforms,
            "given_index": given_index,
            "caller_transforms": caller_indices,
            "target_index": target_index,
            "given": given_ids,
        },
        nodes=it.nodes,
        transforms=ordered_transforms,
        properties=it.properties,
        dep_nodes=dep_nodes,
        given_transform=given_tr,
        given_application=given_appl,
    )

def decode_plan(encoded: EncodedProblem, reply: dict) -> Solution:
    """Rebuild a `Solution` from the engine's indices.

    Endpoints come back parents-first, so one forward pass builds them. A row
    naming a `source_node` is one of the caller's own objects -- an ancestor that
    `rectify` carried through untouched -- and is handed back as itself rather
    than as an equal copy, which is what `plan.py` expects when it looks a
    lineage up against the endpoints it supplied.

    The diagnostic fields (`_frontier`, `_history`, `_refiner_histories`, and the
    heuristic maps) come back empty. They are not consumed anywhere in the
    codebase, and shipping the frontier of a 256-iteration search across a pipe
    to fill fields nobody reads would be a real cost for no reader. Said here
    rather than discovered later.
    """
    props = encoded.properties
    eps: list[Node] = []
    for row in reply["endpoints"]:
        src = row["source_node"]
        original = encoded.nodes[src] if src is not None else None
        if isinstance(original, Endpoint):
            eps.append(original)
            continue
        eps.append(Endpoint(
            {props[i] for i in row["props"]},
            parents={eps[j] for j in row["parents"]},
        ))

    def _slot(i: int) -> Dependency:
        d = encoded.dep_nodes.get(i)
        assert d is not None, f"node {i} is a plan slot but was never a dependency"
        return d

    steps = [
        Application(
            initial_timeline=st["timeline"],
            transform=encoded.transforms[st["transform"]],
            used={_slot(d): eps[e] for d, e in st["used"]},
            produced=[{_slot(d): eps[e] for d, e in g} for g in st["produced"]],
        )
        for st in reply["steps"]
    ]

    return Solution(
        complete=reply["complete"],
        dependency_plan=steps,
        merged_endpoints={eps[k]: {eps[x] for x in v} for k, v in reply["merged"]},
        _frontier=[],
        _history=[],
        _refiner_histories=[],
        _heuristics={"engine": ENGINE_HEURISTICS_NOTE},
        _iterations=reply["iterations"],
        _refiner_iterations=[(a, b) for a, b in reply["refiner_iterations"]],
        _relavent_transforms=[encoded.transforms[i] for i in reply["relevant_transforms"]],
    )

ENGINE_HEURISTICS_NOTE = (
    "solved by msm_solver; the python solver's telemetry maps are not carried"
    " across the wire because nothing reads them"
)

def solve_via_engine(
    info,
    given: Sequence[set[Endpoint]],
    transforms: Iterable[Transform],
    target: Transform,
    *,
    seed: int,
    max_iter: int,
    max_refine: int,
) -> Solution:
    """Encode, call, decode. Errors propagate: past the handshake, a failure is
    a bug in one of the two implementations, and falling back would hide it."""
    from .solver_engine import SOLVER_WIRE_VERSION, CallEngine

    encoded = encode_problem(
        given, transforms, target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
        wire_version=SOLVER_WIRE_VERSION,
    )
    return decode_plan(encoded, CallEngine(info, "solve", encoded.payload))
