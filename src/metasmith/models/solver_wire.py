from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from .solver import Application, Dependency, Endpoint, Node, Solution, Transform

@dataclass
class EncodedProblem:
    payload: dict
    nodes: list[Node]
    transforms: list[Transform]
    properties: list[str]
    dep_nodes: dict[int, Dependency]
    given_transform: Transform
    given_application: Application

class _Interner:
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
        parents = sorted(self.node(p) for p in sorted(n.parents, key=lambda x: x.Signature()))
        props = sorted(self.property(p) for p in sorted(n.properties))
        i = len(self.nodes)
        self._node_index[n] = i
        self.nodes.append(n)
        self.encoded.append({"props": props, "parents": parents})
        return i

def build_given_transform(given: Sequence[set[Endpoint]]) -> tuple[Transform, Application, list[list[Endpoint]]]:
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
    from .solver_engine import SOLVER_WIRE_VERSION, CallEngine

    encoded = encode_problem(
        given, transforms, target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
        wire_version=SOLVER_WIRE_VERSION,
    )
    return decode_plan(encoded, CallEngine(info, "solve", encoded.payload))
