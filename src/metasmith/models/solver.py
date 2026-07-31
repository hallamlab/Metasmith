from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable, Generator, Any, TypeVar, Generic
import json
import re
from pathlib import Path
from collections import deque

from ..hashing import KeyGenerator
from .dag_renderer import DagRenderer, Label, LabelMode, NodeKind
from .solver_rng import DecisionStream, argmax_index, argmin_index
from .solver_math import entropy

# Both search phases weight the same three moves: two exploit arms and one
# explore arm. Named here because the refiner and the mcts phase must not
# drift apart, and because the Rust port reads them as constants.
_SELECTION_WEIGHTS = (75, 20, 5)
_SELECTION_TOP_K = 1

class Node:
    PROPERTY_FIELD = "properties"
    NO_KEY = "_"
    def __init__(
        self,
        properties: set[str],
        parents: set[Node],
        _sig: str|None=None,
    ) -> None:
        super().__init__()
        assert isinstance(properties, set)
        assert isinstance(parents, set)
        self.properties = properties
        self.parents = parents
        self._sig = _sig
        self.hash, self.key = KeyGenerator.FromStr(self.Signature())
        # self._diffs = set()
        # self._sames = set()

    def __hash__(self) -> int:
        return self.hash

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, Node) and self.hash == __value.hash

    def __str__(self) -> str:
        return f"<{self._json_dumps(self.Pack(parents=False)['properties']).replace('"', '')}:{self.key}>"

    def __repr__(self) -> str:
        return f"{self}"

    def IsA(self, other: Node) -> bool:
        """
        if x.IsA(y), then x can be used to replace y
        """
        return other.properties.issubset(self.properties)

    def Signature(self):
        if self._sig is None:
            psig = ",".join(sorted(p.key for p in self.parents))
            sig = "".join(sorted(self.properties))
            _, sig = KeyGenerator.FromStr(sig)
            self._sig = f'{sig}:[{psig}]' if len(self.parents)>0 else sig
        return self._sig

    def RefreshHash(self):
        self._sig = None
        self.hash, self.key = KeyGenerator.FromStr(self.Signature())

    def Clone(self, properties_only: bool=False):
        clone = self.__class__(
            properties=set(self.properties),
            parents=set(p.Clone() for p in self.parents),
            _sig=None if properties_only else self._sig,
        )
        return clone

    def WithLineage(self, parents: Iterable[Node]):
        image = self.__class__(
            properties=self.properties,
            parents=set(parents),
        )
        return image

    @classmethod
    def _json_dumps(cls, d):
        return json.dumps(d, separators=(',', ':'), sort_keys=True)

    @classmethod
    def Unpack(cls, d: dict):
        NO_KEY = cls.NO_KEY
        raw_props = d[cls.PROPERTY_FIELD]
        props = set()
        if type(raw_props) in {list, set}: # all properties didn't have keys
            for v in raw_props:
                assert type(v) not in {list, dict}
                props.add(v)
        elif isinstance(raw_props, dict):
            for k, v in raw_props.items():
                assert type(v) not in {dict}
                if k == NO_KEY:
                    assert type(v) in {list}
                    props.update(v)
                    continue
                if isinstance(v, list):
                    if len(v)==1:
                        v = v[0]
                if isinstance(v, list):
                    props.update(cls._json_dumps({k:x}) for x in v)
                else:
                    props.add(cls._json_dumps({k:v}))
        else:
            assert False, f"unexpected format [{type(raw_props)}: {raw_props}]"
        m = cls(
            properties=props,
            parents=set(),
        )
        if "parents" in d:
            m.parents = {cls.Unpack(x) for x in d["parents"]}
        if "_hash" in d:
            m.hash, m.key = d["_hash"].split("/")
            m.hash = int(m.hash)
        return m

    def Pack(self, parents=False):
        NO_KEY = self.NO_KEY
        props = sorted(list(self.properties))
        formatted_props = {}
        def _try_keyval(p: str):
            try:
                e = json.loads(p)
                if len(e)>1: return NO_KEY, p
                k, v = next(iter(e.items()))
                return k, v
            except json.JSONDecodeError:
                return NO_KEY, p
        for p in props:
            k, v = _try_keyval(p)
            formatted_props[k] = formatted_props.get(k, [])+[v]
        # collapse singletons, unless they didn't have a key
        for k in list(formatted_props.keys()):
            if k == NO_KEY: continue
            if len(formatted_props[k])==1:
                formatted_props[k] = formatted_props[k][0]
        # if all didn't have keys, just save as list
        if len(formatted_props) == 1 and NO_KEY in formatted_props:
            formatted_props = formatted_props[NO_KEY]
        d = {
            "properties": formatted_props,
        }
        if len(self.parents)>0 and parents:
            d["parents"] = [x.Pack() for x in self.parents]
        return d

    def GetPreferredFileExtension(self):
        for p in self.properties:
            for hit in re.finditer(r'(ext=|"ext":")([\.\w\s]*[\w])', p):
                ext = str(hit.group(2))
                if not ext.startswith("."): ext = f".{ext}"
                return ext
        return ""

# of a Transform
class Dependency(Node):
    def __init__(self, properties: set[str], parents: set[Dependency]) -> None:
        super().__init__(properties=properties, parents=set(parents))

    def __str__(self) -> str:
        return f"(D:{'-'.join(sorted(list(self.properties)))})"

# as in a free floating data type
class Endpoint(Node):
    def __init__(self, properties: set[str], parents: set[Endpoint]|None=None) -> None:
        p: set[Node] = set(parents) if parents is not None else set()
        super().__init__(properties=properties, parents=p)

class Transform:
    def __init__(self) -> None:
        super().__init__()
        self.requires: list[Dependency] = list()
        self.produces: list[list[Dependency]] = [[]]
        self._group: int = 0
        self._update_hash()

    def __str__(self) -> str:
        def _props(d: Dependency):
            return "{"+"-".join(sorted(d.properties))+"}"
        return f"{','.join(_props(r) for r in self.requires)}->{'|'.join(','.join(_props(p) for p in g) for g in self.produces)}"

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return self.hash

    def _update_hash(self):
        self.hash, self.key = KeyGenerator.FromStr(str(self))

    def AddRequirement(self, example: Node|None=None, properties: Iterable[str]|None=None, parents: set[Dependency]|None=None):
        return self._add_dependency(destination=self.requires, example=example, properties=properties, parents=parents)

    def _get_product_group(self):
        if self._group >= len(self.produces):
            self.produces.append([])
        return self.produces[self._group]

    def NewProductGroup(self):
        prod = self._get_product_group()
        if len(prod) > 0:
            self._group += 1

    def AddProduct(self, example: Node|None=None, properties: Iterable[str]|None=None, parents: set[Dependency]|None=None):
        prod = self._get_product_group()
        return self._add_dependency(destination=prod, example=example, properties=properties, parents=parents)

    def _add_dependency(self, destination: list[Dependency], example: Node|None=None, properties: Iterable[str]|None=None, parents: set[Dependency]|None=None):
        assert example is not None or properties is not None
        if example is not None:
            properties = example.properties.copy()
        if parents is None: parents = set()
        _properties = set(properties) if properties else set()
        _dep = Dependency(properties=_properties, parents=parents)
        _parents = _dep.parents
        destination.append(_dep)
        if destination == self.requires:
            i = len(self.requires)-1
            for p in _parents:
                assert p in self.requires, f"{p} not added as a requirement"
        self._update_hash()
        return _dep

@dataclass
class SolverState:
    k: int
    steps: list[Application]
    production: dict[Dependency, list[Endpoint]] # product dep to produced endpoint
    have: set[Endpoint]
    candidate_transforms: set[Transform] # may not be valid, holds use count

@dataclass
class Application:
    initial_timeline: int
    transform: Transform
    used: dict[Dependency, Endpoint]
    produced: list[dict[Dependency, Endpoint]]
    score: list[float] = field(default_factory=list)
    _iteration: int = -1
    _sig: str|None = None
    _hash: int|None = None
    def Signature(self):
        if self._sig is None: 
            parts = []
            for dep in self.transform.requires:
                if dep not in self.used:
                    continue
                parts.append(f"{dep.key}:{self.used[dep].key}")
            self._sig = self.transform.key + "|" + "|".join(parts)
        return self._sig
    def __hash__(self) -> int:
        if self._hash is None:
            self._hash, _ = KeyGenerator.FromStr(self.Signature())
        return self._hash
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Application): return False
        return self.Signature() == value.Signature()

@dataclass
class RefinerState:
    steps: list[Application]
    scores: list[float] = field(default_factory=lambda: [0.0])
    valid: bool = False
    _sig: str|None = None
    _hash: int = 0
    _iteration: int = -1
    def Signature(self):
        if self._sig is None:
            self._sig = "".join(sorted(s.Signature() for s in self.steps))
        return self._sig
    def __hash__(self) -> int:
        if self._hash is None:
            self._hash, _ = KeyGenerator.FromStr(self.Signature())
        return self._hash
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, RefinerState): return False
        return self._hash == value._hash

@dataclass
class Solution:
    complete: bool
    dependency_plan: list[Application]
    merged_endpoints: dict[Endpoint, set[Endpoint]]
    _frontier: list[Application]
    _history: list[list[SolverState]]
    _refiner_histories: list[list[RefinerState]]
    _heuristics: dict[str, dict|list]
    _iterations: int
    _refiner_iterations: list[tuple[int, int]] # found at, total expanded
    _relavent_transforms: list[Transform]

    def BuildDAG(self, *, font: str = 'Arial', keys: bool = True, show_step_order: bool = False, label_mode: LabelMode = LabelMode.COLUMN, colour: str = "module", theme: str = "light", background: bool = True) -> DagRenderer:
        r = DagRenderer(font=font, label_mode=label_mode, colour=colour, theme=theme, background=background)
        for i, step in enumerate(self.dependency_plan):
            if keys:
                shown   = f"{step.transform.key}"
                inputs  = [f"{u.key}" for u in step.used.values()]
                outputs = [f"{o.key}" for pgroup in step.produced for o in pgroup.values()]
            else:
                shown   = f"{step.transform}"
                inputs  = [f"{u}" for u in step.used.values()]
                outputs = [f"{o}" for pgroup in step.produced for o in pgroup.values()]
            # numbered id, unnumbered label — two applications of one transform
            # share a key and would otherwise collapse into a single node
            transform_name = f"{i+1} {shown}"
            r.add_node(NodeKind.TRANSFORM, transform_name, Label(
                name=shown,
                namespace=f"step {i+1}" if show_step_order else "",
                full=transform_name,
            ))
            for name in inputs:
                r.add_edge(name, transform_name)
            for name in outputs:
                r.add_edge(transform_name, name)
        return r

    def RenderDAG(self, path_base: Path|str, format: str ='svg', *, font: str = 'Arial', keys: bool = True, show_step_order: bool = False, label_mode: LabelMode = LabelMode.COLUMN, colour: str = "module", theme: str = "light", background: bool = True):
        return self.BuildDAG(font=font, keys=keys, show_step_order=show_step_order, label_mode=label_mode, colour=colour, theme=theme, background=background).render(path_base, format)
    
def solve_by_mcts(
    given: list[set[Endpoint]],
    transforms: Iterable[Transform],
    target: Transform,
    seed: int=42,
    max_iter: int=256,
    max_refine: int=256,
) -> Solution:
    # The Rust engine, when this build has one that advertises `solve`. Absence
    # is normal and silent -- a source checkout ships no binaries -- and
    # `METASMITH_SOLVER_ENGINE=python` forces this path so the fallback is a
    # thing CI runs rather than a thing CI contains. Past the handshake an error
    # is not caught: the engine has already claimed the right wire version and
    # the right capability, so falling back would turn a real defect into a
    # mysterious slowdown.
    from .solver_engine import EngineFor
    _engine = EngineFor("solve")
    if _engine is not None:
        from .solver_wire import solve_via_engine
        return solve_via_engine(
            _engine, given, transforms, target,
            seed=seed, max_iter=max_iter, max_refine=max_refine,
        )

    # One stream for the whole solve, owned by this call. The old
    # `np.random.seed(seed)` mutated process-global state: two solves in one
    # process could not be independent, and any other numpy consumer silently
    # shared the solver's stream.
    rng = DecisionStream(seed)
    # ---
    # monte carlo tree search

    def _collect_all_ancestors(endpoints: set[Endpoint]) -> set[Endpoint]:
        """Recursively collect all ancestors of the given endpoints."""
        ancestors: set[Endpoint] = set()
        todo = list(endpoints)
        while todo:
            ep = todo.pop()
            for parent in ep.parents:
                if parent not in ancestors:
                    ancestors.add(parent)
                    todo.append(parent)
        return ancestors

    given_tr = Transform()
    given_appl = Application(initial_timeline=0, transform=given_tr, used={}, produced=[])
    inherent_parents: set[Endpoint] = set()
    assert len(given)>0, "nothing given"
    for i, group in enumerate(given):
        assert len(group)>0, f"input group [{i}] was empty"
        if i>0: given_tr.NewProductGroup()
        pgroup = {}
        # the caller hands us a `set`, so the product order is ours to state
        for e in sorted(group, key=lambda x: x.Signature()):
            d = given_tr.AddProduct(e)
            pgroup[d] = e
        given_appl.produced.append(pgroup)
        # Collect ALL ancestors, not just immediate parents
        inherent_parents.update(_collect_all_ancestors(group))

    given_endpoints: set[Endpoint] = set()
    for pgroup in given_appl.produced:
        given_endpoints.update(pgroup.values())

    _last_state_k = -1
    _state2child = {}
    def new_state_k(source: int):
        nonlocal _last_state_k
        _last_state_k += 1
        _state2child[source] = _state2child.get(source, [])+[_last_state_k]
        return _last_state_k
    def get_all_children(state: int):
        todo = [state]
        seen: set[int] = set()
        while len(todo)>0:
            s = todo.pop()
            if s in seen: continue
            seen.add(s)
            todo += _state2child.get(s, [])
        return seen

    starting_state = SolverState(
        k=new_state_k(-1),
        steps=[],
        production={},
        have=set(),
        candidate_transforms=set(),
    )
    # given_appl = Application(starting_state, given_tr, used={}, produced=[{}])

    # given_appl = Application(initial_timeline=starting_state.k, transform=given, used={}, produced=[{}])
    # for pg in given.produces:
    # for e in given:
        # p = given_tr.AddProduct(properties=e.properties)
        # given_appl.produced[0][p] = e
    given_tr = given_appl.transform
    def _iter_transforms():
        yield given_tr
        for tr in transforms: yield tr
        yield target

    # The iteration-order contract.
    #
    # Several of the searches below iterate a `set`, and the order they get is
    # CPython's hash-table layout -- which reaches the plan, because it decides
    # which application is appended to the frontier first and the selection
    # rules break ties by index. Salting `Node.__hash__` (leaving every
    # signature, key and equality untouched) moves 4 of the 8 corpus
    # fingerprints, so this is not theoretical. Two runs of one interpreter
    # agree; nothing else does, and the Rust port least of all.
    #
    # So every order-bearing iteration goes through an explicit rank assigned
    # once, here. Transforms rank by position in the caller's own sequence --
    # identity-keyed, so two duplicate transforms stay distinguishable, which a
    # signature-keyed rank could not do. Dependencies rank by first appearance
    # walking that same sequence; equal dependencies collapse, exactly as they
    # already do in `demand2product`. Endpoints and applications rank by
    # signature, which is unique within any one set because that is what their
    # `__eq__` compares.
    _transform_rank: dict[Transform, int] = {}
    for _tr in _iter_transforms():
        _transform_rank.setdefault(_tr, len(_transform_rank))
    _dep_rank: dict[Dependency, int] = {}
    for _tr in _iter_transforms():
        for _d in _tr.requires:
            _dep_rank.setdefault(_d, len(_dep_rank))
        for _pgroup in _tr.produces:
            for _d in _pgroup:
                _dep_rank.setdefault(_d, len(_dep_rank))
    _rank_of_transform = _transform_rank.__getitem__ # C-level, and these are hot
    _rank_of_dependency = _dep_rank.__getitem__
    def _by_transform(trs) -> list[Transform]:
        return sorted(trs, key=_rank_of_transform)
    def _by_dependency(deps) -> list[Dependency]:
        return sorted(deps, key=_rank_of_dependency)

    # produced dependency to consuming transform
    product2consumer: dict[Dependency, set[Transform]] = {}
    for parent in _iter_transforms():
        for child in _iter_transforms():
            if parent == child: continue
            for pgroup in parent.produces:
                for p in pgroup:
                    if not any(p.IsA(c) for c in child.requires): continue
                    product2consumer[p] = product2consumer.get(p, set())|{child}
    # requirement prototype of consumer
    # to production prototype of producer
    demand2product: dict[Dependency, set[Dependency]] = {}
    demand2producer: dict[Dependency, set[Transform]] = {}
    for child in _iter_transforms():
        for parent in _iter_transforms():
            if parent == child: continue
            for c in child.requires:
                found = False
                for pgroup in parent.produces:
                    for p in pgroup:
                        if not p.IsA(c): continue
                        demand2product[c] = demand2product.get(c, set())|{p}
                        found = True
                if found:
                    demand2producer[c] = demand2producer.get(c, set())|{parent}
    # Frozen into rank order once, here, and read as ordered sequences from now
    # on. Sorting at the point of use instead cost ~30% on the search-bound
    # corpus: the distance walk below reaches `demand2producer` 4.9 million
    # times on `wide-search` alone.
    demand2product = {c: _by_dependency(v) for c, v in demand2product.items()}
    demand2producer = {c: _by_transform(v) for c, v in demand2producer.items()}

    @dataclass
    class DistNode:
        step: Transform
        dist: int
        path: set[str] = field(default_factory=set)
    # estimate distance of nodes to target to provide guiding metric
    # filter out nodes that don't contribute to production of targets
    opportunity_scores: dict[Transform, int] = {}
    distance_scores: dict[Transform, int] = {}
    todo: list[DistNode] = [DistNode(target, -1)]
    while len(todo)>0:
        curr = todo.pop()
        node, consumer_distance = curr.step, curr.dist
        if node.key in curr.path: continue
        path = curr.path|{node.key}
        dist = consumer_distance+1
        other_dist = distance_scores.get(node, -1)
        if dist>other_dist:
            distance_scores[node] = dist
        opportunity_scores[node] = opportunity_scores.get(node, 1)+dist
        for p in node.requires:
            for producer in demand2producer.get(p, ()): # when tr requires a terminal endpoint that is not given
                todo.append(DistNode(producer, dist, path))
    relavent_transforms = [tr for tr in transforms if tr in distance_scores]
    if given_appl.transform not in distance_scores:
        # no path from givens to target; bail with a structured Solution
        # carrying the maps the diagnostic helper needs.
        return Solution(
            complete=False,
            dependency_plan=[],
            merged_endpoints={},
            _frontier=[],
            _history=[],
            _refiner_histories=[],
            _heuristics={
                "demand2producer": demand2producer,
                "demand2product": demand2product,
                "product2consumer": product2consumer,
                # object-keyed, unlike the D2T telemetry below, which is keyed by
                # a transform's printed key and so collapses duplicates
                "distance_scores": distance_scores,
                "opportunity_scores": opportunity_scores,
                "no_path_possible": True,
            },
            _iterations=0,
            _refiner_iterations=[],
            _relavent_transforms=list(relavent_transforms),
        )
    max_distance_score = max(distance_scores.values())

    # for telemetry
    D2T_KEY = "distance to target"
    d2t_report = {k.key:float(v) for k, v in distance_scores.items()}

    def _prune_irrelavent_values(d: dict, value_whitelist: set):
        # order-preserving for the two maps already frozen into rank order;
        # `intersection` would hand them back as a set and lose it again
        for k, v in d.items():
            d[k] = [x for x in v if x in value_whitelist] if isinstance(v, list) \
                else value_whitelist.intersection(v)
        # for k in list(d):
        #     if len(d[k])==0: del d[k]
    rts = set(relavent_transforms)|{given_tr, target}
    _prune_irrelavent_values(product2consumer, rts)
    _prune_irrelavent_values(demand2producer, rts)
    rtsp = {p for t in rts for g in t.produces for p in g}
    _prune_irrelavent_values(demand2product, rtsp)

    # should not perform mutations
    # produces list[possiblities] where each possibility is a list[Dependency]
    def generate_applications_of_transform(
        state_k: int,
        production: dict[Dependency, list[Endpoint]],
        blacklist: set[str],
        tr: Transform,
        mock_produced: list[dict[Dependency, Endpoint]]|None=None
    ) -> list[Application]:
        # production = state.production
        if len(tr.requires)==0:
            appl = Application(initial_timeline=state_k, transform=tr, used={}, produced=[{}])
            if appl.Signature() in blacklist: return []
            appl.produced = [{p:Endpoint(properties=p.properties) for p in pgroup} for pgroup in tr.produces]
            return [appl]
        
        # if mock_produced is given, do not check for lineage,
        # return all possibilities, and use mock_produced for the new applications
        handle_lineage = mock_produced is None

        # Transforms define lineage constraints (LC) first.
        # The endpoint matched to the LC must also be used to satisfy all instances.
        # That is, if a transform specifies A via P and B via P, 
        # then endpoint P' matched to P must be used to create both A and B
        # Walk e's ancestry transitively, with cycle protection. Input endpoints
        # store .parents as a nested tree (one direct level per endpoint),
        # whereas produced endpoints get a one-hop flattened set per transform
        # step (solver.py:603-605). A direct `matched in e.parents` check only
        # succeeds on the flattened shape, so it silently dropped any input-
        # rooted DAG whose lineage chains more than one level deep.
        def _is_ancestor(target: Endpoint, e: Endpoint, seen: set[Endpoint]) -> bool:
            if target in e.parents:
                return True
            for parent in e.parents:
                if parent in seen:
                    continue
                seen.add(parent)
                if _is_ancestor(target, parent, seen):
                    return True
            return False

        def _satisfies_lineage(e: Endpoint, p: Dependency, used: dict[Dependency, Endpoint]):
            for parent in p.parents:
                assert isinstance(parent, Dependency)
                matched = used[parent]
                if not _is_ancestor(matched, e, set()): return False
            return True
        
        def _find_endpoints(p: Dependency, include_produced: bool):
            given_candidates: list[Endpoint] = []
            produced_candidates: list[Endpoint] = []
            for product in demand2product.get(p, ()): # already in rank order
                if product not in production: continue
                for e in production[product]:
                    assert e.IsA(p)
                    if e in given_endpoints:
                        given_candidates.append(e)
                    else:
                        produced_candidates.append(e)
            # Per-requirement preference for given: if any given satisfies this
            # dep, use only given here — independent of whether OTHER deps of
            # the same transform need produced upstreams. The two-pass scaffold
            # in _resolve still flips include_produced=True as a lineage-failure
            # fallback (see test_given_fails_lineage_falls_back_to_produced).
            if include_produced:
                return given_candidates + produced_candidates
            if given_candidates:
                return given_candidates
            return produced_candidates

        # Two-pass: prefer given. If the given-only pass cannot reach a
        # lineage-viable leaf at all, fall back to given+produced so a
        # produced alternative can rescue a transform whose given candidates
        # all fail _satisfies_lineage. The original single-pass short-circuit
        # on IsA alone silently dropped such transforms.
        #
        # Important: only fall back when given-only failed for *lineage*
        # reasons, not because the resulting Application was already in the
        # blacklist (i.e., already explored by MCTS). Otherwise we'd flood the
        # frontier with redundant produced variants every time MCTS revisits
        # a transform.
        def _resolve(include_produced: bool) -> tuple[list[Application], bool]:
            viable_input_sets: list[Application] = []
            reached_leaf = False
            matches: dict[Dependency, list[Endpoint]] = {}
            for p in tr.requires:
                candidates = _find_endpoints(p, include_produced=include_produced)
                if len(candidates) == 0: return viable_input_sets, reached_leaf
                matches[p] = candidates

            INITIAL_I = 0
            todo: list[tuple[int, Endpoint, dict[Dependency, Endpoint]]] = [
                (INITIAL_I, e, dict()) for e in matches[tr.requires[INITIAL_I]]
            ]
            while len(todo)>0:
                p_i, e, used = todo.pop()
                p = tr.requires[p_i]
                used = used|{p:e}
                if handle_lineage and not _satisfies_lineage(e, p, used): continue
                if p_i >= len(tr.requires)-1:
                    reached_leaf = True
                    appl = Application(initial_timeline=state_k, transform=tr, used=used, produced=[{}])
                    if appl.Signature() in blacklist: continue # just check first
                    if handle_lineage:
                        lineage: set = {ancestor for e in used.values() for ancestor in e.parents}
                        lineage.update(used.values())
                        appl.produced = [{p:Endpoint(p.properties, parents=lineage) for p in pgroup} for pgroup in tr.produces]
                    else:
                        appl.produced = [mock for _, mock in zip(tr.produces, mock_produced)]
                    viable_input_sets.append(appl)
                    continue # at leaf (end of required dependencies)
                next_i = p_i+1
                todo += [
                    (next_i, e, used) for e in matches[tr.requires[next_i]]
                ]
            return viable_input_sets, reached_leaf

        viable_input_sets, reached_leaf = _resolve(include_produced=False)
        if not reached_leaf:
            viable_input_sets, _ = _resolve(include_produced=True)
        return viable_input_sets

    # ---
    # prune spurious nodes, assumes last step is target
    def prune_steps(steps: list[Application]) -> list[Application]:
        e2source: dict[Endpoint, Application] = {}
        for step in steps:
            for pgroup in step.produced:
                for e in pgroup.values():
                    e2source[e] = step
    
        @dataclass
        class PruneNode:
            ref: Application|Endpoint

            def GetKey(self):
                if isinstance(self.ref, Application):
                    return self.ref.Signature()
                else:
                    return self.ref.key
                
            def GetChildren(self):
                if isinstance(self.ref, Application):
                    for x in self.ref.used.values():
                        yield x
                else:
                    if self.ref not in e2source: return
                    appl = e2source[self.ref]
                    yield appl

        start = PruneNode(steps[-1]) # last should be target
        todo: deque[PruneNode] = deque()
        todo.append(start)
        seen: dict[str, PruneNode] = {}
        while len(todo)>0:
            node = todo.popleft()
            key = node.GetKey()
            if key in seen: continue
            seen[key] = node
            for x in node.GetChildren():
                todo.append(PruneNode(x))
        required = [x.ref for x in seen.values() if isinstance(x.ref, Application)]
        required.reverse()
        return required
    
    # ---
    # order nodes by steps to create
    def get_order(steps: list[Application]):
        seen: set[str] = set()
        _have: set[Endpoint] = set()
        order: dict[str, int] = {e.key:0 for e in _have}
        while len(seen)<len(steps):
            # A list, not a set: `seen` already makes the signatures within one
            # layer unique -- which is exactly what a `set[Application]` was
            # collapsing on -- so taking them in `steps` order costs nothing and
            # states the order instead of inheriting the hash table's.
            reachable: list[Application] = []
            # find and process separately to ensure 1 layer at a time
            for step in steps:
                if step.Signature() in seen: continue
                if any(e not in _have for e in step.used.values()): continue
                seen.add(step.Signature())
                reachable.append(step)
            if len(reachable)==0: break # shouldn't happen/needed, but here to prevent endless loop
            for step in reachable:
                if len(step.used)>0:
                    step_depth = max(order[e.key] for e in step.used.values())+1
                else:
                    step_depth = 1
                order[step.Signature()] = step_depth
                for pgroup in step.produced:
                    for e in pgroup.values():
                        if e in order: continue
                        order[e.key] = step_depth+1
                _have |= {e for pgroup in step.produced for e in pgroup.values()}
        max_depth = max(order.values())+1
        for step in steps:
            k = step.Signature()
            if k in order: continue
            order[k] = max_depth
        return order
    
    def order_steps(order: dict[str, int], steps: list[Application]):
        return sorted(steps, key=lambda s: order[s.Signature()]*10000+len(s.used))

    # produce new set of endpoints so hashes are valid
    # and prune steps
    def rectify(solution: list[Application], prune=True, insert_given=True):
        steps = [given_appl]+solution if insert_given else solution
        steps = [
            Application(
                initial_timeline=step.initial_timeline,
                transform=step.transform,
                used=step.used.copy(),
                produced=step.produced.copy(),
                score=step.score,
                _iteration=step._iteration,
            ) for step in steps
        ]

        # prune steps, place target step last, as required
        _targeti = -1
        for i, s in enumerate(steps):
            if all(len(g) == 0 for g in s.produced):
                _targeti = i
                break
        assert _targeti >= 0
        steps[_targeti], steps[-1] = steps[-1], steps[_targeti]
        if prune:
            steps = prune_steps(steps) # may be dangerous, since endpoint hashes are not yet fixed

        _product2consumer: dict[Endpoint, list[Application]] = {}
        for step in steps:
            for e in step.used.values():
                _product2consumer[e] = _product2consumer.get(e, [])+[step]

        endpoint_map: dict[Endpoint, Endpoint] = {}
        rev_emap: dict[Endpoint, Endpoint] = {}
        def _fix_endpoints(appl: Application):
            lineage: set[Endpoint] = set()
            for p in appl.transform.requires:
                e = appl.used[p]
                e = endpoint_map.get(e, e)
                appl.used[p] = e # update to new endpoint
                lineage.add(e)
                lineage.update(e.parents) # type: ignore
            # fix lineage of endpoints
            new_produced = []
            for pgroup in appl.produced:
                new_pgroup = {}
                for p, e in pgroup.items():
                    if e in endpoint_map:
                        new_e = endpoint_map[e]
                        new_e.parents|=lineage|(e.parents&inherent_parents)
                        new_e.RefreshHash()
                    else:
                        new_e = Endpoint(e.properties, parents=lineage|(e.parents&inherent_parents)) # type: ignore
                    new_pgroup[p] = new_e
                    endpoint_map[e] = new_e
                    rev_emap[new_e] = e
                new_produced.append(new_pgroup)
            appl.produced = new_produced
            # force regenerate signature
            appl._sig = None
            appl._hash = None
            appl.Signature()
        
        node_order = get_order(steps)
        todo: list[Application] = steps.copy()
        order = [node_order[s.Signature()] for s in todo]
        while len(todo)>0:
            si: int = argmin_index(order) # first minimum; introselect's was arbitrary
            todo[si], todo[-1] = todo[-1], todo[si]
            order[si], order[-1] = order[-1], order[si]
            order.pop()
            appl = todo.pop()
            _fix_endpoints(appl) # mutates appl
        return steps
    
    @dataclass
    class RefinerResult:
        steps: list[Application]
        _history: list[RefinerState]
        _iterations: int
        _found_on: int
    def refine_mcts(initial_solution: list[Application], max_iters: int):
        def validate_node(state: RefinerState):
            produced_from: dict[Endpoint, list[Endpoint]] = {}
            for appl in state.steps:
                _from = list(appl.used.values())
                for pgroup in appl.produced:
                    for e in pgroup.values():
                        produced_from[e] = _from
            def _has_ancestor(e: Endpoint, a: Endpoint):
                todo = [e]
                seen = {e}
                while len(todo)>0:
                    e = todo.pop()
                    if e == a: return True
                    for parent in produced_from[e]:
                        if parent in seen: continue
                        todo.append(parent)
                        seen.add(parent)

            def _iter_steps():
                yield given_appl
                for step in state.steps:
                    yield step

            def _get_target():
                _targeti = -1
                for i, s in enumerate(state.steps):
                    if all(len(g) == 0 for g in s.produced):
                        _targeti = i
                        break
                if _targeti == -1: return None
                return state.steps[_targeti]

            # checks lineage constaint and no loops
            def _is_valid(target_appl: Application):
                # print(">>>")
                e2appl: dict[Endpoint, list[Application]] = {}
                for appl in _iter_steps():
                    for e in appl.used.values():
                        e2appl[e] = e2appl.get(e, [])+[appl]
                todo = [(given_appl, set())]
                produced: set[Endpoint] = set()
                while len(todo)>0:
                    current, history = todo.pop()

                    # print("  .")
                    # print(f"  {current.transform}")
                    # for d, e in current.used.items():
                    #     print(f"    {d} {e}")
                    # # print(f"        ---")
                    # for pgroup in current.produced:
                    #     print(f"    .")
                    #     for d, e in pgroup.items():
                    #         print(f"    {d} {e}")
                    if current.Signature() in history: return False # looped
                    history = history|{current.Signature()}
                    for pgroup in current.produced:
                        produced.update(pgroup.values())
                        for e in pgroup.values():
                            for appl in e2appl.get(e, []):
                                todo.append((appl, history))

                # no loops from the start, but do we actually get to the end?
                missing = set(target_appl.used.values()) - produced
                if len(missing)>0: return False

                # if here, then no loops
                # now check lineage
                for step in _iter_steps():
                    for p, e in step.used.items():
                        for pproto in p.parents:
                            lineage_constraint_e = step.used[pproto] # type: ignore
                            if not _has_ancestor(e, lineage_constraint_e): return False
                return True
            # `_is_valid` is an AND of three independent, side-effect-free terms
            # and it runs them most-expensive-first: the path-dependent loop walk
            # dominates the whole solve while the lineage term is nearly free.
            # On `metagenomics_from_paired_reads` *every one* of the 19,683
            # refiner validations fails on lineage, after paying for the walk.
            # Reordering an AND is exact by construction, so run the cheap term
            # first and only fall through to the full check when it passes.
            def _lineage_ok():
                for step in _iter_steps():
                    for p, e in step.used.items():
                        for pproto in p.parents:
                            lineage_constraint_e = step.used[pproto] # type: ignore
                            if not _has_ancestor(e, lineage_constraint_e): return False
                return True

            target_appl = _get_target()
            if target_appl is None:
                state.valid = False
            else:
                try:
                    # `_has_ancestor` indexes `produced_from` unguarded, and
                    # reaching it earlier than the original order can hit a state
                    # the loop walk would have rejected first. A KeyError here is
                    # therefore "the prefilter cannot answer", not "invalid" --
                    # fall through to the unchanged check below.
                    rejected = not _lineage_ok()
                except KeyError:
                    rejected = False
                state.valid = False if rejected else _is_valid(target_appl)
            # print(f"<<< {state.valid}")

                
        def score_node(state: RefinerState):
            validate_node(state)
            used_as_lineage: set[Endpoint] = set()
            for step in state.steps:
                for p in step.used.keys():
                    used_as_lineage |= {step.used[pproto] for pproto in p.parents} # type: ignore
            _steps = state.steps
            lineage_usage: dict[Endpoint, int] = {}
            for step in _steps:
                for p, e in step.used.items():
                    if not e in used_as_lineage: continue
                    lineage_usage[e] = lineage_usage.get(e, 0)+1
            # `solver_math.entropy`, not the numpy expression this used to be:
            # `ndarray.sum` is pairwise and `np.log2` is not libm's, so the score
            # differed in its last bit from anything that isn't numpy. See that
            # module for the measurements.
            e_score = entropy(list(lineage_usage.values()))

            _product2producer: dict[Endpoint, Application] = {}
            for step in _steps:
                for pgroup in step.produced:
                    for e in pgroup.values():
                        _product2producer[e] = step
            # The walk below depends only on where it *starts*: the destination
            # is a plain equality test during traversal, and `seen` guarantees
            # one visit per node. So one walk per distinct source answers every
            # destination asked of it -- and the refiner asks about far fewer
            # sources than pairs (17.8 vs 22.0 per `score_node` on the
            # metagenomics template).
            #
            # The depth map reproduces the original's two quirks exactly, and
            # both feed the score: a LIFO stack with an up-front `seen` check
            # records depth at *first pop*, not the true maximum, and the
            # `max_d > 0` test below makes a distance of zero indistinguishable
            # from not-found.
            _depth_maps: dict[Endpoint, dict[Endpoint, int]] = {}
            def _depths_from(e: Endpoint) -> dict[Endpoint, int]:
                depths = _depth_maps.get(e)
                if depths is not None: return depths
                depths = {}
                todo = [(e, 0)]
                while len(todo)>0:
                    n, d = todo.pop()
                    if n in depths: continue
                    depths[n] = d
                    prod = _product2producer[n]
                    for pe in prod.used.values():
                        todo.append((pe, d+1))
                _depth_maps[e] = depths
                return depths
            def _max_distance_to(e: Endpoint, a: Endpoint):
                max_d = _depths_from(e).get(a, -1)
                return max_d/len(_steps) if max_d>0 else 1.0
            lin_distances: list[float] = []
            for step in _steps:
                for p in step.transform.requires:
                    # A sixth ordered site, and T5a missed it because it is a
                    # *summation* order rather than a selection order: these
                    # distances are summed below, and floating-point addition is
                    # not associative. `p.parents` is a `set`, so on a
                    # requirement with two lineage constraints the score would
                    # depend on the hash table.
                    #
                    # It is unobservable on everything currently measured -- no
                    # dependency in the four templates or in a thousand generated
                    # problems carries more than one lineage parent, and a
                    # one-element sum has no order -- which is exactly why it is
                    # worth stating now rather than after a transform that does.
                    # The other two reads of `p.parents` need no ordering: one is
                    # an AND and the other builds a set.
                    for lin_p in _by_dependency(p.parents):
                        e = step.used[p]
                        pe= step.used[lin_p] # type: ignore
                        lin_distances.append(_max_distance_to(e, pe))
            if len(lin_distances)>0:
                lin_score = -sum(lin_distances)/len(lin_distances)
            else:
                lin_score = 0
            score = e_score*1000+lin_score
            # _, k = KeyGenerator.FromStr(state.Signature(), l=4)
            vscore = score*state.valid
            state.scores = [score, vscore]
        
        def select_node(frontier: list[RefinerState]) -> int:
            # weights are [score, score * valid, explore]
            p_i = rng.weighted_index(_SELECTION_WEIGHTS)
            if p_i<len(_SELECTION_WEIGHTS)-1: # exploit
                return rng.pick_top_k([s.scores[p_i] for s in frontier], _SELECTION_TOP_K)
            else: # explore
                return rng.bounded_int(len(frontier))
        
        def remove_node(frontier: list[RefinerState], index: int):
            frontier[index], frontier[-1] = frontier[-1], frontier[index]
            return frontier.pop() # O(1) vs O(n) for arr.remove()

        def expand_node(state: RefinerState):
            current_applications = {s.Signature() for s in state.steps}
            production: dict[Dependency, list[Endpoint]] = {}
            for step in state.steps:
                for pgroup in step.produced:
                    for p, e in pgroup.items():
                        production[p] = production.get(p, [])+[e]
            for step in state.steps:
                # Neither of these depends on the candidate application, and
                # ~90% of candidates are about to be discarded as duplicates --
                # so they are hoisted out of the loop that builds them.
                # NOTE: the signature comparison drops *both* members of a
                # colliding pair. That is a latent bug (see
                # `tests/solver/test_refiner_validity.py`), preserved verbatim
                # here because this change is a performance change.
                base = [s for s in state.steps if s.Signature() != step.Signature()]
                base_sigs = sorted(s.Signature() for s in base)
                # reuse the current endpoints and simply look for alternate edge comparisons
                # lineage constraint checked separately
                for appl in generate_applications_of_transform(
                    state_k=step.initial_timeline,
                    production=production,
                    blacklist=current_applications,
                    tr=step.transform,
                    mock_produced=step.produced, # rectify later
                ):
                    appl._iteration = step._iteration
                    # The state's signature is the sorted join of its steps'
                    # signatures, so it can be had without the state. Yielding
                    # it lets the caller reject a duplicate before anything is
                    # constructed -- 193,280 `RefinerState` builds become
                    # 19,683 on the metagenomics template.
                    yield "".join(sorted(base_sigs+[appl.Signature()])), base, appl

        initial_state = RefinerState(
            steps=initial_solution,
            valid=True,
        )
        score_node(initial_state)
        frontier: list[RefinerState] = [initial_state]
        seen: set[str] = {initial_state.Signature()}
        valids: list[RefinerState] = [initial_state]
        history: list[RefinerState] = []
        i = 0
        while len(frontier)>0 and i<max_iters:
            i += 1
            statei = select_node(frontier)
            state = remove_node(frontier, statei)
            state._iteration = i
            history.append(state)
            if state.valid:
                valids.append(state)
            for sig, base, appl in expand_node(state):
                if sig in seen: continue
                seen.add(sig)
                child = RefinerState(steps=base+[appl], _sig=sig)
                score_node(child)
                frontier.append(child)
        # take the valid score; first maximum wins, where introselect picked
        # whichever index its partition happened to leave in that slot
        si: int = argmax_index([s.scores[1] for s in valids])
        refined = valids[si]
        return RefinerResult(
            steps=rectify(refined.steps),
            _history=history,
            _iterations=i,
            _found_on=refined._iteration,
        )

    @dataclass
    class MctsResult:
        complete: bool
        state: SolverState
        merged_endpoints: dict[Endpoint, set[Endpoint]]
        _frontier: list[Application]
        _history: list[list[SolverState]]
        _refiner_histories: list[list[RefinerState]]
        _iterations: int
        _refiner_iterations: list[tuple[int, int]]
        _production_depths: list[dict[str, float]]
    def mcts(max_iter: int):
        def is_solved(state: SolverState):
            last_transform = state.steps[-1].transform
            return last_transform == target

        def score_node(appl: Application):
            dist = distance_scores[appl.transform]
            dist = 1-dist/max_distance_score
            opportunity = opportunity_scores[appl.transform]
            opportunity = 1-(1/(1+opportunity/10))
            appl.score = [dist, opportunity]
            return appl

        def select_node(frontier: list[Application]):
            # weights are [dist, opportunity, explore]
            p_i = rng.weighted_index(_SELECTION_WEIGHTS)
            if p_i<len(_SELECTION_WEIGHTS)-1: # exploit
                return rng.pick_top_k([s.score[p_i] for s in frontier], _SELECTION_TOP_K)
            else: # explore
                return rng.bounded_int(len(frontier))

        def remove_node(frontier: list[Application], index: int):
            frontier[index], frontier[-1] = frontier[-1], frontier[index]
            return frontier.pop() # O(1) vs O(m) for arr.remove()

        def expand_node(state: SolverState, appl: Application) -> list[SolverState]:
            possibilities = []
            # Multi-pgroup applications carry two distinct intents that share a
            # data shape:
            #   - given_appl: each pgroup is an alternative sample family →
            #     branch into N timelines, one per sample.
            #   - user multi-output transforms (e.g. multi_slot_producer,
            #     failing_at_slot_k): every pgroup is co-produced by one
            #     invocation → keep them all in a single timeline so downstream
            #     transforms that require multiple slots can apply.
            # The DSL is symmetric, so disambiguate by identity against given_tr.
            is_sample_branching = appl.transform is given_tr and len(appl.produced) > 1
            if is_sample_branching:
                state_ks = [new_state_k(state.k) for _ in appl.produced]
                for group, state_k in zip(appl.produced, state_ks):
                    candidate_transforms = state.candidate_transforms.copy() # was free transform
                    production = state.production.copy()
                    for dep, ep in group.items():
                        if dep not in product2consumer: continue
                        for linked in product2consumer[dep]:
                            candidate_transforms.add(linked)
                    for dep, ep in group.items():
                        production[dep] = production.get(dep, [])+[ep]
                    appl_variant = Application(
                        initial_timeline=state_k,
                        transform=appl.transform,
                        used=appl.used,
                        produced=[group], # limit to each each possibility
                        score=appl.score,
                    )
                    possibilities.append(SolverState(
                        k=state_k,
                        steps=state.steps+[appl_variant],
                        have=state.have|set(group.values()),
                        candidate_transforms=candidate_transforms,
                        production=production,
                    ))
            else:
                candidate_transforms = state.candidate_transforms.copy()
                production = state.production.copy()
                co_produced: set[Endpoint] = set()
                for group in appl.produced:
                    for dep, ep in group.items():
                        if dep in product2consumer:
                            for linked in product2consumer[dep]:
                                candidate_transforms.add(linked)
                        production[dep] = production.get(dep, [])+[ep]
                        co_produced.add(ep)
                possibilities.append(SolverState(
                    k=state.k,
                    steps=state.steps+[appl],
                    have=state.have|co_produced,
                    candidate_transforms=candidate_transforms,
                    production=production,
                ))
            return possibilities

        free_transforms = [t for t in relavent_transforms if len(t.requires)==0]
        def generate_child_nodes(state: SolverState, frontier_signatures: set[str]):
            def _iter_transforms():
                for tr in _by_transform(state.candidate_transforms):
                    yield tr
                for tr in free_transforms:
                    yield tr
            for tr in _iter_transforms():
                # print("$ ", tr)
                for appl in generate_applications_of_transform(
                    state_k=state.k,
                    production=state.production,
                    blacklist=frontier_signatures,
                    tr=tr
                ):
                    yield appl

        merged_endpoints: dict[Endpoint, set[Endpoint]] = {}
        def merge_states(source: SolverState, alt: SolverState) -> SolverState:
            # print(f"{source.k} << {alt.k}")
            e2consumer: dict[Endpoint, list[Application]] = {}
            for step in alt.steps:
                for d, e in step.used.items():
                    e2consumer[e] = e2consumer.get(e, [])+[step]
            e2producer: dict[Endpoint, Application] = {}
            for step in alt.steps:
                for pgroup in step.produced:
                    for d, e in pgroup.items():
                        e2producer[e] = step
            # _lin_cache = {}
            # def _get_lineage_constraints(step0: Application):
            #     todo = [step0]
            #     lineage_constraints: set[Endpoint] = set()
            #     produced: set[Endpoint] = set()
            #     while len(todo)>0:
            #         step = todo.pop()
            #         if step in _lin_cache:
            #             new_lin, new_p = _lin_cache[step]
            #             lineage_constraints.update(new_lin)
            #             produced.update(new_p)
            #             continue
            #         for d in step.used:
            #             for p in d.parents:
            #                 lineage_constraints.add(step.used[p]) # type: ignore
            #         for pgroup in step.produced:
            #             for d, e in pgroup.items():
            #                 produced.add(e)
            #                 for appl in e2consumer.get(e, []):
            #                     todo.append(appl)
            #     _lin_cache[step0] = lineage_constraints, produced
            #     return lineage_constraints-produced
            
            def _get_substitute(alt_step: Application):
                # first, get steps with substitutable signatures as candidates
                candidates = source_tr2appl.get(alt_step.transform, [])
                # print("  ", len(candidates))
                # for each candidate to merge into,
                # check that if endpoints are substituted, all steps that use
                # any of the swapped endpoints are still valid
                for src_step in candidates:
                    subs: list[tuple[Endpoint, Endpoint]] = []
                    for d, se in src_step.used.items():
                        ae = alt_step.used[d]
                        subs.append((ae, se))
                    src_pool, alt_pool = [], []
                    for sg, ag in zip(src_step.produced, alt_step.produced):
                        added = set()
                        # note that in the current interface,
                        # output groups with shared dependencies within a single transform
                        # is impossible...
                        for d, se in sg.items():
                            if d not in ag: # ... so this if will trigger or not trigger for entire loop
                                src_pool.append(se)
                                continue
                            ae = ag[d]
                            subs.append((ae, se))
                            added.add(d)
                        for d, ae in ag.items():
                            if d not in added: # ... same here
                                alt_pool.append(ae)

                    ok = True
                    for ae, se in subs:
                        if ae not in e2consumer: continue
                        for appl in e2consumer[ae]:
                            if appl == alt_step: continue
                            for d, e in appl.used.items():
                                if e != ae: continue
                                if not se.IsA(d):
                                    ok = False
                                    break
                    if ok: return src_step
                return None

            source_timelines = {s.initial_timeline for s in source.steps}
            alt_timelines = {s.initial_timeline for s in alt.steps}
            source_tr2appl: dict[Transform, list[Application]] = {}
            for step in source.steps:
                if step.initial_timeline in alt_timelines: continue
                source_tr2appl[step.transform] = source_tr2appl.get(step.transform, [])+[step]
            to_check = [s for s in alt.steps if s.initial_timeline not in source_timelines]
            to_check.reverse() # target -> given

            to_merge: list[tuple[Application, Application]] = []
            to_add_from_alt: list[Application] = []
            # check applications not from the same timeline:
            # for each alt step
            # if there is a transform in src where if swapped with alt step,
            # lineage constraints of downstream in alt are satisfied,
            # then src step can be merged with alt step
            for step in to_check:
                # print(step.transform)
                # print(step.transform.key, len(step.transform.requires), sum(len(g) for g in step.produced))
                src_step = _get_substitute(step)
                # print("+" if src_step is None else "x")
                # print()
                if src_step is None:
                    to_add_from_alt.append(step)
                else:
                    to_merge.append((step, src_step))
            to_add_from_alt.reverse() # given -> target
            to_merge.reverse() # given -> target
            common_steps: list[Application] = [s for s in alt.steps if s.initial_timeline in source_timelines]
            _merged = {src_step for _, src_step in to_merge}
            to_add_from_src: list[Application] = [s for s in source.steps if s.initial_timeline not in alt_timelines and s not in _merged]
            merged_steps: list[Application] = []
            swapped_endpoints: dict[Endpoint, Endpoint] = {}
            # merge steps by pointing used from alt to that of souce
            # and combining the production groups if step caused the branching
            for alt_step, src_step in to_merge:
                # print(f"{src_step.transform} <<< {alt_step.transform}")
                for ad, ae in alt_step.used.items():
                    se = src_step.used[ad]
                    se = swapped_endpoints.get(se, se) # in case used merged endpoint
                    src_step.used[ad] = se
                    # print(f"  {se} -<- {ae}")
                    swapped_endpoints[ae] = se # register for to_add_from_alt
                    merged_endpoints[se] = merged_endpoints.get(se, {se})|{ae}
                merged_pgroup = src_step.produced.copy()
                # print(f"  {src_step.transform} {len(src_step.produced)}")
                for mp in alt_step.produced:
                    mk = set(mp)
                    if any(mk==set(pgroup) for pgroup in src_step.produced): continue
                    merged_pgroup.append(mp)
                src_step.produced = merged_pgroup
                src_step._sig = None
                src_step.Signature() # recalculate hash
                merged_steps.append(src_step)
            for step in merged_steps: # fix potential swaps in outputs
                for pgroup in step.produced:
                    for d in pgroup:
                        e = pgroup[d]
                        pgroup[d] = swapped_endpoints.get(e, e)

            # connect the merged endpoints
            for step in to_add_from_alt:
                # print(f" _ {step.transform}")
                for pgroup in step.produced:
                    for d in pgroup:
                        e = pgroup[d]
                        pgroup[d] = swapped_endpoints.get(e, e)
                        # print(f" _   {pgroup[d]} -<-{e}")
            source.k = alt.k
            source.steps = common_steps+to_add_from_src+to_add_from_alt+merged_steps
            order = get_order(source.steps)
            source.steps = order_steps(order, source.steps)
            source.steps = rectify(source.steps, prune=False, insert_given=False)
            return source

        # pseudocode:
        # Solver state captures available endpoints and applied transforms.
        # Branching produces alternate states
        # Current states is list of alternate states.
        # Frontier is list of applications of transforms
        # Each application specifies the state on which to be applied 
        #   if state no longer exists, application is applied to all branched children 
        # At start, there is only 1 state and frontier contains only 1 application,
        #   which is a transform that adds the given endpoints to the state
        # At each iteration:
        #   select application from frontier based on eploit vs explore
        #   exploit has 2 modes: 
        #       get closer to target
        #       or increase the number of available endpoints (opportunity)
        #   apply application to all valid current states
        #   update current branching factor (number of current states)
        #   if a state is solved, stop considering it
        #   >>> if there are no more current states, return solved states
        #   add new applications to frontier based on new states
        #   update current states to only those that are not solved
        # notes:
        # - each node in the graph that the solver traverses
        #   is itself a graph representing the workflow.
        #   that is, the solver is not searching the workflow graph,
        #   but rather the space of possible workflow graphs.
        current_timelines = [starting_state]
        frontier: list[Application] = [score_node(given_appl)]
        frontier_signatures: set[str] = {s.Signature() for s in frontier}
        history: list[list[SolverState]] = []
        solved_state: SolverState|None = None
        _refiner_iterations = []
        _refiner_histories = []
        _production_depths = []
        i: int = 0
        while len(frontier)>0 and i < max_iter:
            i += 1
            nodei = select_node(frontier)
            node = remove_node(frontier, nodei)
            node._iteration = i

            valid_timeline_ks = get_all_children(node.initial_timeline)
            source_states = [s for s in current_timelines if s.k in valid_timeline_ks]
            carry_over = [s for s in current_timelines if s.k not in valid_timeline_ks]
            next_states = [s for g in [expand_node(s, node) for s in source_states] for s in g]
            history.append(carry_over+next_states)
            remain: list[SolverState] = []
            for s in next_states:
                if is_solved(s):
                    # this must be done here so that assumption of only 1 producer per ep is true
                    # after merging, workflow will have, well, merges where multiple steps produce
                    # the same ep
                    s.steps = prune_steps(s.steps)
                    refined = refine_mcts(s.steps, max_refine)
                    _refiner_histories.append(refined._history)
                    _refiner_iterations.append((refined._found_on, refined._iterations))
                    _order = get_order(refined.steps)
                    _production_depths.append({k:float(v) for k, v in _order.items()})
                    s.steps = order_steps(_order, refined.steps)
                    solved_state = merge_states(solved_state, s) if solved_state is not None else s
                else:
                    remain.append(s)

            if len(remain)+len(carry_over) == 0:
                if solved_state is None: break
                s = solved_state
                return MctsResult(
                    complete=True,
                    state=solved_state,
                    merged_endpoints=merged_endpoints,
                    _frontier=frontier,
                    _history=history,
                    _iterations=i,
                    _refiner_iterations=_refiner_iterations,
                    _refiner_histories=_refiner_histories,
                    _production_depths=_production_depths,
                )
            for state in remain: # applications for carry over should have already been added
                applied_transforms: set[Transform] = set()
                for child in generate_child_nodes(state, frontier_signatures):
                    # print(f"c", child.transform)
                    child = score_node(child)
                    child._iteration = -i
                    frontier_signatures.add(child.Signature())
                    applied_transforms.add(child.transform)
                    frontier.append(child)
                state.candidate_transforms -= applied_transforms # all possibilities per tr explored
            current_timelines = carry_over+remain
            
        return MctsResult(
            # Falling out of the loop is not the same as failing. The early
            # return above fires only when every timeline resolves on the same
            # pass; a search that instead runs its frontier down still holds a
            # merged solution for the timelines that did solve, and on
            # multi-sample problems that is the normal exit -- 1250 of 1250
            # generated multi-given instances leave by this path with a plan
            # the checker passes. What actually distinguishes "no answer" is
            # `solved_state is None`, in which case `state` below is an
            # arbitrary unfinished timeline.
            complete=solved_state is not None,
            state=solved_state if solved_state is not None else current_timelines[0],
            merged_endpoints=merged_endpoints,
            _frontier=frontier,
            _history=history,
            _iterations=i,
            _refiner_iterations=_refiner_iterations,
            _refiner_histories=_refiner_histories,
            _production_depths=_production_depths,
        )

    solution = mcts(max_iter=max_iter)

    return Solution(
        # Carry the search's own verdict. This was hardcoded `True`, which made
        # `WorkflowPlan.Generate`'s `not result.complete` guard dead and let an
        # exhausted search return whatever timeline it happened to be holding
        # -- a plan of exactly `max_iter` steps that never reaches the target,
        # reported as a solution. `tests/solver/test_incomplete_search.py`.
        complete=solution.complete,
        dependency_plan=solution.state.steps,
        merged_endpoints=solution.merged_endpoints,
        _frontier=solution._frontier,
        _history=solution._history,
        _refiner_histories=solution._refiner_histories,
        _heuristics={
            "production depth": solution._production_depths,
            D2T_KEY: d2t_report,
            "demand2producer": demand2producer,
            "demand2product": demand2product,
            "product2consumer": product2consumer,
            "distance_scores": distance_scores,
            "opportunity_scores": opportunity_scores,
        },
        _iterations=solution._iterations,
        _refiner_iterations=solution._refiner_iterations,
        _relavent_transforms=relavent_transforms,
    )
    
