from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable, Generator, Any, TypeVar, Generic
import json
import re
from pathlib import Path
from collections import deque
from operator import attrgetter

from ..hashing import KeyGenerator
from .dag_renderer import DagRenderer, Label, LabelMode, NodeKind
from .solver_rng import DecisionStream, argmax_index, argmin_index
from .solver_math import entropy
from .solver_bound import min_depth_between, objective_ceiling
from .solver_policy import ActivePolicy, Arm

# Attribute reads for the non-adaptive selection path. `attrgetter` is a C-level
# call, which is the point: this runs once per frontier entry per selection and
# a Python lambda here is measurable on the default path.
_mcts_scores = attrgetter("score")
_refiner_scores = attrgetter("scores")

#: POC. Off by default: the cutoff is only sound while the ceiling in
#: `solver_bound` stays above the true optimum, and a ceiling that slips below it
#: changes plans and every fingerprint with them.
REFINER_ORACLE: bool = False

class UseRefinerOracle:
    """Enable the refiner's branch and bound cutoff for the duration of a block."""
    def __init__(self, enabled: bool=True): self._want = enabled; self._prev = False
    def __enter__(self):
        global REFINER_ORACLE
        self._prev, REFINER_ORACLE = REFINER_ORACLE, self._want
        return self
    def __exit__(self, *_):
        global REFINER_ORACLE
        REFINER_ORACLE = self._prev
        return False



def active_policy_name() -> str:
    return ActivePolicy().name

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

    def __hash__(self) -> int:
        return self.hash

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, Node) and self.hash == __value.hash

    def __str__(self) -> str:
        return f"<{self._json_dumps(self.Pack(parents=False)['properties']).replace('"', '')}:{self.key}>"

    def __repr__(self) -> str:
        return f"{self}"

    def IsA(self, other: Node) -> bool:
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
        if type(raw_props) in {list, set}:
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
        for k in list(formatted_props.keys()):
            if k == NO_KEY: continue
            if len(formatted_props[k])==1:
                formatted_props[k] = formatted_props[k][0]
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

class Dependency(Node):
    def __init__(self, properties: set[str], parents: set[Dependency]) -> None:
        super().__init__(properties=properties, parents=set(parents))

    def __str__(self) -> str:
        return f"(D:{'-'.join(sorted(list(self.properties)))})"

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
    production: dict[Dependency, list[Endpoint]]
    have: set[Endpoint]
    candidate_transforms: set[Transform]

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
    #: Transform of the step this state swapped in, or None for the state the
    #: refiner started from. Only the selection policy reads it: it is the key
    #: statistics are shared under, and nothing about the search or the score
    #: depends on it.
    _swapped_in: Any = None
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
    _refiner_iterations: list[tuple[int, int]]
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
    
def _canonicalise_givens(given: list[set[Endpoint]]):
    """Collapse the givens and their lineage to one object per logical endpoint.

    A caller may hand in the same logical input twice as two objects -- a parent
    reached through `Clone()` beside the given it is a parent of -- and five of
    the eleven shipped templates do. `encode_problem`'s interner already collapses
    them for the rust path, so only the python path ever saw the duplicates, and
    the two backends disagreed about how many endpoints a problem even has.

    Rewrites `parents` to the canonical object rather than rebuilding anything, so
    the caller keeps the identity of every endpoint it actually passed. A
    signature is built from its parents' keys, and equal parents have equal keys,
    so no signature moves and one pass suffices. `Endpoint.__eq__` is signature
    equality, so no caller can observe the substitution except through `is`.
    """
    roots = [e for group in given for e in group]
    todo, seen, order = list(roots), set(), []
    while todo:
        e = todo.pop()
        if id(e) in seen: continue
        seen.add(id(e))
        order.append(e)
        todo.extend(e.parents)
    # Roots first, so a given the caller passed always wins over a lineage-only
    # copy of itself.
    canon: dict[str, Endpoint] = {}
    for e in roots: canon.setdefault(e.Signature(), e)
    for e in order: canon.setdefault(e.Signature(), e)
    for e in order:
        if len(e.parents) > 0:
            e.parents = {canon[p.Signature()] for p in e.parents} # type: ignore

def solve_by_mcts(
    given: list[set[Endpoint]],
    transforms: Iterable[Transform],
    target: Transform,
    seed: int=42,
    max_iter: int=256,
    max_refine: int=256,
) -> Solution:
    from .solver_backend import _get_solver_class
    _canonicalise_givens(given)
    return _get_solver_class()().Solve(
        given, transforms, target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
    )

def _solve_by_mcts_python(
    given: list[set[Endpoint]],
    transforms: Iterable[Transform],
    target: Transform,
    seed: int=42,
    max_iter: int=256,
    max_refine: int=256,
) -> Solution:
    rng = DecisionStream(seed)

    def _collect_all_ancestors(endpoints: set[Endpoint]) -> set[Endpoint]:
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
        for e in sorted(group, key=lambda x: x.Signature()):
            d = given_tr.AddProduct(e)
            pgroup[d] = e
        given_appl.produced.append(pgroup)
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

    given_tr = given_appl.transform
    def _iter_transforms():
        yield given_tr
        for tr in transforms: yield tr
        yield target

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
    _rank_of_transform = _transform_rank.__getitem__
    _rank_of_dependency = _dep_rank.__getitem__
    def _by_transform(trs) -> list[Transform]:
        return sorted(trs, key=_rank_of_transform)
    def _by_dependency(deps) -> list[Dependency]:
        return sorted(deps, key=_rank_of_dependency)

    product2consumer: dict[Dependency, set[Transform]] = {}
    for parent in _iter_transforms():
        for child in _iter_transforms():
            if parent == child: continue
            for pgroup in parent.produces:
                for p in pgroup:
                    if not any(p.IsA(c) for c in child.requires): continue
                    product2consumer[p] = product2consumer.get(p, set())|{child}
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
    demand2product = {c: _by_dependency(v) for c, v in demand2product.items()}
    demand2producer = {c: _by_transform(v) for c, v in demand2producer.items()}

    opportunity_scores: dict[Transform, int] = {}
    distance_scores: dict[Transform, int] = {}
    todo: deque[tuple[Transform, int]] = deque([(target, -1)])
    while len(todo)>0:
        node, consumer_distance = todo.popleft()
        dist = consumer_distance+1
        opportunity_scores[node] = opportunity_scores.get(node, 1)+dist
        if node in distance_scores: continue
        distance_scores[node] = dist
        for p in node.requires:
            for producer in demand2producer.get(p, ()):
                todo.append((producer, dist))
    relavent_transforms = [tr for tr in transforms if tr in distance_scores]
    if given_appl.transform not in distance_scores:
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
                "distance_scores": distance_scores,
                "opportunity_scores": opportunity_scores,
                "no_path_possible": True,
            },
            _iterations=0,
            _refiner_iterations=[],
            _relavent_transforms=list(relavent_transforms),
        )
    max_distance_score = max(distance_scores.values())

    D2T_KEY = "distance to target"
    d2t_report = {k.key:float(v) for k, v in distance_scores.items()}

    def _prune_irrelavent_values(d: dict, value_whitelist: set):
        for k, v in d.items():
            d[k] = [x for x in v if x in value_whitelist] if isinstance(v, list) \
                else value_whitelist.intersection(v)
    rts = set(relavent_transforms)|{given_tr, target}
    _prune_irrelavent_values(product2consumer, rts)
    _prune_irrelavent_values(demand2producer, rts)
    rtsp = {p for t in rts for g in t.produces for p in g}
    _prune_irrelavent_values(demand2product, rtsp)

    def generate_applications_of_transform(
        state_k: int,
        production: dict[Dependency, list[Endpoint]],
        blacklist: set[str],
        tr: Transform,
        mock_produced: list[dict[Dependency, Endpoint]]|None=None
    ) -> list[Application]:
        if len(tr.requires)==0:
            appl = Application(initial_timeline=state_k, transform=tr, used={}, produced=[{}])
            if appl.Signature() in blacklist: return []
            appl.produced = [{p:Endpoint(properties=p.properties) for p in pgroup} for pgroup in tr.produces]
            return [appl]
        
        handle_lineage = mock_produced is None

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
            for product in demand2product.get(p, ()):
                if product not in production: continue
                for e in production[product]:
                    assert e.IsA(p)
                    if e in given_endpoints:
                        given_candidates.append(e)
                    else:
                        produced_candidates.append(e)
            if include_produced:
                return given_candidates + produced_candidates
            if given_candidates:
                return given_candidates
            return produced_candidates

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
                    if appl.Signature() in blacklist: continue
                    if handle_lineage:
                        lineage: set = {ancestor for e in used.values() for ancestor in e.parents}
                        lineage.update(used.values())
                        appl.produced = [{p:Endpoint(p.properties, parents=lineage) for p in pgroup} for pgroup in tr.produces]
                    else:
                        appl.produced = [mock for _, mock in zip(tr.produces, mock_produced)]
                    viable_input_sets.append(appl)
                    continue
                next_i = p_i+1
                todo += [
                    (next_i, e, used) for e in matches[tr.requires[next_i]]
                ]
            return viable_input_sets, reached_leaf

        viable_input_sets, reached_leaf = _resolve(include_produced=False)
        if not reached_leaf:
            viable_input_sets, _ = _resolve(include_produced=True)
        return viable_input_sets

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

        start = PruneNode(steps[-1])
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
    
    def get_order(steps: list[Application]):
        seen: set[str] = set()
        _have: set[Endpoint] = set()
        order: dict[str, int] = {e.key:0 for e in _have}
        while len(seen)<len(steps):
            reachable: list[Application] = []
            for step in steps:
                if step.Signature() in seen: continue
                if any(e not in _have for e in step.used.values()): continue
                seen.add(step.Signature())
                reachable.append(step)
            if len(reachable)==0: break
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

        _targeti = -1
        for i, s in enumerate(steps):
            if all(len(g) == 0 for g in s.produced):
                _targeti = i
                break
        assert _targeti >= 0
        steps[_targeti], steps[-1] = steps[-1], steps[_targeti]
        if prune:
            steps = prune_steps(steps)

        _product2consumer: dict[Endpoint, list[Application]] = {}
        for step in steps:
            for e in step.used.values():
                _product2consumer[e] = _product2consumer.get(e, [])+[step]

        # The search builds endpoints as VALUES -- a fresh object per candidate
        # application -- so one logical product exists as several equal objects,
        # and this map is the only thing that turns them into one instance. It is
        # therefore keyed by signature: a consumer holds a different object from
        # the one its producer emitted, and equality is all they share.
        #
        # What it must NOT do is reuse an instance across two producing steps.
        # That merges two transforms' outputs into one endpoint with two
        # producers, which compiles to two processes writing one file. Every
        # product below is a fresh instance for that reason.
        endpoint_map: dict[Endpoint, Endpoint] = {}
        def _fix_endpoints(appl: Application):
            if appl.transform is given_tr:
                # A given is already what it claims to be. `lineage` is empty and
                # `e.parents & inherent_parents` is `e.parents`, because a given's
                # parents are ancestors of a given by definition -- so rebuilding
                # is a copy with no change of content. Making that copy mints a
                # second object for one input while the lineage kept below still
                # names the first, which is where the endpoint twins came from.
                for pgroup in appl.produced:
                    for e in pgroup.values():
                        endpoint_map[e] = e
            else:
                lineage: set[Endpoint] = set()
                for p in appl.transform.requires:
                    e = appl.used[p]
                    e = endpoint_map.get(e, e)
                    appl.used[p] = e
                    lineage.add(e)
                    lineage.update(e.parents) # type: ignore
                new_produced = []
                for pgroup in appl.produced:
                    new_pgroup = {}
                    for p, e in pgroup.items():
                        # Always fresh. Reusing a mapped instance here is what
                        # gave one endpoint two producers.
                        new_e = Endpoint(e.properties, parents=lineage|(e.parents&inherent_parents)) # type: ignore
                        new_pgroup[p] = new_e
                        endpoint_map[e] = new_e
                    new_produced.append(new_pgroup)
                appl.produced = new_produced
            appl._sig = None
            appl._hash = None
            appl.Signature()
        
        node_order = get_order(steps)
        todo: list[Application] = steps.copy()
        order = [node_order[s.Signature()] for s in todo]
        while len(todo)>0:
            si: int = argmin_index(order)
            todo[si], todo[-1] = todo[-1], todo[si]
            order[si], order[-1] = order[-1], order[si]
            order.pop()
            appl = todo.pop()
            _fix_endpoints(appl)
        return steps
    
    @dataclass
    class RefinerResult:
        steps: list[Application]
        _history: list[RefinerState]
        _iterations: int
        _found_on: int
        #: Set when the cutoff proved no reachable plan can beat the incumbent.
        _certified: bool = False
        _ceiling: float|None = None
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

            def _is_valid(target_appl: Application):
                have: set[Endpoint] = {
                    e for pgroup in given_appl.produced for e in pgroup.values()
                }
                pending: list[Application] = list(state.steps)
                while len(pending) > 0:
                    ready = [
                        s for s in pending
                        if all(e in have for e in s.used.values())
                    ]
                    if len(ready) == 0: return False # looped
                    for s in ready:
                        have |= {e for pgroup in s.produced for e in pgroup.values()}
                    scheduled = {id(s) for s in ready}
                    pending = [s for s in pending if id(s) not in scheduled]


                for step in _iter_steps():
                    for p, e in step.used.items():
                        for pproto in p.parents:
                            lineage_constraint_e = step.used[pproto] # type: ignore
                            if not _has_ancestor(e, lineage_constraint_e): return False
                return True
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
                    rejected = not _lineage_ok()
                except KeyError:
                    rejected = False
                state.valid = False if rejected else _is_valid(target_appl)

                
        _slot_cache: dict[Transform, list[tuple[Dependency, Dependency]]] = {}
        # The (requirement, lineage anchor) pairs of a transform, in scoring
        # order. Held across candidates: the sort inside is over a transform
        # that does not change, and the refiner rescores a whole plan per
        # candidate.
        def _lineage_slots(tr: Transform) -> list[tuple[Dependency, Dependency]]:
            slots = _slot_cache.get(tr)
            if slots is None:
                slots = [
                    (p, lin_p)
                    for p in tr.requires
                    for lin_p in _by_dependency(p.parents)
                ]
                _slot_cache[tr] = slots
            return slots

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
            e_score = entropy(list(lineage_usage.values()))

            # Keyed by `Node.hash` rather than by the node: `Node.__eq__` is
            # already hash equality, so this decides the same thing, and the
            # walk below is the refiner's hottest loop by a wide margin -- a
            # python-level `__hash__` per dict probe there is most of its cost.
            _product2producer: dict[int, Application] = {}
            for step in _steps:
                for pgroup in step.produced:
                    for e in pgroup.values():
                        _product2producer[e.hash] = step
            # A walk is resumable rather than run to exhaustion. The stack is
            # LIFO and the first visit to a node fixes its depth, so the value
            # for the anchor is final the moment the walk reaches it and the
            # rest of the traversal cannot change it. `_depth_walks` holds the
            # suspended stack; its absence for a source means that source's
            # walk ran out, so a miss there is a real "not an ancestor".
            _depth_maps: dict[int, dict[int, int]] = {}
            _depth_walks: dict[int, list[tuple[int, int]]] = {}
            def _depth_between(e: Endpoint, a: Endpoint) -> int:
                ek, ak = e.hash, a.hash
                depths = _depth_maps.get(ek)
                if depths is None:
                    depths = {}
                    _depth_maps[ek] = depths
                    todo = [(ek, 0)]
                    _depth_walks[ek] = todo
                else:
                    d = depths.get(ak)
                    if d is not None: return d
                    todo = _depth_walks.get(ek)
                    if todo is None: return -1
                p2p = _product2producer
                while len(todo)>0:
                    n, d = todo.pop()
                    if n in depths: continue
                    depths[n] = d
                    nd = d+1
                    for pe in p2p[n].used.values():
                        todo.append((pe.hash, nd))
                    if n == ak: return d
                del _depth_walks[ek]
                return -1
            _n_steps = len(_steps)
            def _max_distance_to(e: Endpoint, a: Endpoint):
                max_d = _depth_between(e, a)
                return max_d/_n_steps if max_d>0 else 1.0
            lin_distances: list[float] = []
            for step in _steps:
                for p, lin_p in _lineage_slots(step.transform):
                    e = step.used[p]
                    pe= step.used[lin_p] # type: ignore
                    lin_distances.append(_max_distance_to(e, pe))
            if len(lin_distances)>0:
                lin_score = -sum(lin_distances)/len(lin_distances)
            else:
                lin_score = 0
            score = e_score*1000+lin_score
            vscore = score*state.valid
            state.scores = [score, vscore]
        
        policy = ActivePolicy().fork()
        _adaptive = policy.wants_observations
        def select_node(frontier: list[RefinerState]) -> int:
            if _adaptive:
                return policy.select(rng, [
                    Arm(s.scores, s._swapped_in, (s.scores[0], 1.0 if s.valid else 0.0))
                    for s in frontier
                ])
            return policy.select_from(rng, frontier, _refiner_scores)

        def remove_node(frontier: list[RefinerState], index: int):
            frontier[index], frontier[-1] = frontier[-1], frontier[index]
            return frontier.pop()

        def expand_node(state: RefinerState):
            current_applications = {s.Signature() for s in state.steps}
            production: dict[Dependency, list[Endpoint]] = {}
            for step in state.steps:
                for pgroup in step.produced:
                    for p, e in pgroup.items():
                        production[p] = production.get(p, [])+[e]
            for step in state.steps:
                base = [s for s in state.steps if s.Signature() != step.Signature()]
                base_sigs = sorted(s.Signature() for s in base)
                for appl in generate_applications_of_transform(
                    state_k=step.initial_timeline,
                    production=production,
                    blacklist=current_applications,
                    tr=step.transform,
                    mock_produced=step.produced,
                ):
                    appl._iteration = step._iteration
                    yield "".join(sorted(base_sigs+[appl.Signature()])), base, appl

        def _ceiling_for(steps: list[Application]) -> float:
            production: dict[Dependency, list[Endpoint]] = {}
            producer_inputs: dict[Endpoint, list[Endpoint]] = {}
            for step in steps:
                _from = list(step.used.values())
                for pgroup in step.produced:
                    for p, e in pgroup.items():
                        production[p] = production.get(p, [])+[e]
                        producer_inputs[e] = _from
            def _candidates(dep: Dependency) -> set:
                out: set = set()
                for product in demand2product.get(dep, ()):
                    out |= set(production.get(product, ()))
                return out
            anchor_candidates: list[set] = []
            min_depths: list[int] = []
            n_slots = 0
            for step in steps:
                for p in step.transform.requires:
                    n_slots += 1
                    for lin_p in _by_dependency(p.parents):
                        anchor_candidates.append(_candidates(lin_p)) # type: ignore
                        min_depths.append(min_depth_between(
                            producer_inputs, _candidates(p), _candidates(lin_p), # type: ignore
                        ))
            return objective_ceiling(
                n_steps=len(steps), n_usages=max(1, n_slots),
                anchor_candidates=anchor_candidates, lineage_min_depths=min_depths,
            )

        initial_state = RefinerState(
            steps=initial_solution,
            valid=True,
        )
        score_node(initial_state)
        _ceiling = _ceiling_for(initial_solution) if REFINER_ORACLE else None
        _certified = False
        frontier: list[RefinerState] = [initial_state]
        seen: set[str] = {initial_state.Signature()}
        valids: list[RefinerState] = [initial_state]
        history: list[RefinerState] = []
        i = 0
        # The incumbent a refiner iteration is trying to beat. `scores[1]` is
        # unusable for this: it is `score*valid` over a score that is never
        # positive, so an invalid state's 0.0 outranks every valid one. The
        # final pick is safe because it maximises over `valids` alone; a reward
        # computed over the frontier is not, and has to read validity itself.
        incumbent = initial_state.scores[0] if initial_state.valid else float("-inf")
        while len(frontier)>0 and i<max_iters:
            if _ceiling is not None and incumbent >= _ceiling - 1e-9:
                _certified = True
                break
            i += 1
            statei = select_node(frontier)
            state = remove_node(frontier, statei)
            state._iteration = i
            history.append(state)
            if state.valid:
                valids.append(state)
            improved = 0.0
            for sig, base, appl in expand_node(state):
                if sig in seen: continue
                seen.add(sig)
                child = RefinerState(steps=base+[appl], _sig=sig)
                score_node(child)
                child._swapped_in = appl.transform
                if child.valid and child.scores[0] > incumbent:
                    incumbent = child.scores[0]
                    improved = 1.0
                frontier.append(child)
            if _adaptive:
                # Through `reward_for` like the mcts site, not as a raw 0/1:
                # otherwise a policy configured not to estimate a value still
                # accumulates one here, and the two callers disagree about what
                # the same configuration means.
                policy.observe(
                    state._swapped_in, policy.reward_for(improved > 0, 0.0, 0.0)
                )
        si: int = argmax_index([s.scores[1] for s in valids])
        refined = valids[si]
        return RefinerResult(
            steps=rectify(refined.steps),
            _history=history,
            _iterations=i,
            _found_on=refined._iteration,
            _certified=_certified,
            _ceiling=_ceiling,
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

        mcts_policy = ActivePolicy().fork()
        _mcts_adaptive = mcts_policy.wants_observations
        def select_node(frontier: list[Application]):
            if _mcts_adaptive:
                return mcts_policy.select(
                    rng, [Arm(a.score, a.transform) for a in frontier]
                )
            return mcts_policy.select_from(rng, frontier, _mcts_scores)

        _target_requirements = list(target.requires)
        def progress_of(state: SolverState) -> float:
            """How close this state is to being able to apply the target.

            Counting the target's satisfied requirements is the load-bearing
            term, and the distance table is a fraction of one requirement
            underneath it as a tie-break. The count is what makes this usable as
            a reward at all: `candidate_transforms` only ever grows, so anything
            read off it alone rises monotonically along every path, and crediting
            an action by that would rank transforms by how late they are usually
            applied rather than by whether they got anywhere.

            A transform becoming a *candidate* is not the same as its inputs
            being satisfiable -- `product2consumer` admits a transform as soon as
            one of its inputs exists -- which is why the requirement count is
            taken against `have` rather than against the candidate set.
            """
            met = 0
            for d in _target_requirements:
                for e in state.have:
                    if e.IsA(d):
                        met += 1
                        break
            closeness = 0.0
            if max_distance_score > 0:
                best = None
                for tr in state.candidate_transforms:
                    dd = distance_scores.get(tr)
                    if dd is None: continue
                    if best is None or dd < best: best = dd
                if best is not None:
                    closeness = max(0.0, 1.0-best/max_distance_score)
            return (met+closeness)/(len(_target_requirements)+1)

        def remove_node(frontier: list[Application], index: int):
            frontier[index], frontier[-1] = frontier[-1], frontier[index]
            return frontier.pop()

        def expand_node(state: SolverState, appl: Application) -> list[SolverState]:
            possibilities = []
            is_sample_branching = appl.transform is given_tr and len(appl.produced) > 1
            if is_sample_branching:
                state_ks = [new_state_k(state.k) for _ in appl.produced]
                for group, state_k in zip(appl.produced, state_ks):
                    candidate_transforms = state.candidate_transforms.copy()
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
                        produced=[group],
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
                for appl in generate_applications_of_transform(
                    state_k=state.k,
                    production=state.production,
                    blacklist=frontier_signatures,
                    tr=tr
                ):
                    yield appl

        merged_endpoints: dict[Endpoint, set[Endpoint]] = {}
        def merge_states(source: SolverState, alt: SolverState) -> SolverState:
            e2consumer: dict[Endpoint, list[Application]] = {}
            for step in alt.steps:
                for d, e in step.used.items():
                    e2consumer[e] = e2consumer.get(e, [])+[step]
            e2producer: dict[Endpoint, Application] = {}
            for step in alt.steps:
                for pgroup in step.produced:
                    for d, e in pgroup.items():
                        e2producer[e] = step
            #                 lineage_constraints.add(step.used[p]) # type: ignore
            
            def _get_substitute(alt_step: Application):
                candidates = source_tr2appl.get(alt_step.transform, [])
                for src_step in candidates:
                    subs: list[tuple[Endpoint, Endpoint]] = []
                    for d, se in src_step.used.items():
                        ae = alt_step.used[d]
                        subs.append((ae, se))
                    src_pool, alt_pool = [], []
                    for sg, ag in zip(src_step.produced, alt_step.produced):
                        added = set()
                        for d, se in sg.items():
                            if d not in ag:
                                src_pool.append(se)
                                continue
                            ae = ag[d]
                            subs.append((ae, se))
                            added.add(d)
                        for d, ae in ag.items():
                            if d not in added:
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
            to_check.reverse()

            to_merge: list[tuple[Application, Application]] = []
            to_add_from_alt: list[Application] = []
            for step in to_check:
                src_step = _get_substitute(step)
                if src_step is None:
                    to_add_from_alt.append(step)
                else:
                    to_merge.append((step, src_step))
            to_add_from_alt.reverse()
            to_merge.reverse()
            common_steps: list[Application] = [s for s in alt.steps if s.initial_timeline in source_timelines]
            _merged = {src_step for _, src_step in to_merge}
            to_add_from_src: list[Application] = [s for s in source.steps if s.initial_timeline not in alt_timelines and s not in _merged]
            merged_steps: list[Application] = []
            swapped_endpoints: dict[Endpoint, Endpoint] = {}
            for alt_step, src_step in to_merge:
                for ad, ae in alt_step.used.items():
                    se = src_step.used[ad]
                    se = swapped_endpoints.get(se, se)
                    src_step.used[ad] = se
                    swapped_endpoints[ae] = se
                    merged_endpoints[se] = merged_endpoints.get(se, {se})|{ae}
                merged_pgroup = src_step.produced.copy()
                for mp in alt_step.produced:
                    mk = set(mp)
                    if any(mk==set(pgroup) for pgroup in src_step.produced): continue
                    merged_pgroup.append(mp)
                src_step.produced = merged_pgroup
                src_step._sig = None
                src_step.Signature()
                merged_steps.append(src_step)
            for step in merged_steps:
                for pgroup in step.produced:
                    for d in pgroup:
                        e = pgroup[d]
                        pgroup[d] = swapped_endpoints.get(e, e)

            for step in to_add_from_alt:
                for pgroup in step.produced:
                    for d in pgroup:
                        e = pgroup[d]
                        pgroup[d] = swapped_endpoints.get(e, e)
            source.k = alt.k
            source.steps = common_steps+to_add_from_src+to_add_from_alt+merged_steps
            order = get_order(source.steps)
            source.steps = order_steps(order, source.steps)
            source.steps = rectify(source.steps, prune=False, insert_given=False)
            return source

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
            # Kept paired with the source it came from, rather than flattened
            # in one comprehension, so the policy can be told what this
            # application *changed* and not merely where it landed. The
            # flattened order is unchanged.
            expansions = [(s, expand_node(s, node)) for s in source_states]
            next_states = [ns for _, group in expansions for ns in group]
            history.append(carry_over+next_states)
            remain: list[SolverState] = []
            for s in next_states:
                if is_solved(s):
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

            if mcts_policy.wants_observations:
                solved_here = len(remain) < len(next_states)
                best_before, best_after = 0.0, 0.0
                if not solved_here and mcts_policy.wants_rewards:
                    for src, group in expansions:
                        before = progress_of(src)
                        for ns in group:
                            after = progress_of(ns)
                            if after-before >= best_after-best_before:
                                best_before, best_after = before, after
                mcts_policy.observe(
                    node.transform,
                    mcts_policy.reward_for(solved_here, best_before, best_after),
                )

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
            for state in remain:
                applied_transforms: set[Transform] = set()
                for child in generate_child_nodes(state, frontier_signatures):
                    child = score_node(child)
                    child._iteration = -i
                    frontier_signatures.add(child.Signature())
                    applied_transforms.add(child.transform)
                    frontier.append(child)
                state.candidate_transforms -= applied_transforms
            current_timelines = carry_over+remain
            
        return MctsResult(
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
