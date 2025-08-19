from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable, Generator, Any, TypeVar, Generic
import numpy as np
import json

from ..hashing import KeyGenerator

class Node:
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
        return other.properties.issubset(self.properties)

    def Signature(self):
        if self._sig is None:
            psig = ",".join(sorted(p.key for p in self.parents))
            sig = "".join(sorted(self.properties))
            _, sig = KeyGenerator.FromStr(sig)
            self._sig = f'{sig}:[{psig}]' if len(self.parents)>0 else sig
        return self._sig

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
        raw_props = d["properties"]
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
        self.produces: list[Dependency] = list()
        self._update_hash()

    def __str__(self) -> str:
        def _props(d: Dependency):
            return "{"+"-".join(sorted(d.properties))+"}"
        return f"{','.join(_props(r) for r in self.requires)}->{','.join(_props(p) for p in self.produces)}"

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return self.hash

    def _update_hash(self):
        self.hash, self.key = KeyGenerator.FromStr(str(self))

    def AddRequirement(self, example: Node|None=None, properties: Iterable[str]|None=None, parents: set[Dependency]|None=None):
        return self._add_dependency(destination=self.requires, example=example, properties=properties, parents=parents)

    def AddProduct(self, example: Node|None=None, properties: Iterable[str]|None=None, parents: set[Dependency]|None=None):
        return self._add_dependency(destination=self.produces, example=example, properties=properties, parents=parents)

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
class Application:
    transform: Transform
    used: dict[Dependency, Endpoint]
    produced: dict[Dependency, Endpoint]
    score: list[float] = field(default_factory=list)
    _iteration: int = -1
    _sig: str|None = None
    _hash: int|None = None
    def Signature(self):
        if self._sig is None: 
            self._sig = self.transform.key + "".join({self.used[p].key for p in self.transform.requires})
        return self._sig
    def __hash__(self) -> int:
        if self._hash is None:
            self._hash, _ = KeyGenerator.FromStr(self.Signature())
        return self._hash
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Application): return False
        return self._hash == value._hash

@dataclass
class SolverState:
    steps: list[Application]
    production: dict[Dependency, list[Endpoint]] # product dep to produced endpoint
    have: set[Endpoint]
    candidate_transforms: set[Transform] # may not be valid, holds use count

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
    _frontier: list[Application]
    _history: list[SolverState]
    _refiner_history: list[RefinerState]
    _heuristics: dict[str, dict[str, float]]
    _iterations: int
    _refiner_iterations: tuple[int, int] # found at, total expanded
    _relavent_transforms: list[Transform]
    
def solve_by_mcts(
    given: Iterable[Endpoint],
    transforms: Iterable[Transform],
    target: Transform,
    seed: int=42,
    max_iter: int=256,
    max_refine: int=256,
) -> Solution:
    np.random.seed(seed)
    # ---
    # monte carlo tree search

    given_tr = Transform()
    given_appl = Application(given_tr, used={}, produced={})
    for e in given:
        p = given_tr.AddProduct(properties=e.properties)
        given_appl.produced[p] = e
    def _iter_transforms():
        yield given_tr
        for tr in transforms: yield tr
        yield target
    # produced dependency to consuming transform
    product2consumer: dict[Dependency, set[Transform]] = {}
    for parent in _iter_transforms():
        for child in _iter_transforms():
            if parent == child: continue
            for p in parent.produces:
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
                for p in parent.produces:
                    if not p.IsA(c): continue
                    demand2product[c] = demand2product.get(c, set())|{p}
                    found = True
                if found:
                    demand2producer[c] = demand2producer.get(c, set())|{parent}

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
            for producer in demand2producer.get(p, []): # when tr requires a terminal endpoint that is not given
                todo.append(DistNode(producer, dist, path))
    relavent_transforms = [tr for tr in transforms if tr in distance_scores]
    max_distance_score = max(distance_scores.values())
    
    def _prune_irrelavent_values(d: dict, value_whitelist: set):
        for k, v in d.items():
            d[k] = value_whitelist.intersection(v)
        # for k in list(d):
        #     if len(d[k])==0: del d[k]
    rts = set(relavent_transforms)|{given_tr, target}
    _prune_irrelavent_values(product2consumer, rts)
    _prune_irrelavent_values(demand2producer, rts)
    rtsp = {p for t in rts for p in t.produces}
    _prune_irrelavent_values(demand2product, rtsp)

    # should not perform mutations
    def generate_applications_of_transform(
        production: dict[Dependency, list[Endpoint]],
        blacklist: set[str],
        tr: Transform,
        mock_produced: dict[Dependency, Endpoint]|None=None
    ) -> list[Application]:
        if len(tr.requires)==0:
            appl = Application(tr, used={}, produced={})
            if appl.Signature() in blacklist: return []
            appl.produced = {p:Endpoint(properties=p.properties) for p in tr.produces}
            return [appl]
        
        # if mock_produced is given, do not check for lineage,
        # return all possibilities, and use mock_produced for the new applications
        handle_lineage = mock_produced is None

        # Transforms define lineage constraints (LC) first.
        # The endpoint matched to the LC must also be used to satisfy all instances.
        # That is, if a transform specifies A via P and B via P, 
        # then endpoint P' matched to P must be used to create both A and B
        def _satisfies_lineage(e: Endpoint, p: Dependency, used: dict[Dependency, Endpoint]):
            for parent in p.parents:
                assert isinstance(parent, Dependency)
                matched = used[parent]
                # print(".   ", matched, e.parents, e)
                if matched not in e.parents: return False
            return True
        
        def _find_endpoints(p: Dependency):
            candidates: set[Endpoint] = set()
            for product in demand2product.get(p, []):
                if product not in production: continue
                for e in production[product]:
                    assert e.IsA(p)
                    candidates.add(e)
            return candidates
        
        # print("?  ", state.have)
        viable_input_sets: list[Application] = []
        matches: dict[Dependency, set[Endpoint]] = {}
        for p in tr.requires:
            candidates = _find_endpoints(p)
            # print("?  ", p, len(candidates))
            if len(candidates) == 0: return viable_input_sets # empty, for type def
            matches[p] = candidates
        # for k, v in matches.items():
            # print(" ?-  ", len(v), k, v)
        
        INITIAL_I = 0
        todo: list[tuple[int, Endpoint, dict[Dependency, Endpoint]]] = [
            (INITIAL_I, e, dict()) for e in matches[tr.requires[INITIAL_I]]
        ]
        while len(todo)>0:
            p_i, e, used = todo.pop()
            p = tr.requires[p_i]
            used = used|{p:e}
            # print("_  ", used)
            if handle_lineage and not _satisfies_lineage(e, p, used): continue
            if p_i >= len(tr.requires)-1:
                appl = Application(tr, used, {})
                if appl.Signature() in blacklist: continue
                if handle_lineage:
                    lineage: set = {ancestor for e in used.values() for ancestor in e.parents}
                    lineage.update(used.values())
                    appl.produced = {product:Endpoint(product.properties, parents=lineage) for product in tr.produces}
                else:
                    appl.produced = mock_produced
                viable_input_sets.append(appl)
                continue # at leaf (end of required dependencies)
            next_i = p_i+1
            todo += [
                (next_i, e, used) for e in matches[tr.requires[next_i]]
            ]
        return viable_input_sets
    
    @dataclass
    class MctsResult:
        complete: bool
        state: SolverState
        _frontier: list[Application]
        _history: list[SolverState]
        _iterations: int
    def mcts(max_iter: int):
        def is_solved(state: SolverState):
            last_transform = state.steps[-1].transform
            return last_transform == target

        def score_node(node: Application):
            dist = distance_scores[node.transform]
            dist = 1-dist/max_distance_score
            opportunity = opportunity_scores[node.transform]
            opportunity = 1-(1/(1+opportunity/10))
            node.score = [dist, opportunity]
            return node

        def select_node(frontier: list[Application]):
            probs = [75, 20, 5] # dist, opportunity, explore
            total_prob = sum(probs)
            probs = [x/total_prob for x in probs]
            p_i = np.random.choice(list(range(len(probs))), 1, p=probs)[0]
            if p_i<len(probs)-1: # exploit
                scores = np.array([s.score[p_i] for s in frontier])
                K = 1
                k = min(K, scores.shape[0])
                candidate_indexes = np.argpartition(scores, -k)[-k:]
                i: int = np.random.choice(candidate_indexes)
            else: # explore
                i = np.random.randint(0, len(frontier))
            return i

        def remove_node(frontier: list[Application], index: int):
            frontier[index], frontier[-1] = frontier[-1], frontier[index]
            return frontier.pop() # O(1) vs O(m) for arr.remove()

        def expand_node(state: SolverState, appl: Application):
            candidate_transforms = state.candidate_transforms.copy() # was free transform
            for p in appl.transform.produces:
                if p not in product2consumer: continue
                for linked in product2consumer[p]:
                    candidate_transforms.add(linked)
            production = state.production.copy()
            for p, e in appl.produced.items():
                production[p] = production.get(p, [])+[e]
            return SolverState(
                steps=state.steps+[appl],
                have=state.have|set(appl.produced.values()),
                candidate_transforms=candidate_transforms,
                production=production,
            )

        free_transforms = [t for t in relavent_transforms if len(t.requires)==0]
        def generate_child_nodes(state: SolverState):
            def _iter_transforms():
                for tr in state.candidate_transforms:
                    yield tr
                for tr in free_transforms:
                    yield tr
            for tr in _iter_transforms():
                # print("$ ", tr)
                for appl in generate_applications_of_transform(state.production, frontier_signatures, tr):
                    yield appl

        current_state = SolverState(
            steps=[],
            production={},
            have=set(),
            candidate_transforms=set(),
        )
        start = score_node(given_appl)
        frontier: list[Application] = [start]
        frontier_signatures: set[str] = {s.Signature() for s in frontier}
        history: list[SolverState] = []
        i: int = 0
        while len(frontier)>0 and i < max_iter:
            i += 1
            nodei = select_node(frontier)
            node = remove_node(frontier, nodei)
            node._iteration = i
            current_state = expand_node(current_state, node)
            # print(i, f"[{len(frontier)}]", node.transform)
            # for k in current_state.candidate_transforms:
            #     print("-", k)
            history.append(current_state)
            if is_solved(current_state):
                return MctsResult(
                    complete=True,
                    state=current_state,
                    _frontier=frontier,
                    _history=history,
                    _iterations=i,
                )
            applied_transforms: set[Transform] = set()
            for child in generate_child_nodes(current_state):
                # print(f"c", child.transform)
                child = score_node(child)
                child._iteration = -i
                frontier_signatures.add(child.Signature())
                applied_transforms.add(child.transform)
                frontier.append(child)
            current_state.candidate_transforms -= applied_transforms # all possibilities per tr explored
            # for s in frontier:
            #     print(f"f", s.transform)
            # print()

        return MctsResult(
            complete=False,
            state=current_state,
            _frontier=frontier,
            _history=history,
            _iterations=i,
        )

    # ---
    # prune spurious nodes, assumes last step is target
    def prune_steps(steps: list[Application]) -> list[Application]:
        e2source: dict[Endpoint, Application] = {}
        for step in steps:
            for e in step.produced.values():
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
        todo: list[PruneNode] = [start]
        seen: dict[str, PruneNode] = {}
        while len(todo)>0:
            node = todo.pop(0)
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
        _have: set[Endpoint] = {e for e in given}
        order: dict[str, int] = {e.key:0 for e in _have}
        while len(seen)<len(steps):
            reachable: set[Application] = set()
            # find and process separately to ensure 1 layer at a time 
            for step in steps:
                if step.Signature() in seen: continue
                if any(e not in _have for e in step.used.values()): continue
                seen.add(step.Signature())
                reachable.add(step)
            if len(reachable)==0: break # shouldn't happen/needed, but here to prevent endless loop
            for step in reachable:
                if len(step.used)>0:
                    step_depth = max(order[e.key] for e in step.used.values())+1
                else:
                    step_depth = 1
                order[step.Signature()] = step_depth
                for e in step.produced.values():
                    if e in order: continue
                    order[e.key] = step_depth+1
                _have |= {e for e in step.produced.values()}
        max_depth = max(order.values())+1
        for step in steps:
            k = step.Signature()
            if k in order: continue
            order[k] = max_depth
        return order
    
    def order_steps(order: dict[str, int], steps: list[Application]):
        return sorted(steps, key=lambda s: order[s.Signature()]*10000+len(s.used))
    
    solution = mcts(
        max_iter=max_iter
    )
    D2T_KEY = "distance to target"
    d2t_report = {k.key:float(v) for k, v in distance_scores.items()}
    if not solution.complete:
        return Solution(
            complete=False,
            dependency_plan=[],
            _frontier=solution._frontier,
            _history=solution._history,
            _refiner_history=[],
            _heuristics={
                D2T_KEY: d2t_report,
            },
            _iterations=solution._iterations,
            _refiner_iterations=0,
            _relavent_transforms=relavent_transforms,
        )

    pruned_steps = prune_steps(solution.state.steps)

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
                for e in appl.produced.values():
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
            # checks lineage constaint and no loops
            def _is_valid():
                e2appl: dict[Endpoint, list[Application]] = {}
                for appl in _iter_steps():
                    for e in appl.used.values():
                        e2appl[e] = e2appl.get(e, [])+[appl]
                todo = [(given_appl, set())]
                while len(todo)>0:
                    current, history = todo.pop()
                    if current.Signature() in history: return False # looped
                    history = history|{current.Signature()}
                    for e in current.produced.values():
                        for appl in e2appl.get(e, []):
                            todo.append((appl, history))
                # if here, then no loops
                # now check lineage
                for step in _iter_steps():
                    for p, e in step.used.items():
                        for pproto in p.parents:
                            lineage_constraint_e = step.used[pproto] # type: ignore
                            if not _has_ancestor(e, lineage_constraint_e): return False
                return True
            state.valid = _is_valid()
                
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
            def _entropy(a) -> float:
                a = np.array(a)
                p = a/a.sum()
                p = p[p>0]
                return float((p*np.log2(p)).sum())
            e_score = _entropy(list(lineage_usage.values()))

            _product2producer: dict[Endpoint, Application] = {}
            for step in _steps:
                for e in step.produced.values():
                    _product2producer[e] = step
            def _max_distance_to(e: Endpoint, a: Endpoint):
                todo = [(e, 0)]
                seen = set()
                max_d = -1
                while len(todo)>0:
                    n, d = todo.pop()
                    if n in seen: continue
                    seen.add(n)
                    if n == a:
                        max_d = max(max_d, d)
                    prod = _product2producer[n]
                    for pe in prod.used.values():
                        todo.append((pe, d+1))
                return max_d/len(_steps) if max_d>0 else 1.0
            lin_distances: list[float] = []
            for step in _steps:
                for p in step.transform.requires:
                    for lin_p in p.parents:
                        e = step.used[p]
                        pe= step.used[lin_p] # type: ignore
                        lin_distances.append(_max_distance_to(e, pe))
            if len(lin_distances)>0:
                lin_score = -sum(lin_distances)/len(lin_distances)
            else:
                lin_score = 0
            score = e_score*1000+lin_score
            _, k = KeyGenerator.FromStr(state.Signature(), l=4)
            vscore = score*state.valid
            state.scores = [score, vscore]
        
        def select_node(frontier: list[RefinerState]) -> int:
            probs = [75, 20, 5] # score, score * valid
            total_prob = sum(probs)
            probs = [x/total_prob for x in probs]
            p_i = np.random.choice(list(range(len(probs))), 1, p=probs)[0]
            if p_i<len(probs)-1: # exploit
                scores = np.array([s.scores[p_i] for s in frontier])
                K = 1
                k = min(K, scores.shape[0])
                candidate_indexes = np.argpartition(scores, -k)[-k:]
                i: int = np.random.choice(candidate_indexes)
            else: # explore
                i = np.random.randint(0, len(frontier))
            return i
        
        def remove_node(frontier: list[RefinerState], index: int):
            frontier[index], frontier[-1] = frontier[-1], frontier[index]
            return frontier.pop() # O(1) vs O(m) for arr.remove()

        def expand_node(state: RefinerState):
            current_applications = {s.Signature() for s in state.steps}
            production: dict[Dependency, list[Endpoint]] = {}
            for step in state.steps:
                for p, e in step.produced.items():
                    production[p] = production.get(p, [])+[e]
            for step in state.steps:
                # reuse the current endpoints and simply look for alternate edge comparisons
                # lineage constraint checked separately
                for appl in generate_applications_of_transform(
                    production, current_applications,
                    step.transform,
                    mock_produced=step.produced, # rectify later
                ):
                    appl._iteration = step._iteration
                    alt_sol = [s for s in state.steps if s.Signature() != step.Signature()]+[appl]
                    alt_state = RefinerState(steps=alt_sol)
                    yield alt_state
        
        # produce new set of endpoints so hashes are valid
        # and prune steps
        def rectify(solution: list[Application]):
            steps = [
                Application(
                    transform=step.transform,
                    used=step.used.copy(),
                    produced=step.produced.copy(),
                    score=step.score,
                    _iteration=step._iteration,
                ) for step in [given_appl]+solution
            ]

            # prune steps, place target step last, as required
            _targeti = -1
            for i, s in enumerate(steps):
                if len(s.produced) == 0:
                    _targeti = i
                    break
            assert _targeti >= 0
            steps[_targeti], steps[-1] = steps[-1], steps[_targeti]
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
                for p in appl.transform.produces:
                    e = appl.produced[p]
                    new_e = Endpoint(e.properties, parents=lineage)
                    appl.produced[p] = new_e
                    endpoint_map[e] = new_e
                    rev_emap[new_e] = e
                # force regenerate signature
                appl._sig = None
                appl._hash = None
            
            node_order = get_order(steps)
            todo: list[Application] = steps.copy()
            order = [node_order[s.Signature()] for s in todo]
            while len(todo)>0:
                si: int = np.argpartition(order, 0)[0]
                todo[si], todo[-1] = todo[-1], todo[si]
                order[si], order[-1] = order[-1], order[si]
                order.pop()
                appl = todo.pop()
                _fix_endpoints(appl) # mutates appl
            return steps

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
            for child in expand_node(state):
                if child.Signature() in seen: continue
                seen.add(child.Signature())
                score_node(child)
                frontier.append(child)
        scores = np.array([s.scores[1] for s in valids]) # take the valid score
        k = 1
        si: int = np.argpartition(scores, -k)[-k:][0]
        refined = valids[si]
        return RefinerResult(
            steps=rectify(refined.steps),
            _history=history,
            _iterations=i,
            _found_on=refined._iteration,
        )

    refined = refine_mcts(pruned_steps, max_refine)
    _steps = refined.steps
    node_order = get_order(_steps)
    ordered_steps = order_steps(node_order, _steps)

    return Solution(
        complete=True,
        dependency_plan=ordered_steps,
        _frontier=solution._frontier,
        _history=solution._history,
        _refiner_history=refined._history,
        _heuristics={
            "production depth": {k:float(v) for k, v in node_order.items()},
            D2T_KEY: d2t_report,
        },
        _iterations=solution._iterations,
        _refiner_iterations=(refined._found_on, refined._iterations),
        _relavent_transforms=relavent_transforms,
    )
