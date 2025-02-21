from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Generator, Iterable
from pathlib import Path
import json

from ..hashing import KeyGenerator
    
class Namespace:
    def __init__(self, key_length=5, seed: int|None=None, key_from_order=False) -> None:
        self.node_signatures: dict[int, str] = {}
        self._last_k: int = 0
        generator = KeyGenerator(seed=seed)
        self._kg = generator
        self._key_from_order = key_from_order
        self._KLEN = key_length
        self._MAX_K = len(self._kg.vocab)**self._KLEN
        self.transforms: dict[str, Transform] = {}

    def NewKey(self):
        self._last_k += 1
        assert self._last_k < self._MAX_K
        if self._key_from_order:
            key = self._kg.FromInt(self._last_k, self._KLEN)
        else:
            key = self._kg.GenerateUID(self._KLEN)
        return self._last_k, key

    def NewTransform(self, name: str|None = None):
        if name is None:
            name = len(self.transforms)
        t = Transform(self)
        self.transforms[name] = t
        return t
    
_DEFAULT_NAMESPACE = Namespace()
def _set_default_namespace(namespace: Namespace):
    global _DEFAULT_NAMESPACE
    _DEFAULT_NAMESPACE = namespace

class Hashable:
    def __init__(self, namespace: Namespace=None) -> None:
        if namespace is None: namespace = _DEFAULT_NAMESPACE
        self._namespace = namespace
        self.hash, self.key = namespace.NewKey()

    def __hash__(self) -> int:
        return self.hash
    
    def __eq__(self, __value: object) -> bool:
        K = "key"
        return hasattr(__value, K) and self.key == getattr(__value, K)

class Node(Hashable):
    def __init__(
        self,
        properties: set[str],
        parents: set[Node],
    ) -> None:
        super().__init__()
        assert isinstance(properties, set)
        assert isinstance(parents, set)
        self.properties = properties
        self.parents = parents
        self._sig: str|None = None
        # self._diffs = set()
        # self._sames = set()

    def __str__(self) -> str:
        return f"<{self._json_dumps(self.Pack(parents=False)['properties'])}:{self.key}>"

    def __repr__(self) -> str:
        return f"{self}"
    
    def IsA(self, other: Node) -> bool:
        # if other.key in self._diffs: return False
        # if other.key in self._sames: return True
        if not other.properties.issubset(self.properties):
            # self._diffs.add(other.key)
            return False
        # self._sames.add(other.key)
        # if compare_lineage: return  other.parents.issubset(self.parents)
        return True

    def Signature(self):
        if self._sig is None:
            psig = ",".join(sorted(p.Signature() for p in self.parents))
            sig = ",".join(sorted(self.properties))
            self._sig = f'{sig}:[{psig}]' if len(self.parents)>0 else sig
        return self._sig
    
    def _props_have_keys(self):
        if len(self.properties)==0: return False
        return next(iter(self.properties)).startswith("{")

    def AddProperty(self, value: str, key: str=None):
        if key is None:
            assert not self._props_have_keys(), "this endpoint's properties have keys"
            self.properties.add(self._json_dumps([value]))
        else:
            assert self._props_have_keys(), "this endpoint's properties do not have keys"
            self.properties.add(self._json_dumps({key:value}))
        return self
    
    def Clone(self, properties_only: bool=False):
        clone = self.__class__(
            properties=set(self.properties),
            parents={p.Clone() for p in self.parents},
        )
        if properties_only: return clone
        clone.hash, clone.key = self.hash, self.key
        return clone
    
    def WithLineage(self, parents: Iterable[Node]):
        image = self.__class__(
            properties=self.properties,
            parents=set(parents),
        )
        return image
    
    def AddAsDependency(self, transform: Transform, mapping: dict[Endpoint, Dependency]=None):
        if mapping is None: mapping = {}
        def _add(e: Endpoint):
            if e in mapping: return mapping[e]
            parent_deps = {_add(p) for p in e.parents}
            d = transform.AddRequirement(node=e, parents=parent_deps)
            mapping[e] = d
            return d
        return _add(self)

    @classmethod
    def _json_dumps(cls, d):
        return json.dumps(d, separators=(',', ':'), sort_keys=True)

    @classmethod
    def _encode_dict_props(cls, properties: dict):
        return {cls._json_dumps({k:v}) for k, v in properties.items()}

    @classmethod
    def _encode_list_props(cls, properties: list):
        return {cls._json_dumps([v]) for v in properties}
    
    @classmethod
    def Unpack(cls, d: dict):
        raw_props = d["properties"]
        if isinstance(raw_props, list):
            props = cls._encode_list_props(raw_props)
        elif isinstance(raw_props, dict):
            props = cls._encode_dict_props(raw_props)
        else:
            assert False, f"cant unpack type [{type(raw_props)}] instance [{raw_props}]"
        m = cls(
            properties=props,
        )
        if "parents" in d:
            m.parents = {cls.Unpack(x) for x in d["parents"]}
        if "_hash" in d:
            m.hash, m.key = d["_hash"].split("/")
            m.hash = int(m.hash)
        return m

    def Pack(self, parents=False, save_hash=False):
        if len(self.properties)==0:
            props = []
        elif next(iter(self.properties)).startswith("{"):
            props = {k:v for p in self.properties for k, v in json.loads(p).items()}
        else:
            def _unlist(s: str):
                if s.startswith("[") and s.endswith("]"): return s[1:-1]
                return s
            props = [_unlist(p) for p in self.properties]
        d = {
            "properties": props,
        }
        if save_hash:
            d["_hash"] = f"{self.hash}/{self.key}"
        if len(self.parents)>0 and parents:
            d["parents"] = [x.Pack() for x in self.parents]
        return d

# of a Transform
class Dependency(Node):
    def __init__(self, properties: set[str], parents: set[Node]) -> None:
        super().__init__(properties=properties, parents=parents)

    def __str__(self) -> str:
        return f"(D:{'-'.join(sorted(list(self.properties)))})"

# as in a free floating data type
class Endpoint(Node):
    def __init__(self, properties: set[str], parents: dict[Endpoint, Node]=dict()) -> None:
        if isinstance(parents, set):
            parents = {p:p for p in parents}
        super().__init__(properties=properties, parents=set(parents.keys()))
        self._parent_map = parents # real, proto
    
    def Iterparents(self):
        """real, prototype"""
        for e, p in self._parent_map.items():
            yield e, p

class Transform(Hashable):
    def __init__(self) -> None:
        super().__init__()
        self.requires: list[Dependency] = list()
        self.produces: list[Dependency] = list()
        self._input_group_map: dict[int, list[Dependency]] = {}
        self._key = self._namespace.NewKey()
        self._seen: set[str] = set()

    def __str__(self) -> str:
        def _props(d: Dependency):
            return "{"+"-".join(sorted(d.properties))+"}"
        return f"{','.join(_props(r) for r in self.requires)}->{','.join(_props(p) for p in self.produces)}"

    def AddRequirement(self, node: Node=None, properties: Iterable[str]=None, parents: set[Dependency]=None):
        return self._add_dependency(destination=self.requires, node=node, properties=properties, parents=parents)

    def AddProduct(self, node: Node=None, properties: Iterable[str]=None, parents: set[Dependency]=None):
        return self._add_dependency(destination=self.produces, node=node, properties=properties, parents=parents)

    def _add_dependency(self, destination: set[Dependency], node: Node=None, properties: Iterable[str]=None, parents: set[Dependency]=None):
        assert node is not None or properties is not None, "must provide either node or properties"
        if parents is None: parents = set()
        _properties = set(node.properties) if node is not None else set(properties)
        _dep = Dependency(properties=_properties, parents=parents)
        _parents = _dep.parents
        destination.append(_dep)
        if destination == self.requires:
            i = len(self.requires)-1
            for p in _parents:
                assert p in self.requires, f"{p} not added as a requirement"
            self._input_group_map[i] = self._input_group_map.get(i, [])+list(_parents)
        return _dep
    
    # just all possibilities regardless of lineage
    def Possibilities(self, have: set[Endpoint], constraints: dict[Dependency, Endpoint]=dict()) -> Generator[list[Endpoint], Any, None]:
        matches: list[list[Endpoint]] = []
        constraints_used = False
        for req in self.requires:
            if req in constraints:
                must_use = constraints[req]
                _m = [must_use]
            else:
                _m = [m for m in have if m.IsA(req)]
            if len(_m) == 0: return None
            matches.append(_m)
        if len(constraints)>0 and not constraints_used: return None

        indexes = [0]*len(matches)
        indexes[0] = -1
        def _advance():
            i = 0
            while True:
                indexes[i] += 1
                if indexes[i] < len(matches[i]): return True
                indexes[i] = 0
                i += 1
                if i >= len(matches): return False
        while _advance():
            yield [matches[i][j] for i, j in enumerate(indexes)]
    
    # filter possibilities based on correct lineage
    def Valids(self, matches: Iterable[list[Endpoint]]):
        black_list: set[tuple[int, Endpoint]] = set()
        white_list: set[tuple[int, Endpoint]] = set()

        choosen: list[Endpoint] = []
        for config in matches:
            ok = True
            for i, (e, r) in enumerate(zip(config, self.requires)):
                k = (i, e)
                if k in black_list: ok=False; break
                if k in white_list: continue
                
                parents = self._input_group_map.get(i, [])
                if len(parents) == 0: # no lineage req.
                    white_list.add(k)
                    continue
                
                for prototype in parents:
                    # parent must already be in choosen, since it must have been added
                    # as a req. before being used as a parent during setup
                    found = False
                    for p in choosen:
                        if not p.IsA(prototype): continue
                        if p in e.parents: found=True; break
                    if not found: black_list.add(k); ok=False; break
                if not ok: break
            if ok: yield config

    def Apply(self, inputs: Iterable[tuple[Endpoint, Node]]):
        # deleted = {}
        # for r, (e, e_proto) in zip(self.requires, inputs):
        #     assert e.IsA(r), f"{e_proto}, {e}, {r}"
        #     if r in self.deletes: deleted[e] = e_proto

        inputs_dict = dict(inputs)
        parent_dict: dict[Any, Any] = {}
        for e, _ in inputs_dict.items():
            for p, pproto in e.Iterparents():
                if p in parent_dict: continue
                parent_dict[p] = pproto
        for e, eproto in inputs_dict.items():
            parent_dict[e] = eproto
        produced = {
            Endpoint(
                properties=out.properties,
                parents=parent_dict
            ):out
        for out in self.produces}
        # return Application(self, inputs_dict, produced, deleted)
        return Application(self, inputs_dict, produced)

# an application of a transform on a set of inputs to produce outputs
@dataclass
class Application:
    transform: Transform
    used: dict[Endpoint, Node]
    produced: dict[Endpoint, Dependency]
    # deleted: dict[Endpoint, Node]

    def __str__(self) -> str:
        # return f"{self.transform} || {','.join(str(e) for e in self.used.keys())} -> {','.join(str(e) for e in self.produced)} |x {','.join(str(e) for e in self.deleted)}"
        return f"{self.transform} || {','.join(str(e) for e in self.used.keys())}->{','.join(str(e) for e in self.produced)}"

    def __repr__(self) -> str:
        return f"{self}"

@dataclass
class Result:
    application: Application
    dependency_plan: list[Application]

    def __len__(self):
        return len(self.dependency_plan)
    
@dataclass
class DependencyResult:
    plan: list[Application]
    endpoint: Endpoint

    def __len__(self):
        return len(self.plan)

# lineage is satisfied at depth 1 (parents of parents are not considered) 
def _solve_by_bounded_dfs(given: Iterable[Endpoint], target: Transform, transforms: Iterable[Transform], horizon: int=64, _debug=False):
    @dataclass
    class State:
        have: dict[Endpoint, Dependency]
        needed: set[Dependency]
        target: Dependency|Transform
        lineage_requirements: dict[Node, Endpoint]
        seen_signatures: set[str]
        depth: int

    def _get_producers_of(target: Dependency):
        for tr in transforms:
            for p in tr.produces:
                if p.IsA(target):
                    yield tr
                    break

    if _debug:
        log_path = Path("./cache/debug_log.txt")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = open("./cache/debug_log.txt", "w")
        debug_print = lambda *args: log.write(" ".join(str(a) for a in args)+"\n") if args[0] != "END" else log.close()
    else:
        debug_print = lambda *args: None

    _apply_cache: dict[str, Application] = {}
    def _apply(target: Transform, inputs: Iterable[tuple[Endpoint, Node]]):
        sig  = "".join(e.key+d.key for e, d in inputs)
        if sig in _apply_cache:
            return _apply_cache[sig]
        appl = target.Apply(inputs)
        _apply_cache[sig] = appl
        return appl

    def _satisfies_lineage(tproto: Dependency, candidate: Endpoint):
        for tp_proto in tproto.parents:
            if all(not p.IsA(tp_proto) for p, _ in candidate.Iterparents()):
                return False
        return True

    def _solve_dep(s: State) -> list[DependencyResult]:
        if s.depth >= horizon:
            if _debug: debug_print(f" <-  HORIZON", s.depth)
            return []
        target: Dependency = s.target
        assert isinstance(target, Dependency), f"{s.target}, not dep"
        if _debug: debug_print(f" ->", s.target, s.lineage_requirements)
        if _debug: debug_print(f"   ", s.have.keys())

        candidates:list[DependencyResult] = []
        for e, eproto in s.have.items():
            if not e.IsA(target): continue
            acceptable = True
            for rproto, r in s.lineage_requirements.items():
                if e == r: continue
                if eproto.IsA(rproto): # e is protype, but explicitly breaks lineage
                    acceptable=False; break

                for p, pproto in e.Iterparents():
                    if rproto.IsA(pproto):
                        if p != r:
                            acceptable=False; break

            if not acceptable:
                continue
            else:
                if _debug: debug_print(f"    ^candidate", e, eproto, e.parents)
                if _debug: debug_print(f"    ^reqs.    ", s.lineage_requirements)
                candidates.append(DependencyResult([], e))
            # elif quality == 2:
            #     if DEBUG: debug_print(f" <-", s.target, e, "DIRECT")
            #     return [DepResult(0, [], e)]

        def _add_result(res: Result):
            ep: Endpoint|None = None
            for e in res.application.produced:
                if e.IsA(target):
                    ep = e; break
            assert isinstance(ep, Endpoint)
            if not _satisfies_lineage(target, ep): return
            candidates.append(DependencyResult(
                res.dependency_plan+[res.application],
                ep,
            ))

        for tr in _get_producers_of(target):
            # if target in tr.deletes: continue
            results = _solve_tr(State(s.have, s.needed, tr, s.lineage_requirements, s.seen_signatures, s.depth))
            for res in results:
                _add_result(res)

        if _debug: debug_print(f" <-", s.target, f"{len(candidates)} sol.", candidates[0].endpoint if len(candidates)>0 else None)
        return candidates

    _transform_cache: dict[str, list[Result]] = {}
    def _solve_tr(s: State) -> list[Result]:
        assert isinstance(s.target, Transform), f"{s.target} not tr"
        target: Transform = s.target
        if _debug: debug_print(f">>>{s.depth:02}", s.target, s.lineage_requirements)
        for h in s.have:
            if _debug: debug_print(f"      ", h)

        # memoization
        sig = "".join(e.key for e in s.have)
        sig += f":{s.target.key}"
        sig += ":"+"".join(e.key for e in s.lineage_requirements.values())
        if sig in _transform_cache:
            if _debug: debug_print(f"<<<{s.depth:02} CACHED: {len(_transform_cache[sig])} solutions")
            return _transform_cache[sig]
        if sig in s.seen_signatures:
            if _debug: debug_print(f"<<<{s.depth:02} FAIL: is loop")
            return []

        plans: list[list[DependencyResult]] = []
        for i, req in enumerate(s.target.requires):
            req_p = {}
            for proto, e in s.lineage_requirements.items():
                if req.IsA(proto): continue
                req_p[proto] = e

            results = _solve_dep(State(s.have, s.needed|{req}, req, req_p, s.seen_signatures|{sig}, s.depth+1))
            
            if len(results) == 0:
                if _debug: debug_print(f"<<< FAIL", s.target, req)
                return []
            else:
                plans.append(results)

        def _gather_valid_inputs():
            valids: list[list[DependencyResult]] = []
            ii = 0
            def _gather(req_i: int, req: Dependency, res: DependencyResult, deps: dict, used: set[Endpoint], inputs: list[DependencyResult]):
                nonlocal ii; ii += 1         
                if _debug: debug_print(f"          ", deps)
                if _debug: debug_print(f"    ___", req, req.parents)
                if _debug: debug_print(f"        __", res.endpoint, list(res.endpoint.Iterparents()))
                if res.endpoint in used:
                    if _debug: debug_print(f"    ___ FAIL: duplicate input", res.endpoint)
                    return
                # used.add(res.endpoint)

                if not _satisfies_lineage(req, res.endpoint):
                    if _debug: debug_print(f"    ___ FAIL: unsatisfied lineage", req)
                    return

                for rproto in req.parents:
                    r = deps[rproto]
                    # if all(not p.IsA(rproto) for p, pproto in res.endpoint.Iterparents()):
                    #     if DEBUG: debug_print(f"    ___ FAIL: unsatisfied lineage", rproto)
                    #     _fail=True; break
                    res_parents = list(res.endpoint.Iterparents())
                    res_parents.reverse()
                    for p, pproto in res_parents:
                        if not p.IsA(rproto): continue
                        if p!=r:
                            if _debug: debug_print(f"    ___ FAIL: lineage mismatch", p, r)
                            return
                        else:
                            break # in the case of asm -> bin, the closest ancestor takes priority
                # deps[req] = res.endpoint

                if req_i >= len(target.requires)-1:
                    valids.append(inputs+[res])
                else:
                    req_i += 1
                    for i, next_res in enumerate(plans[req_i]):
                        _gather(req_i, target.requires[req_i], next_res, deps|{req:res.endpoint}, used|{res.endpoint}, inputs+[res])
            req_i = 0
            for i, next_res in enumerate(plans[req_i]):
                _gather(0, target.requires[req_i], next_res, {}, set(), [])
            total = 1
            for s in plans:
                total *= len(s)
            if _debug: debug_print(f"    ## {ii} visited, {total} combos")
            return valids

        if _debug: debug_print(f"<<<{s.depth:02}", s.target, s.lineage_requirements)
        if _debug: debug_print(f"     ", [len(x) for x in plans])
        solutions: list[Result] = []
        # for inputs in _iter_satisfies():
        for inputs in _gather_valid_inputs():
            my_appl = _apply(s.target, [(res.endpoint, req) for req, res in zip(s.target.requires, inputs)])
            consolidated_plan: list[Application] = []
            produced_sigs: set[str] = {p.Signature() for p in my_appl.produced}
            # if DEBUG: debug_print(f"   __", my_appl)
            for res in inputs:
                for appl in res.plan:
                    if all(p.Signature() in produced_sigs for p in appl.produced): continue
                    consolidated_plan.append(appl)
                    produced_sigs = produced_sigs.union(p.Signature() for p in appl.produced)
            solutions.append(Result(
                my_appl,
                consolidated_plan,
            ))
            # if DEBUG: debug_print(f"    *", my_appl)
            # if DEBUG: debug_print(f"     ", [res.endpoint for res in inputs])
            # if DEBUG: debug_print(f"    .", target.requires)
            # for appl in consolidated_plan:
            #     if DEBUG: debug_print(f"    __", appl)
        if _debug: debug_print(f"     ", f"{len(solutions)} sol.", solutions[0].application.produced if len(solutions)>0 else None)
        solutions = sorted(solutions, key=lambda s: len(s))
        _transform_cache[sig] = solutions
        return solutions

    input_tr = Transform()
    given_dict = {g:input_tr.AddProduct(properties=g.properties) for g in given}
    res = _solve_tr(State(given_dict, set(), target, {}, set(), 0))
    if _debug: debug_print("END")
    return res
