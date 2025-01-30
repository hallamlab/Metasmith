from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Generator, Iterable
import numpy as np
from pathlib import Path

class KeyGenerator:
    def __init__(self, full=False, seed=None) -> None:
        ascii_vocab = [(48, 57), (65, 90), (97, 122)]
        vocab = [chr(i) for g in [range(a, b+1) for a, b in ascii_vocab] for i in g]
        if full: vocab += [c for c in "-_"]
        self.vocab = vocab
        self._generator = np.random.default_rng(seed)

    def GenerateUID(self, l:int=8, blacklist: set[str]=set()) -> str:
        key: str|None = None
        while key is None or key in blacklist:
            digits = self._generator.integers(0, len(self.vocab), l)
            key = "".join([self.vocab[i] for i in digits])
        return key
    
    def FromInt(self, i: int, l: int=8, little_endian=False):
        chunks = [self.vocab[0]]*l
        place = 0
        while i > 0 and place < l:
            chunk_k = i % len(self.vocab)
            i = (i - chunk_k) // len(self.vocab)
            chunks[place] = self.vocab[chunk_k]
            place += 1
        if not little_endian: chunks.reverse()
        return "".join(chunks)
    
    def FromHex(self, hex: str, l: int=8, little_endian=False):
        return self.FromInt(int(hex, 16), l, little_endian)
    
class Namespace:
    def __init__(self, key_length=4, seed: int=42, key_from_order=False) -> None:
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

class Hashable:
    def __init__(self, ns: Namespace) -> None:
        self.namespace = ns
        self.hash, self.key = ns.NewKey()

    def __hash__(self) -> int:
        return self.hash
    
    def __eq__(self, __value: object) -> bool:
        K = "key"
        return hasattr(__value, K) and self.key == getattr(__value, K)

class Node(Hashable):
    def __init__(
        self,
        ns: Namespace,
        properties: set[str],
        parents: set[Node],
    ) -> None:
        super().__init__(ns)
        self.namespace = ns
        self.properties = properties
        self.parents = parents
        self._sig: str|None = None
        # self._diffs = set()
        # self._sames = set()

    def __str__(self) -> str:
        return f"({'-'.join(self.properties)}:{self.key})"

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

# of a Transform
class Dependency(Node):
    def __init__(self, namespace: Namespace, properties: set[str], parents: set[Node]) -> None:
        super().__init__(namespace, properties, parents)

    def __str__(self) -> str:
        return f"(D:{'-'.join(self.properties)})"

# as in a free floating data type
class Endpoint(Node):
    def __init__(self, namespace: Namespace, properties: set[str], parents: dict[Endpoint, Node]=dict()) -> None:
        super().__init__(namespace, properties, set(parents))
        self._parent_map = parents # real, proto

    def Iterparents(self):
        """real, prototype"""
        for e, p in self._parent_map.items():
            yield e, p

class Transform(Hashable):
    def __init__(self, ns: Namespace) -> None:
        super().__init__(ns)
        self.requires: list[Dependency] = list()
        self.produces: list[Dependency] = list()
        self.deletes: set[Dependency] = set()
        self._ns = ns
        self._input_group_map: dict[int, list[Dependency]] = {}
        self._key = ns.NewKey()
        self._seen: set[str] = set()

    def __str__(self) -> str:
        def _props(d: Dependency):
            return "{"+"-".join(d.properties)+"}"
        return f"{','.join(_props(r) for r in self.requires)}->{','.join(_props(p) for p in self.produces)}"

    def __repr__(self): return f"{self}"

    def AddRequirement(self, properties: Iterable[str], parents: set[Dependency]=set()):
        prototype = Dependency(properties=set(properties), parents=parents, namespace=self._ns)
        return self._add_dependency(self.requires, prototype)

    def AddProduct(self, properties: Iterable[str], parents: set[Dependency]=set()):
        prototype = Dependency(properties=set(properties), parents=parents, namespace=self._ns)
        for d in self.deletes:
            assert not prototype.IsA(d), f"can not produce and delete product[{d}]"
        return self._add_dependency(self.produces, prototype)
    
    # def AddDeletion(self, to_delete: Dependency):
    #     print("warning!, deletion not implemented")
    #     assert to_delete in self.requires, f"{to_delete} not in requirements"
    #     if to_delete in self.deletes: return # already added
    #     for p in self.produces:
    #         assert not p.IsA(to_delete), f"can not produce and delete product [{p}]"
    #     self.deletes.add(to_delete)

    def _add_dependency(self, destination: list[Dependency], prototype: Dependency):
        # _dep = Dependency(properties=set(properties), parents=_parents, namespace=self._ns)
        _dep = prototype
        _parents = _dep.parents
        # assert not any(e.IsA(_dep) for e in destination), f"prev. dep ⊆ new dep"
        # assert not any(_dep.IsA(e) for e in destination), f"new dep ⊆ prev. dep "
        # destination.add(_dep)
        destination.append(_dep)
        if destination == self.requires:
            i = len(self.requires)-1
            for p in _parents:
                assert p in self.requires, f"{p} not added as a requirement"
            self._input_group_map[i] = self._input_group_map.get(i, [])+list(_parents)
        return _dep

    def _sig(self, endpoints: Iterable[Endpoint]):
        # return "".join(e.key for e in endpoints)
        return self.key+"-"+ "".join(e.key for e in endpoints)

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
                namespace=self._ns,
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
class HasSteps:
    steps: int

@dataclass
class Result(HasSteps):
    application: Application
    dependency_plan: list[Application]
    
@dataclass
class DependencyResult(HasSteps):
    plan: list[Application]
    endpoint: Endpoint

class WorkflowSolver:
    def __init__(self) -> None:
        pass

    def AddTransform(self, transform: Transform):
        pass

def Solve(given: Iterable[Endpoint], target: Transform, transforms: Iterable[Transform], _debug=False):
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

    HORIZON=64
    def _solve_dep(s: State) -> list[DependencyResult]:
        if s.depth >= HORIZON:
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
                candidates.append(DependencyResult(0, [], e))
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
                res.steps,
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
                len(consolidated_plan),
                my_appl,
                consolidated_plan,
            ))
            # if DEBUG: debug_print(f"    *", my_appl)
            # if DEBUG: debug_print(f"     ", [res.endpoint for res in inputs])
            # if DEBUG: debug_print(f"    .", target.requires)
            # for appl in consolidated_plan:
            #     if DEBUG: debug_print(f"    __", appl)
        if _debug: debug_print(f"     ", f"{len(solutions)} sol.", solutions[0].application.produced if len(solutions)>0 else None)
        solutions = sorted(solutions, key=lambda s: s.steps)
        _transform_cache[sig] = solutions
        return solutions

    input_tr = Transform(target._ns)
    given_dict = {g:input_tr.AddProduct(g.properties) for g in given}
    res = _solve_tr(State(given_dict, set(), target, {}, set(), 0))
    if _debug: debug_print("END")
    return res
