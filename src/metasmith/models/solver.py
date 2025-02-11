from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Generator, Iterable

from ..hashing import KeyGenerator
    
class Namespace:
    def __init__(self, key_length=4, seed: int|None=None, key_from_order=False) -> None:
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
        return f"({','.join(self.properties)}:{self.key})"

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
class Result:
    application: Application # overall inputs and outputs
    dependency_plan: list[Application]

    def __len__(self):
        return len(self.dependency_plan)
    
@dataclass
class DependencyResult:
    plan: list[Application]
    endpoint: Endpoint

    def __len__(self):
        return len(self.plan)
