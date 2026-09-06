from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable
import json
import re
from pathlib import Path

from ..hashing import KeyGenerator
from .dag_renderer import DagRenderer, Label, LabelMode, NodeKind

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
class Solution:
    complete: bool
    dependency_plan: list[Application]
    merged_endpoints: dict[Endpoint, set[Endpoint]]
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
    max_refine: int|None=None,
) -> Solution:
    # Resolved here and nowhere else. Every layer above forwards `None`, so the
    # shipped budget cannot be pinned to a stale value by a signature default --
    # which is exactly how `REFINER_BUDGET` came to be unreachable: it was wired
    # into the solver alone while every caller kept passing its own 256.
    from .solver_backend import REFINER_BUDGET, solve_with_engine
    if max_refine is None: max_refine = REFINER_BUDGET
    _canonicalise_givens(given)
    return solve_with_engine(
        given, transforms, target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
    )

