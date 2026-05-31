from __future__ import annotations
from dataclasses import dataclass, field, InitVar
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Generator, Iterable, Literal, TypeVar
import os
import itertools
import yaml
import json
from hashlib import md5

from ..coms.containers import Container, ContainerRuntime
from .libraries import DataTypeLibrary
from .libraries import DataInstanceLibraryView, DataInstanceLibrary, DataInstance
from .libraries import TransformInstance, TransformInstanceLibrary, TransformInstanceLibraryView
from .paths import PathMap
from .remote import Logistics, Source, SourceType
from .solver import Application, Endpoint, Dependency, Transform, solve_by_mcts, Solution as SolverResult
from ..hashing import KeyGenerator
from ..logging import Log

METADATA_FILE = ".command.metadata"
BIND_FILE = ".command.binds"

@dataclass
class WorkflowStep:
    order: int
    dependency_map: InitVar[dict[Dependency, list[DataInstance]]]
    transform: TransformInstance
    transform_library: TransformInstanceLibrary
    uses: list[DataInstance] = field(default_factory=list)
    produces: list[list[DataInstance]] = field(default_factory=list)
    _raw_dependency_map: dict|None = None
    _raw_instances: dict[str, DataInstance]|None = None

    def __post_init__(self, dependency_map: dict[Dependency, list[DataInstance]]):
        # Backing storage for the dependency_map property. Initialized
        # before assignment so the setter's `self._dependency_map = …`
        # never runs against an undefined attribute.
        self._dependency_map: dict[Dependency, list[DataInstance]] = {}
        self.dependency_map = dependency_map

    # `dependency_map` is bound as a property below the class body so that
    # the @dataclass decorator doesn't see a class-attribute default
    # shadowing the InitVar declaration above. The setter auto-refreshes
    # `uses` and `produces`; an empty dict is treated as 'not resolved
    # yet' (Unpack pre-`_resolve_dependency_map`) and skips the refresh
    # so explicitly-passed views survive.
    def _get_dependency_map(self) -> dict[Dependency, list[DataInstance]]:
        return self._dependency_map

    def _set_dependency_map(self, value: dict[Dependency, list[DataInstance]]):
        self._dependency_map = value
        if value:
            self.RefreshViews()

    @property
    def group_by_instances(self):
        return self.dependency_map.get(self.transform.group_by, [])

    def RefreshViews(self):
        """Recompute `uses`/`produces` from the current `dependency_map`.

        Called automatically by the `dependency_map` setter; you only
        need to call it manually if you reach into `_dependency_map`
        directly (which you shouldn't — go through the property).
        """
        self.uses = [
            inst
            for dep in self.transform.model.requires
            for inst in self.dependency_map.get(dep, [])
        ]
        self.produces = [
            [
                inst
                for dep in dep_group
                for inst in self.dependency_map.get(dep, [])
            ]
            for dep_group in self.transform.model.produces
        ]

    def Pack(self):
        all_instances: dict[str, DataInstance] = {}
        for lst in self.dependency_map.values():
            for inst in lst:
                all_instances[inst.instance_id] = inst
        return dict(
            order=self.order,
            schema="v2",
            instances={k:v.Pack() for k, v in all_instances.items()},
            dependency_map={k.key:[v.instance_id for v in lst] for k, lst in self.dependency_map.items()},
            transform=f"{self.transform_library.GetKey()}::{self.transform._path}",
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, transform_path = raw["transform"].split("::")
        lib = libraries[lib_key]
        assert isinstance(lib, TransformInstanceLibrary)
        tr = lib.GetTransform(transform_path)
        assert tr is not None
        raw_instances: dict[str, DataInstance]|None = None
        uses: list[DataInstance] = []
        produces: list[list[DataInstance]] = []
        if "instances" in raw:
            raw_instances = {k:DataInstance.Unpack(v, libraries) for k, v in raw["instances"].items()}
        else:
            uses = [DataInstance.Unpack(inst, libraries) for inst in raw.get("uses", [])]
            produces = [[DataInstance.Unpack(inst, libraries) for inst in g] for g in raw.get("produces", [])]
        return cls(
            order=raw["order"],
            dependency_map={}, # needs workflow plan to sort out
            uses=uses,
            produces=produces,
            _raw_dependency_map = raw["dependency_map"],
            _raw_instances=raw_instances,
            transform=tr,
            transform_library=lib,
        )
    
    def _resolve_dependency_map(self):
        assert self._raw_dependency_map is not None
        if self._raw_instances is not None:
            data = {}
            for inst in self._raw_instances.values():
                for k in {inst.instance_id, inst._key, inst.legacy_key}:
                    data[k] = inst
        else:
            raw_data = itertools.chain(self.uses, [d for g in self.produces for d in g])
            data = {}
            for inst in raw_data:
                for k in {inst.instance_id, inst._key, inst.legacy_key}:
                    data[k] = inst
        tr = self.transform.model
        deps = {d.key:d for d in itertools.chain(tr.requires, [d for g in tr.produces for d in g])}
        dep_map: dict[Dependency, list[DataInstance]] = {}
        for dep_key, ids in self._raw_dependency_map.items():
            if dep_key not in deps:
                continue
            missing = [v for v in ids if v not in data]
            if len(missing)>0:
                Log.Warn(f"missing [{len(missing)}] DataInstances for dependency [{dep_key}] while unpacking workflow step [{self.order}]")
            dep_map[deps[dep_key]] = [data[v] for v in ids if v in data]
        self.dependency_map = dep_map

# Bind dependency_map as a property here (post-class-body) so the
# @dataclass decorator above does not see a class-attribute default
# shadowing the InitVar declaration in WorkflowStep.
WorkflowStep.dependency_map = property(  # type: ignore[assignment]
    WorkflowStep._get_dependency_map,
    WorkflowStep._set_dependency_map,
)

@dataclass
class WorkflowTarget:
    name: str
    instance: DataInstance
    producing_step: WorkflowStep

    def Pack(self):
        return dict(
            name=self.name,
            instance=self.instance.Pack(),
            producing_step=dict(order=self.producing_step.order, name=self.producing_step.transform.name),
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary], given: dict[str, DataInstance], steps: dict[int, WorkflowStep]):
        inst = DataInstance.Unpack(raw["instance"], libraries)
        producing_step = steps[raw["producing_step"]["order"]]
        return cls(
            name=raw["name"],
            instance=inst,
            producing_step=producing_step,
        )

@dataclass
class NextflowGenContext:
    workflow_file: str
    work_dir: Path
    external_work: Path
    home_dir: Path
    external_home: Path
    container_runtime: ContainerRuntime
    resources_file: str
    external_home_var: str = "${params.home}"
    external_work_var: str = "${params.workspace}"
    bootstrap_var: str = "${params.bootstrap_def}"
    # S3 — lineage-addressed task cache. cache_root defaults to
    # <external_home>/task_cache; set to None to disable cache integration
    # (synthetic channels, publishDir-to-cache, probe). The env var
    # METASMITH_CACHE=0 also disables, regardless of this setting.
    cache_root: Path | None = None
    # Materialization strategy for publishDir into the cache: 'link'
    # (hardlink, local FS) or 'copy' (network FS). S6 picks this from
    # mountinfo; for now the default is 'link'.
    cache_hit_strategy: str = "link"

@dataclass
class PlanHint:
    kind: str
    target: str
    message: str
    chain: list[str] = field(default_factory=list)
    candidate_transforms: list[str] = field(default_factory=list)
    near_misses: list[str] = field(default_factory=list)


def _diagnose_plan_failure(
    target_model: Transform,
    target_names: list[str],
    given_map: dict[Endpoint, list[DataInstance]],
    transform2inst: dict[Transform, TransformInstance],
    solver_result: SolverResult|None,
    type_lookups: list = None,
    max_hops: int = 6,
    max_hints_per_target: int = 4,
    near_miss_top_n: int = 3,
) -> list[PlanHint]:
    """Build PlanHint objects explaining why no plan was found.

    Three passes:
        a. unreachable target: no transform produces a match.
        b. multi-hop reverse-BFS: chain dead-ends at a demand with no
           producer and no given match.
        c. lineage mismatch: a given matches a requirement by properties
           but does not descend from a required parent.
    """
    from collections import deque

    hints: list[PlanHint] = []

    all_givens: set[Endpoint] = set(given_map.keys())

    def _matches_any_given(d: Dependency) -> bool:
        return any(g.IsA(d) for g in all_givens)

    def _producers_of(demand: Dependency) -> list[Transform]:
        out: list[Transform] = []
        for model in transform2inst:
            hit = False
            for pgroup in model.produces:
                for p in pgroup:
                    if p.IsA(demand):
                        hit = True
                        break
                if hit:
                    break
            if hit:
                out.append(model)
        return out

    # build endpoint -> name cache from data instances, target_names, and any
    # supplied type lookups (typically the transform libs)
    _name_cache: dict = {}
    for d, nm in zip(target_model.requires, target_names):
        _name_cache.setdefault(Endpoint(d.properties), nm)
    for ep, insts in given_map.items():
        for inst in insts:
            if inst.dtype_name:
                _name_cache.setdefault(ep, inst.dtype_name)
                break
    for lookup in (type_lookups or []):
        # walk the inner DataTypeLibrary namespaces to capture every typed
        # Endpoint (lookup.Iterate() only yields stored instances, not type
        # definitions)
        try:
            for ns, tlib in lookup.types.items():
                for type_name, ep in tlib.types.items():
                    _name_cache.setdefault(ep, f"{ns}::{type_name}")
        except Exception:
            pass

    def _name(d) -> str:
        cached = _name_cache.get(d)
        if cached:
            return cached
        # node hashes by properties+parents; try property-only match
        for ep, nm in _name_cache.items():
            if ep.properties == d.properties:
                _name_cache[d] = nm
                return nm
        if not d.properties:
            return "<unspecified>"
        return "{" + ", ".join(sorted(d.properties)) + "}"

    def _model_name(model: Transform|None) -> str:
        if model is None:
            return "<target>"
        ti = transform2inst.get(model)
        if ti is not None and ti.name:
            return ti.name
        return "<unnamed>"

    def _jaccard(a: set[str], b: set[str]) -> float:
        u = a | b
        return len(a & b) / len(u) if u else 0.0

    def _property_keys(props: set[str]) -> set[str]:
        keys = set()
        for p in props:
            try:
                obj = json.loads(p)
                if isinstance(obj, dict):
                    keys.update(obj.keys())
                    continue
            except (json.JSONDecodeError, TypeError):
                pass
            keys.add(p)
        return keys

    def _shape_key(d) -> str:
        # canonical key that ignores parent identity — collapses two demands
        # with the same property bag (but different `parents={...}`) into one
        return "|".join(sorted(d.properties)) or "<unspecified>"

    def _similarity_to_givens(demand) -> float:
        if not all_givens:
            return 0.0
        best = max(
            (_jaccard(g.properties, demand.properties) for g in all_givens),
            default=0.0,
        )
        if best > 0:
            return best
        dkeys = _property_keys(demand.properties)
        if not dkeys:
            return 0.0
        # 0.5 factor keeps shape-match scores strictly below value-match scores
        return max(
            (_jaccard(_property_keys(g.properties), dkeys) * 0.5 for g in all_givens),
            default=0.0,
        )

    def _rank_near_misses_among_givens(demand: Dependency) -> list[str]:
        scored: list[tuple[float, str, str, list[str]]] = []
        for ep, insts in given_map.items():
            if ep.IsA(demand):
                continue
            score = _jaccard(ep.properties, demand.properties)
            if score <= 0:
                continue
            overlap = sorted(ep.properties & demand.properties)
            for inst in insts:
                scored.append((score, inst.dtype_name or _name(ep), str(inst.path), overlap))
        scored.sort(key=lambda x: -x[0])
        out = [
            f"{name} @ {path} (overlap {score:.2f}: {overlap})"
            for score, name, path, overlap in scored[:near_miss_top_n]
        ]
        if out:
            return out
        # fallback: rank by property-KEY overlap (helps when same shape but
        # different value, e.g. ext=bam vs ext=fq.gz)
        demand_keys = _property_keys(demand.properties)
        if not demand_keys:
            return []
        key_scored: list[tuple[float, str, str, list[str]]] = []
        for ep, insts in given_map.items():
            if ep.IsA(demand):
                continue
            ep_keys = _property_keys(ep.properties)
            key_score = _jaccard(ep_keys, demand_keys)
            if key_score <= 0:
                continue
            shared_keys = sorted(ep_keys & demand_keys)
            for inst in insts:
                key_scored.append((key_score, inst.dtype_name or _name(ep), str(inst.path), shared_keys))
        key_scored.sort(key=lambda x: -x[0])
        return [
            f"{name} @ {path} (shape-match {score:.2f}: shared keys {keys})"
            for score, name, path, keys in key_scored[:near_miss_top_n]
        ]

    def _rank_near_misses_among_products(demand: Dependency) -> list[str]:
        scored: list[tuple[float, str, list[str]]] = []
        for model in transform2inst:
            for pgroup in model.produces:
                for p in pgroup:
                    if p.IsA(demand):
                        continue
                    score = _jaccard(p.properties, demand.properties)
                    if score <= 0:
                        continue
                    overlap = sorted(p.properties & demand.properties)
                    scored.append((score, _model_name(model), overlap))
        scored.sort(key=lambda x: -x[0])
        seen: set[str] = set()
        out: list[str] = []
        for score, name, overlap in scored:
            if name in seen:
                continue
            seen.add(name)
            out.append(f"{name} produces a near-match (overlap {score:.2f}: {overlap})")
            if len(out) >= near_miss_top_n:
                break
        return out

    # ---- pass (a) + (b): per-target reverse-BFS ----
    for tr_req in target_model.requires:
        target_name = _name(tr_req)
        producers = _producers_of(tr_req)
        if not producers:
            hints.append(PlanHint(
                kind='unreachable_target',
                target=target_name,
                message=f"no transform in the loaded libraries produces a type that satisfies {target_name}",
                near_misses=_rank_near_misses_among_products(tr_req),
            ))
            continue

        queue: deque[tuple[Dependency, list[str], int]] = deque()
        for prod in producers:
            link = (
                f"{_model_name(prod)} produces {target_name} but needs "
                + ", ".join(_name(r) for r in prod.requires)
            )
            for sub in prod.requires:
                queue.append((sub, [f"target needs {target_name}", link], 1))

        visited: set[str] = set()
        dead_ends: dict[str, tuple[Dependency, list[str]]] = {}
        steps_budget = max_hints_per_target * 16
        while queue and steps_budget > 0:
            steps_budget -= 1
            d, chain, hops = queue.popleft()
            if d.key in visited:
                continue
            visited.add(d.key)
            if _matches_any_given(d):
                continue
            sub_producers = _producers_of(d)
            if not sub_producers:
                sk = _shape_key(d)
                if sk not in dead_ends:
                    dead_ends[sk] = (d, chain + [f"<no producer for {_name(d)}>"])
                continue
            if hops >= max_hops:
                sk = _shape_key(d)
                if sk not in dead_ends:
                    dead_ends[sk] = (d, chain + [f"<hop limit reached at {_name(d)}>"])
                continue
            for prod in sub_producers[:3]:
                link = (
                    f"{_model_name(prod)} produces {_name(d)} but needs "
                    + ", ".join(_name(r) for r in prod.requires)
                )
                for sub in prod.requires:
                    queue.append((sub, chain + [link], hops + 1))

        ranked = sorted(
            dead_ends.values(),
            key=lambda dc: (
                -_similarity_to_givens(dc[0]),
                len(dc[1]),
                _name(dc[0]),
            ),
        )
        for d, chain in ranked[:max_hints_per_target]:
            hints.append(PlanHint(
                kind='missing_input',
                target=target_name,
                message=(
                    f"to produce {target_name}, the chain dead-ends at {_name(d)}: "
                    f"no transform produces it and no given input matches"
                ),
                chain=chain,
                near_misses=_rank_near_misses_among_givens(d),
            ))

    # ---- pass (c): lineage mismatch ----
    def _collect_ancestors(ep: Endpoint) -> set[Endpoint]:
        seen: set[Endpoint] = set()
        todo = [ep]
        while todo:
            cur = todo.pop()
            for p in cur.parents:
                if p in seen:
                    continue
                seen.add(p)
                todo.append(p)
        return seen

    seen_lineage_keys: set[str] = set()
    checked: list[tuple[Dependency, Transform|None, str]] = []
    for tr_req in target_model.requires:
        checked.append((tr_req, None, _name(tr_req)))
    for model in transform2inst:
        ctx_name = _model_name(model)
        for req in model.requires:
            checked.append((req, model, ctx_name))

    for req, model, ctx in checked:
        if not req.parents:
            continue
        prop_match = [g for g in all_givens if g.properties >= req.properties]
        if not prop_match:
            continue
        for req_parent in req.parents:
            parent_match = [g for g in all_givens if g.properties >= req_parent.properties]
            for child in prop_match:
                if child in parent_match:
                    continue
                ancestors = _collect_ancestors(child)
                if any(pm in ancestors for pm in parent_match):
                    continue
                key = f"{req.key}|{req_parent.key}|{child.key}"
                if key in seen_lineage_keys:
                    continue
                seen_lineage_keys.add(key)
                req_name = _name(req)
                req_parent_name = _name(req_parent)
                child_insts = given_map.get(child, [])
                child_label = child_insts[0].dtype_name if child_insts and child_insts[0].dtype_name else _name(child)
                near_misses: list[str] = []
                parent_path_hint = None
                for parent_ep in parent_match:
                    insts = given_map.get(parent_ep, [])
                    if insts:
                        parent_path_hint = str(insts[0].path)
                        break
                if parent_path_hint is None:
                    parent_path_hint = f"<{req_parent_name} instance>"
                for inst in child_insts:
                    near_misses.append(
                        f"add parents=[{parent_path_hint}] when registering {inst.path} "
                        f"so it descends from a {req_parent_name}"
                    )
                hints.append(PlanHint(
                    kind='lineage_mismatch',
                    target=ctx,
                    message=(
                        f"{child_label} matches {req_name} by properties, "
                        f"but it is not registered as a descendant of any {req_parent_name}; "
                        f"{_model_name(model)} requires that parent in its data lineage"
                    ),
                    chain=[f"{_model_name(model)} needs {req_name} with parent {req_parent_name}"],
                    near_misses=near_misses,
                ))
    return hints


@dataclass
class WorkflowPlan:
    given: list[DataInstance]
    targets: list[WorkflowTarget] # target, used givens
    steps: list[WorkflowStep]
    _solver_result: SolverResult|None=None
    _archetype_translation: dict[DataInstance, DataInstance]|None = None
    dropped_targets: list[str] = field(default_factory=list)
    hints: list[PlanHint] = field(default_factory=list)
    publish_intermediates: bool = True

    def __post_init__(self):
        self._update_hash()

    def _update_hash(self):
        steps = [step.transform.model.key for step in self.steps]
        given_ids = sorted(inst.instance_id for inst in self.given)
        self._hash, self._key = KeyGenerator.FromStr("".join(steps) + "".join(given_ids), l=8)

    def __hash__(self) -> int:
        return self._hash
    
    def __len__(self):
        return len(self.steps)

    def Pack(self):
        dtypes: dict[str, tuple[str, Endpoint]] = {}
        for inst in self.given:
            dtypes[inst.dtype.key] = inst.dtype_name, inst.dtype
        for target in self.targets:
            inst = target.instance
            dtypes[inst.dtype.key] = inst.dtype_name, inst.dtype
        for step in self.steps:
            for inst in step.uses:
                dtypes[inst.dtype.key] = inst.dtype_name, inst.dtype
            for g in step.produces:
                for inst in g:
                    dtypes[inst.dtype.key] = inst.dtype_name, inst.dtype

        def _pack_type(name: str, e: Endpoint):
            d = e.Pack()
            to_add: list[tuple[str, str, Endpoint]] = []
            if len(e.parents)>0:
                d["parents"] = [p.key for p in e.parents]
                for p in e.parents:
                    if p.key in dtypes: continue
                    _n, _e = f"parent_{p.key}", p
                    dtypes[p.key] = _n, _e # type: ignore # p is Node
                    to_add.append((p.key, _n, _e)) # type: ignore # p is Node
            d["name"] = name
            return d, to_add
        
        packed_types: dict[str, dict] = {}
        todo = [(k, n, e) for k, (n, e) in dtypes.items()]
        while len(todo)>0:
            k, n, e = todo.pop()
            t, to_add = _pack_type(n, e)
            packed_types[k] = t
            for _k, _n, _e in to_add:
                todo.append((_k, _n, _e))

        return dict(
            types=packed_types,
            given=[inst.Pack() for inst in self.given],
            targets=[inst.Pack() for inst in self.targets],
            steps=[step.Pack() for step in self.steps],
            publish_intermediates=self.publish_intermediates,
        )

    def Save(self, path: Path):
        with open(path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        all_types: dict[str, Endpoint] = {}
        while len(all_types) < len(raw["types"]):
            changed = False
            for k, v in raw["types"].items():
                if k in all_types: continue
                parent_keys = v.get("parents", [])
                if any(p not in all_types for p in parent_keys): continue
                parents = {all_types[p] for p in parent_keys}
                proto = Endpoint.Unpack(dict(properties=v["properties"]))
                all_types[k] = Endpoint(properties=proto.properties, parents=parents)
                changed = True
            if not changed: break

        def _unpack_given(raw: dict):
            inst = DataInstance.Unpack(raw, libraries)
            inst.dtype = all_types[raw["type_id"]]
            inst.RecalculateKey()
            if "instance_id" in raw:
                inst.instance_id = raw["instance_id"]
                inst._key = inst.instance_id
                inst._hash, _ = KeyGenerator.FromStr(inst.instance_id, l=10)
            return inst

        def _unpack_step(raw: dict):
            step = WorkflowStep.Unpack(raw, libraries)
            def _iter():
                if "instances" in raw and step._raw_instances is not None:
                    for k, r in raw["instances"].items():
                        if k not in step._raw_instances:
                            continue
                        yield step._raw_instances[k], r
                    return
                for inst, r in zip(step.uses, raw.get("uses", [])):
                    yield inst, r
                for g, rg in zip(step.produces, raw.get("produces", [])):
                    for inst, r in zip(g, rg):
                        yield inst, r
            for inst, r in _iter():
                inst.dtype = all_types[r["type_id"]]
                inst.RecalculateKey()
                if "instance_id" in r:
                    inst.instance_id = r["instance_id"]
                    inst._key = inst.instance_id
                    inst._hash, _ = KeyGenerator.FromStr(inst.instance_id, l=10)
            step._resolve_dependency_map()
            return step

        given=[_unpack_given(d) for d in raw["given"]]
        given_map = {inst._key: inst for inst in given}
        steps = [_unpack_step(d) for d in raw["steps"]]
        step_map = {step.order: step for step in steps}

        def _unpack_target(raw: dict):
            target = WorkflowTarget.Unpack(raw, libraries, given_map, step_map)
            target.instance.dtype = all_types[raw["instance"]["type_id"]]
            return target

        return cls(
            given=given,
            targets=[_unpack_target(d) for d in raw["targets"]],
            steps=steps,
            publish_intermediates=raw.get("publish_intermediates", True),
        )

    @classmethod
    def Generate(
        cls,
        given: list[list[DataInstanceLibraryView]],
        transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
        target_names: list[str],
        target_model: Transform,
        max_iter: int=256, max_refine: int=256, seed: int=42,
    ):
        given_map: dict[Endpoint, list[DataInstance]] = {}
        _seen_instances: set[tuple] = set()  # (path, ep) dedup key
        _view_eps_cache: dict[int, set[Endpoint]] = {}  # id(view) -> cached endpoints
        given_endpoints: list[set[Endpoint]] = []
        _seen_group_keys: set[tuple] = set()
        for group in given:
            # Skip groups whose views are identical to already-processed groups
            group_key = tuple((id(lib._original), lib._mask_key) for lib in group)
            if group_key in _seen_group_keys:
                continue
            _seen_group_keys.add(group_key)

            eps = set()
            for lib in group:
                view_id = id(lib)
                if view_id in _view_eps_cache:
                    # Same view object reused across groups (e.g. shared resources)
                    eps.update(_view_eps_cache[view_id])
                    continue
                lib_eps = set()
                for path, ep_name, ep in lib.Iterate():
                    dedup_key = (path, ep)
                    if dedup_key not in _seen_instances:
                        _seen_instances.add(dedup_key)
                        given_map.setdefault(ep, []).append(DataInstance(
                            path=path,
                            dtype=ep,
                            dtype_name=ep_name,
                            parent_lib=lib._original,
                        ))
                    lib_eps.add(ep)
                _view_eps_cache[view_id] = lib_eps
                eps.update(lib_eps)
            if len(given_endpoints)>0 and any(g==eps for g in given_endpoints): continue
            given_endpoints.append(eps)

        # target_e2d: dict[Endpoint, Dependency] = {}
        # def _add(tr: Transform, e: Endpoint) -> Dependency:
        #     if e in target_e2d: return target_e2d[e]
        #     parent_deps = {_add(tr, p) for p in e.parents} # type: ignore
        #     d = tr.AddRequirement(e, parents=parent_deps)
        #     target_e2d[e] = d
        #     return d
        # target_model = Transform()
        # for t in targets:
        #     _add(target_model, t)

        transform2inst: dict[Transform, TransformInstance] = {}
        inst2trlib: dict[TransformInstance, TransformInstanceLibrary] = {}
        for trlib in transforms:
            base = trlib._original if isinstance(trlib, TransformInstanceLibraryView) else trlib
            for path, tr in trlib.IterateTransforms():
                model = tr.model
                if model in transform2inst:
                    Log.Warn(f"transform [{model}] of [{trlib}] is masked")
                    continue
                transform2inst[model] = tr
                inst2trlib[tr] = base

        _pl1 = "" if len(given)==1 else "s"
        _pl2 = "" if len(given_endpoints)==1 else "s"
        Log.Info(f"solving plan for [{len(given)}] sample{_pl1} as [{len(given_endpoints)}] unique case{_pl2}")

        result = solve_by_mcts(
            given=given_endpoints,
            target=target_model,
            transforms=transform2inst.keys(),
            max_iter=max_iter,
            max_refine=max_refine,
            seed=seed,
        )
        # result.RenderDAG("./dag")

        def _dedupe_instances(instances: list[DataInstance]):
            out = []
            seen: dict[str, DataInstance] = {}
            for inst in instances:
                if inst.instance_id in seen:
                    existing = seen[inst.instance_id]
                    # merge parent lineage from duplicate into the kept instance
                    for p_path, p_list in inst.parent_lib.parents.items():
                        if p_path not in existing.parent_lib.parents:
                            existing.parent_lib.parents[p_path] = list(p_list)
                        else:
                            existing_keys = {f"{x.library_key}/{x.path}" for x in existing.parent_lib.parents[p_path]}
                            existing.parent_lib.parents[p_path].extend(
                                p for p in p_list if f"{p.library_key}/{p.path}" not in existing_keys
                            )
                    continue
                seen[inst.instance_id] = inst
                out.append(inst)
            return out

        if not result.complete or not result.dependency_plan:
            # solver couldn't connect givens to target — return an empty plan
            # decorated with structured hints instead of raising or returning
            # the raw Solution.
            failure_hints = _diagnose_plan_failure(
                target_model=target_model,
                target_names=target_names,
                given_map=given_map,
                transform2inst=transform2inst,
                solver_result=result,
                type_lookups=list(transforms),
            )
            return cls(
                given=[],
                targets=[],
                steps=[],
                _solver_result=result,
                dropped_targets=list(target_names),
                hints=failure_hints,
            )

        # remap given inputs if changed by solver
        result_given = result.dependency_plan[0]
        for pgroup in result_given.produced:
            for d in pgroup:
                e = pgroup[d]
                # for e in result.merged_endpoints.get(ek, [ek]):
                # the given (input) endpoints are added as dependencies
                # to the transform that models inputs
                # this allows given_map[d]
                # when a given endpoint is used directly, but some other branch requires a conversion
                # d may not be in given_map
                if e in given_map:
                    current = set(given_map[e])
                    given_map[e] += [x for x in given_map.get(d, []) if x not in current] # type: ignore
                else:
                    for ge in list(given_map):
                        if not e.properties==ge.properties: continue
                        given_map[e] = given_map[ge]
                        break

        for e, me in result.merged_endpoints.items():
            if len(me)<2: continue
            if e not in given_map: continue
            _to_add = []
            for x in me:
                if x==e: continue
                for oe in given_map.get(x, []):
                    _to_add.append(oe.WithDType(e))
            given_map[e] = _dedupe_instances(given_map[e] + _to_add)

        solution = result

        # for e, lst in given_map.items():
        #     if '{"Format":"OCI"}' in e.properties: continue
        #     print(e.key)
        #     for x in lst:
        #         print(x.dtype_name, x.path)
        #     print()

        # for i, appl in enumerate(solution.dependency_plan):
        #     if appl.transform in transform2inst:
        #         tr = transform2inst[appl.transform]
        #         name = tr.name
        #     else:
        #         name = ""
        #     print(i+1, name)
        #     for d, e in appl.used.items():
        #         if '{"Format":"OCI"}' in e.properties: continue
        #         print(e.key, [x.key for x in solution.merged_endpoints.get(e, [])], d)
        #     print(" -->")
        #     for i, g in enumerate(appl.produced):
        #         for d, e in g.items():
        #             print(i, e.key, d)
        #     print()
        #     print()


        # print("---")
        # print()
        # print()

        class CanonicalInstanceRegistry:
            def __init__(self):
                self._registry: dict[tuple[str, ...], DataInstance] = {}

            def get_or_create(self, key: tuple[str, ...], factory):
                if key not in self._registry:
                    self._registry[key] = factory()
                return self._registry[key]

        canonical = CanonicalInstanceRegistry()
        instance_map: dict[Endpoint, list[DataInstance]] = {k:_dedupe_instances(v) for k, v in given_map.items()}
        steps: dict[Application, WorkflowStep] = {}
        used_endpoints: set[Endpoint] = set()
        target_meta: dict[Endpoint, list[WorkflowTarget]] = {}
        target_appls = [a for a in solution.dependency_plan if sum(len(g) for g in a.produced)==0]
        # target_appl = solution.dependency_plan[-1]
        target_endpoints = {e for appl in target_appls for e in appl.used.values()}
        # map each target_model.Dependency to its declared name (positional alignment).
        dep_to_name: dict[Dependency, str] = dict(zip(target_model.requires, target_names))
        # for the producer-labeling loop: walk the solver's terminal target_appls and
        # queue (alias, target-dep) pairs per produced Endpoint. Lineage-distinct
        # target deps land on distinct Endpoint objects, so each queue holds the
        # right number of slots per endpoint.
        ep_dep_queue: dict[Endpoint, list[Dependency]] = {}
        for _appl in target_appls:
            for _d, _e in _appl.used.items():
                if _d in dep_to_name:
                    ep_dep_queue.setdefault(_e, []).append(_d)
        resolved_deps: set[Dependency] = set()
        applies = [a for a in solution.dependency_plan if len(a.used)>0 and sum(len(g) for g in a.produced)>0]
        # applies = solution.dependency_plan[1:-1]
        for i, appl in enumerate(applies):
            tr = transform2inst[appl.transform]
            _lib = inst2trlib[tr]

            _insts: dict[tuple, DataInstance] = {}
            for j, pgroup in enumerate(appl.produced):
                for d, e in pgroup.items():
                    # dtname = None
                    # for x in targets:
                    #     if e.IsA(x):
                    #         dtname = targets[x]
                    #         break
                    # if dtname is None:
                    dk = Endpoint(d.properties)
                    dtname = _lib.GetName(dk)

                    _instance = DataInstance(
                        path = Path(e.key+e.GetPreferredFileExtension()),
                        dtype = e, # we actually dont want lineage at this stage so that the hashes match
                        dtype_name = dtname, 
                        parent_lib = _lib,
                    )
                    instance_key = (
                        "generated",
                        appl.Signature(),
                        d.key,
                        e.key,
                        dtname,
                    )
                    _instance = canonical.get_or_create(instance_key, lambda: _instance)
                    instance_map[e] = _dedupe_instances(instance_map.get(e, []) + [_instance])
                    _insts[(j, d, e)] = _instance

            # print(f">> {tr.name}")
            # for e in appl.used.values():
            #     if '{"Format":"OCI"}' in e.properties: continue
            #     print(f"   {e in instance_map}", e)

            used_endpoints |= {e for e in appl.used.values()}
            step = WorkflowStep(
                order=i+1,
                dependency_map={d:list(instance_map[e]) for d, e in itertools.chain(appl.used.items(), [(d, e) for pgroup in appl.produced for d, e in pgroup.items()])},
                transform=tr,
                transform_library=_lib,
            )
            steps[appl] = step
            for j, pgroup in enumerate(appl.produced):
                for d, e in pgroup.items():
                    if e not in target_endpoints: continue
                    queue = ep_dep_queue.get(e)
                    assert queue, f"no target dep matched produced endpoint [{e}]"
                    d_target = queue.pop(0)
                    dtname = dep_to_name[d_target]
                    resolved_deps.add(d_target)
                    t = _insts[(j, d, e)]
                    target_meta[e] = target_meta.get(e, [])+[
                        WorkflowTarget(
                            name=dtname,
                            instance=t,
                            producing_step=step,
                        )
                    ]

        # expand used_endpoints to include all transitive lineage ancestors
        # that exist in given_map, so PrepareNextflow's topological sort
        # can resolve the full parent chain
        def _collect_ancestor_endpoints(endpoints: set[Endpoint], pool: set[Endpoint]) -> set[Endpoint]:
            result = set(endpoints)
            frontier = set(endpoints)
            while frontier:
                next_frontier = set()
                for ep in frontier:
                    for parent in ep.parents:
                        if parent not in result and parent in pool:
                            result.add(parent)
                            next_frontier.add(parent)
                frontier = next_frontier
            return result

        given_pool = set(given_map.keys())
        used_endpoints = _collect_ancestor_endpoints(used_endpoints, given_pool)

        _given = []
        _seen_given = set()
        for e, lst in given_map.items():
            if e not in used_endpoints:
                continue
            for inst in lst:
                if inst.instance_id in _seen_given:
                    continue
                _seen_given.add(inst.instance_id)
                _given.append(inst)
        dropped_targets = []
        for d, nm in dep_to_name.items():
            if d not in resolved_deps:
                Log.Warn(f"target [{nm}] was requested but not included in plan"
                         " — check if group_by dependency can be satisfied from given inputs")
                dropped_targets.append(nm)

        # if the plan is empty or has dropped targets, attach hints
        built_steps = [s for a, s in steps.items()]
        plan_hints: list[PlanHint] = []
        if not built_steps or dropped_targets:
            plan_hints = _diagnose_plan_failure(
                target_model=target_model,
                target_names=target_names,
                given_map=given_map,
                transform2inst=transform2inst,
                solver_result=result,
                type_lookups=list(transforms),
            )

        return cls(
            given=_given,
            targets=[x for g in target_meta.values() for x in g],
            steps=built_steps,
            _solver_result=result,
            dropped_targets=dropped_targets,
            hints=plan_hints,
        )

    def RenderDAG(self, path_base: Path|str, format: str ='svg', *, font: str = 'Arial', blacklist_namespaces: set[str]={"lib", "containers"}):
        # do some ju jitsu to prevent graphviz from dumping out garbage into the logs
        # todo: propogate errors, those might be important...
        import logging
        _temp = logging.getLogger
        class DummyLogger:
            def debug(self, *args, **kwargs):
                pass
            def info(self, *args, **kwargs):
                pass
            def warn(self, *args, **kwargs):
                pass
            def error(self, *args, **kwargs):
                pass
        logging.getLogger = lambda *args, **kwargs: DummyLogger()
        import graphviz
        logging.getLogger = _temp

        path_base = Path(path_base)
        ext = path_base.suffix
        if ext:
            format = ext.replace(".", "")
            path_base = path_base.with_suffix("")
        todo = [(graphviz, 0)]
        while len(todo)>0:
            m, depth = todo.pop()
            if hasattr(m, "log") and hasattr(m.log, "setLevel"):
                m.log.setLevel(logging.ERROR)
            if depth >= 2: continue
            if hasattr(m, "__dict__"):
                todo += [(x, depth+1) for x in m.__dict__.values()]

        class NodeType(Enum):
            TRANSFORM = 1
            DATA      = 2
        def _render_node(type: NodeType, name: str) -> str:
            match type:
                case NodeType.TRANSFORM:
                    return f'"{name}" [shape="oval", style="filled", fillcolor="#CCCCCC"]'
                case NodeType.DATA:
                    return f'"{name}" [shape="box"]'
        def _get_ns(name: str):
            if "::" in name: 
                ns, d = name.split("::", maxsplit=1)
                return ns
            else:
                return name
        def _as_DAG(*, font: str = 'Arial') -> str:
            # def _join(names):
            #     MAXN = 3
            #     if len(names)>MAXN:
            #         _names = list(names)[:MAXN]+["..."]
            #     else:
            #         _names = names
            #     return "/".join(_names)
    
            lines = ["digraph G {"]
            
            lines += [
                f'graph [fontname="{font}"];',
                f'node  [fontname="{font}"];',
                f'edge  [fontname="{font}"];',
            ]
            lines.append(_render_node(NodeType.TRANSFORM, "given"))
            k2names: dict[Endpoint, set[str]] = {}
            for x in self.given:
                if _get_ns(x.dtype_name) in blacklist_namespaces: continue
                k = x.dtype
                k2names[k] = k2names.get(k, set())|{x.dtype_name}
            # for x in self.given:
            #     print(x.dtype_name, x.dtype)
            parents: set[Endpoint] = set()
            for e in k2names:
                for p in e.parents:
                    parents.add(p) # type: ignore
            shown_parents: set[str] = set()
            for p in parents:
                if p not in k2names: continue
                for inst in k2names[p]:
                    if _get_ns(inst) in blacklist_namespaces: continue
                    shown_parents.add(inst)
            for e, insts_all in k2names.items():
                insts = [i for i in insts_all if _get_ns(i) not in blacklist_namespaces]
                if len(insts)==0: continue
                for inst_name in insts:
                    for p in e.parents:
                        if p not in k2names: continue
                        pinsts = k2names[p] # type: ignore
                        pinsts = [i for i in pinsts if i in shown_parents]
                        for pname in pinsts:
                            lines.append(f'    "{pname}" -> "{inst_name}";')
                    lines.append(f'    "given" -> "{inst_name}";')
            seen = set()
            e2name = {}
            for step in self.steps:
                transform_name = f"{step.order} {step.transform.name}"
                lines.append(_render_node(NodeType.TRANSFORM, str(transform_name)))
                inputs, outputs = [], []
                for acc, deps in [
                    (inputs, step.transform.model.requires),
                    (outputs, [d for g in step.transform.model.produces for d in g]),
                ]:
                    for d in deps:
                        insts = step.dependency_map[d]
                        inst_names = {x.dtype_name for x in insts if _get_ns(x.dtype_name) not in blacklist_namespaces}
                        if len(inst_names)==0: continue
                        # _name = _join(inst_names)
                        # acc.append(_name)
                        acc += list(inst_names)
                        for inst in insts:
                            e2name[inst.dtype] = inst.dtype_name
                # inputs = []
                # for d in step.transform.model.requires:
                #     insts = step.dependency_map[d]
                #     inst_names = {x.dtype_name for x in insts if _get_ns(x.dtype_name) not in blacklist_namespaces}
                #     if len(inst_names)==0: continue
                #     inputs.append(_join(inst_names))
                for name in inputs:
                    lines.append(_render_node(NodeType.DATA, name))
                    x = f'    "{name}" -> "{transform_name}";'
                    if x not in seen: lines.append(x)
                    seen.add(x)
                for name in outputs:
                    lines.append(_render_node(NodeType.DATA, name))
                    x = f'    "{transform_name}" -> "{name}";'
                    if x not in seen: lines.append(x)
                    seen.add(x)
            lines.append(_render_node(NodeType.TRANSFORM, "target"))
            # for x in self.targets:
            #     print(x.instance.dtype_name, x.instance.dtype)
            # target_names = {}
            # for target in self.targets:
            #     e = target.instance.dtype
            #     target_names[e] = target_names.get(e, set())|{target.instance.dtype_name}
            for target in {x.instance.dtype_name for x in self.targets}:
            # for e, names in target_names.items():
            #     target = _join(names)
                lines.append(f'    "{target}" -> "target";')
            lines.append("}")
            return "\n".join(lines)
        
        dag_str = _as_DAG(font=font)
        src = graphviz.Source(dag_str, filename=path_base, format=format)
        src.render(cleanup=True, quiet=True)
        return path_base.parent/(path_base.name+f".{format}")

@dataclass
class WorkflowTask:
    ok: bool
    plan: WorkflowPlan
    data_libraries: list[DataInstanceLibrary] = field(default_factory=list)
    transform_libraries: list[TransformInstanceLibrary] = field(default_factory=list)

    def __post_init__(self):
        self._update_hash()

    def _update_hash(self):
        # self._hash, self._key = KeyGenerator.FromStr("".join(p._key for g in self.plans for p in g), l=8)
        self._hash, self._key = self.plan._hash, self.plan._key

    def GetKey(self):
        return self._key

    def _apply_fs_strategy(self, context: NextflowGenContext) -> None:
        """Refuse straddle-mounts; pick publishDir mode from mountinfo.

        S6 contract:
        - If cache_root and work_dir live on different mounts, raise
          StraddleMountError. Rename across mounts is non-atomic; this
          would break promote's loser-of-race contract.
        - Otherwise set context.cache_hit_strategy to 'copy' on a network
          FS (Lustre / NFS / GPFS / BeeGFS / etc.) and 'link' on a local
          FS. The default was 'link'; this only widens it when needed.

        Silently skipped when cache_root is None or METASMITH_CACHE is
        falsy (cache integration disabled).
        """
        if context.cache_root is None:
            return
        if os.environ.get("METASMITH_CACHE", "1").lower() in {
            "0", "false", "off", "no"
        }:
            return
        from ..caching.fs import assert_same_mount, detect_strategy

        # cache_root may not exist yet on a fresh workspace; resolve()
        # walks up to the first existing parent for the mountinfo match.
        anchor = context.cache_root
        while not anchor.exists() and anchor != anchor.parent:
            anchor = anchor.parent
        assert_same_mount(anchor, context.work_dir)
        context.cache_hit_strategy = detect_strategy(
            anchor, default=context.cache_hit_strategy
        )

    def _compute_cache_decisions(
        self, context: NextflowGenContext
    ) -> dict[int, dict]:
        """Compute per-step cache keys + probe results.

        Returns a dict[step.order, {cache_key, hit, entry?, transform_key,
        signature, sorted_input_ids, out_instance_ids}]. The OUTPUT
        instance_ids are needed by downstream steps as their input
        identities, so the walk runs in topological (step.order) order.

        Cache integration is skipped (returns {} effectively) when:
        - context.cache_root is None
        - env METASMITH_CACHE is set to "0" / "false" / "off"
        """
        if context.cache_root is None:
            return {}
        if os.environ.get("METASMITH_CACHE", "1").lower() in {
            "0", "false", "off", "no"
        }:
            return {}

        from ..caching.keys import (
            KEY_PREFIX,
            canonical_cbor,
            lineage_key,
            multihash_key,
        )
        from ..caching.store import CacheStore

        # Cache STORE is optional — probe-only flow doesn't require the
        # SQLite db to exist. Only open if the cache_root already exists
        # on disk (this matches "fresh workspace -> nothing to probe"
        # and avoids materializing an empty task_cache/ dir during the
        # first ever run).
        store = None
        if context.cache_root.exists():
            try:
                store = CacheStore.open(context.cache_root)
            except Exception:
                store = None

        decisions: dict[int, dict] = {}
        # Per-(step.order, slot_key, branch_idx) → output instance_id (hex).
        # Downstream steps use this to look up their inputs' ids when the
        # input came from an upstream step's output (not a given leaf).
        out_id_by_producer: dict[tuple[int, str, int], str] = {}

        def _input_instance_id(inst, source_step: int | None) -> bytes:
            """Encode the input's instance_id to bytes for the lineage key.

            We accept either a multihash hex string (new leaf / lineage
            ids minted by S2's AddItem path) or a legacy 10-char digest
            (DataInstances created without library-level mint, e.g.
            transform-library instances whose paths fall through to the
            legacy `_resolve_instance_meta` formula). In both cases the
            UTF-8 encoding is a stable, lossless byte rendering — the
            actual digest format does not matter for the cache_key, only
            that two identical inputs produce identical bytes.
            """
            return inst.instance_id.encode("utf-8")

        for step in self.plan.steps:
            transform_key = step.transform.GetKey() or step.transform.name or ""
            signature = str(step.transform._hash)

            sorted_inputs: list[tuple[str, bytes]] = []
            for dep in step.transform.model.requires:
                insts = step.dependency_map.get(dep, [])
                if not insts:
                    continue
                # Aggregate every instance feeding this slot. Sort the
                # ids to remove ordering noise from the input set.
                slot_ids = sorted(
                    _input_instance_id(i, None).hex() for i in insts
                )
                sorted_inputs.append((dep.key, "+".join(slot_ids).encode()))
            sorted_inputs.sort(key=lambda kv: kv[0])

            cache_key = lineage_key(transform_key, signature, sorted_inputs)

            # Compute per-output slot_ids (cache_key + dep.key + branch_idx).
            # G1 (C4): these `derived_hex` values ARE the slot_ids — the
            # production-channel identity for the (transform, slot, branch)
            # triple. They're stored on each produced DataInstance's
            # `instance_id` field so downstream steps see slot-identity on
            # their input sides. File-level identity (file_instance_id) is
            # minted post-facto by CollectResults (C6) over (slot_id, path)
            # and never travels on the Nextflow channel.
            #
            # dependency_map shares the same DataInstance object reference
            # between the producing step's produced dep and the consuming
            # step's required dep (via canonical get_or_create in
            # WorkflowPlan.Generate), so a single mutation propagates. We
            # run topologically, so each consumer iteration above sees ids
            # already rewritten.
            out_slot_ids: dict[tuple[str, int], str] = {}
            for branch_idx, dep_group in enumerate(step.transform.model.produces):
                for dep in dep_group:
                    slot_id_bytes = multihash_key(
                        canonical_cbor(
                            {"ck": cache_key, "s": dep.key, "b": branch_idx}
                        )
                    )
                    slot_id = slot_id_bytes.hex()
                    out_slot_ids[(dep.key, branch_idx)] = slot_id
                    out_id_by_producer[(step.order, dep.key, branch_idx)] = slot_id
                    for inst in step.dependency_map.get(dep, []):
                        inst.instance_id = slot_id
                        inst.origin = "lineage"
                        inst._refresh_derived_keys()
            step.RefreshViews()

            entry = None
            hit = False
            if store is not None:
                entry = store.probe(cache_key)
                if entry is not None and store.files_exist(entry):
                    hit = True

            decisions[step.order] = {
                "cache_key": cache_key,
                "transform_key": transform_key,
                "signature": signature,
                "sorted_inputs": sorted_inputs,
                "out_instance_ids": out_slot_ids,
                "hit": hit,
                "entry": entry,
                "cacheable": getattr(step.transform, "cacheable", True),
            }

        # C7 — emit the per-run trace.jsonl at compile time as v2
        # InvocationEvent rows. On each compile: if a prior trace.jsonl
        # exists, rotate it to `trace.<prev_session_id>.jsonl` (the
        # session_id read from its SessionStart sentinel, or 0 fallback);
        # then allocate a fresh session_id via the cache sqlite counter
        # and open a clean file headed by a SessionStart sentinel. All
        # subsequent emits in this compile carry the new session_id.
        # Post-exec promote (promote.py) appends miss/promoted/fail rows
        # carrying the same session_id, rediscovered from the sentinel.
        from ..models.lineage import (
            INVOCATION_EVENT_SCHEMA_VERSION,
            InvocationEvent,
            ProducedFile,
            SessionStart,
            append_invocation_event,
        )
        from ..constants import VERSION

        trace_dir = context.work_dir / "_metasmith"
        trace_dir.mkdir(parents=True, exist_ok=True)
        trace_path = trace_dir / "trace.jsonl"

        prev_session_id = 0
        if trace_path.exists():
            try:
                first_line = next(
                    (l for l in trace_path.read_text().splitlines() if l.strip()),
                    "",
                )
                if first_line:
                    head = json.loads(first_line)
                    if head.get("event") == SessionStart.EVENT_NAME:
                        prev_session_id = int(head.get("session_id", 0))
            except Exception:
                prev_session_id = 0
            rotated = trace_dir / f"trace.{prev_session_id}.jsonl"
            try:
                trace_path.rename(rotated)
            except OSError:
                # Falling back to truncate-overwrite is non-fatal: the
                # archived rows are lost but the fresh session proceeds.
                pass

        if store is not None:
            session_id = store.allocate_session_id()
        else:
            session_id = prev_session_id + 1

        sentinel = SessionStart(
            session_id=session_id,
            compile_started_at="",  # Date.now() omitted — set at writer
            metasmith_version=VERSION,
            schema_version=INVOCATION_EVENT_SCHEMA_VERSION,
        )
        with open(trace_path, "w", encoding="utf-8") as f:
            f.write(sentinel.to_jsonl() + "\n")

        for order, decision in sorted(decisions.items()):
            if not decision["hit"]:
                continue
            step_name = ""
            for step in self.plan.steps:
                if step.order == order:
                    step_name = step.transform.name or ""
                    break
            produces: list[ProducedFile] = []
            for (slot_key, branch_idx), slot_id in decision["out_instance_ids"].items():
                produces.append(
                    ProducedFile(
                        file_instance_id=slot_id,  # cache-hit: file_instance_id = slot_id until promote re-mints
                        slot_id=slot_id,
                        path="",
                        dtype_key=slot_key,
                    )
                )
            consumes: dict[str, list[str]] = {}
            for slot_key, instance_id_bytes in decision["sorted_inputs"]:
                consumes.setdefault(slot_key, []).append(instance_id_bytes.hex())
            event = InvocationEvent(
                task_hash=decision["cache_key"].hex(),
                transform_key=decision["transform_key"],
                status="hit",
                consumes=consumes,
                produces=produces,
                session_id=session_id,
                step_order=order,
                step_name=step_name,
                cache_key=decision["cache_key"].hex(),
            )
            append_invocation_event(trace_path, event)

        if store is not None:
            store.close()
        hits = sum(1 for d in decisions.values() if d["hit"])
        if hits:
            Log.Info(
                f"cache probe matched {hits}/{len(decisions)} step(s); "
                f"will short-circuit via synthetic Channel.of(...) emission"
            )
        return decisions

    def PrepareNextflow(self, context: NextflowGenContext):
        TAB = "\t"
        def _strip_var(s: str):
            return s[2:-1]
        if context.cache_root is None:
            context.cache_root = context.external_home / "task_cache"
        self._apply_fs_strategy(context)
        cache_decisions = self._compute_cache_decisions(context)
        # Derive task key from the per-task workspace name. external_work
        # is always <external_home>/runs/<task_key> by the StageWorkflow
        # invariant (agents.py:874-878), so the basename IS the task key.
        path_map = PathMap(
            extern_home=context.external_home,
            task_key=context.external_work.name,
        )
        bootstrap = [
            f"{_strip_var(context.bootstrap_var)} = '''",
            f"CONTAINER={context.home_dir}",
            f"DIRECT={context.external_home}",
            "function bootstrap {",
            TAB+f"if [ -e $CONTAINER ]; then",
            TAB+TAB+f"$CONTAINER/lib/msm_bootstrap $@",
            TAB+f"elif [ -e $DIRECT ]; then",
            TAB+TAB+f"$DIRECT/lib/msm_bootstrap $@",
            TAB+f"else",
            TAB+TAB+'echo "critical error: could not find metasmith bootstrap script"',
            TAB+f"fi",
            "}",
            f"'''",
            "",
            "def in(f, l) {",
            "    def rows = Channel.fromPath(f).splitCsv(header: false)",
            "    if (f in l) {",
            "        rows = Channel.fromList(l[f]).merge(rows)",
            "    }",
            "    return rows.map { row ->",
            "        if (row.size()>1) {",
            "            def (ri, rx) = row",
            "            return tuple(ri, file(rx))",
            "        } else {",
            "            def i = [:]",
            "            return tuple(i, file(row[0]))",
            "        }",
            "    }",
            "}",
            "",
            "",
        ]
        # The Nextflow HEADER assigns the params.home / params.workspace
        # variables. params.home is the literal host path; params.workspace
        # is rendered via the groovy dialect so the substitution is
        # prefix-aware (not str.replace, which would corrupt inner
        # occurrences — see tests/path_overhaul/test_str_replace_path_overlap.py).
        HEADER = "\n".join([
            "params.testSpread=1",
            f"{_strip_var(context.external_home_var)} = '{context.external_home}'",
            f'{_strip_var(context.external_work_var)} = "{path_map.Render(context.external_work, dialect="groovy")}"',
        ]+bootstrap)
        MAX_FILE_SIZE = int(2**16 * 0.95) # nextflow is 65536

        _archetypes: dict[DataInstance, DataInstance] = {}
        def get_archetype(candidates: list[DataInstance]):
            """Pick a stable representative for a set of equivalent instances.

            The closure body is the canonical-name dance: if any candidate
            already has a recorded archetype, reuse it (transitive merge);
            otherwise the first candidate becomes the archetype. The
            `_archetypes` dict is closure state — extraction would force
            it to become a class attribute on NextflowGenContext, adding
            indirection with no behavioral win (A3 decision).
            """
            a = None
            for c in candidates:
                if c not in _archetypes: continue
                a = _archetypes[c]
            if a is None:
                a = candidates[0]
            for c in candidates:
                _archetypes[c] = a
            return a
        # except for given data instances
        # intermediates are "collapsed"
        # we do not know the arity as steps can produce multiples instances,
        # at which point nextflow will branch automatically and flow into gropby junctions, etc.
        def get_io_signature(step: WorkflowStep):
            used_archetypes: list[DataInstance] = []
            for d in step.transform.model.requires:
                insts = step.dependency_map[d]
                archetype = get_archetype(insts)
                used_archetypes.append(archetype)
            produced_archetypes: list[list[DataInstance]] = []
            for dg in step.transform.model.produces:
                g = []
                for d in dg:
                    insts = step.dependency_map[d]
                    archetype = get_archetype(insts)
                    g.append(archetype)
                produced_archetypes.append(g)
            return used_archetypes, produced_archetypes

        def prepare_step(step: WorkflowStep):
            k = f"p{step.order:02}"
            process_name = str(step.transform.name)
            process_name = process_name.replace('/', '_')
            process_name = f"{k}__{process_name}"
            src = [f"process {process_name}"+" {"]
            src += [
                TAB+f"label 'x{step.transform.GetKey()}x'",
            ] + [
                TAB+f"label 'x{x}x'"
                for x in step.transform.labels
            ]
            # S3 — emit a publishDir directive into <cache_root>/<key>.tmp/
            # for cacheable miss steps so Nextflow itself stages outputs
            # into the cache staging area as it normally would for
            # publishDir. The post-exec promote step (S5) then validates
            # and renames the .tmp directory into its final cache slot.
            # `cacheable=False` and the env kill-switch skip this entirely.
            decision = cache_decisions.get(step.order)
            if decision is not None and decision.get("cacheable", True):
                cache_tmp = (
                    context.cache_root / f"{decision['cache_key'].hex()}.tmp"
                )
                src += [
                    TAB + (
                        f"publishDir \"{cache_tmp}\", "
                        f"mode: '{context.cache_hit_strategy}', "
                        "overwrite: true, "
                        "failOnError: true, "
                        "pattern: '*'"
                    )
                ]

            def _make_bind_var(i: int, is_assignment=False):
                s = "\\$" if not is_assignment else ""
                return f"{s}b{i+1}"
            raw_external_binds = set()
            for inst in step.uses:
                p = inst.ResolvePath()
                if not p.is_absolute(): continue
                p = path_map.LocalToExternal(p)
                raw_external_binds.add(p.parent)
            external_binds = self._get_common_folders(raw_external_binds)
            external_binds_param = ""
            if len(external_binds)>0:
                external_binds_param = Container(
                    image="",
                    binds=[
                        (_make_bind_var(i), _make_bind_var(i))
                        for i, _ in enumerate(external_binds)
                    ],
                    runtime=context.container_runtime,
                ).MakeBindsParam()

            res = step.transform.resources
            src_res = [] # goes to config to not mess with caching
            if res is not None: 
                src_res += [x for x in res.AsNextflowFormat(is_config=True)] # config!
            duration_is_strict = res is not None and res.duration is not None and res.duration.strict
            memory_is_strict = res is not None and res.memory is not None and res.memory.strict
            if duration_is_strict and memory_is_strict:
                src_res += [
                    "errorStrategy 'ignore'" # no point in retrying if not changing resource requests
                ]
            used_archetypes, produced_archetypes = get_io_signature(step)
            dep_in = {
                d.key: [inst.instance_id for inst in step.dependency_map.get(d, [])]
                for d in step.transform.model.requires
            }
            dep_out = [
                {
                    d.key: [inst.instance_id for inst in step.dependency_map.get(d, [])]
                    for d in dep_group
                }
                for dep_group in step.transform.model.produces
            ]
            structure_arity = {
                d.key: len(step.dependency_map.get(d, []))
                for d in itertools.chain(
                    step.transform.model.requires,
                    [d for g in step.transform.model.produces for d in g],
                )
            }
            sample_arity = len(step.group_by_instances)
            step_meta_file = f"workflow.step_{step.order}.meta"
            cache_decision = cache_decisions.get(step.order)
            with open(context.work_dir / step_meta_file, "w") as f:
                f.write(f"din {json.dumps(dep_in, separators=(',',':'))}\n")
                f.write(f"dot {json.dumps(dep_out, separators=(',',':'))}\n")
                f.write(f"sar {json.dumps(structure_arity, separators=(',',':'))}\n")
                f.write(f"par {sample_arity}\n")
                # S3 — cache_key + per-output instance_ids land in the
                # step meta so the post-exec promote step (S5) can locate
                # what to write, and `msm status <key>` (S8) can render
                # per-task provenance. cacheable comes from the
                # TransformInstance (S4 default True); the post-exec
                # promote skips write when False.
                if cache_decision is not None:
                    f.write(
                        f"cache_key {cache_decision['cache_key'].hex()}\n"
                    )
                    out_ids_serialized = {
                        f"{slot}::{branch}": iid
                        for (slot, branch), iid
                        in cache_decision["out_instance_ids"].items()
                    }
                    f.write(
                        "out_identities "
                        f"{json.dumps(out_ids_serialized, separators=(',',':'))}\n"
                    )
                    f.write(
                        f"cacheable {'true' if cache_decision['cacheable'] else 'false'}\n"
                    )
                    f.write(f"transform_key {cache_decision['transform_key']}\n")
            mock_outputs = [
                f'"1-1-{branch+1}.test$hash-{x.dtype.key}{x.dtype.GetPreferredFileExtension()}"'
                for branch, g in enumerate(produced_archetypes) for x in g
            ]

            if len(produced_archetypes)>1: # if there is branching, outputs must be set to optional
                optional = ", optional: true"
            else:
                optional = ""
            src += [
                "input:",
                TAB+f'tuple '+','.join(['val(index)']+[f'path(_{i+1:02})' for i, x in enumerate(used_archetypes)])
            ] + [
                "output:",
            ] + [
                # TAB+f'tuple val(index),path("{add_prefix(x.path)}")'
                TAB+f'tuple val(index),path("*-{branch+1}.*-{x.dtype.key}{x.dtype.GetPreferredFileExtension()}"){optional}'
                for branch, g in enumerate(produced_archetypes) for x in g
            ] + [
                "script:",
                '"""',
                f'echo "step {step.order}, sample $index"',    # this is used to extract logs in agent.RunWorkflow()
                f'echo "{step.transform.name}"',
                f'echo "res $task.cpus/$task.memory/$task.attempt" >>{METADATA_FILE}',
                # C4 — wrap the channel's index map in the LinPayload v2
                # envelope `{"v": 2, "entries": <index>}`. Orchestrator.groovy
                # is untouched; the JSON literal is composed in bash from the
                # raw `Orchestrator.JsonforEcho(index)` output. Bootstrap (C5)
                # parses this via `LinPayload.from_json`.
                f'echo "lin {{\\"v\\":2,\\"entries\\":${{Orchestrator.JsonforEcho(index)}}}}" >>{METADATA_FILE}',
                f'echo "fmt 2" >>{METADATA_FILE}',
                f'cat ${{params.workspace}}/{step_meta_file} >>{METADATA_FILE}',
                f'echo "inp {",".join(x.dtype.key for x in used_archetypes)}" >>{METADATA_FILE}',
                f'echo "out {";".join(",".join(x.dtype.key for x in g) for g in produced_archetypes)}" >>{METADATA_FILE}',
            # ] + [
            #     f'echo "i{i+1:02} $_{i+1:02}">>{METADATA_FILE}'
            #     for i, x in enumerate(used_archetypes)
            ] + [
                f'{_make_bind_var(i, is_assignment=True)}="{p}"'
                for i, p in enumerate(external_binds)
            ] + [
                f'echo "{external_binds_param}" >{BIND_FILE}',
                f'{context.bootstrap_var}',
                f'bootstrap {context.external_work_var} "{step.order}" ${{params.hostName}}',
                f'[ -e .command.success ] && exit 0 || exit 1', # in case slurm silently kills proc from oom/timeout
                '"""',
                'stub:',
                'def dt = new Random().nextFloat()*params.testSpread',
                'def hash = "${index[0].sort().collectEntries { k, v -> [k, v.sort()] }}".md5()[0..11]', # 12 characters
                f'"""',
                f'sleep $dt',
                f'touch {" ".join(mock_outputs)}',
                f'"""',
                "}",
                ""
            ]
            return process_name, "\n".join(src), src_res

        def ensure_local_folder(n):
            d = context.work_dir/n
            d.mkdir(exist_ok=True)
            return d
        
        the_plan = self.plan
        _given = set(the_plan.given)
        used_given = {x for s in the_plan.steps for x in s.uses if x in _given}
        given_endpoints = {x.dtype for x in used_given}

        # find merges
        inputs_dir = ensure_local_folder("inputs")
        e2producer: dict[Endpoint, list[WorkflowStep]] = {}
        for step in the_plan.steps:
            for pg in step.produces:
                for inst in pg:
                    e2producer[inst.dtype] = e2producer.get(inst.dtype, [])+[step]
        final_steps_for_merging: dict[int, set[Endpoint]] = {}
        for e, steps in e2producer.items():
            if e not in given_endpoints and len(steps)<2: continue
            k = max(s.order for s in steps)
            final_steps_for_merging[k] = final_steps_for_merging.get(k, set())|{e}
        output_copies: dict[str, int] = {}
        to_merge_names: dict[Endpoint, list[str]] = {}
        def get_prod_name(x: Endpoint, force_singular=False):
            k = x.key
            arity = len(e2producer.get(x, []))+int(x in given_endpoints)
            if not force_singular and arity>1:
                i = output_copies.get(k, 0)+1
                output_copies[k] = i
                name = f"{k}_{i}"
                _curr = to_merge_names.get(x, [])
                if name not in _curr: _curr.append(name)
                to_merge_names[x] = _curr
            else:
                name = f"{k}"
            return name
        
        # goal:
        # _tK9GI0FH = (o.post([in("inputs/tK9GI0FH")], ["tK9GI0FH"]))[0] // lib::pangenome_heatmap.py
        # _7A15qSzL = (o.post([in("inputs/7A15qSzL")], ["7A15qSzL"]))[0] // containers::python_for_data_science.oci
        # _urCt2PG9 = (o.post([in("inputs/urCt2PG9")], ["urCt2PG9"]))[0] // sequences::gbk
        # _seen = set()
        unsorted_input_channels: dict[Endpoint, list[DataInstance]] = {}
        _child2parents: dict[Endpoint, set[Endpoint]] = {}
        for inst in the_plan.given:
            e = inst.dtype
            unsorted_input_channels[e] = unsorted_input_channels.get(e, [])+[inst]
            _child2parents[e] = _child2parents.get(e, set())|e.parents # type: ignore
        input_channels: dict[Endpoint, list[DataInstance]] = {}
        # sort givens by lineage
        # parents must be registered (and processed) first!
        while len(_child2parents)>0:
            to_add = []
            for ce, pes in _child2parents.items():
                if len(pes)>0: continue
                to_add.append(ce)
            to_add = sorted(to_add, key=lambda e: unsorted_input_channels[e][0].dtype_name)
            for e in to_add:
                input_channels[e] = unsorted_input_channels[e]
                del _child2parents[e]
            added = set(to_add)
            for e in _child2parents:
                _child2parents[e] = _child2parents[e]-added

        given2order = {}
        for i, x in enumerate(the_plan.given):
            given2order[x] = i
        prepared_given: list[tuple[Path, str, str]] = []
        _seen_paths = set()
        _given_by_prod_name: dict[str, list[DataInstance]] = {}
        _path2prod_name = {}
        for i, (_, lst) in enumerate(input_channels.items()):
            inst = get_archetype(lst)
            p = inputs_dir/f"{get_prod_name(inst.dtype, force_singular=True)}"
            if p in _seen_paths: continue
            _seen_paths.add(p)
            v = get_prod_name(inst.dtype)
            k = p, v, "/".join({i.dtype_name for i in lst})
            prepared_given.append(k)
            with open(p, "w") as f:
                unique_lst = set(lst)
                if len(unique_lst)==1:
                    to_write = [inst]
                else:
                    to_write = sorted(lst, key=lambda x: given2order[x])
                _given_by_prod_name[v] = to_write
                for i, x in enumerate(to_write):
                    _path = x.ResolvePath()
                    _path2prod_name[_path] = v
                    f.write(f"{_path}"+"\n")
        
        _given_lineage = {}
        given_lineage_by_keys = {}
        for prod_name, to_write in _given_by_prod_name.items():
            _indexes = []
            for x in to_write:
                _index = {}
                for p in [x.parent_lib.Get(p.path).ResolvePath() for p in x.parent_lib.parents.get(x.path, [])]:
                    if p not in _path2prod_name: continue # spurious parent, not used in wf
                    _prod_name = _path2prod_name[p]
                    _hash = md5(str(p).encode()).hexdigest()
                    _hash = int(_hash[:15], 16) # 15 is important as it allows us to disregard the sign of a long and match with java
                    _index[_prod_name] = _index.get(_prod_name, [])+[_hash]
                _indexes.append(_index)
            if all(len(idx)>0 for idx in _indexes):
                k = prod_name.split('_')[0] # in case this will be merged and has a "_1" suffix
                _given_lineage[str(inputs_dir.relative_to(context.work_dir)/k)] = _indexes
                given_lineage_by_keys[k] = given_lineage_by_keys.get(k, set())|{k for x in _indexes for k in x.keys()}
        LINEAGE_FILE = "workflow.lineage_of_given.json"
        _lineage_file_data = {
            "lineage": _given_lineage,
            "child2parent": {k: sorted(v) for k, v in given_lineage_by_keys.items()},
        }
        with open(context.work_dir/LINEAGE_FILE, "w") as f:
            json.dump(_lineage_file_data, f, separators=(',', ':'))

        # goal:
        # k = ['h']
        # h = (o.post([*p1(o.group('f', o.using([f], k)))], k))[0]
        # or this for when batching
        # y = (o.post(o.debatch([*b1(o.batch(o.group('g', o.using([g], k)), 3))]), k))[0]
        # (multi-output processes still use parenthesized destructure, e.g.
        #  (h, y) = o.post([*p1(...)], k) — strict syntax accepts >=2 vars)
        target_endpoints = {x.instance.dtype for x in the_plan.targets}
        src_process = []
        wf_main = []
        wf_publish = set()       
        published_channels: dict[str, tuple[int, DataInstance]] = {}
        resources = {}

        # NOTE: DSL2 implicitly forks channels even when wrapped in [name, channel]
        # tuples and consumed inside Orchestrator.group(). multiMap forking was
        # added in a94a3d1 but proven unnecessary — see tests:
        #   test_channel_reuse_across_group_calls
        #   test_stream_reuse_works_in_orchestrator
        # Do not re-add multiMap here.

        for step in the_plan.steps:
            decision = cache_decisions.get(step.order)
            is_hit = bool(decision and decision.get("hit"))
            used_archetypes, produced_archetypes = get_io_signature(step)
            produced_names = [get_prod_name(x.dtype) for g in produced_archetypes for x in g]
            produced_snames = [get_prod_name(x.dtype, force_singular=True) for g in produced_archetypes for x in g]
            produced = ", ".join(f"_{x}" for x in produced_names)
            produced_k = [f"'{x}'" for x in produced_snames]
            produced_k = ", ".join(produced_k)
            wf_main.append(f"k = [{produced_k}]")

            if is_hit:
                # S3 — synthetic Channel.of for cache hits. Replace the
                # process call with N channels (one per produced dep,
                # ordered by branch then dep) where each channel emits
                # `(index, file)` tuples for the cached files matching
                # the canonical `1-1-{branch+1}.*-{dtype_key}{ext}`
                # filename shape. The tuple re-enters o.post() exactly
                # as a real process output would (Critic E#1 pin).
                cache_out = (
                    context.cache_root
                    / decision["cache_key"].hex()[:2]
                    / decision["cache_key"].hex()[2:]
                    / "out"
                )
                cached_channels: list[str] = []
                cached_channel_var = f"__cached_step_{step.order}"
                channel_exprs: list[str] = []
                for branch_idx, dep_group in enumerate(step.transform.model.produces):
                    for dep in dep_group:
                        insts = step.dependency_map.get(dep, [])
                        if not insts:
                            channel_exprs.append("Channel.empty()")
                            continue
                        out_inst = insts[0]
                        ext = out_inst.dtype.GetPreferredFileExtension()
                        suffix = f"-{out_inst.dtype.key}{ext}"
                        branch_prefix = f"1-1-{branch_idx + 1}."
                        cached_files = sorted(
                            f for f in (cache_out.glob("*") if cache_out.exists() else [])
                            if f.is_file()
                            and f.name.startswith(branch_prefix)
                            and f.name.endswith(suffix)
                        )
                        if not cached_files:
                            channel_exprs.append("Channel.empty()")
                            continue
                        tuples = ", ".join(
                            f"[[:], file('{fp}')]" for fp in cached_files
                        )
                        channel_exprs.append(f"Channel.of({tuples})")
                wf_main.append(
                    f"def {cached_channel_var} = [{', '.join(channel_exprs)}]"
                )
                if len(produced_names) == 1:
                    wf_main.append(
                        f"_{produced_names[0]} = "
                        f"(o.post(o.asStreams({cached_channel_var}), k))[0]"
                    )
                else:
                    wf_main.append(
                        f"({produced}) = "
                        f"o.post(o.asStreams({cached_channel_var}), k)"
                    )
                # Final-step merging still applies if the cached step is
                # the producer of a target.
                if step.order in final_steps_for_merging:
                    for e in final_steps_for_merging[step.order]:
                        names = to_merge_names[e]
                        to_mix = [f"_{x}" for x in names]
                        name = get_prod_name(e, force_singular=True)
                        wf_main.append(
                            f"_{name} = o.mix([{', '.join(to_mix)}])"
                        )
                if the_plan.publish_intermediates:
                    to_pubish = [x for g in produced_archetypes for x in g]
                else:
                    to_pubish = [
                        x for g in produced_archetypes for x in g
                        if x.dtype in target_endpoints
                    ]
                for inst in to_pubish:
                    k = inst.dtype.key
                    wf_publish.add(k)
                    published_channels[k] = (step.order, inst)
                continue

            process_name, src, src_res = prepare_step(step)
            resources[process_name] = src_res
            src_process.append(src)
            if len(used_archetypes)>0:
                _inst = step.group_by_instances
                _dtypes = {x.dtype.key for x in _inst}
                if len(_dtypes)>1:
                    Log.Warn(f"unexpected plural groupby instance refernce for [{step.transform.name}:{step.transform.group_by}]: [{_inst}]")
                _inst = _inst[0]
                gb = _inst.dtype.key
                using_symbols = ", ".join(f"_{x.dtype.key}" for x in used_archetypes)
                used = f"o.group('{gb}', [{using_symbols}], k, {step.transform.batch_size})"
            else:
                used = ""
            if len(produced_names) == 1:
                # Nextflow 26.04+ strict syntax rejects single-element parenthesized
                # multiple-assignment `(_x) = expr`; use indexed access instead.
                # It also rejects `[*proc(...)]` (spread in list literal), so we
                # route through `o.asStreams(...)` (defined in Orchestrator.groovy,
                # which is loaded via -lib and not subject to strict syntax).
                wf_main.append(
                    f"_{produced_names[0]} = (o.post(o.asStreams({process_name}({used})), k))[0]"
                )
            else:
                wf_main.append(
                    f"({produced}) = o.post(o.asStreams({process_name}({used})), k)"
                )
            if step.order in final_steps_for_merging:
                for e in final_steps_for_merging[step.order]:
                    names = to_merge_names[e]
                    to_mix = [f"_{x}" for x in names]
                    name = get_prod_name(e, force_singular=True)
                    wf_main.append(
                        f"_{name} = o.mix([{', '.join(to_mix)}])"
                    )

            if the_plan.publish_intermediates:
                to_pubish = [x for g in produced_archetypes for x in g]
            else:
                to_pubish = [x for g in produced_archetypes for x in g if x.dtype in target_endpoints]
            for inst in to_pubish:
                k = inst.dtype.key
                wf_publish.add(k)
                published_channels[k] = (step.order, inst)

        with open(context.work_dir/context.resources_file, "w") as f:
            _src = [
                "process {"
            ]
            for name, lines in resources.items():
                _src += [
                    TAB+f"withName: '{name}' "+"{"
                ] + [
                    TAB+TAB+l for l in lines
                ] + [
                    TAB+"}"
                ]
            _src.append("}")
            for line in _src:
                f.write(line+"\n")

        wf_output = []
        _e2target = {x.instance.dtype:x for x in the_plan.targets}
        for ch, (step_order, inst) in published_channels.items():
            spec_name = inst.dtype_name.replace(' ', '_').replace("::", "-")
            if inst.dtype in _e2target:
                out_name = _e2target[inst.dtype].name.replace(' ', '_').replace("::", "-")
            else:
                out_name = f"{step_order}_{spec_name}"
            wf_output += [
                TAB+f"_{ch}"+"{",
                TAB+TAB+f"path '{out_name}'",
                TAB+TAB+f"index {{ path '_manifests/{spec_name}.{inst.dtype.key}.{inst.instance_id}.json' }}",
                TAB+"}",
            ]
            
        content = [
            f"workflow"+" {",
            "main:",
            f'o = new Orchestrator(Channel.fromList([null])) // cant create channels in groovy',
            f'_lf = new groovy.json.JsonSlurper().parseText(file("{LINEAGE_FILE}").text)',
            f'l = _lf.lineage',
            f'o.seedParents(_lf.child2parent)',
        ] + [
            f'_{v} = (o.postIn([in("{p.relative_to(context.work_dir)}", l)], ["{p.name}"]))[0] // {n}'
            for p, v, n in prepared_given # this must be (and is) sorted in lineage order
        ] + [
            line for line in wf_main
        ] + [
            "",
            "publish:",
        ] + [
            f"_{k} = o.publish(_{k})"
            for k in wf_publish
        ] + [
            "}",
            "",
            "output {",
        ] + [
            line for line in wf_output
        ] + [
            "}",
        ]
        
        with open(context.work_dir/context.workflow_file, "w") as f:
            f.write("\n".join([HEADER]+src_process+content))

    def _get_common_folders(self, folders:Iterable[Path]):
        roots: list[str] = []
        def join(a, b):
            return os.path.commonpath([a, b])

        def update(p: str):
            nonlocal roots
            bi, best, result = None, None, ""
            for i, r in enumerate(roots):
                x = join(r, p)
                if x == "/": continue
                score = len(r)-len(x)
                if best is None or score<best:
                    bi, best, result = i, score, x
            if bi is not None:
                roots[bi] = result
            else:
                roots.append(p)

        for path in folders:
            update(str(path))
        return [Path(p) for p in roots]

    def GetCommonInputFolders(self, method="external"):
        """
        @method is: external | internal | all
        """
        assert method in {"external", "internal", "all"}
        def should_keep(inst: DataInstance):
            match(method):
                case "external":
                    return inst.path.is_absolute()
                case "internal":
                    return not inst.path.is_absolute()
                case "all":
                    return True
        given = {inst.ResolvePath().parent for inst in self.plan.given if should_keep(inst)}
        return self._get_common_folders(given)

    def Pack(self):
        return dict(
            ok=self.ok,
            data_libraries=[lib.GetKey() for lib in self.data_libraries],
            transform_libraries=[lib.GetKey() for lib in self.transform_libraries],
        )

    def SaveAs(self, dest: Source, partial: str|Literal[False]=False):
        assert partial in {"data_only", "transforms_only", False}
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            _task_path = temp_dir/"task.yml"
            with open(_task_path, "w") as f:
                yaml.dump(dict(
                    task=self.Pack(),
                    plan=self.plan.Pack(),
                ), f)
            _mover = Logistics()
            _mover.QueueTransfer(
                src=Source(address=str(temp_dir), type=SourceType.DIRECT),
                dest=dest,
            )
            if partial != "transforms_only":
                for lib in self.data_libraries:
                    _temp_mover = lib.PrepTransfer(dest/f"data/{lib.GetKey()}")
                    _mover._queue.extend(_temp_mover._queue)
            if partial != "data_only":
                for lib in self.transform_libraries:
                    _temp_mover = lib.PrepTransfer(dest/f"transforms/{lib.GetKey()}")
                    _mover._queue.extend(_temp_mover._queue)
            res = _mover.ExecuteTransfers(wait_for_complete=True)
            return res

    @classmethod
    def Load(cls, path: Path|str, alt_data_paths: list[Path|str]|None=None):
        path = Path(path)
        with open(path/"task.yml") as f:
            d = yaml.safe_load(f)
        raw_task = d["task"]
        raw_plan = d["plan"]

        _data_lib_paths = [Path(p) for p in alt_data_paths] if alt_data_paths else []
        _data_lib_paths += [path/"data"] # prefer alts first
        def load_lib(lib_key: str):
            for d in _data_lib_paths:
                p = d/lib_key
                if p.exists():
                    return DataInstanceLibrary.Load(p)
            raise FileNotFoundError(f"could not find data library [{lib_key}], tried {_data_lib_paths}")
        data_libs = {n: load_lib(n) for n in raw_task["data_libraries"]}
        tr_libs = {n: TransformInstanceLibrary.Load(path/f"transforms/{n}") for n in raw_task["transform_libraries"]}
        _libraries: dict[str, DataInstanceLibrary] = data_libs|tr_libs
        plan =  WorkflowPlan.Unpack(raw_plan, _libraries)
        return cls(
            ok=raw_task["ok"],
            plan=plan,
            data_libraries=[data_libs[n] for n in raw_task["data_libraries"]],
            transform_libraries=[tr_libs[n] for n in raw_task["transform_libraries"]],
        )
