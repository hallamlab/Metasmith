from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import yaml

from ...hashing import KeyGenerator
from ...logging import Log
from ..dag_renderer import DagMode, DagRenderer, Label, LabelMode, NodeKind
from ..paths import is_deferred
from ..libraries import (
    DataInstance, DataInstanceLibrary, DataInstanceLibraryView,
    TransformInstance, TransformInstanceLibrary, TransformInstanceLibraryView,
)
from ..solver import (
    Application, Dependency, Endpoint, Transform, solve_by_mcts,
    Solution as SolverResult,
)
from .diagnostics import PlanHint, _diagnose_plan_failure
from .steps import WorkflowStep, WorkflowTarget


class GivenNotImportedError(ValueError):
    """A given's identity was minted here rather than read from a record."""


# A given's identity has to be a record, not something this process worked out.
# `stat_leaf_id` folds the absolute path and the mtime of whatever this process
# can see, so it moves when the file is touched, and it is invented outright
# when the path belongs to another host -- which is every input of a run driven
# from a workstation. `_mint_leaf_id` marks what it invented, `Pack` does not
# carry the mark, and so the question "did somebody register this here, or did
# it come from somewhere that already knew?" survives exactly one process.
#
# A deferred path is exempt. There is nothing at it yet, so there is nothing to
# import, and its id is its own name rather than a reading of a filesystem.
# `StageWorkflow.RefuseIfDeferred` is what catches one that never got a source.
_OFFENDERS_SHOWN = 20

_NOT_IMPORTED = """\
a plan's givens are references to data a pool already holds, and {n} of these {is_} not:
{items}
Import each one on the agent that will run the plan, then reference it by name:

  metasmith data import <path> --dtype <NS::TYPE> --name <name> --agent-home <home>
  given = agent.GivenLibrary(["<name>", ...], location=<dir>)

Registering a path here mints an identity from the filesystem this process can
see. It moves when the file is touched, and it is invented outright when the
path lives on another host -- which is why the plan key moved on every
submission and why -resume found nothing. An import assigns an identity once
and the pool records it, so referencing one costs nothing and never moves."""


def _was_minted_here(inst: DataInstance) -> bool:
    lib = inst.parent_lib
    meta = getattr(lib, "instance_meta", {}).get(inst.path)
    return bool(meta and meta.get("minted"))


def _refuse_unminted_givens(instances: list[DataInstance]) -> None:
    """Refuse a given this process registered instead of referencing.

    Scoped to the given path on purpose. A transform registers its own
    products and its deferred slots in its own library, and those are mints by
    construction -- they are outputs, not givens, and nothing here sees them.
    """
    offenders: list[DataInstance] = []
    seen: set[str] = set()
    for inst in instances:
        if is_deferred(inst.path) or not _was_minted_here(inst):
            continue
        # One path can carry several endpoints, and each is its own instance.
        # The caller has one thing to fix, so name it once.
        if inst.instance_id in seen:
            continue
        seen.add(inst.instance_id)
        offenders.append(inst)
    if not offenders:
        return
    shown = [
        f"  [{inst.ResolvePath()}] as [{inst.dtype_name}]"
        for inst in offenders[:_OFFENDERS_SHOWN]
    ]
    if len(offenders) > _OFFENDERS_SHOWN:
        shown.append(f"  ... and {len(offenders) - _OFFENDERS_SHOWN} more")
    raise GivenNotImportedError(_NOT_IMPORTED.format(
        n=len(offenders),
        is_="is" if len(offenders) == 1 else "are",
        items="\n".join(shown),
    ))


@dataclass
class CaseIndex:
    group_case: list[int] = field(default_factory=list)
    group_label: list[str] = field(default_factory=list)
    inst_cases: dict[str, set[int]] = field(default_factory=dict)

    def samples_of(self, cases: set[int]) -> list[str]:
        return [lbl for c, lbl in zip(self.group_case, self.group_label) if c in cases]


def CollectSolverInputs(
    given: list[list[DataInstanceLibraryView]],
    transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
) -> tuple[
    dict[Endpoint, list[DataInstance]],
    list[set[Endpoint]],
    dict[Transform, TransformInstance],
    dict[TransformInstance, TransformInstanceLibrary],
    CaseIndex,
]:
    given_map: dict[Endpoint, list[DataInstance]] = {}
    _key2inst: dict[tuple, DataInstance] = {}
    _view_eps_cache: dict[int, set[Endpoint]] = {}
    _view_keys_cache: dict[int, list[tuple]] = {}
    given_endpoints: list[set[Endpoint]] = []
    _seen_group_keys: dict[tuple, tuple[int, str]] = {}
    cases = CaseIndex()
    for group in given:
        group_key = tuple((id(lib._original), lib._mask_key) for lib in group)
        if group_key in _seen_group_keys:
            case, label = _seen_group_keys[group_key]
            cases.group_case.append(case)
            cases.group_label.append(label)
            continue

        eps = set()
        group_keys: list[tuple] = []
        for lib in group:
            view_id = id(lib)
            if view_id in _view_eps_cache:
                eps.update(_view_eps_cache[view_id])
                group_keys += _view_keys_cache[view_id]
                continue
            lib_eps = set()
            lib_keys = []
            for path, ep_name, ep in lib.Iterate():
                dedup_key = (path, ep)
                if dedup_key not in _key2inst:
                    inst = DataInstance(
                        path=path,
                        dtype=ep,
                        dtype_name=ep_name,
                        parent_lib=lib._original,
                    )
                    _key2inst[dedup_key] = inst
                    given_map.setdefault(ep, []).append(inst)
                lib_eps.add(ep)
                lib_keys.append(dedup_key)
            _view_eps_cache[view_id] = lib_eps
            _view_keys_cache[view_id] = lib_keys
            eps.update(lib_eps)
            group_keys += lib_keys
        case = next((i for i, g in enumerate(given_endpoints) if g==eps), None)
        if case is None:
            given_endpoints.append(eps)
            case = len(given_endpoints)-1
        label = ", ".join(sorted({str(path) for path, _ in group_keys}))
        _seen_group_keys[group_key] = case, label
        cases.group_case.append(case)
        cases.group_label.append(label)
        for k in group_keys:
            cases.inst_cases.setdefault(_key2inst[k].instance_id, set()).add(case)

    _refuse_unminted_givens(
        [inst for insts in given_map.values() for inst in insts]
    )

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
    return given_map, given_endpoints, transform2inst, inst2trlib, cases


def _is_target(a: Application) -> bool:
    return len(a.used) > 0 and sum(len(g) for g in a.produced) == 0


def _products(a: Application) -> list[Endpoint]:
    return [e for g in a.produced for e in g.values()]


def _nested(given_endpoints: list[set[Endpoint]]) -> bool:
    smallest = min(given_endpoints, key=len)
    return all(smallest <= g for g in given_endpoints)


def _route(given: set[Endpoint], plan: list[Application]) -> list[Application]|None:
    # The steps of `plan` that `given` alone drives to the target. A step whose
    # every product is already at hand is skipped, so a given intermediate is
    # used and never remade.
    have = set(given)
    fired: list[Application] = []
    pending = [a for a in plan if a.used]
    progressed = True
    while progressed:
        progressed = False
        for a in list(pending):
            if not all(e in have for e in a.used.values()):
                continue
            pending.remove(a)
            products = _products(a)
            if products and all(e in have for e in products):
                continue
            fired.append(a)
            progressed = True
            if not products:
                return fired
            have.update(products)
    return None


def _union_of_routes(routes: list[list[Application]]) -> tuple[list[Application], dict[int, set[int]]]:
    # One step per distinct application across the cases' routes: the same
    # transform object bound to equal endpoints, counted per occurrence. Each
    # step is scoped to the cases whose route contains it.
    keyed: dict[tuple, Application] = {}
    scope: dict[int, set[int]] = {}
    rank: dict[int, tuple[int, int]] = {}
    for c, route in enumerate(routes):
        seen: dict[tuple, int] = {}
        for pos, a in enumerate(route):
            sig = (id(a.transform), a.Signature())
            n = seen.get(sig, 0)
            seen[sig] = n+1
            u = keyed.setdefault((*sig, n), a)
            scope.setdefault(id(u), set()).add(c)
            rank.setdefault(id(u), (c, pos))
    apps = list({id(a): a for a in keyed.values()}.values())
    producers: dict[Endpoint, list[Application]] = {}
    for a in apps:
        for e in _products(a):
            producers.setdefault(e, []).append(a)
    preds: dict[int, set[int]] = {
        id(a): {id(p) for e in a.used.values() for p in producers.get(e, []) if p is not a}
        for a in apps
    }
    order: list[Application] = []
    done: set[int] = set()
    while len(order) < len(apps):
        ready = [a for a in apps if id(a) not in done and preds[id(a)] <= done]
        assert ready, "the cases' routes do not order into one plan"
        nxt = min(ready, key=lambda a: rank[id(a)])
        order.append(nxt)
        done.add(id(nxt))
    return order, scope


def _route_problems(
    apps: list[Application], given: set[Endpoint], transform2inst: dict[Transform, TransformInstance],
) -> list[str]:
    # Drive one case's givens through the steps that run for it. The case is
    # covered when a target step fires, and every slot a fired step reads has
    # exactly one source: the case's own given or one fired producer.
    have = set(given)
    fired: list[Application] = []
    pending = [a for a in apps if a.used]
    progressed = True
    while progressed:
        progressed = False
        for a in list(pending):
            if all(e in have for e in a.used.values()):
                pending.remove(a)
                fired.append(a)
                have.update(_products(a))
                progressed = True
    if not any(_is_target(a) for a in fired):
        return ["no step reaches the target"]
    problems = []
    for a in fired:
        name = transform2inst[a.transform].name if a.transform in transform2inst else "target"
        if any(e in given for e in _products(a)):
            problems.append(f"[{name}] remakes a given")
        for d, e in a.used.items():
            n = int(e in given) + sum(1 for p in fired if p is not a and e in _products(p))
            if n != 1:
                problems.append(f"[{name}] reads [{d.key}] from {n} sources")
    return problems


@dataclass
class WorkflowPlan:
    given: list[DataInstance]
    targets: list[WorkflowTarget]
    steps: list[WorkflowStep]
    _solver_result: SolverResult|None=None
    _archetype_translation: dict[DataInstance, DataInstance]|None = None
    dropped_targets: list[str] = field(default_factory=list)
    dropped_samples: list[str] = field(default_factory=list)
    hints: list[PlanHint] = field(default_factory=list)
    # A run publishes its targets and nothing else. The per-step outputs stay in
    # the work dir and the cache store, so what a collect copies back is what was
    # asked for -- at the cost of a run that dies early leaving an empty results
    # folder, where the per-step logs are then the only record.
    publish_intermediates: bool = False
    _solver_inputs: tuple|None = None

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
            publish_intermediates=raw.get("publish_intermediates", False),
        )

    @classmethod
    def Generate(
        cls,
        given: list[list[DataInstanceLibraryView]],
        transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
        target_names: list[str],
        target_model: Transform,
        max_iter: int=256, max_refine: int|None=None, seed: int=42,
    ):
        given_map, given_endpoints, transform2inst, inst2trlib, cases = CollectSolverInputs(
            given, transforms,
        )

        _pl1 = "" if len(given)==1 else "s"
        _pl2 = "" if len(given_endpoints)==1 else "s"
        Log.Info(f"solving plan for [{len(given)}] sample{_pl1} as [{len(given_endpoints)}] unique case{_pl2}")

        solver_inputs = (given_endpoints, list(transform2inst.keys()), target_model)
        common = dict(
            solver_inputs=solver_inputs, given_map=given_map, transform2inst=transform2inst,
            transforms=transforms, target_names=target_names, target_model=target_model,
        )
        if len(given_endpoints) > 1 and _nested(given_endpoints):
            return cls._AssembleCases(
                given_endpoints=given_endpoints, cases=cases, inst2trlib=inst2trlib,
                max_iter=max_iter, max_refine=max_refine, seed=seed, **common,
            )

        result = solve_by_mcts(
            given=given_endpoints,
            target=target_model,
            transforms=transform2inst.keys(),
            max_iter=max_iter,
            max_refine=max_refine,
            seed=seed,
        )
        if len(given_endpoints) > 1 and result.complete and result.dependency_plan:
            # The joint solve shares one frontier blacklist across the cases'
            # timelines (`frontier_sigs`, mcts.rs), so it can starve a case
            # and still report complete.
            failed: dict[int, list[str]] = {}
            for c, g in enumerate(given_endpoints):
                have = set(g) | {e for e, me in result.merged_endpoints.items() if any(x in g for x in me)}
                problems = _route_problems(result.dependency_plan, have, transform2inst)
                if problems:
                    failed[c] = problems
            if failed:
                return cls._Refuse(result=result, failed=failed, cases=cases, **common)
        return cls._Assemble(result=result, inst2trlib=inst2trlib, **common)

    @classmethod
    def _AssembleCases(
        cls, given_endpoints: list[set[Endpoint]], cases: CaseIndex,
        inst2trlib: dict[TransformInstance, TransformInstanceLibrary],
        max_iter: int, max_refine: int|None, seed: int, **common,
    ):
        # Nested cases: one case's givens are contained in every other's, as
        # when some samples carry an intermediate the rest must make. Each case
        # is solved alone, on one timeline, and the routes are unioned with
        # every step scoped to the cases it serves.
        transform2inst = common["transform2inst"]
        results = [
            solve_by_mcts(
                given=[g], target=common["target_model"], transforms=transform2inst.keys(),
                max_iter=max_iter, max_refine=max_refine, seed=seed,
            )
            for g in given_endpoints
        ]
        solved = [h for h, r in enumerate(results) if r.complete and r.dependency_plan]
        routes: list[list[Application]|None] = []
        for c, g in enumerate(given_endpoints):
            # A case with no route of its own may still follow another case's
            # plan, skipping the steps whose products it was given.
            candidates = ([c] if c in solved else []) + [h for h in solved if h != c]
            routes.append(next(
                (r for r in (_route(g, results[h].dependency_plan) for h in candidates) if r is not None),
                None,
            ))
        failed = {c: ["no step reaches the target"] for c, r in enumerate(routes) if r is None}
        if failed:
            return cls._Refuse(result=results[min(failed)], failed=failed, cases=cases, **common)

        order, scope = _union_of_routes(routes)  # type: ignore # none are None
        for c, g in enumerate(given_endpoints):
            problems = _route_problems([a for a in order if c in scope[id(a)]], g, transform2inst)
            if problems:
                failed[c] = problems
        if failed:
            return cls._Refuse(result=results[min(failed)], failed=failed, cases=cases, **common)

        merged: dict[Endpoint, set[Endpoint]] = {}
        for r in results:
            for e, me in r.merged_endpoints.items():
                merged.setdefault(e, set()).update(me)
        given_steps = [r.dependency_plan[0] for r in results if r.dependency_plan and not r.dependency_plan[0].used]
        combined = SolverResult(
            complete=True,
            dependency_plan=given_steps[:1] + order,
            merged_endpoints=merged,
            _iterations=sum(r._iterations for r in results),
            _refiner_iterations=[x for r in results for x in r._refiner_iterations],
            _relavent_transforms=list({id(t): t for r in results for t in r._relavent_transforms}.values()),
        )
        return cls._Assemble(
            result=combined, inst2trlib=inst2trlib,
            scope=scope, cases=cases, n_cases=len(given_endpoints), given_steps=given_steps,
            **common,
        )

    @classmethod
    def _Refuse(
        cls, result, failed: dict[int, list[str]], cases: CaseIndex,
        solver_inputs: tuple,
        given_map: dict[Endpoint, list[DataInstance]],
        transform2inst: dict[Transform, TransformInstance],
        transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
        target_names: list[str],
        target_model: Transform,
    ):
        names = cases.samples_of(set(failed))
        lines = [
            f"  [{lbl}]: {'; '.join(failed[c])}"
            for c in sorted(failed) for lbl in cases.samples_of({c})
        ]
        msg = f"no plan carries [{len(names)}] sample(s) to {target_names}:\n" + "\n".join(lines)
        Log.Error(msg)
        hints = [PlanHint(kind="dropped_samples", target=",".join(target_names), message=msg)]
        hints += _diagnose_plan_failure(
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
            _solver_inputs=solver_inputs,
            dropped_targets=list(target_names),
            dropped_samples=names,
            hints=hints,
        )

    @classmethod
    def _Assemble(
        cls,
        result,
        solver_inputs: tuple,
        given_map: dict[Endpoint, list[DataInstance]],
        transform2inst: dict[Transform, TransformInstance],
        inst2trlib: dict[TransformInstance, TransformInstanceLibrary],
        transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
        target_names: list[str],
        target_model: Transform,
        scope: dict[int, set[int]]|None = None,
        cases: CaseIndex|None = None,
        n_cases: int = 1,
        given_steps: list[Application]|None = None,
    ):
        def _dedupe_instances(instances: list[DataInstance]):
            out = []
            seen: dict[str, DataInstance] = {}
            for inst in instances:
                if inst.instance_id in seen:
                    existing = seen[inst.instance_id]
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
                _solver_inputs=solver_inputs,
                dropped_targets=list(target_names),
                hints=failure_hints,
            )

        for result_given in (given_steps or [result.dependency_plan[0]]):
            for pgroup in result_given.produced:
                for d in pgroup:
                    e = pgroup[d]
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
            pool = list(given_map[e])
            for x in me:
                if x==e: continue
                pool += given_map.get(x, [])
            given_map[e] = _dedupe_instances([
                inst if inst.dtype.key==e.key else inst.WithDType(e)
                for inst in pool
            ])

        solution = result


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
        target_endpoints = {e for appl in target_appls for e in appl.used.values()}
        dep_to_name: dict[Dependency, str] = dict(zip(target_model.requires, target_names))
        ep_dep_queue: dict[Endpoint, list[Dependency]] = {}
        for _appl in target_appls:
            for _d, _e in _appl.used.items():
                if _d in dep_to_name:
                    ep_dep_queue.setdefault(_e, []).append(_d)
        resolved_deps: set[Dependency] = set()
        applies = [a for a in solution.dependency_plan if len(a.used)>0 and sum(len(g) for g in a.produced)>0]
        for i, appl in enumerate(applies):
            tr = transform2inst[appl.transform]
            _lib = inst2trlib[tr]

            _insts: dict[tuple, DataInstance] = {}
            for j, pgroup in enumerate(appl.produced):
                for d, e in pgroup.items():
                    dk = Endpoint(d.properties)
                    dtname = _lib.GetName(dk)

                    _instance = DataInstance(
                        path = Path(e.key+e.GetPreferredFileExtension()),
                        dtype = e,
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
                    if scope is not None:
                        cases.inst_cases.setdefault(_instance.instance_id, set()).update(scope[id(appl)])


            used_endpoints |= {e for e in appl.used.values()}
            if scope is None:
                _serves = lambda inst: True
            else:
                _sc = scope[id(appl)]
                _serves = lambda inst, _sc=_sc: bool(cases.inst_cases.get(inst.instance_id, set()) & _sc)
            step = WorkflowStep(
                order=i+1,
                dependency_map={d:[x for x in instance_map[e] if _serves(x)] for d, e in itertools.chain(appl.used.items(), [(d, e) for pgroup in appl.produced for d, e in pgroup.items()])},
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
        if scope is not None:
            for appl, step in steps.items():
                _sc = scope[id(appl)]
                if len(_sc) == n_cases: continue
                step.excluded_given = sorted({
                    inst.instance_id for inst in _given
                    if not (cases.inst_cases.get(inst.instance_id, set()) & _sc)
                })
        dropped_targets = []
        for d, nm in dep_to_name.items():
            if d not in resolved_deps:
                Log.Warn(f"target [{nm}] was requested but not included in plan"
                         " — check if group_by dependency can be satisfied from given inputs")
                dropped_targets.append(nm)

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
            _solver_inputs=solver_inputs,
            dropped_targets=dropped_targets,
            hints=plan_hints,
        )

    def BuildDAG(self, *, font: str = 'Arial', blacklist_namespaces: set[str]={"lib", "containers", "env"}, show_step_order: bool = False, show_namespaces: bool = True, label_mode: LabelMode = LabelMode.COLUMN, target_sink: bool = False, colour: str = "module", theme: str = "light", background: bool = True, mode: DagMode = DagMode.PLAIN, blacklist: Iterable[Endpoint] = (), legend_columns: int = 0, monochrome: bool = False, colour_palette: Sequence[str] | None = None, colour_overrides: Mapping[str, str] | None = None, given_root: bool = False) -> DagRenderer:
        def _get_ns(name: str) -> str:
            if "::" in name:
                ns, _ = name.split("::", maxsplit=1)
                return ns
            return name

        # A library may declare its own environments (bench::checkm2.env), outside the env namespace.
        def _hidden(name: str) -> bool:
            return _get_ns(name) in blacklist_namespaces or ("env" in blacklist_namespaces and name.endswith(".env"))

        r = DagRenderer(font=font, label_mode=label_mode, colour=colour, theme=theme, background=background, mode=mode, blacklist=blacklist, legend_columns=legend_columns, monochrome=monochrome, colour_palette=colour_palette, colour_overrides=colour_overrides)
        def _type_label(dtype_name: str) -> Label:
            if "::" in dtype_name:
                ns, name = dtype_name.split("::", maxsplit=1)
                if not show_namespaces:
                    return Label(name=name)
                return Label(name=name, namespace=ns, full=dtype_name)
            return Label(name=dtype_name, full=dtype_name)

        if given_root:
            r.add_node(NodeKind.TRANSFORM, "given")

        given_inst_names: set[str] = set()
        k2names: dict[Endpoint, set[str]] = {}
        for x in self.given:
            if _hidden(x.dtype_name): continue
            k2names[x.dtype] = k2names.get(x.dtype, set()) | {x.dtype_name}
        parents: set[Endpoint] = set()
        for e in k2names:
            for p in e.parents:
                parents.add(p) # type: ignore
        shown_parents: set[str] = set()
        for p in parents:
            if p not in k2names: continue
            for inst in k2names[p]:
                if _hidden(inst): continue
                shown_parents.add(inst)
        for e, insts_all in k2names.items():
            insts = [i for i in insts_all if not _hidden(i)]
            if len(insts) == 0: continue
            for inst_name in insts:
                for p in e.parents:
                    if p not in k2names: continue
                    pinsts = [i for i in k2names[p] if i in shown_parents] # type: ignore
                    for pname in pinsts:
                        r.add_edge(pname, inst_name)
                r.add_node(NodeKind.DATA, inst_name, _type_label(inst_name), dtype=e)
                if given_root:
                    r.add_edge("given", inst_name)
                else:
                    r.mark_given(inst_name)
                given_inst_names.add(inst_name)

        given_ids = {x.instance_id for x in self.given}

        def _data_node(x) -> str:
            """The node a data instance draws as.

            A given is one node per TYPE. Its instance ids are per-sample leaf
            ids, so a 208-sample plan would otherwise put 208 nodes where the
            plan means one input.

            Everything a step makes is one node per INSTANCE, because that id is
            the slot, and a slot is structural: one per branch, and the same
            whether the plan carries one sample or two hundred. Keying those by
            type name too is what drew two arms binning their own assembly as a
            single `binning_bam` fed by both aligners and read by both binners
            -- a join neither arm runs.
            """
            if x.instance_id in given_ids:
                return x.dtype_name
            r.add_node(NodeKind.DATA, x.instance_id, _type_label(x.dtype_name), dtype=x.dtype)
            return x.instance_id

        for step in self.steps:
            transform_name = f"{step.order} {step.transform.name}"
            tool = step.transform.name or step.transform.GetKey()
            r.add_node(NodeKind.TRANSFORM, transform_name, Label(
                name=tool,
                namespace=f"step {step.order}" if show_step_order else "",
                full=transform_name if show_step_order else tool,
            ))
            inputs, outputs = [], []
            for acc, deps in [
                (inputs, step.transform.model.requires),
                (outputs, [d for g in step.transform.model.produces for d in g]),
            ]:
                for d in deps:
                    insts = step.dependency_map[d]
                    nodes = {
                        _data_node(x) for x in insts
                        if not _hidden(x.dtype_name)
                    }
                    if len(nodes) == 0: continue
                    acc += list(nodes)
            for name in inputs:
                r.add_edge(name, transform_name)
            for name in outputs:
                r.add_edge(transform_name, name)
            # What the tool asks for, rather than what this step bound it to.
            # The legend draws a transform once, and a requirement met by a
            # subtype at four different steps is still one requirement -- only
            # the library knows the declared name, since a Dependency carries
            # properties and no name of its own. A dep that signs with lineage
            # is not in the library under any name; leaving it out is right,
            # because it is not what the transform declared either.
            _lib = getattr(step, "transform_library", None)
            if _lib is not None:
                named: dict[str, Endpoint] = {}
                def _named(deps):
                    out = []
                    for d in deps:
                        t = Endpoint(d.properties)
                        try:
                            n = _lib.GetName(t)
                        except KeyError:
                            continue
                        if n and not _hidden(n):
                            out.append(n)
                            named[n] = t
                    return out
                r.declare(
                    transform_name,
                    _named(step.transform.model.requires),
                    _named([d for g in step.transform.model.produces for d in g]),
                    named,
                )

        target_names = {_data_node(x.instance) for x in self.targets}
        for inst_name in given_inst_names:
            if inst_name in target_names: continue
            if r.out_degree(inst_name) == 0:
                r.remove_node(inst_name)
        if "given" not in target_names and r.out_degree("given") == 0:
            r.remove_node("given")

        for target in target_names:
            r.mark(NodeKind.TARGET, target)
        if target_sink:
            r.add_node(NodeKind.TRANSFORM, "target")
            for target in target_names:
                r.add_edge(target, "target")

        return r

    def RenderDAG(self, path_base: Path|str, format: str ='svg', *, font: str = 'Arial', blacklist_namespaces: set[str]={"lib", "containers", "env"}, show_step_order: bool = False, show_namespaces: bool = True, label_mode: LabelMode = LabelMode.COLUMN, target_sink: bool = False, colour: str = "module", theme: str = "light", background: bool = True, mode: DagMode = DagMode.PLAIN, blacklist: Iterable[Endpoint] = (), legend_columns: int = 0, monochrome: bool = False, colour_palette: Sequence[str] | None = None, colour_overrides: Mapping[str, str] | None = None, given_root: bool = False):
        return self.BuildDAG(
            font=font,
            blacklist_namespaces=blacklist_namespaces,
            show_step_order=show_step_order,
            show_namespaces=show_namespaces,
            label_mode=label_mode,
            target_sink=target_sink,
            colour=colour,
            theme=theme,
            background=background,
            mode=mode,
            blacklist=blacklist,
            legend_columns=legend_columns,
            monochrome=monochrome,
            colour_palette=colour_palette,
            colour_overrides=colour_overrides,
            given_root=given_root,
        ).render(path_base, format)
