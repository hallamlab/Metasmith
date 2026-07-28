"""The plan: what the solver found, and how to ask it again.

`Generate` works backwards from the targets, carrying solver state of what it
has and which transforms remain candidates. When it comes back with nothing it
does not raise a bare "no plan" -- it calls into `diagnostics` and attaches
structured hints, which every consumer is expected to surface.

A plan is also where sampling branches. `sample_type=None` plans the library as
it stands, one sample holding everything; naming a type splits it into one run
per item of that type. That is a way of branching a plan, not a precondition
for one, which is why the GUI can pass None and the CLI's --sample-type is
optional.

`BuildDAG`/`RenderDAG` blacklist the same three namespaces `ops.workflow`'s
renderer does -- an environment is a declared dependency like any other, so
without that every plan DAG grows an `env::*` node per step. Three defaults
have to agree; `tests/unit/test_dag_blacklist_defaults.py` is what notices when
they stop.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ...hashing import KeyGenerator
from ...logging import Log
from ..dag_renderer import DagRenderer, Label, LabelMode, NodeKind
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

        # Collapse merged endpoints onto a single canonical dtype.
        #
        # When several distinct subtypes (e.g. provides:bbtools / megahit /
        # seqkit) all structurally satisfy one transform requirement, the solver
        # records them in merged_endpoints[canonical] = {all subtype endpoints}
        # and the remap loop above gathers their instances into
        # given_map[canonical] — but leaves each instance carrying its ORIGINAL
        # dtype. Downstream codegen keys the group_by channel by instance dtype,
        # so a mixed-dtype list emits a malformed o.group (the by-key names a
        # channel that isn't wired in) -> runtime NullPointerException, and only
        # one of the N inputs is ever consumed.
        #
        # Retype every instance bound to a merged endpoint onto the canonical
        # dtype so the group_by requirement sees a single dtype: one input
        # channel carrying N paths, homogeneous fan-out (identical to the
        # multi-sample path that already works). WithDType preserves instance_id
        # (it keys off path/dtype_name/parent_lib, not dtype), so we retype
        # BEFORE deduping — otherwise the un-retyped originals (same instance_id)
        # win the dedup and the collapse is silently undone. dtype_name is kept,
        # so manifests still label each container by its true subtype.
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

    def BuildDAG(self, *, font: str = 'Arial', blacklist_namespaces: set[str]={"lib", "containers", "env"}, show_step_order: bool = False, label_mode: LabelMode = LabelMode.COLUMN, target_sink: bool = False, colour: str = "module", theme: str = "light") -> DagRenderer:
        """The plan as a renderer, so callers that want the graph — a stress
        harness, a comparison — do not have to write a file to get it."""
        def _get_ns(name: str) -> str:
            if "::" in name:
                ns, _ = name.split("::", maxsplit=1)
                return ns
            return name

        r = DagRenderer(font=font, label_mode=label_mode, colour=colour, theme=theme)
        r.add_node(NodeKind.TRANSFORM, "given")

        k2names: dict[Endpoint, set[str]] = {}
        for x in self.given:
            if _get_ns(x.dtype_name) in blacklist_namespaces: continue
            k2names[x.dtype] = k2names.get(x.dtype, set()) | {x.dtype_name}
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
            if len(insts) == 0: continue
            for inst_name in insts:
                for p in e.parents:
                    if p not in k2names: continue
                    pinsts = [i for i in k2names[p] if i in shown_parents] # type: ignore
                    for pname in pinsts:
                        r.add_edge(pname, inst_name)
                r.add_edge("given", inst_name)

        for step in self.steps:
            # the id keeps the step number: transform names are just the
            # definition file's stem, so the three binner-specific checkm steps
            # are all literally "checkm" and only the number tells them apart
            transform_name = f"{step.order} {step.transform.name}"
            r.add_node(NodeKind.TRANSFORM, transform_name, Label(
                name=step.transform.name or step.transform.GetKey(),
                namespace=f"step {step.order}" if show_step_order else "",
                full=transform_name,
            ))
            inputs, outputs = [], []
            for acc, deps in [
                (inputs, step.transform.model.requires),
                (outputs, [d for g in step.transform.model.produces for d in g]),
            ]:
                for d in deps:
                    insts = step.dependency_map[d]
                    inst_names = {x.dtype_name for x in insts if _get_ns(x.dtype_name) not in blacklist_namespaces}
                    if len(inst_names) == 0: continue
                    acc += list(inst_names)
            for name in inputs:
                r.add_edge(name, transform_name)
            for name in outputs:
                r.add_edge(transform_name, name)

        # The requested outputs are marked on the nodes themselves. Collecting
        # them into one sink instead costs every target a lane held from
        # wherever it is produced down to the last row — on the spanish-lakes
        # metagenomics plan that is 10 of 24 lanes, by far the most expensive
        # thing in the drawing, and it says nothing the ring does not.
        for target in {x.instance.dtype_name for x in self.targets}:
            r.mark(NodeKind.TARGET, target)
        if target_sink:
            r.add_node(NodeKind.TRANSFORM, "target")
            for target in {x.instance.dtype_name for x in self.targets}:
                r.add_edge(target, "target")

        return r

    def RenderDAG(self, path_base: Path|str, format: str ='svg', *, font: str = 'Arial', blacklist_namespaces: set[str]={"lib", "containers", "env"}, show_step_order: bool = False, label_mode: LabelMode = LabelMode.COLUMN, target_sink: bool = False, colour: str = "module", theme: str = "light"):
        return self.BuildDAG(
            font=font,
            blacklist_namespaces=blacklist_namespaces,
            show_step_order=show_step_order,
            label_mode=label_mode,
            target_sink=target_sink,
            colour=colour,
            theme=theme,
        ).render(path_base, format)
