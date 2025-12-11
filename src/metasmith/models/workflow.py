from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Generator, Iterable, Literal, TypeVar
import os
import itertools
import yaml

from ..coms.containers import Container, ContainerRuntime
from .libraries import DataTypeLibrary
from .libraries import DataInstanceLibraryView, DataInstanceLibrary, DataInstance
from .libraries import TransformInstance, TransformInstanceLibrary
from .remote import Logistics, Source, SourceType
from .solver import Endpoint, Dependency, Transform, solve_by_mcts, Solution as SolverResult
from ..hashing import KeyGenerator
from ..logging import Log

METADATA_FILE = ".command.metadata"
BIND_FILE = ".command.binds"

@dataclass
class WorkflowStep:
    order: int
    uses: list[DataInstance]
    produces: list[list[DataInstance]]
    dependency_map: dict[Dependency, list[DataInstance]]
    transform: TransformInstance
    transform_library: TransformInstanceLibrary
    _raw_dependency_map: dict|None = None

    def Pack(self):
        return dict(
            order=self.order,
            uses=[inst.Pack() for inst in self.uses],
            produces=[[inst.Pack() for inst in g] for g in self.produces],
            dependency_map={k.key:[v._key for v in lst] for k, lst in self.dependency_map.items()},
            transform=f"{self.transform_library.GetKey()}::{self.transform.name}",
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, transform_name = raw["transform"].split("::")
        lib = libraries[lib_key]
        assert isinstance(lib, TransformInstanceLibrary)
        tr = lib.GetTransform(transform_name)
        assert tr is not None
        return cls(
            order=raw["order"],
            uses=[DataInstance.Unpack(inst, libraries) for inst in raw["uses"]],
            produces=[[DataInstance.Unpack(inst, libraries) for inst in g] for g in raw["produces"]],
            dependency_map={}, # needs workflow plan to sort out
            _raw_dependency_map = raw["dependency_map"],
            transform=tr,
            transform_library=lib,
        )
    
    def _resolve_dependency_map(self):
        assert self._raw_dependency_map is not None
        data = {d._key:d for d in itertools.chain(self.uses, [d for g in self.produces for d in g])}
        tr = self.transform.model
        deps = {d.key:d for d in itertools.chain(tr.requires, [d for g in tr.produces for d in g])}
        self.dependency_map = {deps[k]:[data[v] for v in lst] for k, lst in self._raw_dependency_map.items()}

@dataclass
class WorkflowTarget:
    instance: DataInstance
    used_givens: list[DataInstance]
    producing_step: WorkflowStep
    _key: str = field(default_factory=lambda: "")

    def __post_init__(self):
        self.RecalculateKey()

    def RecalculateKey(self):
        self.instance.RecalculateKey()
        self._key = self.instance._key

    def Pack(self):
        return dict(
            instance=self.instance.Pack(),
            parents=[dict(id=inst._key, path=str(inst.path)) for inst in self.used_givens],
            producing_step=dict(order=self.producing_step.order, name=self.producing_step.transform.name),
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary], given: dict[str, DataInstance], steps: dict[int, WorkflowStep]):
        inst = DataInstance.Unpack(raw["instance"], libraries)
        used_givens = [given[d["id"]] for d in raw["parents"]]
        producing_step = steps[raw["producing_step"]["order"]]
        return cls(
            instance=inst,
            used_givens=used_givens,
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
    external_home_var: str = "${params.home}"
    external_work_var: str = "${params.workspace}"
    bootstrap_var: str = "${params.bootstrap_def}"

@dataclass
class WorkflowPlan:
    given: list[DataInstance]
    targets: list[WorkflowTarget] # target, used givens
    steps: list[WorkflowStep]
    _solver_result: SolverResult|None=None
    _archetype_translation: dict[DataInstance, DataInstance]|None = None

    def __post_init__(self):
        self._update_hash()

    def _update_hash(self):
        # given = [inst._key for inst in self.given]
        # targets = [inst._key for inst in self.targets]
        steps = [step.transform.model.key for step in self.steps]
        self._hash, self._key = KeyGenerator.FromStr("".join(steps), l=8)

    def __hash__(self) -> int:
        return self._hash
    
    def __len__(self):
        return len(self.steps)

    def Pack(self):
        dtypes: dict[str, tuple[str, Endpoint]] = {}
        for inst in self.given:
            dtypes[inst.dtype.key] = inst.dtype_name, inst.dtype
        for target in self.targets:
            dtypes[target.instance.dtype.key] = target.instance.dtype_name, target.instance.dtype
        for step in self.steps:
            for inst in step.uses:
                dtypes[inst.dtype.key] = inst.dtype_name, inst.dtype
            for g in step.produces:
                for inst in g:
                    dtypes[inst.dtype.key] = inst.dtype_name, inst.dtype

        def _pack_type(name: str, e: Endpoint):
            d = e.Pack()
            if len(e.parents)>0: d["parents"] = [p.key for p in e.parents]
            d["name"] = name
            return d

        return dict(
            types={k: _pack_type(n, e) for k, (n, e) in dtypes.items()},
            given=[inst.Pack() for inst in self.given],
            targets=[inst.Pack() for inst in self.targets],
            steps=[step.Pack() for step in self.steps],
        )

    def Save(self, path: Path):
        with open(path, "w") as f:
            yaml.dump(self.Pack(), f)

    # def TryApplyingTo(self, alt_given: list[DataInstance]):
    #     my_given = set(self.given)
    #     used = {x for s in self.steps for x in s.uses if x in my_given}
    #     viability = {} # number of times an inst in alt can be used
    #     possible_substitutions = {} # candiates replacements for each original given
    #     for alt in alt_given:
    #         count = 0
    #         for original in used:
    #             if not alt.dtype.IsA(original.dtype): continue
    #             count += 1
    #             possible_substitutions[original] = possible_substitutions.get(original, [])+[alt]
    #         viability[alt] = count

    #     replacement_plan: dict[DataInstance, DataInstance] = {}
    #     for original in used:
    #         if original not in possible_substitutions: return
    #         candidates = possible_substitutions[original]
    #         candidates = sorted(list(zip(candidates, [viability[c] for c in candidates])), key=lambda t: t[-1], reverse=True)
    #         replacement_plan[original], _ = candidates[0]

    #     new_steps: list[WorkflowStep] = []
    #     for step in self.steps:
    #         new_steps.append(WorkflowStep(
    #             order = step.order,
    #             uses = [replacement_plan.get(x, x) for x in step.uses],
    #             produces = step.produces,
    #             dependency_map = {d:replacement_plan.get(x, x) for d, x in step.dependency_map.items()},
    #             transform=step.transform,
    #             transform_library=step.transform_library,
    #             _raw_dependency_map = {d:replacement_plan.get(x, x) for d, x in step._raw_dependency_map.items()} if step._raw_dependency_map is not None else None,
    #         ))
    #     return WorkflowPlan(
    #         alt_given,
    #         targets=self.targets,
    #         steps=new_steps,
    #         _solver_result=self._solver_result,
    #         _archetype_translation = replacement_plan
    #     )

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
            return inst

        def _unpack_step(raw: dict):
            step = WorkflowStep.Unpack(raw, libraries)
            def _iter():
                for inst, r in zip(step.uses, raw["uses"]):
                    yield inst, r
                for g, rg in zip(step.produces, raw["produces"]):
                    for inst, r in zip(g, rg):
                        yield inst, r
            for inst, r in _iter():
                inst.dtype = all_types[r["type_id"]]
                inst.RecalculateKey()
            step._resolve_dependency_map()
            return step

        given=[_unpack_given(d) for d in raw["given"]]
        given_map = {inst._key: inst for inst in given}
        steps = [_unpack_step(d) for d in raw["steps"]]
        step_map = {step.order: step for step in steps}

        def _unpack_target(raw: dict):
            target = WorkflowTarget.Unpack(raw, libraries, given_map, step_map)
            target.instance.dtype = all_types[raw["instance"]["type_id"]]
            target.RecalculateKey()
            return target

        return cls(
            given=given,
            targets=[_unpack_target(d) for d in raw["targets"]],
            steps=steps,
        )

    @classmethod
    def Generate(
        cls,
        given: list[list[DataInstanceLibraryView]],
        transforms: list[TransformInstanceLibrary],
        targets: Iterable[Endpoint],
        max_iter: int=256, max_refine: int=256, seed: int=42,
    ):
        given_map: dict[Endpoint, list[DataInstance]] = {}
        given_endpoints: list[set[Endpoint]] = []
        for group in given:
            eps = set()
            for lib in group:
                for path, ep_name, ep in lib.Iterate():
                    given_map[ep] = given_map.get(ep, [])+[DataInstance(
                        path=path,
                        dtype=ep,
                        dtype_name=ep_name,
                        parent_lib=lib._original,
                    )]
                    eps.add(ep)
            if len(given_endpoints)>0 and all(g==eps for g in given_endpoints): continue
            given_endpoints.append(eps)

        target_e2d: dict[Endpoint, Dependency] = {}
        def _add(tr: Transform, e: Endpoint) -> Dependency:
            if e in target_e2d: return target_e2d[e]
            parent_deps = {_add(tr, p) for p in e.parents} # type: ignore
            d = tr.AddRequirement(e, parents=parent_deps)
            target_e2d[e] = d
            return d
        target_model = Transform()
        for t in targets:
            _add(target_model, t)

        transform2inst: dict[Transform, TransformInstance] = {}
        inst2trlib: dict[TransformInstance, TransformInstanceLibrary] = {}
        for trlib in transforms:
            for path, tr in trlib.IterateTransforms():
                model = tr.model
                if model in transform2inst:
                    Log.Warn(f"transform [{model}] of [{trlib}] is masked")
                    continue
                transform2inst[model] = tr
                inst2trlib[tr] = trlib

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

        # remap given inputs if changed by solver
        result_given = result.dependency_plan[0]
        for pgroup in result_given.produced:
            for d in pgroup:
                e = pgroup[d]
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
                        # del given_map[ge]

        # assert result.complete, "failed to make plan!"
        if not result.complete:
            return result
        solution = result

        instance_map: dict[Endpoint, set[DataInstance]] = {k:set(v) for k, v in given_map.items()}
        steps: list[WorkflowStep] = []
        target_meta: dict[Endpoint, list[WorkflowTarget]] = {}
        used_endpoints: set[Endpoint] = set()
        for i, appl in enumerate(solution.dependency_plan[1:-1]): # first is mock tr for given, last is for target
            tr = transform2inst[appl.transform]
            _lib = inst2trlib[tr]

            for pgroup in appl.produced:
                for d, e in pgroup.items():
                    _instance = DataInstance(
                        path = Path(e.key+e.GetPreferredFileExtension()),
                        dtype = e, # we actually dont want lineage at this stage so that the hashes match
                        dtype_name = _lib.GetName(d), # type: ignore # Dependency not assignable to Endpoint
                        parent_lib = _lib,
                    )
                    instance_map[e] = instance_map.get(e, set())|{_instance}

            used_endpoints |= {e for e in appl.used.values()}
            step = WorkflowStep(
                order=i+1,
                uses=[inst for e in appl.used.values() for inst in instance_map[e]],
                produces=[[inst for e in pgroup.values() for inst in instance_map[e]] for pgroup in appl.produced],
                dependency_map={d:list(instance_map[e]) for d, e in itertools.chain(appl.used.items(), [(d, e) for pgroup in appl.produced for d, e in pgroup.items()])},
                transform=tr,
                transform_library=_lib,
            )
            steps.append(step)

        for appl in solution.dependency_plan:
            for pgroup in appl.produced:
                for d, e in pgroup.items():
                    for target in targets:
                        if not e.IsA(target): continue
                        if not target.parents.issubset(e.parents): continue
                        _used_givens = []
                        for p in target.parents:
                            if p not in given_map: continue
                            _used_givens.append(given_map[p]) # type: ignore # Node not assignable to Endpoint
                        used_endpoints.add(e)
                        for t in instance_map[e]:
                            target_meta[target] = target_meta.get(target, [])+[
                                WorkflowTarget(
                                    instance=t,
                                    used_givens=_used_givens,
                                    producing_step=step,
                                )
                            ]
                        break

        return cls(
            given=[i for e, lst in given_map.items() for i in lst if e in used_endpoints],
            targets=[x for g in target_meta.values() for x in g],
            steps=steps,
            _solver_result=result,
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
            lines = ["digraph G {"]
            lines += [f'graph [fontname="{font}"];', f'node  [fontname="{font}"];', f'edge  [fontname="{font}"];']
            lines.append(_render_node(NodeType.TRANSFORM, "given"))
            k2name = {x.dtype:x.dtype_name for x in self.given}
            # for x in self.given:
            #     print(x.dtype_name, x.dtype)
            parents: set[Endpoint] = set()
            for e in k2name:
                for p in e.parents:
                    parents.add(p) # type: ignore
            shown_parents = set()
            for p in parents:
                if p not in k2name: continue
                inst = k2name[p]
                if _get_ns(inst) in blacklist_namespaces: continue
                shown_parents.add(inst)
            for e, inst in k2name.items():
                if _get_ns(inst) in blacklist_namespaces: continue
                for p in e.parents:
                    if p not in k2name: continue
                    pinst = k2name[p] # type: ignore
                    if pinst not in shown_parents: continue
                    lines.append(f'    "{pinst}" -> "{inst}";')
                lines.append(f'    "given" -> "{inst}";')
            seen = set()
            for step in self.steps:
                transform_name = step.transform.name
                lines.append(_render_node(NodeType.TRANSFORM, str(transform_name)))
                uses = {x.dtype:x for x in step.uses}
                produces = {x.dtype:x for o in step.produces for x in o}
                inputs  = [x.dtype_name for x in uses.values() if _get_ns(x.dtype_name) not in blacklist_namespaces]
                outputs = [x.dtype_name for x in produces.values() if _get_ns(x.dtype_name) not in blacklist_namespaces]
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
            for target in {x.instance.dtype_name for x in self.targets}:
                lines.append(f'    "{target}" -> "target";')
            lines.append("}")
            return "\n".join(lines)
        
        dag_str = _as_DAG(font=font)
        src = graphviz.Source(dag_str, filename=path_base, format=format)
        src.render(cleanup=True, quiet=True)

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

    def PrepareNextflow(self, context: NextflowGenContext):
        TAB = "\t"
        def _strip_var(s: str):
            return s[2:-1]
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
            "def in(f) {",
            TAB+"return Channel.fromPath(f).splitCsv(header: false).map(row -> {",
            TAB+TAB+"def i = [:] // this will be filld by post()",
            TAB+TAB+"return tuple(i, file(row[0]))",
            TAB+"})",
            "}",
            "",
            "",
        ]
        HEADER = "\n".join([
            f"{_strip_var(context.external_home_var)} = '{context.external_home}'",
            f'{_strip_var(context.external_work_var)} = "{context.external_work}"'.replace(str(context.external_home), context.external_home_var),
        ]+bootstrap)
        MAX_FILE_SIZE = int(2**16 * 0.95) # nextflow is 65536

        _archetypes: dict[DataInstance, DataInstance] = {}
        def get_archetype(candidates: list[DataInstance]):
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
            process_name = f"{k}__{step.transform.name}"
            src = [f"process {process_name}"+" {"]
            src += [
                TAB+f"label 'x{step.transform.GetKey()}x'",
            ] + [
                TAB+f"label 'x{x}x'"
                for x in step.transform.labels
            ]
            
            def _make_bind_var(i: int, is_assignment=False):
                s = "\\$" if not is_assignment else ""
                return f"{s}b{i+1}"
            external_binds = set()
            for inst in step.uses:
                p = inst.path
                if p.is_relative_to(Path(".")): continue
                external_binds.add(p.parent)
            external_binds = list(external_binds)
            external_binds_param =""
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
            if res is not None: 
                src += [TAB+x for x in res.AsNextflowFormat()]
            duration_is_strict = res is not None and res.duration is not None and res.duration.strict
            memory_is_strict = res is not None and res.memory is not None and res.memory.strict
            if duration_is_strict and memory_is_strict:
                src += [
                    "errorStrategy 'ignore'" # no point in retrying if not changing resource requests
                ]
            used_archetypes, produced_archetypes = get_io_signature(step)

            src += [
                "input:",
                TAB+f'tuple '+','.join(['val(index)']+[f'path(_{i+1:02})' for i, x in enumerate(used_archetypes)])
            ] + [
                "output:",
            ] + [
                # TAB+f'tuple val(index),path("{add_prefix(x.path)}")'
                TAB+f'tuple val(index),path("*.{x.dtype.key}{x.dtype.GetPreferredFileExtension()}")'
                for g in produced_archetypes for x in g
            ] + [
                "script:",
                '"""',
                f'echo "step {step.order}, sample $index"',    # this is used to extract logs in agent.RunWorkflow()
                f'echo "{step.transform.name}"',
                f'echo "res $task.cpus/$task.memory/$task.attempt" >>{METADATA_FILE}',
                f'echo "lin ${{Orchestrator.JsonforEcho(index)}}" >>{METADATA_FILE}',
                f'echo "inp {','.join(x.dtype.key for x in used_archetypes)}" >>{METADATA_FILE}',
                f'echo "out {';'.join(','.join(x.dtype.key for x in g) for g in produced_archetypes)}" >>{METADATA_FILE}',
            ] + [
                f'echo "i{i+1:02} $_{i+1:02}">>{METADATA_FILE}'
                for i, x in enumerate(used_archetypes)
            ] + [
                f'{_make_bind_var(i, is_assignment=True)}="{p}"'
                for i, p in enumerate(external_binds)
            ] + [
                f'echo "{external_binds_param}" >{BIND_FILE}',
                f'{context.bootstrap_var}',
                f'bootstrap {context.external_work_var} "{step.order}"',
                f'[ -e .command.success ] && exit 0 || exit 1', # in case slurm silently kills proc from oom/timeout
                '"""',
                "}",
                ""
            ]
            return process_name, "\n".join(src)

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
                to_merge_names[x] = to_merge_names.get(x, [])+[name]
            else:
                name = f"{k}"
            return name
        
        # goal:
        # (_tK9GI0FH) = o.post([in("inputs/tK9GI0FH")], ["tK9GI0FH"]) // lib::pangenome_heatmap.py
        # (_7A15qSzL) = o.post([in("inputs/7A15qSzL")], ["7A15qSzL"]) // containers::python_for_data_science.oci
        # (_urCt2PG9) = o.post([in("inputs/urCt2PG9")], ["urCt2PG9"]) // sequences::gbk
        input_channels: dict[Dependency, list[DataInstance]] = {}
        for step in the_plan.steps:
            for dep, lst in step.dependency_map.items():
                for inst in lst:
                    if inst not in used_given: continue
                    # k = inst.dtype
                    input_channels[dep] = input_channels.get(dep, [])+[inst]
        prepared_given: set[tuple[Path, str, str]] = set()
        _seen_paths = set()
        for _, lst in input_channels.items():
            inst = get_archetype(lst)
            p = inputs_dir/f"{get_prod_name(inst.dtype, force_singular=True)}"
            if p in _seen_paths: continue
            _seen_paths.add(p)
            v = get_prod_name(inst.dtype)
            n = inst.dtype_name
            k = p, v, n
            prepared_given.add(k)
            with open(p, "w") as f:
                unique_lst = set(lst)
                if len(unique_lst)==1:
                    to_write = [inst]
                else:
                    to_write = lst
                for x in to_write:
                    f.write(f"{x.ResolvePath()}"+"\n")
        # goal:
        # k = ['h']
        # (h) = o.post([*p1(o.group('f', o.using([f], k)))], k)
        # or this for when batching
        # (y) = o.post(o.debatch([*b1(o.batch(o.group('g', o.using([g], k)), 3))]), k)
        targets = {x.instance for x in the_plan.targets}
        src_process = []
        wf_main = []
        wf_publish = set()       
        published_channels: dict[str, DataInstance] = {}
        for step in the_plan.steps:
            process_name, src = prepare_step(step)
            src_process.append(src)
            used_archetypes, produced_archetypes = get_io_signature(step)
            produced_names = [get_prod_name(x.dtype) for g in produced_archetypes for x in g]
            produced_snames = [get_prod_name(x.dtype, force_singular=True) for g in produced_archetypes for x in g]
            produced = ", ".join(f"_{x}" for x in produced_names)
            if len(used_archetypes)>0:
                _inst = step.dependency_map[step.transform.group_by]
                _dtypes = {x.dtype.key for x in _inst}
                if len(_dtypes)>1:
                    Log.Warn(f"unexpected plural groupby instance refernce for [{step.transform.name}:{step.transform.group_by}]: [{_inst}]")
                _inst = _inst[0]
                gb = _inst.dtype.key
                using_symbols = ", ".join(f"_{x.dtype.key}" for x in used_archetypes)
                used = f"o.group('{gb}', o.using([{using_symbols}], k))"
            else:
                used = ""
            produced_k = [f"'{x}'" for x in produced_snames]
            produced_k = ", ".join(produced_k)
            wf_main.append(f"k = [{produced_k}]")
            if step.transform.batch_size==1:
                wf_main.append(
                    f"({produced}) = o.post([*{process_name}({used})], k)"
                )
            else:
                wf_main.append(
                    f"({produced}) = o.post(o.debatch([*{process_name}(o.batch({used}, {step.transform.batch_size}))]), k)"
                )
            if step.order in final_steps_for_merging:
                for e in final_steps_for_merging[step.order]:
                    names = to_merge_names[e]
                    to_mix = [f"_{x}" for x in names]
                    name = get_prod_name(e, force_singular=True)
                    wf_main.append(
                        f"_{name} = o.mix([{', '.join(to_mix)}])"
                    )
            to_pubish = [x for g in produced_archetypes for x in g if x in targets]
            for inst in to_pubish:
                k = inst.dtype.key
                wf_publish.add(k)
                published_channels[k] = inst
        wf_output = []
        for ch, inst in published_channels.items():
            out_name = inst.dtype_name.replace(' ', '_').replace("::", "-")
            wf_output += [
                TAB+f"_{ch}"+"{",
                TAB+TAB+f"path '{out_name}'",
                TAB+TAB+f"index {{ path '_manifests/{out_name}.{inst.dtype.key}.csv' }}",
                TAB+"}",
            ]
            
        content = [
            f"workflow"+" {",
            "main:",
            f'o = new Orchestrator(Channel.fromList([null])) // cant create channels in groovy',
        ] + [
            f'(_{v}) = o.post([in("{p.relative_to(context.work_dir)}")], ["{p.name}"]) // {n}'
            for p, v, n in prepared_given
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

    def GetCommonInputFolders(self, method="external"):
        """
        @method is: external | internal | all
        """
        assert method in {"external", "internal", "all"}
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

        def should_keep(inst: DataInstance):
            match(method):
                case "external":
                    return inst.path.is_absolute()
                case "internal":
                    return not inst.path.is_absolute()
                case "all":
                    return True
        given = {inst.ResolvePath().parent for inst in self.plan.given if should_keep(inst)}
        for path in given:
            update(str(path.parent))
        return roots

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
