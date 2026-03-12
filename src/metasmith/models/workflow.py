from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Generator, Iterable, Literal, TypeVar
import os
import itertools
from collections import Counter
import yaml
import json
from hashlib import md5

from ..coms.containers import Container, ContainerRuntime
from .libraries import DataTypeLibrary
from .libraries import DataInstanceLibraryView, DataInstanceLibrary, DataInstance
from .libraries import TransformInstance, TransformInstanceLibrary
from .remote import Logistics, Source, SourceType
from .solver import Application, Endpoint, Dependency, Transform, solve_by_mcts, Solution as SolverResult
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
            transform=f"{self.transform_library.GetKey()}::{self.transform._path}",
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, transform_path = raw["transform"].split("::")
        lib = libraries[lib_key]
        assert isinstance(lib, TransformInstanceLibrary)
        tr = lib.GetTransform(transform_path)
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
        target_names: dict[Endpoint, str],
        target_model: Transform,
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
                for oe in given_map[x]:
                    oe.dtype = e
                    oe.RecalculateKey()
                    _to_add.append(oe)
            given_map[e] += _to_add

        # assert result.complete, "failed to make plan!"
        if not result.complete:
            return result
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

        instance_map: dict[Endpoint, set[DataInstance]] = {k:set(v) for k, v in given_map.items()}
        steps: dict[Application, WorkflowStep] = {}
        used_endpoints: set[Endpoint] = set()
        target_meta: dict[Endpoint, list[WorkflowTarget]] = {}
        target_appls = [a for a in solution.dependency_plan if sum(len(g) for g in a.produced)==0]
        # target_appl = solution.dependency_plan[-1]
        target_endpoints = {e for appl in target_appls for e in appl.used.values()}
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
                    instance_map[e] = instance_map.get(e, set())|{_instance}
                    _insts[(j, d, e)] = _instance

            # print(f">> {tr.name}")
            # for e in appl.used.values():
            #     if '{"Format":"OCI"}' in e.properties: continue
            #     print(f"   {e in instance_map}", e)

            used_endpoints |= {e for e in appl.used.values()}
            step = WorkflowStep(
                order=i+1,
                uses=[inst for e in appl.used.values() for inst in instance_map[e]],
                produces=[[inst for e in pgroup.values() for inst in instance_map[e]] for pgroup in appl.produced],
                dependency_map={d:list(instance_map[e]) for d, e in itertools.chain(appl.used.items(), [(d, e) for pgroup in appl.produced for d, e in pgroup.items()])},
                transform=tr,
                transform_library=_lib,
            )
            steps[appl] = step
            for j, pgroup in enumerate(appl.produced):
                for d, e in pgroup.items():
                    if e not in target_endpoints: continue
                    dtname = None
                    for x in target_names:
                        if e.IsA(x):
                            dtname = target_names[x]
                    assert dtname is not None
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

        return cls(
            given=list({i for e, lst in given_map.items() for i in lst if e in used_endpoints}),
            targets=[x for g in target_meta.values() for x in g],
            steps=[s for a, s in steps.items()],
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
            "import groovy.json.JsonSlurper",
            "def in(f, l) {",
            "    def rows = Channel.fromPath(f).splitCsv(header: false)",
            "    if (f in l) {",
            "        rows = Channel.fromList(l[f]).merge(rows)",
            "    }",
            "    return rows.map((row) -> {",
            "        if (row.size()>1) {",
            "            def (ri, rx) = row",
            "            return tuple(ri, file(rx))",
            "        } else {",
            "            def i = [:]",
            "            return tuple(i, file(row[0]))",
            "        }",
            "    })",
            "}",
            "",
            "",
        ]
        HEADER = "\n".join([
            "params.testSpread=1",
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
            
            def _make_bind_var(i: int, is_assignment=False):
                s = "\\$" if not is_assignment else ""
                return f"{s}b{i+1}"
            raw_external_binds = set()
            for inst in step.uses:
                p = inst.path
                if p.is_relative_to(Path(".")): continue
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
                f'echo "lin ${{Orchestrator.JsonforEcho(index)}}" >>{METADATA_FILE}',
                f'echo "inp {','.join(x.dtype.key for x in used_archetypes)}" >>{METADATA_FILE}',
                f'echo "out {';'.join(','.join(x.dtype.key for x in g) for g in produced_archetypes)}" >>{METADATA_FILE}',
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
                'def hash = "${index[0].sort().collectEntries((k, v) -> [k, v.sort()])}".md5()[0..3]', # 4 characters
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
        # (_tK9GI0FH) = o.post([in("inputs/tK9GI0FH")], ["tK9GI0FH"]) // lib::pangenome_heatmap.py
        # (_7A15qSzL) = o.post([in("inputs/7A15qSzL")], ["7A15qSzL"]) // containers::python_for_data_science.oci
        # (_urCt2PG9) = o.post([in("inputs/urCt2PG9")], ["urCt2PG9"]) // sequences::gbk
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
        with open(context.work_dir/LINEAGE_FILE, "w") as f:
            json.dump(_given_lineage, f, separators=(',', ':'))

        # goal:
        # k = ['h']
        # (h) = o.post([*p1(o.group('f', o.using([f], k)))], k)
        # or this for when batching
        # (y) = o.post(o.debatch([*b1(o.batch(o.group('g', o.using([g], k)), 3))]), k)
        target_endpoints = {x.instance.dtype for x in the_plan.targets}
        src_process = []
        wf_main = []
        wf_publish = set()       
        published_channels: dict[str, DataInstance] = {}
        resources = {}

        # Pre-scan: count how many group() calls reference each stream symbol.
        # Symbols used more than once need to be forked via multiMap.
        stream_group_usage = Counter()
        for step in the_plan.steps:
            _used, _ = get_io_signature(step)
            if len(_used) > 0:
                for x in _used:
                    stream_group_usage[x.dtype.key] += 1
        fork_next_index = {k: 0 for k, v in stream_group_usage.items() if v > 1}
        fork_emitted: set[str] = set()

        for step in the_plan.steps:
            process_name, src, src_res = prepare_step(step)
            resources[process_name] = src_res
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
                symbol_parts = []
                for x in used_archetypes:
                    key = x.dtype.key
                    if key in fork_next_index:
                        symbol_parts.append(f"_{key}_{fork_next_index[key]}")
                        fork_next_index[key] += 1
                    else:
                        symbol_parts.append(f"_{key}")
                using_symbols = ", ".join(symbol_parts)
                used = f"o.group('{gb}', [{using_symbols}], k, {step.transform.batch_size})"
            else:
                used = ""
            produced_k = [f"'{x}'" for x in produced_snames]
            produced_k = ", ".join(produced_k)
            wf_main.append(f"k = [{produced_k}]")
            wf_main.append(
                f"({produced}) = o.post([*{process_name}({used})], k)"
            )
            # Emit multiMap fork code for any produced symbols that are multi-use
            for sname in produced_snames:
                if sname in fork_next_index and sname not in fork_emitted:
                    count = stream_group_usage[sname]
                    branches = "; ".join(f"f{i}: item" for i in range(count))
                    wf_main.append(f"def _{sname}_forks = _{sname}[1].multiMap {{ item -> {branches} }}")
                    for i in range(count):
                        wf_main.append(f"_{sname}_{i} = [_{sname}[0], _{sname}_forks.f{i}]")
                    fork_emitted.add(sname)
            if step.order in final_steps_for_merging:
                for e in final_steps_for_merging[step.order]:
                    names = to_merge_names[e]
                    to_mix = [f"_{x}" for x in names]
                    name = get_prod_name(e, force_singular=True)
                    wf_main.append(
                        f"_{name} = o.mix([{', '.join(to_mix)}])"
                    )
                    # Emit multiMap fork code for mixed symbols that are multi-use
                    if name in fork_next_index and name not in fork_emitted:
                        count = stream_group_usage[name]
                        branches = "; ".join(f"f{i}: item" for i in range(count))
                        wf_main.append(f"def _{name}_forks = _{name}[1].multiMap {{ item -> {branches} }}")
                        for i in range(count):
                            wf_main.append(f"_{name}_{i} = [_{name}[0], _{name}_forks.f{i}]")
                        fork_emitted.add(name)
            
            to_pubish = [x for g in produced_archetypes for x in g if x.dtype in target_endpoints]
            for inst in to_pubish:
                k = inst.dtype.key
                wf_publish.add(k)
                published_channels[k] = inst

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
        for ch, inst in published_channels.items():
            spec_name, out_name = [
                n.replace(' ', '_').replace("::", "-")
                for n in [
                    inst.dtype_name,
                    _e2target[inst.dtype].name
                ]
            ]
            wf_output += [
                TAB+f"_{ch}"+"{",
                TAB+TAB+f"path '{out_name}'",
                TAB+TAB+f"index {{ path '_manifests/{spec_name}.{inst.dtype.key}.json' }}",
                TAB+"}",
            ]
            
        # Generate fork lines for postIn-produced streams
        postin_fork_lines = []
        for _, v, _ in prepared_given:
            if v in fork_next_index and v not in fork_emitted:
                count = stream_group_usage[v]
                branches = "; ".join(f"f{i}: item" for i in range(count))
                postin_fork_lines.append(f"def _{v}_forks = _{v}[1].multiMap {{ item -> {branches} }}")
                for i in range(count):
                    postin_fork_lines.append(f"_{v}_{i} = [_{v}[0], _{v}_forks.f{i}]")
                fork_emitted.add(v)

        content = [
            f"workflow"+" {",
            "main:",
            f'o = new Orchestrator(Channel.fromList([null])) // cant create channels in groovy',
            f'l = new JsonSlurper().parseText(file("{LINEAGE_FILE}").text)',
        ] + [
            f'o.child2parent["{k}"] = ([{", ".join(f'"{x}"' for x in vset)}] as Set)'
            for k, vset in given_lineage_by_keys.items()
        ] + [
            f'(_{v}) = o.postIn([in("{p.relative_to(context.work_dir)}", l)], ["{p.name}"]) // {n}'
            for p, v, n in prepared_given # this must be (and is) sorted in lineage order
        ] + postin_fork_lines + [
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
