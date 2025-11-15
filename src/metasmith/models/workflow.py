from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Generator, Iterable, TypeVar
import os
import itertools
import yaml

from ..coms.containers import Container, ContainerRuntime
from .libraries import DataTypeLibrary
from .libraries import DataInstanceLibrary, DataInstance
from .libraries import TransformInstance, TransformInstanceLibrary
from .remote import Logistics, Source, SourceType
from .solver import Endpoint, Dependency, Transform, solve_by_mcts, Solution as SolverResult
from ..hashing import KeyGenerator
from ..logging import Log

METADATA_FILE = ".command.metadata"

@dataclass
class WorkflowStep:
    order: int
    uses: list[DataInstance]
    produces: list[DataInstance]
    dependency_map: dict[Dependency, DataInstance]
    transform: TransformInstance
    transform_library: TransformInstanceLibrary
    _raw_dependency_map: dict|None = None

    def Pack(self):
        return dict(
            order=self.order,
            uses=[inst.Pack() for inst in self.uses],
            produces=[inst.Pack() for inst in self.produces],
            dependency_map={k.key:v._key for k, v in self.dependency_map.items()},
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
            produces=[DataInstance.Unpack(inst, libraries) for inst in raw["produces"]],
            dependency_map={}, # needs workflow plan to sort out
            _raw_dependency_map = raw["dependency_map"],
            transform=tr,
            transform_library=lib,
        )
    
    def _resolve_dependency_map(self):
        assert self._raw_dependency_map is not None
        data = {d._key:d for d in itertools.chain(self.uses, self.produces)}
        tr = self.transform.model
        deps = {d.key:d for d in itertools.chain(tr.requires, tr.produces)}
        self.dependency_map = {deps[k]:data[v] for k, v in self._raw_dependency_map.items()}

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
    work_dir: Path
    external_work: Path
    home_dir: Path
    external_home: Path
    container_runtime: ContainerRuntime
    external_home_var: str = "${params.home}"
    external_work_var: str = "${params.workspace}"
    bootstrap_var: str = "${params.bootstrap}"

@dataclass
class NextflowGenResult:
    workflow_name: str
    content: str

@dataclass
class WorkflowPlan:
    given: list[DataInstance]
    targets: list[WorkflowTarget] # target, used givens
    steps: list[WorkflowStep]
    _solver_result: SolverResult|None=None

    def __post_init__(self):
        self._update_hash()

    def _update_hash(self):
        given = [inst._key for inst in self.given]
        targets = [inst._key for inst in self.targets]
        steps = [step.transform.model.key for step in self.steps]
        self._hash, self._key = KeyGenerator.FromStr("".join(given+targets+steps), l=8)

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
            for inst in step.produces:
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

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        all_types: dict[str, Endpoint] = {}
        while len(all_types) < len(raw["types"]):
            for k, v in raw["types"].items():
                if k in all_types: continue
                parent_keys = v.get("parents", [])
                if any(p not in all_types for p in parent_keys): continue
                parents = {all_types[p] for p in parent_keys}
                proto = Endpoint.Unpack(dict(properties=v["properties"]))
                all_types[k] = Endpoint(properties=proto.properties, parents=parents)

        def _unpack_given(raw: dict):
            inst = DataInstance.Unpack(raw, libraries)
            inst.dtype = all_types[raw["type_id"]]
            inst.RecalculateKey()
            return inst

        def _unpack_step(raw: dict):
            step = WorkflowStep.Unpack(raw, libraries)
            for inst, r in itertools.chain(zip(step.uses, raw["uses"]), zip(step.produces, raw["produces"])):
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
        given: Iterable[DataInstanceLibrary], transforms: Iterable[TransformInstanceLibrary], targets: Iterable[Endpoint],
        max_iter: int=256, max_refine: int=256, seed: int=42,
    ):
        given_map: dict[Endpoint, DataInstance] = {}
        for lib in given:
            for path, ep_name, ep in lib.Iterate():
                if ep in given_map:
                    Log.Warn(f"[{ep}] of [{lib}] is masked")
                    continue
                given_map[ep] = DataInstance(
                    path=path,
                    dtype=ep,
                    dtype_name=ep_name,
                    parent_lib=lib,
                )

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

        result = solve_by_mcts(
            given=given_map.keys(),
            target=target_model,
            transforms=transform2inst.keys(),
            max_iter=max_iter,
            max_refine=max_refine,
            seed=seed,
        )

        assert result.complete, "failed to make plan!"
        solution = result

        instance_map: dict[Endpoint, DataInstance] = {k:v for k, v in given_map.items()}
        steps: list[WorkflowStep] = []
        target_meta: dict[Endpoint, WorkflowTarget] = {}
        used_endpoints: set[Endpoint] = set()
        for i, appl in enumerate(solution.dependency_plan[1:-1]): # first is mock tr for given, last is for target
            tr = transform2inst[appl.transform]
            _lib = inst2trlib[tr]

            for d, e in appl.produced.items():
                p = tr.output_signature[d]
                _instance = DataInstance(
                    path = Path(p),
                    dtype = e, # we actually dont want lineage at this stage so that the hashes match
                    dtype_name = _lib.GetName(d), # type: ignore # Dependency not assignable to Endpoint
                    parent_lib = _lib,
                )
                instance_map[e] = _instance

            used_endpoints |= {e for e in appl.used.values()}
            step = WorkflowStep(
                order=i+1,
                uses=[instance_map[e] for e in appl.used.values()],
                produces=[instance_map[e] for e in appl.produced.values()],
                dependency_map={d:instance_map[e] for d, e in itertools.chain(appl.used.items(), appl.produced.items())},
                transform=tr,
                transform_library=_lib,
            )
            steps.append(step)

            for d, e in appl.produced.items():
                for target in targets:
                    if not e.IsA(target): continue
                    if not target.parents.issubset(e.parents): continue
                    _used_givens = []
                    for p in target.parents:
                        if p not in given_map: continue
                        _used_givens.append(given_map[p]) # type: ignore # Node not assignable to Endpoint
                    target_meta[target] = WorkflowTarget(
                        instance=instance_map[e],
                        used_givens=_used_givens,
                        producing_step=step,
                    )
                    break

        return cls(
            given=[i for e, i in given_map.items() if e in used_endpoints],
            targets=list(target_meta.values()),
            steps=steps,
            _solver_result=result,
        )

    def PrepareNextflow(self, wf_name: str, context: NextflowGenContext):
        # todo dynamic resources
        # https://www.nextflow.io/docs/latest/process.html#dynamic-task-resources
        TAB = " "*4
        process_definitions = []
        workflow_definition = []
        target_instances = {x.instance for x in self.targets}
        _cwd = Path(".")
        for step in self.steps:
            name = f"s{step.order:08}_{step.transform.model.key}__{step.transform.name}"
            src = [f"process {name}"+" {"]
            to_pubish = [x for x in step.produces if x in target_instances]
            for x in to_pubish:
                src.append(TAB+f'publishDir "$params.output/{step.order:08}", mode: "copy", pattern: "{x.path}"')
            if len(to_pubish)>0:
                src.append("") # newline

            def _make_bind_var(i: int, is_assignment=False):
                s = "\\$" if not is_assignment else ""
                return f"{s}b{i+1:02}"
            external_binds = set()
            for inst in step.uses:
                p = inst.path
                if p.is_relative_to(_cwd): continue
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
                ).MakeBindsParam(defaults=False)

            src += [
                TAB+"input:",
                TAB+TAB+f'val step_index',
            ] + [
                TAB+TAB+f'path _{i+1:02} // {x.dtype_name}' for i, x in enumerate(step.uses)
            ] + [
                "",
                TAB+"output:",
            ] + [
                TAB+TAB+f'path "{x.path}"' for x in step.produces
            ] + [
                "",
                TAB+'script:',
                TAB+'"""',
                TAB+f'{context.bootstrap_var}',
                TAB+f'echo "$task.cpus/$task.memory" >{METADATA_FILE}',
            ] + [
                TAB+f'{_make_bind_var(i, is_assignment=True)}="{p}"'
                for i, p in enumerate(external_binds)
            ] + [
                TAB+f'echo "step $step_index"',
                TAB+f'echo "{external_binds_param}" >>{METADATA_FILE}',
                TAB+f'bootstrap {context.external_work_var} $step_index',
                TAB+f'[ -e .command.success ] && exit 0 || exit 1', # in case slurm silently kills proc from oom/timeout
                TAB+'"""',
                "}"
            ]
            process_definitions.append("\n".join(src))
            output_vars = [f"_{x.dtype.key}" for x in step.produces]
            output_vars = ', '.join(output_vars)
            if len(step.produces) > 1:
                output_vars = f"({output_vars})"
            input_vars = [f'{step.order}']+[f"_{x.dtype.key}" for x in step.uses]
            input_vars = ', '.join(input_vars)
            workflow_definition.append(TAB+f'{output_vars} = {name}({input_vars})')

        workflow_definition = [
            f"workflow {wf_name} "+"{",
        ] + [
            TAB+f'_{x.dtype.key}'+f' = Channel.fromPath("{str(x.ResolvePath()).replace(str(context.home_dir), context.external_home_var)}") // {x.dtype_name}' for x in self.given
        ] + [
            "",
        ] + workflow_definition + [
            "}",
        ]

        return NextflowGenResult(
            workflow_name=wf_name,
            content="\n".join([
                "\n\n".join(process_definitions),
                "",
                "\n".join(workflow_definition),
                "",
            ]),
        )

    def RenderDAG(self, path_base: Path|str, format: str ='svg', *, font: str = 'Arial', hide_images: bool = True):
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

        def _as_DAG(*, font: str = 'Arial', hide_images: bool = True) -> str:
            lines = ["digraph G {"]
            lines += [f'graph [fontname="{font}"];', f'node  [fontname="{font}"];', f'edge  [fontname="{font}"];']
            for step in self.steps:
                transform_name = step.transform.name
                lines.append(_render_node(NodeType.TRANSFORM, transform_name))
                if hide_images:
                    inputs  = [u.dtype_name for u in step.uses if "oci_image" not in u.dtype_name]
                    outputs = [o.dtype_name for o in step.produces if "oci_image" not in o.dtype_name]
                else:
                    inputs  = [u.dtype_name for u in step.uses]
                    outputs = [o.dtype_name for o in step.produces]
                for name in inputs:
                    lines.append(_render_node(NodeType.DATA, name))
                    lines.append(f'    "{name}" -> "{transform_name}";')
                for name in outputs:
                    lines.append(_render_node(NodeType.DATA, name))
                    lines.append(f'    "{transform_name}" -> "{name}";')
            lines.append("}")
            return "\n".join(lines)
        
        dag_str = _as_DAG(font=font, hide_images=hide_images)
        src = graphviz.Source(dag_str, filename=path_base, format=format)
        src.render(cleanup=True, quiet=True)

@dataclass
class WorkflowTask:
    plans: list[WorkflowPlan]
    data_libraries: list[DataInstanceLibrary] = field(default_factory=list)
    transform_libraries: list[TransformInstanceLibrary] = field(default_factory=list)
    config: dict = field(default_factory=dict)

    def __post_init__(self):
        self._update_hash()

    def _update_hash(self):
        self._hash, self._key = KeyGenerator.FromStr("".join(p._key for p in self.plans), l=8)

    def GetKey(self):
        return self._key

    @classmethod
    def Merge(cls, tasks: Iterable[WorkflowTask], config=None):
        if config is None: _config = {}
        plans: list[WorkflowPlan] = []
        data_libraries: dict[str, DataInstanceLibrary] = {}
        transform_libraries = {}
        for t in tasks:
            plans += t.plans
            data_libraries |= {l.GetKey():l for l in t.data_libraries}
            transform_libraries |= {l.GetKey():l for l in t.transform_libraries}
            if config is None: _config|=t.config
        for i, s in enumerate(s for p in plans for s in p.steps):
            s.order = i+1
        return WorkflowTask(
            plans=plans,
            data_libraries=list(data_libraries.values()),
            transform_libraries=list(transform_libraries.values()),
            config=_config if config is None else config,
        )

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
            "",
        ]
        HEADER = "\n".join([
            f"{_strip_var(context.external_home_var)} = '{context.external_home}'",
            f'{_strip_var(context.external_work_var)} = "{context.external_work}"'.replace(str(context.external_home), context.external_home_var),
        ]+bootstrap)
        MAX_FILE_SIZE = int(2**16 * 0.95) # nextflow is 65536

        plan_defs = [p.PrepareNextflow(f"p{i+1:04}", context) for i, p in enumerate(self.plans)]
        top_level: list[NextflowGenResult] = []
        _group: list[NextflowGenResult] = []
        def _submit_group():
            if len(_group)==0: return
            i = len(top_level)+1
            wf_name = f"d1n{i:04}"
            top_level.append(
                NextflowGenResult(
                    workflow_name=wf_name,
                    content=HEADER+"\n".join([
                        g.content for g in _group
                    ]+[
                        "",
                        f"workflow {wf_name}"+"{",
                    ] + [
                        TAB+f"{g.workflow_name}()"
                        for g in _group
                    ] + [
                        "}",
                    ])
                )
            )
            _group.clear()
        for d in plan_defs:
            gsize = sum(len(g.content)+(2*len(g.workflow_name)) for g in _group)+len(HEADER)
            dsize = len(d.content)
            if gsize+dsize>=MAX_FILE_SIZE:
                _submit_group()
            _group.append(d)
        _submit_group()
        
        T = TypeVar("T")
        def batchify(iterable: Iterable[T], n) -> Generator[list[T], Any, None]:
            batch: list[T] = []
            for x in iterable:
                if len(batch) >= n:
                    yield batch
                    batch = []
                batch.append(x)
            if len(batch) > 0: yield batch
        def create_batch(wf_name: str, batch: list[NextflowGenResult], lib_dir="."):
            return NextflowGenResult(
                workflow_name=wf_name,
                content="\n".join(
                    [
                        "include { "+g.workflow_name+" } from '"+f"{lib_dir}/{g.workflow_name}'"
                        for g in batch
                    ] + [
                        f"workflow {wf_name}"+"{",
                    ] + [
                        f"{g.workflow_name}()"
                        for g in batch
                    ] + [
                        "}",
                    ],
                ),
            )
        committed_files: list[NextflowGenResult] = top_level.copy()
        MAX_BATCH = 1000
        depth = 2
        while len(top_level)>MAX_BATCH:
            _batches = list(batchify(top_level, MAX_BATCH))
            top_level.clear()
            for i, _batch in enumerate(_batches):
                i += 1
                wf_name = f"d{depth}n{i:04}"
                b = create_batch(wf_name, _batch)
                top_level.append(b)
                committed_files.append(b)
            depth += 1
        plans_dir = context.work_dir/"plans"
        plans_dir.mkdir(exist_ok=True)
        for p in committed_files:
            with open(plans_dir/f"{p.workflow_name}.nf", "w") as f:
                f.write(p.content)
        main = create_batch("", top_level, f"./{plans_dir.name}")
        with open(context.work_dir/"workflow.nf", "w") as f:
             f.write(main.content)

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
        given = {inst.ResolvePath().parent for plan in self.plans for inst in plan.given if should_keep(inst)}
        for path in given:
            update(str(path.parent))
        return roots

    def Pack(self):
        optional = {}
        if len(self.config) > 0:
            optional["config"] = self.config
        return dict(
            data_libraries=[lib.GetKey() for lib in self.data_libraries],
            transform_libraries=[lib.GetKey() for lib in self.transform_libraries],
        ) | optional

    def SaveAs(self, dest: Source):
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            _task_path = temp_dir/"task.yml"
            with open(_task_path, "w") as f:
                yaml.dump(dict(
                    task=self.Pack(),
                    plans=[p.Pack() for p in self.plans],
                ), f)
            _mover = Logistics()
            _mover.QueueTransfer(
                src=Source(address=str(temp_dir), type=SourceType.DIRECT),
                dest=dest,
            )
            for lib in self.data_libraries:
                _temp_mover = lib.PrepTransfer(dest/f"data/{lib.GetKey()}")
                _mover._queue.extend(_temp_mover._queue)
            for lib in self.transform_libraries:
                _temp_mover = lib.PrepTransfer(dest/f"transforms/{lib.GetKey()}")
                _mover._queue.extend(_temp_mover._queue)
            res = _mover.ExecuteTransfers()
            return res

    @classmethod
    def Load(cls, path: Path|str, alt_data_paths: list[Path|str]|None=None):
        path = Path(path)
        with open(path/"task.yml") as f:
            d = yaml.safe_load(f)
        raw_task = d["task"]
        raw_plans = d["plans"]

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
        plans = [WorkflowPlan.Unpack(p, _libraries) for p in raw_plans]
        return cls(
            plans=plans,
            data_libraries=[data_libs[n] for n in raw_task["data_libraries"]],
            transform_libraries=[tr_libs[n] for n in raw_task["transform_libraries"]],
            config=raw_task.get("config", {}),
        )
