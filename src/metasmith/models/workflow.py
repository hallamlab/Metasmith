from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable
import yaml
from .libraries import DataTypeLibrary
from .libraries import DataInstanceLibrary, DataInstance
from .libraries import TransformInstance, TransformInstanceLibrary
from .libraries import ExecutionContext, ExecutionResult
from .remote import Logistics, Source, SourceType
from .solver import Endpoint, Dependency, Transform, _solve_by_bounded_dfs
from ..agents.ssh import Agent
from ..hashing import KeyGenerator
from ..logging import Log

@dataclass
class WorkflowStep:
    order: int
    uses: list[DataInstance]
    produces: list[DataInstance]
    transform: TransformInstance
    transform_library: TransformInstanceLibrary

    def Pack(self):
        return dict(
            order=self.order,
            uses=[inst.Pack() for inst in self.uses],
            produces=[inst.Pack() for inst in self.produces],
            transform=f"{self.transform_library.GetKey()}::{self.transform.name}",
        )
    
    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, transform_name = raw["transform"].split("::")
        lib = libraries[lib_key]
        assert isinstance(lib, TransformInstanceLibrary)
        tr = lib.GetTransform(transform_name)
        return cls(
            order=raw["order"],
            uses=[DataInstance.Unpack(inst, libraries) for inst in raw["uses"]],
            produces=[DataInstance.Unpack(inst, libraries) for inst in raw["produces"]],
            transform=tr,
            transform_library=lib,
        )

@dataclass
class WorkflowPlan:
    given: list[DataInstance]
    targets: list[DataInstance]
    steps: list[WorkflowStep]

    def __post_init__(self):
        given = [inst._key for inst in self.given]
        targets = [inst._key for inst in self.targets]
        steps = [step.transform.model.key for step in self.steps]
        self._hash, self._key = KeyGenerator.FromStr("".join(given+targets+steps), l=5)

    def __len__(self):
        return len(self.steps)
    
    def Pack(self):
        return dict(
            given=[inst.Pack() for inst in self.given],
            targets=[inst.Pack() for inst in self.targets],
            steps=[step.Pack() for step in self.steps],
        )
    
    def Save(self, path: Path):
        with open(path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataTypeLibrary]):
        return cls(
            given=[DataInstance.Unpack(inst, libraries) for inst in raw["given"]],
            targets=[DataInstance.Unpack(inst, libraries) for inst in raw["targets"]],
            steps=[WorkflowStep.Unpack(step, libraries) for step in raw["steps"]],
        )
    
    @classmethod
    def Load(cls, path: Path):
        with open(path) as f:
            raw = yaml.load(f)
        return cls.Unpack(raw)

    @classmethod
    def Generate(
        cls,
        given: Iterable[DataInstanceLibrary], transforms: Iterable[TransformInstanceLibrary], targets: list[Endpoint],
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
        target_model = Transform()
        for t in targets:
            t.AddAsDependency(target_model, target_e2d)

        transform2inst: dict[Transform, TransformInstance] = {}
        inst2trlib: dict[TransformInstance, TransformInstanceLibrary] = {}
        for trlib in transforms:
            for path, name, tr in trlib.IterateTransforms():
                model = tr.model
                if model in transform2inst:
                    Log.Warn(f"transform [{model}] of [{trlib}] is masked")
                    continue
                transform2inst[model] = tr
                inst2trlib[tr] = trlib

        solutions = _solve_by_bounded_dfs(
            given=given_map.keys(),
            target=target_model,
            transforms=transform2inst.keys(),
        )

        assert len(solutions) > 0, "failed to make plan!"
        solution = solutions[0]

        _instance_map: dict[Endpoint, DataInstance] = given_map.copy()
        steps: list[WorkflowStep] = []
        for i, appl in enumerate(solution.dependency_plan):
            tr = transform2inst[appl.transform]
            _lib = inst2trlib[tr]
            for e, d in appl.produced.items():
                p = tr.output_signature[d]
                print(_lib.GetName(d))
                _instance = DataInstance(
                    path = Path(p),
                    dtype = d, # we actually dont want lineage at this stage so that the hashes match
                    dtype_name = _lib.GetName(d),
                    parent_lib = _lib,
                )
                _instance_map[e] = _instance
            
            step = WorkflowStep(
                order=i+1,
                uses=[_instance_map[e] for e in appl.used],
                produces=[_instance_map[e] for e in appl.produced],
                transform=tr,
                transform_library=_lib,
            )
            steps.append(step)

        _sol_produces_d2e = {d:e for e, d in solution.application.used.items()}
        _sol_target_instances: list[DataInstance] = []
        for e in targets:
            d = target_e2d[e]
            _appl_e = _sol_produces_d2e[d]
            _inst = _instance_map[_appl_e]
            _sol_target_instances.append(_inst)

        return cls(
            given=list(given_map.values()),
            targets=_sol_target_instances,
            steps=steps,
        )
    
    def PrepareNextflow(self, work_dir: Path, external_work: Path):
        TAB = " "*4
        metasmith_dir = work_dir/"_metasmith"
        external_metasmith_dir = external_work/metasmith_dir.name
        wf_path = work_dir/"workflow.nf"
        def _path_as_external(p: Path):
            p_str = str(p)
            if p_str.startswith(str(work_dir)):
                sub = p_str[len(str(work_dir)):]
                if sub.startswith("/"):
                    sub = sub[1:]
                p = external_work/sub
            return p
        process_definitions = {}
        workflow_definition = []
        target_endpoints = {x for x in self.targets}
        for step in self.steps:
            name = f"{step.transform.name}__{step.transform.model.key}"
            if name not in process_definitions:
                src = [f"process {name}"+" {"]
                to_pubish = [x for x in step.produces if x in target_endpoints]
                for x in to_pubish:
                    src.append(TAB+f'publishDir "$params.output", mode: "copy", pattern: "{x.path}"')
                if len(to_pubish)>0:
                    src.append("") # newline

                src += [
                    TAB+"input:",
                    TAB+TAB+f'path bootstrap',
                    TAB+TAB+f'val step_index',
                ] + [
                    TAB+TAB+f'path _{i+1:02} // {x.dtype_name} [{x.dtype}]' for i, x in enumerate(step.uses)
                ] + [
                    "",
                    TAB+"output:",
                ] + [
                    TAB+TAB+f'path "{x.path}"' for x in step.produces
                ] + [
                    "",
                    TAB+'script:',
                    TAB+'"""',
                ] + [
                    TAB+f'bash $bootstrap/msm_bootstrap $step_index',
                    TAB+'"""',
                    "}"
                ]
                process_definitions[name] = "\n".join(src)

            output_vars = [f"_{x.dtype.key}" for x in step.produces]
            output_vars = ', '.join(output_vars)
            if len(step.produces) > 1:
                output_vars = f"({output_vars})"
            input_vars = ['bootstrap', f'{step.order}']+[f"_{x.dtype.key}" for x in step.uses]
            input_vars = ', '.join(input_vars)
            workflow_definition.append(TAB+f'{output_vars} = {name}({input_vars})')

        
        workflow_definition = [
            "workflow {",
            TAB+f'bootstrap = Channel.fromPath("{external_metasmith_dir}")',
        ] + [
            "",
        ] + [
            TAB+f'_{x.dtype.key}'+f' = Channel.fromPath("{_path_as_external(x.ResolvePath())}") // {x.dtype_name} [{x.dtype}]' for x in self.given
        ] + [
            "",
        ] + workflow_definition + [
            "}",
        ]

        wf_contents = [
            "\n\n".join(process_definitions.values()),
            "\n\n",
            "\n".join(workflow_definition),
            "\n",
        ]
        wf_contents = ''.join(wf_contents)
        with open(wf_path, "w") as f:
            f.write(wf_contents)

@dataclass
class WorkflowTask:
    plan: WorkflowPlan
    agent: Agent
    data_libraries: list[DataInstanceLibrary] = field(default_factory=list)
    transform_libraries: list[TransformInstanceLibrary] = field(default_factory=list)
    config: dict = field(default_factory=dict)

    def Pack(self):
        return dict(
            agent=self.agent.Pack(),
            config=self.config,
            data_libraries=[lib.GetKey() for lib in self.data_libraries],
            transform_libraries=[lib.GetKey() for lib in self.transform_libraries],
        )
    
    def SaveAs(self, dest: Source):
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            _task_path = temp_dir/"task.yml"
            with open(_task_path, "w") as f:
                yaml.dump(self.Pack(), f)
            _plan_path = temp_dir/"plan.yml"
            self.plan.Save(_plan_path)
            _mover = Logistics()
            for _path in [_task_path, _plan_path]:
                _mover.QueueTransfer(
                    src=Source(address=_path, type=SourceType.DIRECT),
                    dest=dest/_path.name,
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
    def Load(cls, path: Path|str):
        path = Path(path)
        with open(path/"task.yml") as f:
            raw_task = yaml.safe_load(f)
        with open(path/"plan.yml") as f:
            raw_plan = yaml.safe_load(f)
        
        data_libs = {n: DataInstanceLibrary.Load(path/f"data/{n}") for n in raw_task["data_libraries"]}
        tr_libs = {n: TransformInstanceLibrary.Load(path/f"transforms/{n}") for n in raw_task["transform_libraries"]}
        _libraries = data_libs|tr_libs
        plan = WorkflowPlan.Unpack(raw_plan, _libraries)

        return cls(
            plan=plan,
            agent=Agent.Unpack(raw_task["agent"]),
            data_libraries=[data_libs[n] for n in raw_task["data_libraries"]],
            transform_libraries=[tr_libs[n] for n in raw_task["transform_libraries"]],
            config=raw_task["config"],
        )
