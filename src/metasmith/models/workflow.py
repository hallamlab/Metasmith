from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
import json

from .libraries import DataTypeLibrary
from .libraries import DataInstance, DataInstanceLibrary
from .libraries import TransformInstance, TransformInstanceLibrary
from .libraries import ExecutionContext, ExecutionResult
from .remote import Source, SourceType
from .solver import Endpoint, Dependency, Transform, _solve_by_bounded_dfs
from ..agents.ssh import Agent
from ..hashing import KeyGenerator
from ..logging import Log

@dataclass
class WorkflowStep:
    order: int
    transform_key: str
    uses: list[DataInstance]
    produces: list[DataInstance]
    transform: TransformInstance = None

    def RelinkTransform(self, lib: TransformInstanceLibrary):
        self.transform = lib.GetByKey(self.transform_key)

    def Pack(self):
        return dict(
            order=self.order,
            transform_key=self.transform_key,
            uses=[inst.Pack() for inst in self.uses],
            produces=[inst.Pack() for inst in self.produces],
        )
    
    @classmethod
    def Unpack(cls, raw: dict):
        return cls(
            order=raw["order"],
            transform_key=raw["transform_key"],
            uses=[DataInstance.Unpack(inst) for inst in raw["uses"]],
            produces=[DataInstance.Unpack(inst) for inst in raw["produces"]],
        )

@dataclass
class WorkflowPlan:
    given: list[DataInstance]
    targets: list[DataInstance]
    steps: list[WorkflowStep]
    _hash: int = None
    _key: str = None

    def __post_init__(self):
        given = [inst._key for inst in self.given]
        targets = [inst._key for inst in self.targets]
        steps = [step.transform_key for step in self.steps]
        self._hash, self._key = KeyGenerator.FromStr("".join(given+targets+steps), l=6)

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
            json.dump(self.Pack(), f, indent=4)

    @classmethod
    def Unpack(cls, raw: dict):
        return cls(
            given=[DataInstance.Unpack(inst) for inst in raw["given"]],
            targets=[DataInstance.Unpack(inst) for inst in raw["targets"]],
            steps=[WorkflowStep.Unpack(step) for step in raw["steps"]],
        )
    
    @classmethod
    def Load(cls, path: Path):
        with open(path) as f:
            raw = json.load(f)
        return cls.Unpack(raw)

    @classmethod
    def Generate(
        cls,
        given: Iterable[DataInstanceLibrary], transforms: TransformInstanceLibrary, targets: DataTypeLibrary,
    ):
        _given_map: dict[Endpoint, DataInstance] = {}
        for lib in given:
            _map = {inst.type:inst for k, inst in lib}
            _given_map.update(_map)
        _transform_map = {t.model:t for _, t in transforms}

        _target_e2d: dict[Endpoint, Dependency] = {}
        _target_model = Transform()
        for t in targets:
            t.AddAsDependency(_target_model, _target_e2d)

        solutions = _solve_by_bounded_dfs(
            given=_given_map.keys(),
            target=_target_model,
            transforms=_transform_map.keys(),
        )
        assert len(solutions) > 0, "failed to make plan!"
        solution = solutions[0]

        _instance_map = _given_map.copy()
        steps: list[WorkflowStep] = []
        for i, appl in enumerate(solution.dependency_plan):
            tr = _transform_map[appl.transform]
            for e, d in appl.produced.items():
                p = tr.output_signature[d]
                _instance = DataInstance(
                    source=Source(address=p, type=SourceType.DIRECT), 
                    type=e,
                )
                _instance_map[e] = _instance
            
            step = WorkflowStep(
                order=i+1,
                transform_key=tr._key,
                uses=[_instance_map[e] for e in appl.used],
                produces=[_instance_map[e] for e in appl.produced],
                transform=tr,
            )
            steps.append(step)

        _sol_produces_d2e = {d:e for e, d in solution.application.used.items()}
        _sol_target_instances: list[DataInstance] = []
        for e in targets:
            d = _target_e2d[e]
            _appl_e = _sol_produces_d2e[d]
            _inst = _instance_map[_appl_e]
            _sol_target_instances.append(_inst)

        return cls(
            given=list(_given_map.values()),
            targets=_sol_target_instances,
            steps=steps,
        )
    
    def PrepareNextflow(self, work_dir: Path, external_work: Path):
        TAB = " "*4
        wf_path = work_dir/"metasmith/workflow.nf"
        context_dir = work_dir/"metasmith/contexts"
        external_context_dir = external_work/"metasmith/contexts"
        external_bootstrap = external_work/"metasmith/msm_bootstrap"
        context_dir.mkdir(parents=True, exist_ok=True)
        contexts: dict[str, Path] = {}
        process_definitions = {}
        workflow_definition = []
        target_endpoints = {x for x in self.targets}
        for step in self.steps:
            name = f"{step.transform._source.GetName(extension=False)}__{step.transform_key}"
            if name not in process_definitions:
                src = [f"process {name}"+" {"]
                to_pubish = [x for x in step.produces if x in target_endpoints]
                for x in to_pubish:
                    src.append(TAB+f'publishDir "$params.output", mode: "copy", pattern: "{x.source.address}"')
                if len(to_pubish)>0:
                    src.append("") # newline

                src += [
                    TAB+"input:",
                    TAB+TAB+f'path bootstrap',
                    TAB+TAB+f'path context',
                ] + [
                    TAB+TAB+f'path _{i+1:02} // {str(x.type).replace(":"+x.type.key, "")}' for i, x in enumerate(step.uses)
                ] + [
                    "",
                    TAB+"output:",
                ] + [
                    TAB+TAB+f'path "{x.source.address}"' for x in step.produces
                ] + [
                    "",
                    TAB+'script:',
                    TAB+'"""',
                ] + [
                    TAB+f'bash $bootstrap $context',
                    TAB+'"""',
                    "}"
                ]
                process_definitions[name] = "\n".join(src)

            step_key = f"{step.order:03}"
            context_path = context_dir/f"{step_key}.yml"
            external_context_path = external_context_dir/f"{step_key}.yml"
            # external_transform_path = step.transform._source.address.replace(str(work_dir), str(external_work))
            context = ExecutionContext(
                inputs = [x for x in step.uses],
                output = [x for x in step.produces],
                transform_key = step.transform_key,
                work_dir = external_work,
            )
            context.Save(context_path)
            contexts[step_key] = external_context_path

            output_vars = [f"_{x.type.key}" for x in step.produces]
            output_vars = ', '.join(output_vars)
            if len(step.produces) > 1:
                output_vars = f"({output_vars})"
            input_vars = ['bootstrap', f'context_{step_key}']+[f"_{x.type.key}" for x in step.uses]
            input_vars = ', '.join(input_vars)
            workflow_definition.append(TAB+f'{output_vars} = {name}({input_vars})')

        workflow_definition = [
            "workflow {",
            TAB+f'bootstrap = Channel.fromPath("{external_bootstrap}")',
        ] + [
            TAB+f'context_{k} = Channel.fromPath("{p}")' for k, p in contexts.items()
        ] + [
            "",
        ] + [
            TAB+f'_{x.type.key}'+f' = Channel.fromPath("{Path(x.source.address)}") // {x.type}' for x in self.given
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
    config: dict = field(default_factory=dict)

    def Pack(self):
        return dict(
            plan=self.plan.Pack(),
            agent=self.agent.Pack(),
            config=self.config,
        )
    
    def Save(self, path: Path):
        with open(path, "w") as f:
            json.dump(self.Pack(), f, indent=4)
    
    @classmethod
    def Unpack(cls, raw: dict):
        return cls(
            plan=WorkflowPlan.Unpack(raw["plan"]),
            agent=Agent.Unpack(raw["agent"]),
            config=raw.get("config", {}),
        )
    
    @classmethod
    def Load(cls, path: Path):
        with open(path) as f:
            raw = json.load(f)
        return cls.Unpack(raw)
