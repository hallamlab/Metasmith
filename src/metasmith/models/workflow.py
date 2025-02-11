from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .libraries import DataTypeLibrary
from .libraries import DataInstance, DataInstanceLibrary
from .libraries import TransformInstance, TransformInstanceLibrary
from .solver import Endpoint, Dependency, Transform, _solve_by_bounded_dfs

@dataclass
class WorkflowStep:
    order: int
    transform_key: str
    uses: list[DataInstance]
    produces: list[DataInstance]
    transform: TransformInstance | Path

@dataclass
class WorkflowPlan:
    given: list[DataInstance]
    targets: list[DataInstance]
    steps: list[WorkflowStep]

    def __len__(self):
        return len(self.steps)
    
    @classmethod
    def Generate(
        cls,
        given: Iterable[DataInstanceLibrary], transforms: TransformInstanceLibrary, targets: DataTypeLibrary,
    ):
        _given_map: dict[Endpoint, DataInstance] = {}
        for lib in given:
            _map = {e.type:e for e in lib.manifest.values()}
            for inst in _map.values():
                inst.source = lib.source/inst.source
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
                _instance = DataInstance(p, e)
                _instance_map[e] = _instance
            
            step = WorkflowStep(
                order=i+1,
                transform_key=tr.model.key,
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
    
@dataclass
class Workspace:
    source: Path

    def Deploy(self) -> None:
        pass

    def Execute(self, plan: WorkflowPlan) -> None:
        pass
