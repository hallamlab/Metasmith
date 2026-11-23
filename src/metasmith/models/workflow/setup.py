from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path

from ..libraries import DataInstance, LOCAL_LABEL, TransformInstance, TransformInstanceLibrary
from ..solver import Endpoint
from .plan import WorkflowPlan
from .steps import PULL_TRANSFORM, WorkflowStep, WorkflowTarget
from .task import WorkflowTask


class NoSetupStepsError(ValueError):
    pass


# A setup run that fetches more than this asks before it starts.
CONFIRM_DOWNLOAD_GB = 100


def setup_subplan(task: WorkflowTask, pull_images: bool = False) -> WorkflowTask:
    # The steps of an already-solved plan that read no sample, with their outputs as
    # targets. It is a slice, never a re-solve: a fresh solve may choose another
    # producer or env, and its cache keys would then miss the real run's.
    #
    # `pull_images` puts one pullContainer step per tool image of the FULL plan in
    # front of the slice. They are built here, after the solve, and no sliced step
    # consumes their products, so no sliced step's cache key moves. Codegen holds each
    # tool step's env until that env's pull is done (`Orchestrator.after`).
    assert task.sample_library_count is not None, (
        "this task predates the record of which libraries hold samples; generate it again"
    )
    sample_keys = {lib.GetKey() for lib in task.data_libraries[:task.sample_library_count]}
    producer = {
        inst.instance_id: step
        for step in task.plan.steps for group in step.produces for inst in group
    }

    kept = []
    kept_orders: set[int] = set()
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        def free(inst) -> bool:
            made_by = producer.get(inst.instance_id)
            if made_by is not None:
                return made_by.order in kept_orders
            return inst.parent_lib is not None and inst.parent_lib.GetKey() not in sample_keys
        if step.uses and all(free(inst) for inst in step.uses):
            kept.append(step)
            kept_orders.add(step.order)
    pulls = _pull_steps(task) if pull_images else []
    if not kept and not pulls:
        raise NoSetupStepsError(f"no step of [{task.GetKey()}] runs without a sample")

    # The bootstrap and the runner find step N at steps[N-1], so the slice is numbered
    # 1..n again, pulls first: codegen declares a pull before the steps it holds back.
    # Copies, because the full plan is staged from the same step objects.
    renumbered = []
    for order, step in enumerate(pulls + kept, start=1):
        twin = copy.copy(step)
        twin.order = order
        renumbered.append(twin)
    kept = renumbered

    consumed = {inst.instance_id for step in kept for inst in step.uses}
    targets = [
        WorkflowTarget(name=_target_name(step, inst), instance=inst, producing_step=step)
        for step in kept for group in step.produces for inst in group
        if inst.instance_id not in consumed
    ]
    plan = WorkflowPlan(
        given=[inst for inst in task.plan.given if inst.instance_id in consumed],
        targets=targets,
        steps=kept,
        keep_intermediates=task.plan.keep_intermediates,
        setup=True,
    )
    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=task.data_libraries[task.sample_library_count:],
        transform_libraries=task.transform_libraries,
        sample_library_count=0,
    )


def _target_name(step: WorkflowStep, inst: DataInstance) -> str:
    # A pull publishes under the image's env name, since every pull makes the same type.
    if step.transform.name == PULL_TRANSFORM:
        return step.uses[0].dtype_name.split("::")[-1]
    return inst.dtype_name


def tool_envs(task: WorkflowTask) -> list[DataInstance]:
    """The given env resources whose tool some step of `task` runs, in step order."""
    given = {inst.instance_id for inst in task.plan.given}
    found: dict[str, DataInstance] = {}
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        for dep in step.transform._env_deps:
            for inst in step.dependency_map.get(dep, []):
                if inst.instance_id in given:
                    found.setdefault(inst.instance_id, inst)
    return list(found.values())


def _pull_transform(task: WorkflowTask) -> tuple[TransformInstance, TransformInstanceLibrary]:
    for lib in task.transform_libraries:
        for _path, tr in lib.IterateTransforms():
            if tr.name == PULL_TRANSFORM:
                return tr, lib
    raise ValueError(
        f"setup pulls tool images with [{PULL_TRANSFORM}], and no transform library of"
        f" [{task.GetKey()}] carries it; plan with the standard library's logistics"
    )


def _pull_steps(task: WorkflowTask) -> list[WorkflowStep]:
    # One step per env stream, because a step's slot reads one stream. Each product's
    # type descends from its env's type, so each pull posts to a stream of its own and
    # codegen can hold back exactly the steps that read that env.
    envs = tool_envs(task)
    if not envs:
        return []
    pull, lib = _pull_transform(task)
    image, = pull.model.requires
    (record,), = pull.model.produces
    by_stream: dict[str, list[DataInstance]] = {}
    for inst in envs:
        by_stream.setdefault(task.plan.streams.get(inst.dtype.key, inst.dtype.key), []).append(inst)
    steps = []
    for members in by_stream.values():
        e = Endpoint(properties=set(record.properties), parents={members[0].dtype})
        product = DataInstance(
            path=Path(e.key + e.GetPreferredFileExtension()),
            dtype=e,
            dtype_name=lib.GetName(record),
            parent_lib=lib,
            origin="lineage",
            instance_id=e.key,
        )
        steps.append(WorkflowStep(
            order=0,
            dependency_map={image: list(members), record: [product]},
            transform=pull,
            transform_library=lib,
        ))
    return steps


@dataclass
class SetupFootprint:
    """What a setup run of a plan fetches, from the download contracts' `download_size`."""
    download_gb: float = 0.0
    images: int = 0
    unsized: list[str] = field(default_factory=list)

    @classmethod
    def Of(cls, task: WorkflowTask) -> "SetupFootprint":
        try:
            steps = setup_subplan(task).plan.steps
        except NoSetupStepsError:
            steps = []
        fp = cls(images=len(tool_envs(task)))
        for step in steps:
            if LOCAL_LABEL not in step.transform.labels:
                continue
            size = step.transform.download_size
            if size is None:
                fp.unsized.append(str(step.transform.name))
            else:
                fp.download_gb += size.value_gb
        return fp

    @property
    def confirm(self) -> bool:
        return self.download_gb >= CONFIRM_DOWNLOAD_GB

    @property
    def line(self) -> str:
        gb = f"~{self.download_gb:.0f} GB" if self.download_gb >= 1 else "<1 GB"
        line = f"setup: {gb} of references, {self.images} image{'s' if self.images != 1 else ''}"
        if self.unsized:
            line += f" (unknown size: {', '.join(self.unsized)})"
        return line

    def Pack(self) -> dict:
        return {
            "download_gb": round(self.download_gb, 1),
            "images": self.images,
            "unsized": list(self.unsized),
            "confirm": self.confirm,
            "line": self.line,
        }
