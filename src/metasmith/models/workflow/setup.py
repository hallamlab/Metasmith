from __future__ import annotations

import copy

from .plan import WorkflowPlan
from .steps import WorkflowTarget
from .task import WorkflowTask


class NoSetupStepsError(ValueError):
    pass


def setup_subplan(task: WorkflowTask) -> WorkflowTask:
    # The steps of an already-solved plan that read no sample, with their outputs as
    # targets. It is a slice, never a re-solve: a fresh solve may choose another
    # producer or env, and its cache keys would then miss the real run's.
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
    if not kept:
        raise NoSetupStepsError(f"no step of [{task.GetKey()}] runs without a sample")

    # The bootstrap and the runner find step N at steps[N-1], so the slice is numbered
    # 1..n again. Copies, because the full plan is staged from the same step objects.
    renumbered = []
    for order, step in enumerate(kept, start=1):
        twin = copy.copy(step)
        twin.order = order
        renumbered.append(twin)
    kept = renumbered

    consumed = {inst.instance_id for step in kept for inst in step.uses}
    targets = [
        WorkflowTarget(name=inst.dtype_name, instance=inst, producing_step=step)
        for step in kept for group in step.produces for inst in group
        if inst.instance_id not in consumed
    ]
    used = {inst.instance_id for step in kept for inst in step.uses}
    plan = WorkflowPlan(
        given=[inst for inst in task.plan.given if inst.instance_id in used],
        targets=targets,
        steps=kept,
        publish_intermediates=task.plan.publish_intermediates,
    )
    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=task.data_libraries[task.sample_library_count:],
        transform_libraries=task.transform_libraries,
        sample_library_count=0,
    )
