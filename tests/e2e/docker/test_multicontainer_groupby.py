"""Regression: heterogeneous-subtype inputs to a single group_by requirement.

Reproduces the multi-container-prefetch bug (filed against 0.18.6): prefetching
more than one container in one workflow crashed the generated Nextflow at runtime
with a NullPointerException in Orchestrator.group, and nothing pulled. Single
container worked.

Root cause: several distinct container subtypes (provides:bbtools / megahit /
seqkit) each structurally satisfy (Node.IsA) one generic `container` group_by
requirement. The solver correctly merges them (merged_endpoints), but the
generator's merged-endpoint collapse joined their instances WITHOUT normalizing
dtype. Codegen keys the group_by channel by instance dtype, so a mixed-dtype
list emitted `o.group('<A>', [<B>], ...)` where the by-key named a channel that
wasn't wired in -> runtime NPE, and only 1 of N inputs flowed.

These are stage-time assertions (no Docker / Nextflow runtime needed): the bug
is fully observable in the emitted workflow.nf and in the planned group_by
dtypes, so the regression is fast and host-independent.
"""

import re
from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import (
    WorkflowPlan,
    WorkflowTask,
    NextflowGenContext,
)
from metasmith.testing.mock_transforms import pull_container_transform

from .conftest import create_transform_library


# distinct subtypes, each IsA the generic container, plus the produced type
_CONTAINER_SUBTYPES = {
    "c_bbtools": {"container", "provides:bbtools"},
    "c_megahit": {"container", "provides:megahit"},
    "c_seqkit": {"container", "provides:seqkit"},
}


@pytest.fixture
def container_types(temp_dir) -> Path:
    """A type library with a generic container, 3 distinct container subtypes,
    and the pulled output type."""
    types = DataTypeLibrary()
    types["container"] = Endpoint(properties={"container"})
    for name, props in _CONTAINER_SUBTYPES.items():
        types[name] = Endpoint(properties=set(props))
    types["pulled"] = Endpoint(properties={"pulled_container"})
    types_path = temp_dir / "container_types.yml"
    types.Save(types_path)
    return types_path


@pytest.fixture
def container_inputs(temp_dir, container_types) -> DataInstanceLibrary:
    """3 .oci instances, each a distinct container subtype."""
    lib = DataInstanceLibrary(temp_dir / "containers.xgdb")
    lib.AddTypeLibrary(container_types, namespace="mock")
    for name in _CONTAINER_SUBTYPES:
        tool = name.split("_", 1)[1]
        f = lib.location / f"{tool}.oci"
        f.write_text(f"docker://example/{tool}:latest\n")
        lib.AddItem(Path(f"{tool}.oci"), f"mock::{name}")
    lib.Save()
    return lib


def _make_plan(container_types, container_inputs, temp_dir, n: int) -> WorkflowPlan:
    """Plan a pullContainer workflow over the first `n` distinct subtypes."""
    tr_lib = create_transform_library(
        temp_dir / "pull_tr", container_types, pull_container_transform()
    )
    samples = list(container_inputs.AsSamples("mock::container"))
    assert len(samples) >= n
    given = [[sv] for sv in samples[:n]]

    target_model = Transform()
    target_model.AddRequirement(properties={"pulled_container"})
    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=["pulled"],
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan), f"plan failed: {plan}"
    return plan, tr_lib


def _stage(task: WorkflowTask, work_dir: Path) -> str:
    work_dir.mkdir(parents=True, exist_ok=True)
    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=work_dir,
        external_work=work_dir,
        home_dir=Path("/msm_home"),
        external_home=work_dir.parent,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )
    task.PrepareNextflow(context)
    return (work_dir / AgentPaths.NXF_WORKFLOW).read_text()


def _group_calls(nxf: str) -> list[tuple[str, list[str]]]:
    """Extract (by_key, [using_symbols]) for every o.group(...) in the workflow."""
    calls = []
    for line in nxf.splitlines():
        m = re.search(r"o\.group\('([^']+)',\s*\[([^\]]*)\]", line)
        if not m:
            continue
        by = m.group(1)
        using = [s.strip().lstrip("_") for s in m.group(2).split(",") if s.strip()]
        calls.append((by, using))
    return calls


@pytest.mark.parametrize("n", [2, 3])
def test_multicontainer_group_by_collapses_to_one_dtype(
    container_types, container_inputs, temp_dir, n
):
    """The merged group_by requirement collapses to a single dtype.key."""
    plan, _ = _make_plan(container_types, container_inputs, temp_dir, n)

    assert len(plan.steps) == 1
    step = plan.steps[0]
    assert step.transform.name == "pullContainer"

    gbi = step.group_by_instances
    assert len(gbi) == n, "all N containers should bind to the group_by requirement"
    distinct = {x.dtype.key for x in gbi}
    assert len(distinct) == 1, (
        f"group_by bound {len(distinct)} distinct dtypes {sorted(distinct)}; "
        "the merged-endpoint collapse must normalize them to one"
    )


@pytest.mark.parametrize("n", [2, 3])
def test_multicontainer_emits_valid_o_group(
    container_types, container_inputs, temp_dir, n
):
    """Every emitted o.group by-key is among its wired using symbols (no NPE)."""
    plan, tr_lib = _make_plan(container_types, container_inputs, temp_dir, n)
    task = WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[container_inputs],
        transform_libraries=[tr_lib],
    )
    nxf = _stage(task, temp_dir / f"stage_{n}")

    calls = _group_calls(nxf)
    assert calls, "expected at least one o.group call"
    for by, using in calls:
        assert by in using, (
            f"o.group by-key '{by}' not among using {using} -> runtime NPE in "
            f"Orchestrator.group"
        )


def test_multicontainer_single_input_channel(
    container_types, container_inputs, temp_dir
):
    """The N merged containers flow through ONE input channel (homogeneous
    fan-out), not N orphaned per-subtype channels."""
    plan, tr_lib = _make_plan(container_types, container_inputs, temp_dir, 3)
    task = WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[container_inputs],
        transform_libraries=[tr_lib],
    )
    nxf = _stage(task, temp_dir / "stage_channel")

    postin_lines = [l for l in nxf.splitlines() if "o.postIn(" in l]
    assert len(postin_lines) == 1, (
        f"expected a single merged input channel, got {len(postin_lines)}:\n"
        + "\n".join(postin_lines)
    )

    # the single channel must reference the same key the o.group keys by
    (by, using), = _group_calls(nxf)
    assert by in using

    # all 3 source .oci paths land in the one channel's input file
    inputs_dir = temp_dir / "stage_channel" / "inputs"
    pooled = "\n".join(p.read_text() for p in inputs_dir.iterdir())
    for tool in ("bbtools", "megahit", "seqkit"):
        assert f"{tool}.oci" in pooled, f"{tool}.oci missing from pooled input channel"
