"""A step that died is not a run that succeeded.

Pins I3 of the annotation-trio investigation. The shipped local preset ends its
`errorStrategy` in `ignore`, which is the right call for a twelve-sample run --
one dead annotator should not destroy the other eleven. What is not right is
that the driver then reports the run completed, with the dead step's product
missing and nothing naming it. The reporter was told their InterProScan run had
finished when it had produced nothing at all.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents import RunWorkflow
from metasmith.constants import AgentPaths
from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask
from metasmith.ops.runtime import read_trace
from metasmith.testing.mock_transforms import alignment_transform
from metasmith.testing.virtual_runtime import FAIL_STEPS_ENV

from .conftest import create_transform_library, stage_task


LOG_DIR = Path("_metasmith/logs.virtual")

# Consumes what the alignment step makes, so the run has already done real work
# by the time it dies -- and has a live sibling to carry on with, which is why
# ignoring it is the right strategy and reporting it is the missing half.
FAILING_BINNER = {
    "failing_binner": '''
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
bam = model.AddRequirement(lib.GetType("mock::bam"))
out = model.AddProduct(lib.GetType("mock::metabat2_bins"))

def protocol(context: ExecutionContext):
    raise RuntimeError("intentional test failure")

TransformInstance(protocol=protocol, model=model, group_by=asm)
''',
    "surviving_binner": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
bam = model.AddRequirement(lib.GetType("mock::bam"))
out = model.AddProduct(lib.GetType("mock::maxbin2_bins"))

def protocol(context: ExecutionContext):
    out_path = Path("bins.fa")
    out_path.write_text("mock bins")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
''',
}


@pytest.fixture
def run_with_a_dead_step(virtual_runtime, tmp_path, mock_samples, mock_types, monkeypatch):
    monkeypatch.setenv(FAIL_STEPS_ENV, "failing_binner")
    tr_lib = create_transform_library(
        tmp_path / "tr", mock_types, alignment_transform() | FAILING_BINNER,
    )
    target_model = Transform()
    target_model.AddRequirement(properties={"bins", "method:metabat2"})
    target_model.AddRequirement(properties={"bins", "method:maxbin2"})
    plan = WorkflowPlan.Generate(
        given=[[sv] for sv in mock_samples.AsSamples("mock::assembly")],
        transforms=[tr_lib],
        target_names=["metabat2_bins", "maxbin2_bins"],
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan)
    task = WorkflowTask(
        ok=True, plan=plan,
        data_libraries=[mock_samples], transform_libraries=[tr_lib],
    )
    key, workspace, _staged = stage_task(task)
    RunWorkflow(key=key, log_dir=LOG_DIR, host=virtual_runtime.host, stub_delay=0.0)
    return workspace


def test_the_dead_step_is_ignored_and_its_siblings_finish(run_with_a_dead_step):
    # The setup, not the claim: this is the behaviour worth keeping. The run
    # goes on past the failure and the healthy branch still lands.
    trace = read_trace(run_with_a_dead_step / LOG_DIR)
    assert trace["failed"] >= 1, trace
    assert trace["done"] >= 1, trace


def test_a_run_with_a_dead_step_is_not_reported_completed(run_with_a_dead_step):
    log = (run_with_a_dead_step / LOG_DIR / AgentPaths.MAIN_LOG_FILE).read_text()
    assert "run completed at" not in log, (
        "the run reported plain completion with a step dead and its product "
        "missing; that is what the reporter was told"
    )


def test_the_failed_step_is_named_in_the_run_log(run_with_a_dead_step):
    log = (run_with_a_dead_step / LOG_DIR / AgentPaths.MAIN_LOG_FILE).read_text()
    assert "failing_binner" in log.split("calling nextflow from container")[-1], (
        "nothing after the nextflow call names the step that died"
    )
