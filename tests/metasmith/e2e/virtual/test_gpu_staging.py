from __future__ import annotations

import json

from metasmith.constants import AgentPaths
from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask

from .conftest import create_transform_library, stage_task


def _gpu_transform(name: str, out_type: str, toggle: str, gpu_memory: str) -> str:
    return f'''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary, TransformInstance,
    ExecutionContext, ExecutionResult, Resources, Size, Gpus,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::{out_type}"))

def protocol(context: ExecutionContext):
    p = Path("out.fa"); p.write_text("{name}")
    return ExecutionResult(manifest=[{{out: p}}], success=True)

TransformInstance(
    protocol=protocol, model=model, group_by=asm,
    resources=Resources(cpus=2, memory=Size.GB(4), gpus=Gpus.{toggle}, gpu_memory={gpu_memory}),
)
'''


def _cpu_transform(name: str, out_type: str) -> str:
    return f'''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary, TransformInstance,
    ExecutionContext, ExecutionResult, Resources, Size,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::{out_type}"))

def protocol(context: ExecutionContext):
    p = Path("out.fa"); p.write_text("{name}")
    return ExecutionResult(manifest=[{{out: p}}], success=True)

TransformInstance(
    protocol=protocol, model=model, group_by=asm,
    resources=Resources(cpus=2, memory=Size.GB(4)),
)
'''


def _stage_mixed(tmp_path, mock_samples, mock_types):
    transforms = {
        "gpu_required": _gpu_transform("gpu_required", "metabat2_bins", "REQUIRED", "Size.GB(40)"),
        "gpu_optional": _gpu_transform("gpu_optional", "maxbin2_bins", "OPTIONAL", "Size.GB(8)"),
        "cpu_only": _cpu_transform("cpu_only", "concoct_bins"),
    }
    tr_lib = create_transform_library(tmp_path / "tr", mock_types, transforms)

    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties={"bins", "method:metabat2"})
    target_model.AddRequirement(properties={"bins", "method:maxbin2"})
    target_model.AddRequirement(properties={"bins", "method:concoct"})

    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=["metabat2_bins", "maxbin2_bins", "concoct_bins"],
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan), plan
    task = WorkflowTask(ok=True, plan=plan, data_libraries=[mock_samples], transform_libraries=[tr_lib])
    return stage_task(task)


def test_gpu_label_only_on_declaring_processes(virtual_runtime, tmp_path, mock_samples, mock_types):
    _, workspace, _ = _stage_mixed(tmp_path, mock_samples, mock_types)
    nf = (workspace / AgentPaths.NXF_WORKFLOW).read_text()

    blocks: dict[str, str] = {}
    current = None
    for line in nf.splitlines():
        if line.startswith("process "):
            current = line.split()[1]
            blocks[current] = ""
        elif current is not None:
            blocks[current] += line + "\n"

    labelled = {n for n, b in blocks.items() if "label 'xgpux'" in b}
    assert any(n.endswith("__gpu_required") for n in labelled)
    assert any(n.endswith("__gpu_optional") for n in labelled)
    assert not any(n.endswith("__cpu_only") for n in labelled), labelled


def test_gpu_manifest_survives_a_line_reader(virtual_runtime, tmp_path, mock_samples, mock_types):
    _, workspace, _ = _stage_mixed(tmp_path, mock_samples, mock_types)
    raw = (workspace / AgentPaths.GPU_MANIFEST).read_text()
    assert raw.endswith("\n")
    lines = [l for l in raw.split("\n") if l]
    assert len(lines) == 1, "manifest must be one line so a line reader cannot split it"
    assert json.loads(lines[0])["steps"]


def test_gpu_manifest_records_each_declaring_step(virtual_runtime, tmp_path, mock_samples, mock_types):
    _, workspace, _ = _stage_mixed(tmp_path, mock_samples, mock_types)
    manifest = json.loads((workspace / AgentPaths.GPU_MANIFEST).read_text())
    steps = manifest["steps"]

    by_transform = {v["transform"]: v for v in steps.values()}
    assert set(by_transform) == {"gpu_required", "gpu_optional"}, by_transform
    assert by_transform["gpu_required"]["gpus"] == "required"
    assert by_transform["gpu_required"]["gpu_memory_gb"] == 40.0
    assert by_transform["gpu_optional"]["gpus"] == "optional"
    assert by_transform["gpu_optional"]["gpu_memory_gb"] == 8.0
    for process, v in steps.items():
        assert v["process"] == process
        assert process.endswith(f"__{v['transform']}")


def test_resources_file_carries_no_gpu_directive(virtual_runtime, tmp_path, mock_samples, mock_types):
    _, workspace, _ = _stage_mixed(tmp_path, mock_samples, mock_types)
    res = (workspace / AgentPaths.NXF_RES).read_text()
    for forbidden in ["clusterOptions", "accelerator", "--gpus", "gres", "beforeScript"]:
        assert forbidden not in res, f"unexpected [{forbidden}] in {AgentPaths.NXF_RES}:\n{res}"


def test_step_meta_carries_the_declaration_for_the_protocol(virtual_runtime, tmp_path, mock_samples, mock_types):
    _, workspace, staged = _stage_mixed(tmp_path, mock_samples, mock_types)
    found = {}
    for step in staged.plan.steps:
        meta = workspace / f"workflow.step_{step.order}.meta"
        line = [l for l in meta.read_text().splitlines() if l.startswith("gpu ")]
        if line:
            found[str(step.transform.name)] = json.loads(line[0][4:])
    assert set(found) == {"gpu_required", "gpu_optional"}
    assert found["gpu_required"] == {"gpus": "required", "gpu_memory_gb": 40.0}
    cpu_steps = [s for s in staged.plan.steps if str(s.transform.name) == "cpu_only"]
    assert cpu_steps
    for s in cpu_steps:
        assert "gpu " not in (workspace / f"workflow.step_{s.order}.meta").read_text()
