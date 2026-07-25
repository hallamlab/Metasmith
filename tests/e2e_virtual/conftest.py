"""Fixtures for virtual (no Docker/Nextflow) E2E tests."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from metasmith.constants import AgentPaths, MODULE_PATH
from metasmith.env import Runtime
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.remote import Source
from metasmith.models.solver import Endpoint
from metasmith.models.workflow import NextflowGenContext, WorkflowTask
from metasmith.testing.virtual_runtime import VirtualE2ERuntime



def configure_agent_paths(monkeypatch, home: Path) -> None:
    monkeypatch.setattr(AgentPaths, "HOME_ROOT", home)
    monkeypatch.setattr(AgentPaths, "WORK_ROOT", home / "_ws")


@pytest.fixture
def virtual_runtime(tmp_path, monkeypatch):
    runtime = VirtualE2ERuntime(tmp_path / "virtual_rt", host="virt-host", force_bounce=False)
    runtime.setup(monkeypatch)
    configure_agent_paths(monkeypatch, runtime.home)
    return runtime


@pytest.fixture
def virtual_runtime_bounce(tmp_path, monkeypatch):
    runtime = VirtualE2ERuntime(tmp_path / "virtual_rt_bounce", host="virt-host", force_bounce=True)
    runtime.setup(monkeypatch)
    configure_agent_paths(monkeypatch, runtime.home)
    return runtime


@pytest.fixture
def mock_types(tmp_path) -> Path:
    types = DataTypeLibrary()

    types["sample_metadata"] = Endpoint(properties={"sample_metadata"})
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})

    types["bam"] = Endpoint(properties={"bam"})
    types["metabat2_bins"] = Endpoint(properties={"bins", "method:metabat2"})
    types["maxbin2_bins"] = Endpoint(properties={"bins", "method:maxbin2"})
    types["concoct_bins"] = Endpoint(properties={"bins", "method:concoct"})

    path = tmp_path / "types.yml"
    types.Save(path)
    return path


@pytest.fixture
def mock_samples(tmp_path, mock_types) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(mock_types, namespace="mock")

    for i in range(3):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / "metadata.json").write_text(f'{{"id": "{sid}"}}', encoding="utf-8")
        (sdir / "reads.fq").write_text(f">read_{i}\nACGT\n", encoding="utf-8")
        (sdir / "assembly.fa").write_text(f">contig_{i}\nACGTACGT\n", encoding="utf-8")

        m = lib.AddItem(Path(f"{sid}/metadata.json"), "mock::sample_metadata")
        r = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads", parents=[m])
        lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[r])

    lib.Save()
    return lib



def create_transform_library(
    base_dir: Path,
    mock_types: Path,
    transforms: dict[str, str],
) -> TransformInstanceLibrary:
    tr_path = base_dir / "transforms.xgdb"
    tr_path.mkdir(parents=True, exist_ok=True)

    meta = tr_path / "_metadata"
    types_dir = meta / "types"
    types_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy(mock_types, types_dir / "mock.yml")
    (types_dir / "transforms.yml").write_text(
        """schema: v1
ontology:
  name: EDAM
  version: '1.25'
  doi: https://doi.org/10.1093/bioinformatics/btt113
  strict: false
types:
  transform:
    properties:
    - metasmith
    - transform
""",
        encoding="utf-8",
    )

    manifest = {}
    for name, code in transforms.items():
        fp = tr_path / f"{name}.py"
        fp.write_text(code, encoding="utf-8")
        manifest[f"{name}.py"] = {"type": "transforms::transform"}

    (meta / "index.yml").write_text(yaml.dump({"manifest": manifest, "schema": "v1"}), encoding="utf-8")
    return TransformInstanceLibrary.Load(tr_path)



def stage_task(task: WorkflowTask) -> tuple[str, Path, WorkflowTask]:
    """Persist task to agent layout and compile Nextflow artifacts."""
    key = task.GetKey()
    task_path = AgentPaths.to_task(key)
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task.SaveAs(Source.FromLocal(task_path))

    staged = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    workspace = task_path.parent.parent
    workspace.mkdir(parents=True, exist_ok=True)

    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=workspace,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        container_runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )

    staged.PrepareNextflow(context)
    lib_dir = workspace / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(MODULE_PATH / "nextflow_config/Orchestrator.groovy", lib_dir / "Orchestrator.groovy")
    return key, workspace, staged
