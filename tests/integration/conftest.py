"""Shared fixtures for E2E workflow tests."""

import pytest
import subprocess
import shutil
import yaml
from pathlib import Path

from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
    TransformInstance,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.coms.containers import ContainerRuntime


@pytest.fixture(scope="session")
def container_runtime():
    """Detect Docker or Apptainer, return None for dry-run mode."""
    for cmd, runtime in [
        (["docker", "info"], ContainerRuntime.DOCKER),
        (["apptainer", "--version"], ContainerRuntime.APPTAINER),
    ]:
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            if result.returncode == 0:
                return runtime
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
    return None


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for tests."""
    yield tmp_path
    # tmp_path is automatically cleaned up by pytest


@pytest.fixture
def mock_types(temp_dir) -> Path:
    """Create DataTypeLibrary with all required types."""
    types = DataTypeLibrary()

    # Input types
    types["sample_metadata"] = Endpoint(properties={"sample_metadata"})
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})

    # Intermediate types
    types["bam"] = Endpoint(properties={"bam"})
    types["scattered"] = Endpoint(properties={"scattered"})
    types["gathered"] = Endpoint(properties={"gathered"})

    # Output types for binning
    types["metabat2_bins"] = Endpoint(properties={"bins", "method:metabat2"})
    types["maxbin2_bins"] = Endpoint(properties={"bins", "method:maxbin2"})
    types["concoct_bins"] = Endpoint(properties={"bins", "method:concoct"})

    # Branching outputs
    types["branch_a"] = Endpoint(properties={"branch_a"})
    types["branch_b"] = Endpoint(properties={"branch_b"})
    types["merged"] = Endpoint(properties={"merged"})

    types_path = temp_dir / "mock_types.yml"
    types.Save(types_path)
    return types_path


@pytest.fixture
def mock_samples(temp_dir, mock_types) -> DataInstanceLibrary:
    """Create 3 samples with lineage: metadata -> reads -> assembly."""
    lib_path = temp_dir / "samples.xgdb"
    lib = DataInstanceLibrary(lib_path)
    lib.AddTypeLibrary(mock_types, namespace="mock")

    for i in range(3):
        sample_id = f"sample_{i:02d}"
        sample_dir = lib.location / sample_id
        sample_dir.mkdir(parents=True, exist_ok=True)

        # Create mock files
        (sample_dir / "metadata.json").write_text(f'{{"id": "{sample_id}"}}')
        (sample_dir / "reads.fq").write_text(f">read_{i}\nACGT\n")
        (sample_dir / "assembly.fa").write_text(f">contig_{i}\nACGTACGT\n")

        # Add with lineage
        meta = lib.AddItem(
            Path(f"{sample_id}/metadata.json"), "mock::sample_metadata"
        )
        reads = lib.AddItem(
            Path(f"{sample_id}/reads.fq"), "mock::reads", parents=[meta]
        )
        lib.AddItem(
            Path(f"{sample_id}/assembly.fa"), "mock::assembly", parents=[reads]
        )

    lib.Save()
    return lib


def create_transform_library(
    temp_dir: Path, mock_types: Path, transforms: dict[str, str]
) -> TransformInstanceLibrary:
    """Create a TransformInstanceLibrary with given transform code.

    Args:
        temp_dir: Directory to create library in
        mock_types: Path to mock types YAML file
        transforms: Dict mapping transform names to Python code

    Returns:
        TransformInstanceLibrary with loaded transforms
    """
    tr_path = temp_dir / "transforms.xgdb"
    tr_path.mkdir(parents=True, exist_ok=True)

    # Create _metadata structure
    meta_dir = tr_path / "_metadata"
    types_dir = meta_dir / "types"
    types_dir.mkdir(parents=True)

    # Copy mock types
    shutil.copy(mock_types, types_dir / "mock.yml")

    # Create transforms.yml
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
"""
    )

    # Create transform files
    manifest = {}
    for name, code in transforms.items():
        transform_file = tr_path / f"{name}.py"
        transform_file.write_text(code)
        manifest[f"{name}.py"] = {"type": "transforms::transform"}

    # Create index.yml
    (meta_dir / "index.yml").write_text(
        yaml.dump(
            {
                "manifest": manifest,
                "schema": "v1",
            }
        )
    )

    return TransformInstanceLibrary.Load(tr_path)


@pytest.fixture
def alignment_transform_code() -> str:
    """Transform code for alignment: reads + assembly -> bam.

    Note: Assembly has reads as parent in lineage (reads -> assembly).
    """
    return '''
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
reads = model.AddRequirement(lib.GetType("mock::reads"))
asm = model.AddRequirement(lib.GetType("mock::assembly"), parents={reads})
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    out_path = Path("aligned.bam")
    out_path.write_text("mock bam content")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
)
'''


@pytest.fixture
def binner_transform_code() -> dict[str, str]:
    """Transform code for binners: assembly + bam -> bins."""
    binners = {}
    for method in ["metabat2", "maxbin2", "concoct"]:
        binners[method] = f'''
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
out = model.AddProduct(lib.GetType("mock::{method}_bins"))

def protocol(context: ExecutionContext):
    out_path = Path("bins.fa")
    out_path.write_text("mock {method} bins")
    return ExecutionResult(manifest=[{{out: out_path}}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
)
'''
    return binners


@pytest.fixture
def batched_transform_code() -> str:
    """Transform code with batch_size > 1."""
    return '''
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
dep = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    results = []
    for batch_ctx in context.AsBatch():
        out_path = batch_ctx.Output(out)
        out_path.local.write_text(f"batch output")
        results.append(ExecutionResult(manifest=[{out: out_path.local}], success=True))
    return results

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
    batch_size=3,
)
'''


@pytest.fixture
def branching_transform_code() -> str:
    """Transform code that produces multiple outputs (branching)."""
    return '''
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
dep = model.AddRequirement(lib.GetType("mock::assembly"))
out_a = model.AddProduct(lib.GetType("mock::branch_a"))
model.NewProductGroup()
out_b = model.AddProduct(lib.GetType("mock::branch_b"))

def protocol(context: ExecutionContext):
    path_a = Path("branch_a.txt")
    path_b = Path("branch_b.txt")
    path_a.write_text("branch a content")
    path_b.write_text("branch b content")
    return ExecutionResult(
        manifest=[{out_a: path_a}, {out_b: path_b}],
        success=True
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
)
'''


@pytest.fixture
def merge_transform_code() -> str:
    """Transform code that merges branches."""
    return '''
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
dep_a = model.AddRequirement(lib.GetType("mock::branch_a"))
dep_b = model.AddRequirement(lib.GetType("mock::branch_b"))
out = model.AddProduct(lib.GetType("mock::merged"))

def protocol(context: ExecutionContext):
    out_path = Path("merged.txt")
    out_path.write_text("merged content")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep_a,
)
'''
