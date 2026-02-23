"""Centralized mock transform code strings for testing.

Each function returns dict[str, str] mapping transform name to Python code string.
These transforms use Path.write_text() for output (no container needed).
"""


def alignment_transform() -> dict[str, str]:
    """Alignment: reads + assembly -> bam.

    Assembly has reads as parent in lineage (reads -> assembly).
    """
    return {
        "alignment": '''
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
    }


def binner_transforms() -> dict[str, str]:
    """Binners: assembly + bam -> {metabat2,maxbin2,concoct}_bins."""
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


def identity_transform(input_type: str, output_type: str) -> dict[str, str]:
    """Simple copy transform for pipeline testing.

    Args:
        input_type: Namespaced input type (e.g. "mock::assembly")
        output_type: Namespaced output type (e.g. "mock::bam")
    """
    name = f"identity_{output_type.split('::')[-1]}"
    return {
        name: f'''
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
dep = model.AddRequirement(lib.GetType("{input_type}"))
out = model.AddProduct(lib.GetType("{output_type}"))

def protocol(context: ExecutionContext):
    inp = context.Input(dep)
    out_path = Path("output" + Path(str(inp.local)).suffix)
    out_path.write_text(inp.local.read_text() if inp.local.exists() else "identity output")
    return ExecutionResult(manifest=[{{out: out_path}}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
)
'''
    }


def batched_transform(batch_size: int = 3) -> dict[str, str]:
    """Transform exercising context.AsBatch().

    Args:
        batch_size: Number of items per batch.
    """
    return {
        "batched": f'''
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
        out_path.local.write_text("batch output")
        results.append(ExecutionResult(manifest=[{{out: out_path.local}}], success=True))
    return results

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
    batch_size={batch_size},
)
'''
    }


def branching_transforms() -> dict[str, str]:
    """Two producers + merger: assembly -> branch_a, assembly -> branch_b, (branch_a + branch_b) -> merged."""
    return {
        "produce_a": '''
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
out = model.AddProduct(lib.GetType("mock::branch_a"))

def protocol(context: ExecutionContext):
    out_path = Path("branch_a.txt")
    out_path.write_text("branch a content")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
''',
        "produce_b": '''
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
out = model.AddProduct(lib.GetType("mock::branch_b"))

def protocol(context: ExecutionContext):
    out_path = Path("branch_b.txt")
    out_path.write_text("branch b content")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
''',
        "merge": '''
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

TransformInstance(protocol=protocol, model=model, group_by=dep_a)
''',
    }


def shared_input_transform() -> dict[str, str]:
    """Shared-input transform: container + assembly -> annotated.

    Container is declared as parent of assembly, so the Orchestrator
    treats it as a shared/broadcast input (via group()'s parent branch
    -> .combine()). This replicates the proteinbert topology where a
    single container instance is broadcast to all per-sample assemblies.
    """
    return {
        "annotate_with_container": '''
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
container = model.AddRequirement(lib.GetType("mock::container"))
asm = model.AddRequirement(lib.GetType("mock::assembly"), parents={container})
out = model.AddProduct(lib.GetType("mock::annotated"))

def protocol(context: ExecutionContext):
    out_path = Path("annotated.txt")
    out_path.write_text("annotated content")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
)
'''
    }


def failing_transform() -> dict[str, str]:
    """Transform that raises an exception for error-path testing."""
    return {
        "failing": '''
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
    raise RuntimeError("intentional test failure")

TransformInstance(protocol=protocol, model=model, group_by=dep)
'''
    }
