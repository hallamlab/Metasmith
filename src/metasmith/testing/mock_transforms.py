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


_BRANCH_NAMES = ["a", "b", "c", "d", "e", "f", "g", "h"]


def branching_transforms(n: int = 2) -> dict[str, str]:
    """N producers + merger: assembly -> branch_<letter> for each of N letters,
    (branch_a + branch_b + ... branch_<n-1>) -> merged.

    Default N=2 preserves the original two-branch contract. N up to 8 is
    supported by the static `_BRANCH_NAMES` table; callers requesting more
    branches get a ValueError so the conftest type fixture stays in sync.
    """
    if not (2 <= n <= len(_BRANCH_NAMES)):
        raise ValueError(
            f"branching_transforms: n must be in [2, {len(_BRANCH_NAMES)}], got {n}"
        )

    names = _BRANCH_NAMES[:n]
    transforms: dict[str, str] = {}

    for letter in names:
        transforms[f"produce_{letter}"] = f'''
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
out = model.AddProduct(lib.GetType("mock::branch_{letter}"))

def protocol(context: ExecutionContext):
    out_path = Path("branch_{letter}.txt")
    out_path.write_text("branch {letter} content")
    return ExecutionResult(manifest=[{{out: out_path}}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
'''

    dep_decls = "\n".join(
        f'dep_{letter} = model.AddRequirement(lib.GetType("mock::branch_{letter}"))'
        for letter in names
    )
    primary = names[0]
    transforms["merge"] = f'''
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
{dep_decls}
out = model.AddProduct(lib.GetType("mock::merged"))

def protocol(context: ExecutionContext):
    out_path = Path("merged.txt")
    out_path.write_text("merged content")
    return ExecutionResult(manifest=[{{out: out_path}}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep_{primary})
'''
    return transforms


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


def multi_slot_producer(slots: int = 2) -> dict[str, str]:
    """Single-input transform with N declared output products (distinct dtypes).

    Each product lives in its own branch via `NewProductGroup` (matches the
    `branching_transforms` shape), so the planner emits N distinct slots and
    the orchestrator routes each downstream consumer independently. Drives
    F1-F4 (fan-out catalog).
    """
    assert slots >= 1, "multi_slot_producer needs at least one slot"
    slot_lines: list[str] = []
    write_lines: list[str] = []
    manifest_entries: list[str] = []
    for i in range(slots):
        if i > 0:
            slot_lines.append(f"model.NewProductGroup()")
        slot_lines.append(
            f'out_{i} = model.AddProduct(lib.GetType("mock::slot_{i}"))'
        )
        write_lines.append(
            f'p_{i} = Path("slot_{i}.txt"); p_{i}.write_text("slot {i} content")'
        )
        manifest_entries.append(f"{{out_{i}: p_{i}}}")
    slots_src = "\n".join(slot_lines)
    writes_src = "\n    ".join(write_lines)
    manifest_src = ", ".join(manifest_entries)
    return {
        "multi_slot_producer": f'''
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
{slots_src}

def protocol(context: ExecutionContext):
    {writes_src}
    return ExecutionResult(manifest=[{manifest_src}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
'''
    }


def group_then_unfold() -> dict[str, str]:
    """Group-then-unfold pair: T1 groups by root, T2 unfolds via `AsBatch`.

    The first transform consumes a per-sample input keyed by a shared root
    parent (`group_by=root`) and writes one aggregate output. The second
    transform iterates `context.AsBatch()` to re-emit per-batch products
    matching the upstream batch shape. Drives GS1-GS3.
    """
    return {
        "group_aggregate": '''
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
root = model.AddRequirement(lib.GetType("mock::sample_metadata"))
dep = model.AddRequirement(lib.GetType("mock::assembly"), parents={root})
out = model.AddProduct(lib.GetType("mock::grouped"))

def protocol(context: ExecutionContext):
    out_path = Path("grouped.txt")
    out_path.write_text("grouped aggregate")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=root)
''',
        "unfold_batch": '''
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
dep = model.AddRequirement(lib.GetType("mock::grouped"))
out = model.AddProduct(lib.GetType("mock::unfolded"))

def protocol(context: ExecutionContext):
    results = []
    for batch_ctx in context.AsBatch():
        out_path = batch_ctx.Output(out)
        out_path.local.write_text("unfolded sample")
        results.append(ExecutionResult(manifest=[{out: out_path.local}], success=True))
    return results

TransformInstance(protocol=protocol, model=model, group_by=dep, batch_size=1)
''',
    }


def failing_at_slot_k(k: int = 1, slots: int = 2) -> dict[str, str]:
    """Multi-slot producer where slot `k` reports failure; other slots succeed.

    Mirrors `multi_slot_producer` but the protocol returns
    `ExecutionResult(success=False)` for slot `k` while the remaining slots
    return success. Drives T3 (failure telemetry) and B3 (sibling-branch
    independence). `k` is 0-indexed; defaults to slot 1.
    """
    assert slots >= 1, "failing_at_slot_k needs at least one slot"
    assert 0 <= k < slots, f"k={k} out of range for slots={slots}"
    slot_lines: list[str] = []
    for i in range(slots):
        if i > 0:
            slot_lines.append("model.NewProductGroup()")
        slot_lines.append(
            f'out_{i} = model.AddProduct(lib.GetType("mock::slot_{i}"))'
        )
    slots_src = "\n".join(slot_lines)
    return {
        "failing_at_slot_k": f'''
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
{slots_src}

def protocol(context: ExecutionContext):
    results = []
    for i in range({slots}):
        if i == {k}:
            raise RuntimeError(f"intentional failure at slot {{i}}")
        p = Path(f"slot_{{i}}.txt")
        p.write_text(f"slot {{i}} content")
        slot_dep = locals()[f"out_{{i}}"]
        results.append(ExecutionResult(manifest=[{{slot_dep: p}}], success=True))
    return results

TransformInstance(protocol=protocol, model=model, group_by=dep)
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
