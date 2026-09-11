def alignment_transform() -> dict[str, str]:
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


def multi_product_one_group(products: int = 2) -> dict[str, str]:
    assert products >= 1, "multi_product_one_group needs at least one product"
    decls = "\n".join(
        f'out_{i} = model.AddProduct(lib.GetType("mock::slot_{i}"))'
        for i in range(products)
    )
    writes = "\n    ".join(
        f'p_{i} = Path("slot_{i}.txt"); p_{i}.write_text("slot {i} content")'
        for i in range(products)
    )
    entry = ", ".join(f"out_{i}: p_{i}" for i in range(products))
    return {
        "multi_product_one_group": f'''
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
{decls}

def protocol(context: ExecutionContext):
    {writes}
    return ExecutionResult(manifest=[{{{entry}}}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
'''
    }


def group_then_unfold() -> dict[str, str]:
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


def pull_container_transform(
    container_type: str = "mock::container",
    pulled_type: str = "mock::pulled",
) -> dict[str, str]:
    return {
        "pullContainer": f'''
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
image = model.AddRequirement(lib.GetType("{container_type}"))
out = model.AddProduct(lib.GetType("{pulled_type}"))

def protocol(context: ExecutionContext):
    out_path = Path("pulled.txt")
    out_path.write_text("pulled")
    return ExecutionResult(manifest=[{{out: out_path}}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
)
'''
    }


def failing_transform() -> dict[str, str]:
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


def labelled_collection() -> dict[str, str]:
    return {
        "label_item": '''
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
    inp = context.Input(dep)
    out_path = Path("item.txt")
    out_path.write_text(inp.local.read_text())
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
''',
        "collect_labelled": '''
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
root  = model.AddRequirement(lib.GetType("mock::sample_metadata"))
label = model.AddRequirement(lib.GetType("mock::label"), parents={root})
item  = model.AddRequirement(lib.GetType("mock::bam"), parents={label})
out   = model.AddProduct(lib.GetType("mock::merged"))

def protocol(context: ExecutionContext):
    lines = []
    for p in context.InputGroup(item):
        src = context.SourceOf(p, label)
        name = src.local.read_text().strip() if src is not None else "UNPAIRED"
        body = p.local.read_text().strip().splitlines()[0]
        lines.append(name + "\\t" + body)
    out_path = context.Output(out)
    out_path.local.write_text("\\n".join(sorted(lines)) + "\\n")
    return ExecutionResult(manifest=[{out: out_path.local}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=root)
''',
    }


# Writes back what the caller said the machine was, so a test can read the
# parameters a protocol actually received rather than the ones it was passed.
def params_transform() -> dict[str, str]:
    return {
        "params_echo": '''
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
    out_path.write_text(
        f"cpus={context.params.get(\'cpus\')} "
        f"memory={context.params.get(\'memory\')} "
        f"attempt={context.params.get(\'attempt\')}"
    )
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
)
'''
    }


# Asks which input its assembly descends from, which is what a collecting transform does
# to recover a sample label. It needs the slot channels and the per-item provenance that a
# compiled workflow gets from the orchestrator and a direct run has to synthesise.
def provenance_transform() -> dict[str, str]:
    return {
        "provenance_echo": '''
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
    src = context.SourceOf(context.Input(asm), reads)
    out_path = Path("aligned.bam")
    out_path.write_text("NONE" if src is None else src.local.name)
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
)
'''
    }
