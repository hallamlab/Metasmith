"""Building and solving `annotation_trio_from_assembly`.

The template a user reported a frozen second run on. Shared because several
axes need the same plan: the cache lane compares its keys across sample counts,
the flow lane compares its steps with and without the databases given, and the
audit lane runs it twice to check it reuses the cache.
"""

from __future__ import annotations

from pathlib import Path


TARGETS = [
    "annotation::kofamscan_results",
    "annotation::kofamscan_descriptions",
    "annotation::diamond_uniref50_results",
    "annotation::diamond_uniref50_descriptions",
    "annotation::interproscan_results",
    "annotation::interproscan_descriptions",
]

TRANSFORM_NAMESPACES = ("logistics", "metagenomics", "functionalAnnotation")


def build_inputs(lib_root: Path, at: Path, n: int = 1):
    """An input library of `n` assemblies, each its own sample."""
    from metasmith.python_api import DataInstanceLibrary

    lib = DataInstanceLibrary(at / "inputs.xgdb")
    lib.AddTypeLibrary(lib_root / "data_types" / "sequences.yml")
    lib.location.mkdir(parents=True, exist_ok=True)
    if n == 1:
        (lib.location / "asm.fna").write_text(">c1\nACGTACGTACGT\n", encoding="utf-8")
        lib.AddItem(Path("asm.fna"), "sequences::assembly")
    else:
        for i in range(n):
            d = lib.location / f"s{i:02}"
            d.mkdir(parents=True, exist_ok=True)
            (d / "asm.fna").write_text(f">c{i}\nACGTACGTACGT\n", encoding="utf-8")
            lib.AddItem(Path(f"s{i:02}/asm.fna"), "sequences::assembly")
    lib.Save()
    return lib


def solve_trio(lib_root: Path, inputs, targets: list[str] | None = None,
               shared: list[str] | None = None):
    from metasmith.python_api import Spec

    spec = Spec(
        input_library=inputs,
        sample_type="sequences::assembly",
        target_types=list(targets if targets is not None else TARGETS),
        shared_input_paths=list(shared or []),
        transform_libraries=[
            lib_root / "transforms" / n for n in TRANSFORM_NAMESPACES
        ],
        resource_libraries=[lib_root / "resources" / "env"],
    )
    task = spec.Solve()
    assert task.ok, f"the trio did not solve: dropped={sorted(task.plan.dropped_targets)}"
    return task
