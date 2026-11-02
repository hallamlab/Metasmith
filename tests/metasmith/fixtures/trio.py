"""Building and solving `annotation_trio_from_assembly`.

The template a user reported a frozen second run on. Shared because several
axes need the same plan: the cache lane compares its keys across sample counts,
the flow lane compares its steps with and without the databases given, and the
audit lane runs it twice to check it reuses the cache.
"""

from __future__ import annotations

from pathlib import Path
from metasmith.testing.pool_fixtures import pool_backed


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
    pool_backed(lib)
    lib.Save()
    return lib


def build_named_inputs(lib_root: Path, at: Path, names: list[str]):
    """An input library of the named assemblies, in that order.

    A file that already exists at its path is left alone, so a second library
    built at the same location keeps the leaf ids of the samples it shares
    with the first: a leaf's id is its path and mtime.
    """
    from metasmith.python_api import DataInstanceLibrary

    lib = DataInstanceLibrary(at / "inputs.xgdb")
    lib.AddTypeLibrary(lib_root / "data_types" / "sequences.yml")
    lib.location.mkdir(parents=True, exist_ok=True)
    for name in names:
        d = lib.location / name
        d.mkdir(parents=True, exist_ok=True)
        f = d / "asm.fna"
        if not f.exists():
            f.write_text(f">{name}\nACGTACGTACGT\n", encoding="utf-8")
        lib.AddItem(Path(f"{name}/asm.fna"), "sequences::assembly")
    pool_backed(lib)
    lib.Save()
    return lib


def solve_trio(lib_root: Path, inputs, targets: list[str] | None = None,
               shared: list[str] | None = None,
               transform_roots: dict[str, Path] | None = None):
    """`transform_roots` swaps a namespace's library for another location."""
    from metasmith.python_api import Spec

    roots = {n: lib_root / "transforms" / n for n in TRANSFORM_NAMESPACES}
    roots.update(transform_roots or {})

    spec = Spec(
        input_library=inputs,
        sample_type="sequences::assembly",
        target_types=list(targets if targets is not None else TARGETS),
        shared_input_paths=list(shared or []),
        transform_libraries=[roots[n] for n in TRANSFORM_NAMESPACES],
        resource_libraries=[lib_root / "resources" / "env"],
    )
    task = spec.Solve()
    assert task.ok, f"the trio did not solve: dropped={sorted(task.plan.dropped_targets)}"
    return task
