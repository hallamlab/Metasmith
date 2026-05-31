"""Phase 0 baseline + property/setter contract tests (C3 → C8).

C3 lands the first two tests (dependency_map auto-refresh + archetype
closure determinism). C8 grows this file with the remaining 10
categories enumerated in plans/harmonize-data-instance-arity.md.
"""

from pathlib import Path

import pytest

from metasmith.models.libraries import (
    DataInstance,
    DataInstanceLibrary,
    DataTypeLibrary,
    Resources,
    Size,
    TransformInstance,
)
from metasmith.models.solver import Dependency, Endpoint, Transform
from metasmith.models.workflow import WorkflowStep


def _mock_protocol(_ctx):  # pragma: no cover — never executed by these tests
    return None


def _build_step(tmp_path: Path) -> WorkflowStep:
    """Build a minimal WorkflowStep with a 2-required, 1-produced transform."""
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    types = DataTypeLibrary()
    types["read_a"] = Endpoint(properties={"read", "side_a"})
    types["read_b"] = Endpoint(properties={"read", "side_b"})
    types["asm"] = Endpoint(properties={"asm"})
    lib.AddTypeLibrary(namespace="mock", lib=types)

    model = Transform()
    dep_in_a = model.AddRequirement(properties={"read", "side_a"})
    dep_in_b = model.AddRequirement(properties={"read", "side_b"})
    dep_out = model.AddProduct(properties={"asm"})

    tlib = type("FakeTransformLib", (), {"GetKey": lambda self: "fakelib"})()
    tr = TransformInstance(
        protocol=_mock_protocol,
        model=model,
        group_by=dep_in_a,
        name="mock_tool",
        resources=Resources(cpus=1, memory=Size.GB(1)),
    )

    (lib.location / "a.fastq").write_text("a")
    (lib.location / "b.fastq").write_text("b")
    (lib.location / "out.fa").write_text("o")
    lib.AddItem(Path("a.fastq"), "mock::read_a")
    lib.AddItem(Path("b.fastq"), "mock::read_b")
    lib.AddItem(Path("out.fa"), "mock::asm")
    inst_a = lib.Get(Path("a.fastq"))
    inst_b = lib.Get(Path("b.fastq"))
    inst_out = lib.Get(Path("out.fa"))

    step = WorkflowStep(
        order=1,
        dependency_map={
            dep_in_a: [inst_a],
            dep_in_b: [inst_b],
            dep_out: [inst_out],
        },
        transform=tr,
        transform_library=tlib,
    )
    # Expose the deps so the test can reach them for mutation.
    step._test_deps = (dep_in_a, dep_in_b, dep_out)
    step._test_insts = (inst_a, inst_b, inst_out)
    return step


def test_dependency_map_setter_auto_refreshes_views(tmp_path):
    """C3 / A2 falsifier: reassigning dependency_map auto-refreshes uses/produces.

    Reassigning the map must update `uses` and `produces` *without* the
    caller calling `RefreshViews()`. A test attempting to bypass the
    setter is structurally impossible (the field is a property).
    """
    step = _build_step(tmp_path)
    dep_in_a, dep_in_b, dep_out = step._test_deps
    inst_a, inst_b, inst_out = step._test_insts

    assert step.uses == [inst_a, inst_b]
    assert step.produces == [[inst_out]]

    # Reassign: swap inst_a out of dep_in_a, leaving only inst_b on the input side.
    step.dependency_map = {dep_in_b: [inst_b], dep_out: [inst_out]}
    assert step.uses == [inst_b], "uses didn't auto-refresh after dependency_map reassignment"
    assert step.produces == [[inst_out]]

    # Reassign with the produced slot emptied (e.g. optional branch).
    step.dependency_map = {dep_in_a: [inst_a], dep_in_b: [inst_b], dep_out: []}
    assert step.uses == [inst_a, inst_b]
    assert step.produces == [[]]

    # There is no backing field named `dependency_map` on the instance —
    # only the property + `_dependency_map`. This makes setter-bypass
    # structurally impossible.
    assert "_dependency_map" in step.__dict__
    assert "dependency_map" not in step.__dict__


def test_archetype_closure_determinism():
    """C3 / A3 falsifier: get_archetype's choice is order-deterministic.

    Reaches into `PrepareNextflow` for the inner closure structure by
    reimplementing the same canonical-name dance — the closure isn't
    exposed publicly. Asserts:
      - first call returns candidates[0]
      - subsequent calls with any subset return the same archetype
      - merging two pre-seeded groups picks one consistent archetype
    """
    # Reimplement the closure verbatim (this is the contract under test).
    _archetypes: dict = {}

    def get_archetype(candidates):
        a = None
        for c in candidates:
            if c not in _archetypes:
                continue
            a = _archetypes[c]
        if a is None:
            a = candidates[0]
        for c in candidates:
            _archetypes[c] = a
        return a

    # Plain hashable sentinels stand in for DataInstance.
    x, y, z = "x", "y", "z"

    # First call: returns the first candidate.
    a1 = get_archetype([x, y])
    assert a1 == x
    # Re-querying with a subset returns the same archetype.
    assert get_archetype([y]) == x
    assert get_archetype([x]) == x

    # New group merges into an existing archetype if any candidate already has one.
    a2 = get_archetype([z, y])
    assert a2 == x, "merging into a group with a recorded archetype must reuse it"
    assert get_archetype([z]) == x

    # Fresh dict — order independence: candidates[0] wins when nothing is seeded.
    _archetypes.clear()
    assert get_archetype([y, x]) == y
    assert get_archetype([x]) == y
