from __future__ import annotations
from pathlib import Path

import pytest

from metasmith.models.libraries import (
    DataInstance,
    DataInstanceLibrary,
    DataTypeLibrary,
    Resources,
    Size,
    TransformInstance,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Dependency, Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowStep


def _mock_protocol(_ctx):  # pragma: no cover — never executed by these tests
    return None


def _make_lib(tmp_path: Path, *, name="samples.xgdb") -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / name)
    types = DataTypeLibrary()
    types["read_a"] = Endpoint(properties={"read", "side_a"})
    types["read_b"] = Endpoint(properties={"read", "side_b"})
    types["asm"] = Endpoint(properties={"asm"})
    types["annot"] = Endpoint(properties={"annot"})
    lib.AddTypeLibrary(namespace="mock", lib=types)
    return lib


def _build_step(tmp_path: Path) -> WorkflowStep:
    lib = _make_lib(tmp_path)

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
    step._test_deps = (dep_in_a, dep_in_b, dep_out)
    step._test_insts = (inst_a, inst_b, inst_out)
    step._test_lib = lib
    return step


def test_data_instance_hash_deterministic(tmp_path):
    lib = _make_lib(tmp_path, name="hash_det.xgdb")
    (lib.location / "x.fa").write_text("x")
    lib.AddItem(Path("x.fa"), "mock::asm")
    a = lib.Get(Path("x.fa"))
    b = lib.Get(Path("x.fa"))
    assert a.instance_id == b.instance_id
    assert a._hash == b._hash
    assert hash(a) == hash(b)


def test_data_instance_hash_differs_by_path(tmp_path):
    lib = _make_lib(tmp_path, name="hash_path.xgdb")
    (lib.location / "x.fa").write_text("x")
    (lib.location / "y.fa").write_text("y")
    lib.AddItem(Path("x.fa"), "mock::asm")
    lib.AddItem(Path("y.fa"), "mock::asm")
    a = lib.Get(Path("x.fa"))
    b = lib.Get(Path("y.fa"))
    assert a.instance_id != b.instance_id
    assert a._hash != b._hash


def test_data_instance_hash_differs_by_dtype(tmp_path):
    lib = _make_lib(tmp_path, name="hash_dtype.xgdb")
    (lib.location / "x.fa").write_text("x")
    lib.AddItem(Path("x.fa"), "mock::asm")
    a = lib.Get(Path("x.fa"))
    annot_ep = Endpoint(properties={"annot"}, parents={a.dtype})
    b = a.WithDType(annot_ep)
    assert a.instance_id == b.instance_id
    assert a.legacy_key != b.legacy_key
    assert a.dtype.key != b.dtype.key


def test_uses_matches_dependency_map_requires(tmp_path):
    step = _build_step(tmp_path)
    dep_in_a, dep_in_b, _ = step._test_deps
    inst_a, inst_b, _ = step._test_insts
    expected = (
        step.dependency_map.get(dep_in_a, [])
        + step.dependency_map.get(dep_in_b, [])
    )
    assert step.uses == expected
    assert step.uses == [inst_a, inst_b]


def test_produces_matches_dependency_map_produces(tmp_path):
    step = _build_step(tmp_path)
    _, _, dep_out = step._test_deps
    _, _, inst_out = step._test_insts
    expected = [
        [
            inst
            for dep in dep_group
            for inst in step.dependency_map.get(dep, [])
        ]
        for dep_group in step.transform.model.produces
    ]
    assert step.produces == expected
    assert step.produces == [[inst_out]]


def test_dependency_map_setter_auto_refreshes_views(tmp_path):
    step = _build_step(tmp_path)
    dep_in_a, dep_in_b, dep_out = step._test_deps
    inst_a, inst_b, inst_out = step._test_insts

    assert step.uses == [inst_a, inst_b]
    assert step.produces == [[inst_out]]

    step.dependency_map = {dep_in_b: [inst_b], dep_out: [inst_out]}
    assert step.uses == [inst_b]
    assert step.produces == [[inst_out]]

    step.dependency_map = {dep_in_a: [inst_a], dep_in_b: [inst_b], dep_out: []}
    assert step.uses == [inst_a, inst_b]
    assert step.produces == [[]]

    assert "_dependency_map" in step.__dict__
    assert "dependency_map" not in step.__dict__


def test_workflow_step_pack_unpack_roundtrip(tmp_path):
    step = _build_step(tmp_path)
    packed = step.Pack()
    assert packed["order"] == 1
    assert packed["schema"] == "v2"
    assert set(packed["instances"].keys()) == {
        inst.instance_id for inst in step._test_insts
    }
    for dep_key, ids in packed["dependency_map"].items():
        for iid in ids:
            assert iid in packed["instances"], (
                f"dependency_map id {iid} ({dep_key}) absent from instances"
            )
    assert "::" in packed["transform"]


def test_group_by_single_dtype(tmp_path):
    step = _build_step(tmp_path)
    dep_in_a, _, _ = step._test_deps
    inst_a, _, _ = step._test_insts
    assert step.transform.group_by is dep_in_a
    assert step.group_by_instances == [inst_a]
    dtypes = {x.dtype.key for x in step.group_by_instances}
    assert len(dtypes) == 1


def test_input_map_reconstruction_round_trips_through_dep_key(tmp_path):
    step = _build_step(tmp_path)
    inst_a, inst_b, _ = step._test_insts

    din = {
        d.key: [inst.instance_id for inst in step.dependency_map.get(d, [])]
        for d in step.transform.model.requires
    }
    inst_lookup = {}
    for insts in step.dependency_map.values():
        for inst in insts:
            for key in {inst.instance_id, inst._key, inst.legacy_key}:
                inst_lookup[key] = inst

    reconstructed: dict[Dependency, list[DataInstance]] = {}
    for dep in step.transform.model.requires:
        reconstructed[dep] = [
            inst_lookup[k] for k in din.get(dep.key, []) if k in inst_lookup
        ]

    reconstructed_flat = [
        inst
        for dep in step.transform.model.requires
        for inst in reconstructed[dep]
    ]
    assert reconstructed_flat == [inst_a, inst_b]
    assert reconstructed_flat == step.uses


def test_output_map_reconstruction_round_trips_through_dep_key(tmp_path):
    step = _build_step(tmp_path)
    _, _, inst_out = step._test_insts
    _, _, dep_out = step._test_deps

    dot = [
        {
            d.key: [inst.instance_id for inst in step.dependency_map.get(d, [])]
            for d in dep_group
        }
        for dep_group in step.transform.model.produces
    ]

    inst_lookup = {}
    for insts in step.dependency_map.values():
        for inst in insts:
            for key in {inst.instance_id, inst._key, inst.legacy_key}:
                inst_lookup[key] = inst

    reconstructed_groups: list[list[DataInstance]] = []
    for i, dep_group in enumerate(step.transform.model.produces):
        raw_group = dot[i] if i < len(dot) else {}
        group_insts: list[DataInstance] = []
        for dep in dep_group:
            group_insts.extend(
                inst_lookup[k] for k in raw_group.get(dep.key, []) if k in inst_lookup
            )
        reconstructed_groups.append(group_insts)

    assert reconstructed_groups == step.produces
    assert reconstructed_groups == [[inst_out]]


def test_archetype_closure_determinism():
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

    x, y, z = "x", "y", "z"
    assert get_archetype([x, y]) == x
    assert get_archetype([y]) == x
    assert get_archetype([x]) == x
    assert get_archetype([z, y]) == x, "merge must preserve existing archetype"
    assert get_archetype([z]) == x

    _archetypes.clear()
    assert get_archetype([y, x]) == y
    assert get_archetype([x]) == y
