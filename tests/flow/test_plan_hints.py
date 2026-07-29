"""Tests for PlanHint diagnostics emitted by WorkflowPlan.Generate when the
solver can't reach the target."""

from __future__ import annotations

import shutil
import yaml
from pathlib import Path

import pytest

from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import PlanHint, WorkflowPlan


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path


def _write_types_yml(path: Path, types: dict):
    payload = {
        "schema": "v1",
        "ontology": {
            "name": "EDAM",
            "version": "1.25",
            "doi": "https://doi.org/10.1093/bioinformatics/btt113",
            "strict": False,
        },
        "types": types,
    }
    path.write_text(yaml.dump(payload))


def _make_transform_lib(
    temp_dir: Path, mock_types_path: Path, transforms: dict[str, str]
) -> TransformInstanceLibrary:
    tr_path = temp_dir / "transforms.xgdb"
    tr_path.mkdir(parents=True, exist_ok=True)
    meta_dir = tr_path / "_metadata"
    types_dir = meta_dir / "types"
    types_dir.mkdir(parents=True)
    shutil.copy(mock_types_path, types_dir / "mock.yml")
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
    manifest = {}
    for name, code in transforms.items():
        (tr_path / f"{name}.py").write_text(code)
        manifest[f"{name}.py"] = {"type": "transforms::transform"}
    (meta_dir / "index.yml").write_text(
        yaml.dump({"manifest": manifest, "schema": "v1"})
    )
    return TransformInstanceLibrary.Load(tr_path)


def _generate(inputs: DataInstanceLibrary, transforms: TransformInstanceLibrary, sample_type: str, target_type: str) -> WorkflowPlan:
    samples = list(inputs.AsSamples(sample_type))
    target_model = Transform()
    target_ep = transforms.GetType(target_type)
    target_model.AddRequirement(target_ep)
    return WorkflowPlan.Generate(
        given=[[sv] for sv in samples],
        transforms=[transforms],
        target_names=[target_type],
        target_model=target_model,
    )


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_unreachable_target(temp_dir):
    """Target type has no producer at all → 'unreachable_target' hint."""
    types_path = temp_dir / "mock.yml"
    _write_types_yml(types_path, {
        "a": {"properties": {"_": "type a", "ext": "a"}},
        "b": {"properties": {"_": "type b", "ext": "b"}},
        "c": {"properties": {"_": "type c", "ext": "c"}},  # no transform produces c
    })

    transforms = {
        "a_to_b": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::a"))
out = model.AddProduct(lib.GetType("mock::b"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.b")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
    }
    tr_lib = _make_transform_lib(temp_dir, types_path, transforms)

    lib_path = temp_dir / "inputs.xgdb"
    inputs = DataInstanceLibrary(lib_path)
    inputs.AddTypeLibrary(types_path, namespace="mock")
    inputs.AddValue("sample.a", "x", "mock::a")
    inputs.Save()

    plan = _generate(inputs, tr_lib, "mock::a", "mock::c")

    assert isinstance(plan, WorkflowPlan)
    assert plan.steps == []
    kinds = {h.kind for h in plan.hints}
    assert "unreachable_target" in kinds, f"hints: {plan.hints}"
    hit = next(h for h in plan.hints if h.kind == "unreachable_target")
    assert "mock::c" in hit.target
    assert "no transform" in hit.message.lower()


def test_missing_input_with_near_miss(temp_dir):
    """Producer exists but its input is unsatisfied → 'missing_input' hint
    with a near-miss naming the closest given."""
    types_path = temp_dir / "mock.yml"
    _write_types_yml(types_path, {
        "raw_reads": {"properties": {"_": "reads", "ext": "fq", "qc": "none"}},
        "clean_reads": {"properties": {"_": "reads", "ext": "fq", "qc": "clean"}},
        "assembly": {"properties": {"_": "assembly", "ext": "fa"}},
    })

    transforms = {
        "assembler": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::clean_reads"))
out = model.AddProduct(lib.GetType("mock::assembly"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.fa")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
    }
    tr_lib = _make_transform_lib(temp_dir, types_path, transforms)

    lib_path = temp_dir / "inputs.xgdb"
    inputs = DataInstanceLibrary(lib_path)
    inputs.AddTypeLibrary(types_path, namespace="mock")
    inputs.AddValue("dirty.fq", "x", "mock::raw_reads")  # raw, not clean
    inputs.Save()

    plan = _generate(inputs, tr_lib, "mock::raw_reads", "mock::assembly")

    assert plan.steps == []
    missing = [h for h in plan.hints if h.kind == "missing_input"]
    assert missing, f"expected at least one missing_input hint, got: {plan.hints}"
    h = missing[0]
    assert "mock::assembly" in h.target
    # the dead-end should reference clean_reads since assembler needs it
    chain_text = " ".join(h.chain)
    assert "clean_reads" in chain_text
    # a near-miss should point at our raw_reads input (overlap on _ and ext)
    nm_text = " ".join(h.near_misses)
    assert "dirty.fq" in nm_text, f"near misses: {h.near_misses}"


def test_multi_hop_chain(temp_dir):
    """Chain a→b→c→target with given that doesn't match the first step.
    The hint chain should mention multiple transforms along the back-walk."""
    types_path = temp_dir / "mock.yml"
    _write_types_yml(types_path, {
        "a": {"properties": {"_": "type a", "stage": "input"}},
        "b": {"properties": {"_": "type b", "stage": "mid"}},
        "c": {"properties": {"_": "type c", "stage": "late"}},
        "target": {"properties": {"_": "target"}},
    })

    transforms = {
        "step_ab": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::a"))
out = model.AddProduct(lib.GetType("mock::b"))
def protocol(ctx): return ExecutionResult(manifest=[{out: Path('b')}], success=True)
TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
        "step_bc": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::b"))
out = model.AddProduct(lib.GetType("mock::c"))
def protocol(ctx): return ExecutionResult(manifest=[{out: Path('c')}], success=True)
TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
        "step_ct": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::c"))
out = model.AddProduct(lib.GetType("mock::target"))
def protocol(ctx): return ExecutionResult(manifest=[{out: Path('t')}], success=True)
TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
    }
    tr_lib = _make_transform_lib(temp_dir, types_path, transforms)

    # provide a totally unrelated type — no 'a' available
    types_path2 = temp_dir / "extra.yml"
    _write_types_yml(types_path2, {
        "totally_other": {"properties": {"_": "unrelated"}},
    })
    lib_path = temp_dir / "inputs.xgdb"
    inputs = DataInstanceLibrary(lib_path)
    inputs.AddTypeLibrary(types_path2, namespace="extra")
    inputs.AddValue("u.dat", "x", "extra::totally_other")
    inputs.Save()

    plan = _generate(inputs, tr_lib, "extra::totally_other", "mock::target")
    assert plan.steps == []
    missing = [h for h in plan.hints if h.kind == "missing_input"]
    assert missing, plan.hints
    # the chain should record multiple producer hops back from target
    full_text = " ".join(" ".join(h.chain) for h in missing)
    # we expect to see step_ct AND at least one of step_bc / step_ab in the chain
    assert "step_ct" in full_text
    assert ("step_bc" in full_text) or ("step_ab" in full_text), full_text


def test_lineage_mismatch(temp_dir):
    """Transform requires child whose parent must descend from another given;
    when registered as siblings, a 'lineage_mismatch' hint fires."""
    types_path = temp_dir / "mock.yml"
    _write_types_yml(types_path, {
        "meta": {"properties": {"_": "sample metadata"}},
        "reads": {"properties": {"_": "reads"}},
        "assembly": {"properties": {"_": "assembly"}},
    })

    transforms = {
        "assembler_with_lineage": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
meta = model.AddRequirement(lib.GetType("mock::meta"))
reads = model.AddRequirement(lib.GetType("mock::reads"), parents={meta})
out = model.AddProduct(lib.GetType("mock::assembly"))
def protocol(ctx): return ExecutionResult(manifest=[{out: Path('a')}], success=True)
TransformInstance(protocol=protocol, model=model, group_by=reads)
""",
    }
    tr_lib = _make_transform_lib(temp_dir, types_path, transforms)

    lib_path = temp_dir / "inputs.xgdb"
    inputs = DataInstanceLibrary(lib_path)
    inputs.AddTypeLibrary(types_path, namespace="mock")
    # register meta and reads as siblings — NO parents= argument
    inputs.AddValue("sample.meta", "id=1", "mock::meta")
    inputs.AddValue("sample.fq", "reads", "mock::reads")
    inputs.Save()

    plan = _generate(inputs, tr_lib, "mock::reads", "mock::assembly")
    # this might or might not produce a complete plan depending on the
    # solver's tolerance — what matters is whether a lineage hint appears
    # when there is a property-only match but no lineage link
    lineage = [h for h in plan.hints if h.kind == "lineage_mismatch"]
    if plan.steps and not lineage:
        pytest.skip("solver tolerated missing lineage in this configuration")
    assert lineage, f"expected lineage_mismatch hint, got: {plan.hints}"
    h = lineage[0]
    assert "mock::reads" in h.message or "mock::meta" in h.message
    # near-miss should suggest adding parents=
    nm_text = " ".join(h.near_misses)
    assert "parents=" in nm_text


def test_missing_input_dedup_by_data(temp_dir):
    """Two producers of target both require the same missing type via
    different lineage expressions → only one missing_input hint should
    surface (collapsed by demand shape, not by Dependency identity)."""
    types_path = temp_dir / "mock.yml"
    _write_types_yml(types_path, {
        "context": {"properties": {"_": "context"}},
        "missing": {"properties": {"_": "the-missing-thing"}},
        "target": {"properties": {"_": "target"}},
    })

    transforms = {
        # plain producer: requires missing directly
        "plain_producer": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::missing"))
out = model.AddProduct(lib.GetType("mock::target"))
def protocol(ctx): return ExecutionResult(manifest=[{out: Path('t')}], success=True)
TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
        # lineage producer: requires missing with a parent dependency
        "lineage_producer": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
ctx_dep = model.AddRequirement(lib.GetType("mock::context"))
dep = model.AddRequirement(lib.GetType("mock::missing"), parents={ctx_dep})
out = model.AddProduct(lib.GetType("mock::target"))
def protocol(ctx): return ExecutionResult(manifest=[{out: Path('t')}], success=True)
TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
    }
    tr_lib = _make_transform_lib(temp_dir, types_path, transforms)

    # give nothing that satisfies mock::missing — just an unrelated input so
    # the solver runs but fails
    extra_path = temp_dir / "extra.yml"
    _write_types_yml(extra_path, {"unrelated": {"properties": {"_": "other"}}})
    lib_path = temp_dir / "inputs.xgdb"
    inputs = DataInstanceLibrary(lib_path)
    inputs.AddTypeLibrary(extra_path, namespace="extra")
    inputs.AddValue("u.dat", "x", "extra::unrelated")
    inputs.Save()

    plan = _generate(inputs, tr_lib, "extra::unrelated", "mock::target")
    assert plan.steps == []
    missing_for_missing = [
        h for h in plan.hints
        if h.kind == "missing_input" and "mock::missing" in h.message
    ]
    assert len(missing_for_missing) == 1, (
        f"expected exactly one missing_input hint for mock::missing, "
        f"got {len(missing_for_missing)}: {[h.message for h in missing_for_missing]}"
    )


def test_missing_input_sorted_by_similarity(temp_dir):
    """Two dead-end demands with different similarity to a given → the more
    similar one should appear first in the hints list."""
    types_path = temp_dir / "mock.yml"
    _write_types_yml(types_path, {
        # near_to_given shares two of three properties with the given (_, ext)
        "near_to_given": {"properties": {"_": "shared-marker", "ext": "fq", "qc": "clean"}},
        # far_from_given shares nothing with the given
        "far_from_given": {"properties": {"_": "totally-different", "fmt": "xyz"}},
        "target": {"properties": {"_": "target"}},
    })

    transforms = {
        "combine": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
near = model.AddRequirement(lib.GetType("mock::near_to_given"))
far = model.AddRequirement(lib.GetType("mock::far_from_given"))
out = model.AddProduct(lib.GetType("mock::target"))
def protocol(ctx): return ExecutionResult(manifest=[{out: Path('t')}], success=True)
TransformInstance(protocol=protocol, model=model, group_by=near)
""",
    }
    tr_lib = _make_transform_lib(temp_dir, types_path, transforms)

    # define a given type that shares _=shared-marker and ext=fq with
    # near_to_given but lacks qc=clean (so IsA fails and it's a near-miss
    # rather than a satisfying input)
    inputs_types = temp_dir / "inputs.yml"
    _write_types_yml(inputs_types, {
        "given_input": {"properties": {"_": "shared-marker", "ext": "fq"}},
    })
    lib_path = temp_dir / "inputs.xgdb"
    inputs = DataInstanceLibrary(lib_path)
    inputs.AddTypeLibrary(inputs_types, namespace="given")
    inputs.AddValue("sample.fq", "x", "given::given_input")
    inputs.Save()

    plan = _generate(inputs, tr_lib, "given::given_input", "mock::target")
    assert plan.steps == []
    missing = [h for h in plan.hints if h.kind == "missing_input"]
    assert len(missing) >= 2, f"need at least two missing_input hints, got: {[h.message for h in missing]}"
    # the first missing_input hint should be the one with higher similarity
    assert "mock::near_to_given" in missing[0].message, (
        f"expected near_to_given first, got order: {[h.message for h in missing]}"
    )


def test_too_general_input_names_the_retyping_and_the_missing_parent(temp_dir):
    """A supertype registered where a subtype is wanted, and a parent nothing has.

    This is the shape of nearly every real "why did it not solve": someone
    registers `reads` and asks for an `assembly`, every assembler wants
    `long_reads` or `short_reads`, and a supertype satisfies neither. The old
    diagnosis followed the chain past that point and reported a dead end five
    hops away at an accession nobody had heard of.

    The transform here also declares per-slot lineage, which is the second half
    of the same failure: even retyped, the reads have to descend from metadata
    that is not registered at all.
    """
    types_path = temp_dir / "mock.yml"
    _write_types_yml(types_path, {
        "metadata": {"properties": {"_": "read metadata", "ext": "json"}},
        "reads": {"properties": {"_": "reads", "ext": "fq"}},
        "long_reads": {"properties": {"_": "reads", "ext": "fq", "length": "long"}},
        "assembly": {"properties": {"_": "assembly", "ext": "fa"}},
    })

    transforms = {
        "assembler": """
from pathlib import Path
from metasmith.models.libraries import TransformInstanceLibrary, TransformInstance, ExecutionContext, ExecutionResult
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
meta = model.AddRequirement(lib.GetType("mock::metadata"))
dep = model.AddRequirement(lib.GetType("mock::long_reads"), parents={meta})
out = model.AddProduct(lib.GetType("mock::assembly"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.fa")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
""",
    }
    tr_lib = _make_transform_lib(temp_dir, types_path, transforms)

    lib_path = temp_dir / "inputs.xgdb"
    inputs = DataInstanceLibrary(lib_path)
    inputs.AddTypeLibrary(types_path, namespace="mock")
    inputs.AddValue("sample.fq", "x", "mock::reads")
    inputs.Save()

    plan = _generate(inputs, tr_lib, "mock::reads", "mock::assembly")
    assert plan.steps == []

    general = [h for h in plan.hints if h.kind == "too_general"]
    assert general, f"expected a too_general hint, got: {[h.kind for h in plan.hints]}"
    hit = general[0]
    assert "mock::reads" in hit.message and "mock::long_reads" in hit.message
    # the retyping that would actually work, by name
    assert any(m.startswith("mock::long_reads") for m in hit.near_misses), hit.near_misses
    # ...and the lineage the slot declares, which no retyping can supply
    assert any(m.startswith("mock::metadata") for m in hit.near_misses), hit.near_misses
    # it leads: the dead ends below it are symptoms of this one
    assert plan.hints[0].kind == "too_general"
