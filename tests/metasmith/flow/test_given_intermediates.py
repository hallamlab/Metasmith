# Some samples arrive with an assembly, the rest with reads alone. Each sample
# runs its own route: a given assembly is used and never remade, a missing one
# is assembled, and the aligner reads exactly one assembly per sample. The
# assembler's product either matches the given assembly's type, so one aligner
# serves every sample, or is a subtype of it, as a MEGAHIT assembly is, so each
# case gets its own aligner.

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.models.workflow import NextflowGenContext, WorkflowPlan
from metasmith.testing import mock_transforms as mt
from metasmith.testing.pool_fixtures import pool_backed

from .conftest import (
    _MOCK_TYPE_PROPERTIES,
    BuiltPlan,
    _build_transform_lib,
    _make_target_model,
    _write_input,
)

SAMPLES = ["s0", "s1", "s2"]
ALIGN = "alignment"


def _built(
    tmp_path: Path, with_asm: set[str], transforms: dict[str, str], with_meta: set[str] = frozenset(),
) -> BuiltPlan:
    types = DataTypeLibrary()
    for n, props in {**_MOCK_TYPE_PROPERTIES, "megahit_assembly": {"assembly", "megahit"}}.items():
        types[n] = Endpoint(properties=props)
    types_path = tmp_path / "types.yml"
    types.Save(types_path)
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    for sid in SAMPLES:
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        _write_input(sdir / "reads.fq", f">{sid}\nACGT\n")
        r = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads")
        if sid in with_asm:
            _write_input(sdir / "asm.fa", f">{sid}\nACGTACGT\n")
            lib.AddItem(Path(f"{sid}/asm.fa"), "mock::assembly", parents=[r])
        if sid in with_meta:
            _write_input(sdir / "meta.json", f'{{"id": "{sid}"}}')
            lib.AddItem(Path(f"{sid}/meta.json"), "mock::sample_metadata", parents=[r])
    pool_backed(lib)
    lib.Save()
    tr_lib = _build_transform_lib(tmp_path / "tr", types_path, transforms)
    plan = WorkflowPlan.Generate(
        given=[[sv] for sv in lib.AsSamples("mock::reads")],
        transforms=[tr_lib],
        target_names=["bam"],
        target_model=_make_target_model([_MOCK_TYPE_PROPERTIES["bam"]]),
    )
    return BuiltPlan(plan=plan, data_library=lib, transform_libraries=[tr_lib])


def _plan(tmp_path: Path, with_asm: set[str], transforms: dict[str, str], **kw) -> WorkflowPlan:
    return _built(tmp_path, with_asm, transforms, **kw).plan


def _sample_of(inst) -> str:
    return Path(str(inst.path)).parts[0]


@pytest.mark.parametrize("produced", ["assembly", "megahit_assembly"])
@pytest.mark.parametrize("with_asm", [{"s0"}, {"s0", "s1"}, {"s1", "s2"}, set(), set(SAMPLES)])
def test_each_sample_takes_its_own_route_to_the_aligner(tmp_path, with_asm, produced):
    assemble = mt.identity_transform("mock::reads", f"mock::{produced}")
    plan = _plan(tmp_path, with_asm, {**assemble, **mt.alignment_transform()})
    assert plan.dropped_targets == [] and plan.dropped_samples == []

    reads_id = {_sample_of(i): i.instance_id for i in plan.given if i.dtype_name == "mock::reads"}
    assert set(reads_id) == set(SAMPLES)

    def serves(step, sid):
        return reads_id[sid] not in step.excluded_given

    assemblers = [s for s in plan.steps if s.transform.name in assemble]
    aligners = [s for s in plan.steps if s.transform.name == ALIGN]
    given = set(plan.given)
    for sid in SAMPLES:
        assembled_by = [s for s in assemblers if serves(s, sid)]
        assert len(assembled_by) == (0 if sid in with_asm else 1), f"{sid} is assembled by {len(assembled_by)} steps"

        aligned_by = [s for s in aligners if serves(s, sid)]
        assert len(aligned_by) == 1, f"{sid} is aligned by {len(aligned_by)} steps"
        align = aligned_by[0]
        asm_dep = next(d for d in align.transform.model.requires if "assembly" in d.properties)
        asm_insts = align.dependency_map[asm_dep]
        from_given = [i for i in asm_insts if i in given and _sample_of(i) == sid]
        from_steps = [
            p for p in plan.steps
            if p is not align and serves(p, sid)
            and any(x in asm_insts for g in p.produces for x in g)
        ]
        assert len(from_given) + len(from_steps) == 1, (
            f"{sid}'s aligner has {len(from_given)} given and {len(from_steps)} produced assemblies"
        )
        assert bool(from_given) == (sid in with_asm)


def _assert_fails_naming(plan: WorkflowPlan, dropped: set[str]) -> None:
    assert plan.steps == []
    assert plan.dropped_targets == ["bam"]
    assert {sid for sid in SAMPLES if any(f"{sid}/" in x for x in plan.dropped_samples)} == dropped
    assert "dropped_samples" in [h.kind for h in plan.hints]


def test_a_sample_with_no_route_fails_the_plan_by_name(tmp_path):
    _assert_fails_naming(_plan(tmp_path, {"s0"}, mt.alignment_transform()), {"s1", "s2"})


# Neither case's inputs contain the other's, so the cases are solved jointly,
# and the joint solve covers only the case that carries an assembly.
def test_a_joint_solve_that_skips_a_case_fails_the_plan_by_name(tmp_path):
    plan = _plan(
        tmp_path, {"s0"}, {**mt.identity_transform("mock::reads", "mock::assembly"), **mt.alignment_transform()},
        with_meta={"s1", "s2"},
    )
    _assert_fails_naming(plan, {"s1", "s2"})


# The shape tests/metasmith/e2e/nextflow/test_given_intermediates.py runs.
def test_a_step_for_some_samples_reads_its_inputs_through_exclude(tmp_path):
    assemble = mt.identity_transform("mock::reads", "mock::assembly")
    bp = _built(tmp_path, {"s0"}, {**assemble, **mt.alignment_transform()})
    at = tmp_path / "ws"
    at.mkdir()
    bp.as_task().PrepareNextflow(NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW, work_dir=at, external_work=at,
        home_dir=at / "home", external_home=at / "home",
        runtime=Runtime.DOCKER, resources_file=AgentPaths.NXF_RES,
    ))
    body = (at / AgentPaths.NXF_WORKFLOW).read_text()

    step = next(s for s in bp.plan.steps if s.transform.name in assemble)
    reads = step.uses[0].dtype.key
    ids = ", ".join(f"'{x}'" for x in step.excluded_given)
    assert f"__excl_{step.order} = [{ids}]" in body
    assert f"o.group('{reads}', [o.exclude(_{reads}, __excl_{step.order})]" in body
    assert body.count("o.exclude(") == 1
