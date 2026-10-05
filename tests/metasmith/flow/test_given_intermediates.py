# Some samples arrive with an assembly, the rest with reads alone. The samples
# split into one case per shape, and each sample runs its own route: a given
# assembly is used and never remade, a missing one is assembled, and the
# aligner reads exactly one assembly per sample. The cases' aligners merge into
# one, whether the assembler's product matches the given assembly's type or is
# a subtype of it, as a MEGAHIT assembly is.

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint
from metasmith.models.workflow import Case, NextflowGenContext, WorkflowPlan
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


def _sample_of(inst) -> str:
    return Path(str(inst.path)).parts[0]


def _built(
    tmp_path: Path, with_asm: set[str], transforms: dict[str, str], with_meta: set[str] = frozenset(),
) -> tuple[BuiltPlan, dict[str, str]]:
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
    cases = Case.ByShape(
        [[sv] for sv in lib.AsSamples("mock::reads")],
        target=_make_target_model([_MOCK_TYPE_PROPERTIES["bam"]]),
        target_names=["bam"],
    )
    case_of = {
        Path(str(p)).parts[0]: c.name
        for c in cases for sample in c.given for view in sample for p, _, _ in view.Iterate()
    }
    plan = WorkflowPlan.Generate(cases=cases, transforms=[tr_lib])
    return BuiltPlan(plan=plan, data_library=lib, transform_libraries=[tr_lib]), case_of


def _assert_each_sample_takes_its_own_route(plan: WorkflowPlan, case_of, assemble, with_asm) -> None:
    assert plan.dropped_targets == [] and plan.dropped_samples == []

    def serves(step, sid):
        return case_of[sid] in step.cases

    assemblers = [s for s in plan.steps if s.transform.name in assemble]
    aligners = [s for s in plan.steps if s.transform.name == ALIGN]
    assert len(aligners) == 1
    (align,) = aligners
    given = set(plan.given)
    asm_dep = next(d for d in align.transform.model.requires if "assembly" in d.properties)
    asm_insts = align.dependency_map[asm_dep]
    for sid in SAMPLES:
        assembled_by = [s for s in assemblers if serves(s, sid)]
        assert len(assembled_by) == (0 if sid in with_asm else 1), f"{sid} is assembled by {len(assembled_by)} steps"
        assert serves(align, sid)
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


@pytest.mark.parametrize("produced", ["assembly", "megahit_assembly"])
@pytest.mark.parametrize("with_asm", [{"s0"}, {"s0", "s1"}, {"s1", "s2"}, set(), set(SAMPLES)])
def test_each_sample_takes_its_own_route_to_the_aligner(tmp_path, with_asm, produced):
    assemble = mt.identity_transform("mock::reads", f"mock::{produced}")
    bp, case_of = _built(tmp_path, with_asm, {**assemble, **mt.alignment_transform()})
    _assert_each_sample_takes_its_own_route(bp.plan, case_of, assemble, with_asm)


# Neither case's givens contain the other's. Each case is solved alone, so
# neither can starve the other.
def test_cases_that_do_not_nest_each_take_their_own_route(tmp_path):
    assemble = mt.identity_transform("mock::reads", "mock::assembly")
    bp, case_of = _built(tmp_path, {"s0"}, {**assemble, **mt.alignment_transform()}, with_meta={"s1", "s2"})
    _assert_each_sample_takes_its_own_route(bp.plan, case_of, assemble, {"s0"})


def test_a_sample_with_no_route_fails_the_plan_by_name(tmp_path):
    plan = _built(tmp_path, {"s0"}, mt.alignment_transform())[0].plan
    assert plan.steps == []
    assert plan.dropped_targets == ["bam"]
    assert {sid for sid in SAMPLES if any(f"{sid}/" in x for x in plan.dropped_samples)} == {"s1", "s2"}
    assert "dropped_samples" in [h.kind for h in plan.hints]


# The shape tests/metasmith/e2e/nextflow/test_given_intermediates.py runs.
def test_a_step_for_some_cases_reads_its_inputs_through_a_case_filter(tmp_path):
    assemble = mt.identity_transform("mock::reads", "mock::assembly")
    bp, _ = _built(tmp_path, {"s0"}, {**assemble, **mt.alignment_transform()})
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
    assert f"__cases_{step.order} = [" in body
    assert f"o.group('{reads}', [o.cases(_{reads}, __cases_{step.order})]" in body
    assert body.count("o.cases(") == 1
    assert "o.exclude(" not in body
