# One solve over several cases. Each case is a group of samples of one shape
# with its own targets. Every case is solved alone and the plans merge where a
# step can serve several cases: same tool, inputs of the same kind. Each case
# still gets exactly one source per slot.

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import DataInstanceLibrary, DataInstanceLibraryView, DataTypeLibrary
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import Case, NextflowGenContext, WorkflowPlan
from metasmith.testing import mock_transforms as mt
from metasmith.testing.pool_fixtures import pool_backed

from .conftest import BuiltPlan, _build_transform_lib, _write_input

TYPES = {
    "study": {"study"},
    "short_reads": {"reads", "short"},
    "long_reads": {"reads", "long"},
    "assembly": {"assembly"},
    "short_assembly": {"assembly", "short"},
    "hybrid_assembly": {"assembly", "hybrid"},
    "long_assembly": {"assembly", "long"},
    "viral_contigs": {"viral_contigs"},
    "votu_table": {"votu_table"},
    "checkv_report": {"checkv_report"},
    "qc_report": {"qc_report"},
    "short_stats": {"short_stats"},
    "long_stats": {"long_stats"},
    "refdb": {"refdb"},
    "annotation": {"annotation"},
    "reads": {"reads"},
    "sample_metadata": {"sample_metadata"},
    "bam": {"bam"},
}

MEGAHIT = mt.step_transform("megahit", {"short": ("mock::short_reads", [])}, "mock::short_assembly", "short")
HYBRID = mt.step_transform(
    "hybrid", {"short": ("mock::short_reads", []), "long": ("mock::long_reads", ["short"])},
    "mock::hybrid_assembly", "short",
)
FLYE = mt.step_transform("flye", {"long": ("mock::long_reads", [])}, "mock::long_assembly", "long")
VIRAL_ID = mt.step_transform("viral_id", {"asm": ("mock::assembly", [])}, "mock::viral_contigs", "asm")
VOTU = mt.step_transform(
    "votu", {"study": ("mock::study", []), "vc": ("mock::viral_contigs", ["study"])}, "mock::votu_table", "study",
)
CHECKV = mt.step_transform("checkv", {"vt": ("mock::votu_table", [])}, "mock::checkv_report", "vt")
ASM_QC = mt.step_transform("asm_qc", {"asm": ("mock::short_assembly", [])}, "mock::qc_report", "asm")
SHORT_STATS = mt.step_transform("short_stats", {"r": ("mock::short_reads", [])}, "mock::short_stats", "r")
LONG_STATS = mt.step_transform("long_stats", {"r": ("mock::long_reads", [])}, "mock::long_stats", "r")
ANNOTATE = mt.step_transform(
    "annotate", {"asm": ("mock::assembly", []), "db": ("mock::refdb", [])}, "mock::annotation", "asm",
)
ALIGN = mt.step_transform(
    "align", {"r": ("mock::short_reads", []), "asm": ("mock::assembly", ["r"])}, "mock::bam", "r",
)
VIROMICS = {**MEGAHIT, **HYBRID, **VIRAL_ID, **VOTU, **CHECKV}


class Study:
    def __init__(self, tmp_path: Path, items: dict[str, tuple[str, list[str]]]):
        types = DataTypeLibrary()
        for n, props in TYPES.items():
            types[n] = Endpoint(properties=props)
        self.types_path = tmp_path / "types.yml"
        types.Save(self.types_path)
        self.tmp_path = tmp_path
        self.lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        self.lib.AddTypeLibrary(self.types_path, namespace="mock")
        added = {}
        for rel, (dtype, parents) in items.items():
            p = self.lib.location / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            _write_input(p, rel)
            added[rel] = self.lib.AddItem(Path(rel), f"mock::{dtype}", parents=[added[x] for x in parents] or None)
        pool_backed(self.lib)
        self.lib.Save()

    def sample(self, *rels: str) -> list[DataInstanceLibraryView]:
        return [DataInstanceLibraryView(original=self.lib, mask={Path(r) for r in rels})]

    def plan(self, transforms: dict[str, str], cases: list[Case]) -> WorkflowPlan:
        self.tr = _build_transform_lib(self.tmp_path / "tr", self.types_path, transforms)
        return WorkflowPlan.Generate(cases=cases, transforms=[self.tr])

    def stage(self, plan: WorkflowPlan, name: str) -> tuple[str, dict]:
        at = self.tmp_path / name
        at.mkdir()
        self.task = BuiltPlan(plan=plan, data_library=self.lib, transform_libraries=[self.tr]).as_task()
        self.task.PrepareNextflow(
            NextflowGenContext(
                workflow_file=AgentPaths.NXF_WORKFLOW, work_dir=at, external_work=at,
                home_dir=at / "home", external_home=at / "home",
                runtime=Runtime.DOCKER, resources_file=AgentPaths.NXF_RES,
            )
        )
        lineage = json.loads((at / "workflow.lineage_of_given.json").read_text())
        return (at / AgentPaths.NXF_WORKFLOW).read_text(), lineage


def target(*spec: tuple[str, list[str]]) -> tuple[Transform, list[str]]:
    t = Transform()
    deps = {}
    for name, parents in spec:
        deps[name] = t.AddRequirement(properties=TYPES[name], parents={deps[p] for p in parents})
    return t, [name for name, _ in spec]


def case(name: str, samples: list[list[DataInstanceLibraryView]], *spec) -> Case:
    t, names = target(*spec)
    return Case(name=name, given=samples, target=t, target_names=names)


VOTU_ONLY = [("votu_table", [])]
HYBRID_VOTU_CHECKV = [("hybrid_assembly", []), ("votu_table", ["hybrid_assembly"]), ("checkv_report", [])]


def steps_of(plan: WorkflowPlan, name: str):
    return [s for s in plan.steps if s.transform.name == name]


def served(plan: WorkflowPlan, name: str) -> list[set[str]]:
    return sorted((set(s.cases) for s in steps_of(plan, name)), key=sorted)


def producers(plan: WorkflowPlan, step, slot_type: str) -> set[str]:
    dep = next(d for d in step.transform.model.requires if d.properties == TYPES[slot_type])
    insts = step.dependency_map[dep]
    return {
        p.transform.name for p in plan.steps
        if any(x in insts for g in p.produces for x in g)
    } | {"<given>" for i in insts if i in set(plan.given)}


def _ab_items(shared_reads: bool) -> dict:
    items = {"studyX.json": ("study", [])}
    items["b/short.fq"] = ("short_reads", ["studyX.json"])
    items["b/long.fq"] = ("long_reads", ["b/short.fq"])
    if not shared_reads:
        items["a/short.fq"] = ("short_reads", ["studyX.json"])
    return items


@pytest.mark.parametrize("shared_reads", [False, True])
def test_a_short_read_case_and_a_hybrid_case_share_viral_id_and_the_votu_table(tmp_path, shared_reads):
    st = Study(tmp_path, _ab_items(shared_reads))
    a_reads = "b/short.fq" if shared_reads else "a/short.fq"
    plan = st.plan(VIROMICS, [
        case("A", [st.sample("studyX.json", a_reads)], *VOTU_ONLY),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], *HYBRID_VOTU_CHECKV),
    ])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert plan.cases == ["A", "B"]
    assert served(plan, "megahit") == [{"A"}]
    assert served(plan, "hybrid") == [{"B"}]
    assert served(plan, "viral_id") == [{"A", "B"}]
    assert served(plan, "votu") == [{"A", "B"}]
    assert served(plan, "checkv") == [{"B"}]
    (viral_id,) = steps_of(plan, "viral_id")
    assert producers(plan, viral_id, "assembly") == {"megahit", "hybrid"}
    (votu,) = steps_of(plan, "votu")
    assert producers(plan, votu, "viral_contigs") == {"viral_id"}
    assert {(t.name, t.case) for t in plan.targets} == {
        ("votu_table", "A"), ("hybrid_assembly", "B"), ("votu_table", "B"), ("checkv_report", "B"),
    }


def test_the_staged_workflow_unions_the_assemblies_and_filters_by_case(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    plan = st.plan(VIROMICS, [
        case("A", [st.sample("studyX.json", "a/short.fq")], *VOTU_ONLY),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], *HYBRID_VOTU_CHECKV),
    ])
    body, lineage = st.stage(plan, "ws")

    def stream(e) -> str:
        return plan.streams.get(e.key, e.key)

    (megahit,), (hybrid,), (viral_id,), (checkv,) = (steps_of(plan, n) for n in ("megahit", "hybrid", "viral_id", "checkv"))
    asm = stream(viral_id.uses[0].dtype)
    assert stream(megahit.produces[0][0].dtype) == stream(hybrid.produces[0][0].dtype) == asm
    assert f"_{asm} = o.mix([_{asm}_1, _{asm}_2])" in body
    assert f"o.group('{asm}', [_{asm}]," in body

    assert f"__cases_{megahit.order} = ['A']" in body
    assert f"__cases_{hybrid.order} = ['B']" in body
    assert f"__cases_{checkv.order} = ['B']" in body
    assert f"__cases_{viral_id.order} =" not in body
    vt = stream(checkv.uses[0].dtype)
    assert f"o.group('{vt}', [o.cases(_{vt}, __cases_{checkv.order})]," in body

    assert f"_{asm} = o.publish(_{asm}, ['B'])" in body
    study = stream(next(x for x in plan.given if str(x.path).endswith("studyX.json")).dtype)
    assert f'["{study}"], [\'A\', \'B\']))[0]' in body
    long = stream(next(x for x in plan.given if str(x.path).endswith("long.fq")).dtype)
    assert f'["{long}"], [\'B\']))[0]' in body
    votu_out = stream(steps_of(plan, "votu")[0].produces[0][0].dtype)
    assert f"_{votu_out} = o.publish(_{votu_out})" in body

    assert "o.seedCases(_lf.cases)" in body
    assert "'CASES'" in body
    study_id = next(x.instance_id for x in plan.given if str(x.path).endswith("studyX.json"))
    assert lineage["cases"][study_id] == ["A", "B"]
    assert all(set(v) <= {"A", "B"} and v for v in lineage["cases"].values())
    assert not any("cases" in row for rows in lineage["lineage"].values() for row in rows)


def test_a_slot_each_case_reads_from_its_own_lane_is_staged_as_a_lane(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=True))
    plan = st.plan({**MEGAHIT, **HYBRID, **ALIGN}, [
        case("A", [st.sample("studyX.json", "b/short.fq")], ("bam", [])),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], ("hybrid_assembly", []), ("bam", ["hybrid_assembly"])),
    ])
    assert served(plan, "align") == [{"A", "B"}]
    (align,) = steps_of(plan, "align")
    assert producers(plan, align, "assembly") == {"megahit", "hybrid"}
    body, _ = st.stage(plan, "ws")
    reads, asm = (
        plan.streams.get(e.key, e.key)
        for e in (align.dependency_map[d][0].dtype for d in align.transform.model.requires)
    )
    groups = [ln for ln in body.splitlines() if "o.group(" in ln]
    (line,) = [ln for ln in groups if f"step: {align.order}," in ln]
    assert line.startswith(f"(__miss_{align.order}, __hit_{align.order}) = o.group('{reads}', [_{reads}, _{asm}],")
    assert [ln for ln in groups if re.search(r"'\], \[[^\]]*\]\)$", ln)] == [line]
    assert line.endswith(f"'], ['{asm}'])")


def test_a_single_case_stages_no_case_machinery(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    plan = st.plan(VIROMICS, [case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], *HYBRID_VOTU_CHECKV)])
    body, lineage = st.stage(plan, "ws")
    assert not re.search(r"o\.cases\(|__cases_|seedCases|'CASES'", body)
    assert "cases" not in lineage


def _collect(st: Study, plan: WorkflowPlan, events: list[tuple[str, list[str] | None, list[str]]]) -> tuple[DataInstanceLibrary, dict]:
    from metasmith.agents.collect import CollectResults
    from metasmith.models.lineage import InvocationEvent, ProducedFile, append_invocation_event

    st.stage(plan, "ws")
    at = st.tmp_path / "ws"
    trace = at / "_metasmith" / "trace.jsonl"
    trace.parent.mkdir(exist_ok=True)
    fids = {}
    for name, cases, parents in events:
        (step,) = steps_of(plan, name)
        out = step.produces[0][0]
        fids[name] = f"f-{name}"
        append_invocation_event(trace, InvocationEvent(
            task_hash=f"h-{name}", transform_key=name, status="miss",
            produces=[ProducedFile(
                file_instance_id=fids[name], slot_id=out.instance_id, path=f"{name}.out",
                dtype_key=out.dtype.key, parents=[fids.get(p) or p for p in parents],
            )],
            step_order=plan.steps.index(step) + 1, cases=cases,
        ))
    output = CollectResults(task=st.task, output_path=at / "results", inputs_dir=at / "inputs")
    return output, fids


def test_results_record_the_cases_of_every_item(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    plan = st.plan(VIROMICS, [
        case("A", [st.sample("studyX.json", "a/short.fq")], *VOTU_ONLY),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], *HYBRID_VOTU_CHECKV),
    ])
    given = {str(x.path): x.instance_id for x in plan.given}
    output, _ = _collect(st, plan, [
        ("megahit", ["A"], [given["a/short.fq"]]),
        ("votu", ["A", "B"], [given["studyX.json"], "megahit"]),
    ])

    def cases_of(lib: DataInstanceLibrary, name: str) -> list[str]:
        (path,) = [p for p in lib.manifest if p.name == name]
        return lib.cases[path]

    reloaded = DataInstanceLibrary.Load(output.location)
    for lib in (output, reloaded):
        assert cases_of(lib, "megahit.out") == ["A"]
        assert cases_of(lib, "votu.out") == ["A", "B"]
        assert cases_of(lib, "short.fq") == ["A"]
        assert cases_of(lib, "studyX.json") == ["A", "B"]

    rows = (output.location / "given.csv").read_text().splitlines()
    assert rows[0] == "instance_id,dtype_key,path,origin,cases"
    by_path = {r.split(",")[2].split("samples.xgdb/")[-1]: r.split(",")[-1] for r in rows[1:]}
    assert by_path == {"studyX.json": "A;B", "a/short.fq": "A", "b/short.fq": "B", "b/long.fq": "B"}


def test_a_single_case_run_records_no_cases(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    plan = st.plan(VIROMICS, [case("A", [st.sample("studyX.json", "a/short.fq")], *VOTU_ONLY)])
    given = {str(x.path): x.instance_id for x in plan.given}
    output, _ = _collect(st, plan, [("megahit", None, [given["a/short.fq"]])])
    assert output.cases == {}
    assert "cases" not in (output.location / "_metadata" / "index.yml").read_text()
    assert (output.location / "given.csv").read_text().splitlines()[0] == "instance_id,dtype_key,path,origin"


def test_each_case_is_valid_alone_and_matches_its_merged_plan(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    a = case("A", [st.sample("studyX.json", "a/short.fq")], *VOTU_ONLY)
    b = case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], *HYBRID_VOTU_CHECKV)
    alone_a = st.plan(VIROMICS, [a])
    alone_b = st.plan(VIROMICS, [b])
    assert sorted(s.transform.name for s in alone_a.steps) == ["megahit", "viral_id", "votu"]
    assert sorted(s.transform.name for s in alone_b.steps) == ["checkv", "hybrid", "viral_id", "votu"]
    merged = st.plan(VIROMICS, [a, b])
    for name, alone in (("A", alone_a), ("B", alone_b)):
        mine = sorted(s.transform.name for s in merged.steps if name in s.cases)
        assert mine == sorted(s.transform.name for s in alone.steps)


def test_three_cases_merge_pairwise_and_all_together(tmp_path):
    st = Study(tmp_path, {
        "studyX.json": ("study", []),
        "c1/short.fq": ("short_reads", ["studyX.json"]),
        "c2/short.fq": ("short_reads", ["studyX.json"]),
        "c2/long.fq": ("long_reads", ["c2/short.fq"]),
        "c3/long.fq": ("long_reads", ["studyX.json"]),
    })
    plan = st.plan({**VIROMICS, **FLYE, **SHORT_STATS, **LONG_STATS}, [
        case("C1", [st.sample("studyX.json", "c1/short.fq")], ("short_stats", []), ("votu_table", [])),
        case("C2", [st.sample("studyX.json", "c2/short.fq", "c2/long.fq")],
             ("short_stats", []), ("long_stats", []), ("hybrid_assembly", []), ("votu_table", ["hybrid_assembly"])),
        case("C3", [st.sample("studyX.json", "c3/long.fq")], ("long_stats", []), ("votu_table", [])),
    ])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert served(plan, "short_stats") == [{"C1", "C2"}]
    assert served(plan, "long_stats") == [{"C2", "C3"}]
    assert served(plan, "viral_id") == [{"C1", "C2", "C3"}]
    assert served(plan, "votu") == [{"C1", "C2", "C3"}]
    (viral_id,) = steps_of(plan, "viral_id")
    assert producers(plan, viral_id, "assembly") == {"megahit", "hybrid", "flye"}


# Case B also needs MEGAHIT's assembly for its own QC, so it carries both
# assemblies. Merging its viral-ID step into A's would hand B two assemblies in
# one slot, so B keeps a viral-ID step of its own. The vOTU tables still merge:
# each viral-ID step serves one case, so each case reads one source.
def test_a_merge_that_gives_a_case_two_sources_is_vetoed(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    plan = st.plan({**VIROMICS, **ASM_QC}, [
        case("A", [st.sample("studyX.json", "a/short.fq")], *VOTU_ONLY),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")],
             ("qc_report", []), *HYBRID_VOTU_CHECKV),
    ])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert served(plan, "megahit") == [{"A", "B"}]
    assert served(plan, "viral_id") == [{"A"}, {"B"}]
    by_case = {frozenset(s.cases): s for s in steps_of(plan, "viral_id")}
    assert producers(plan, by_case[frozenset({"A"})], "assembly") == {"megahit"}
    assert producers(plan, by_case[frozenset({"B"})], "assembly") == {"hybrid"}
    assert served(plan, "votu") == [{"A", "B"}]


@pytest.mark.parametrize("b_db,annotate_steps", [("dbs/v1.db", [{"A", "B"}]), ("dbs/v2.db", [{"A"}, {"B"}])])
def test_cases_with_different_unrelated_givens_do_not_pool_them(tmp_path, b_db, annotate_steps):
    items = _ab_items(shared_reads=False)
    items["dbs/v1.db"] = ("refdb", [])
    items["dbs/v2.db"] = ("refdb", [])
    st = Study(tmp_path, items)
    plan = st.plan({**MEGAHIT, **HYBRID, **ANNOTATE}, [
        case("A", [st.sample("studyX.json", "a/short.fq", "dbs/v1.db")], ("annotation", [])),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq", b_db)],
             ("hybrid_assembly", []), ("annotation", ["hybrid_assembly"])),
    ])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert served(plan, "annotate") == annotate_steps


# The veto above leaves two viral-ID steps, one on MEGAHIT's assembly (case A)
# and one on the hybrid assembly (case B). Case C reads a given assembly and
# either step could take it. A candidate whose input is the same kind as C's
# wins; otherwise the first in case order does.
@pytest.mark.parametrize("c_asm,joins", [("hybrid_assembly", "B"), ("long_assembly", "A")])
def test_an_ambiguous_merge_picks_deterministically(tmp_path, c_asm, joins):
    items = _ab_items(shared_reads=False)
    items["c/asm.fa"] = (c_asm, ["studyX.json"])
    st = Study(tmp_path, items)
    plan = st.plan({**MEGAHIT, **HYBRID, **ASM_QC, **VIRAL_ID}, [
        case("A", [st.sample("studyX.json", "a/short.fq")], ("viral_contigs", [])),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")],
             ("qc_report", []), ("hybrid_assembly", []), ("viral_contigs", ["hybrid_assembly"])),
        case("C", [st.sample("studyX.json", "c/asm.fa")], ("viral_contigs", [])),
    ])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert any(set(s.cases) == {joins, "C"} for s in steps_of(plan, "viral_id")), served(plan, "viral_id")


def test_the_merged_plan_does_not_depend_on_the_seed(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    cases = [
        case("A", [st.sample("studyX.json", "a/short.fq")], *VOTU_ONLY),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], *HYBRID_VOTU_CHECKV),
    ]
    tr = _build_transform_lib(tmp_path / "tr", st.types_path, VIROMICS)
    shapes = {
        tuple(sorted((s.transform.name, tuple(sorted(s.cases))) for s in WorkflowPlan.Generate(
            cases=cases, transforms=[tr], seed=seed).steps))
        for seed in (1, 7, 42)
    }
    assert len(shapes) == 1


def test_a_case_with_no_route_fails_the_plan_by_name(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    plan = st.plan({**MEGAHIT, **VIRAL_ID, **VOTU}, [
        case("A", [st.sample("studyX.json", "a/short.fq")], *VOTU_ONLY),
        case("B", [st.sample("studyX.json", "b/short.fq", "b/long.fq")], *HYBRID_VOTU_CHECKV),
    ])
    assert plan.steps == []
    assert plan.dropped_targets
    assert any("b/" in x for x in plan.dropped_samples)
    assert not any("a/" in x for x in plan.dropped_samples)
    assert "dropped_samples" in [h.kind for h in plan.hints]


def test_a_case_whose_samples_differ_in_shape_is_refused_by_name(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    with pytest.raises(ValueError, match="mixed"):
        st.plan(VIROMICS, [case(
            "mixed",
            [st.sample("studyX.json", "a/short.fq"), st.sample("studyX.json", "b/short.fq", "b/long.fq")],
            *VOTU_ONLY,
        )])


def test_a_target_with_outputs_is_refused(tmp_path):
    st = Study(tmp_path, _ab_items(shared_reads=False))
    t, names = target(*VOTU_ONLY)
    t.AddProduct(properties={"votu_table"})
    with pytest.raises(ValueError, match="no outputs"):
        st.plan(VIROMICS, [Case(name="A", given=[st.sample("studyX.json", "a/short.fq")], target=t, target_names=names)])
