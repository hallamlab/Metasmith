# A transform with several product groups is a fork: each group is one outcome.
# Every outcome becomes a case of its own, solved and merged into one plan,
# and the plan fails if any outcome cannot reach the target.

from __future__ import annotations

import json

import pytest

from metasmith.models.workflow import case_merge
from metasmith.testing import mock_transforms as mt

from .test_cases import FLYE, MEGAHIT, SHORT_STATS, TYPES, VIRAL_ID, VOTU, Study, case, producers, served, steps_of

ACC = {"acc": ("mock::accession", [])}
# Every outcome carries the sample metadata. The reads differ.
FETCH = mt.fork_transform("fetch", ACC, [
    {"meta": "mock::sample_metadata", "short": "mock::short_reads"},
    {"meta": "mock::sample_metadata", "long": "mock::long_reads"},
], "acc")
FETCH_HYBRID = mt.fork_transform("fetch", ACC, [
    {"short": "mock::short_reads"},
    {"short": "mock::short_reads", "long": "mock::long_reads"},
], "acc")
SPLIT = mt.fork_transform("split", {"r": ("mock::short_reads", [])}, [
    {"s": "mock::short_stats"},
    {"s": "mock::short_stats", "q": "mock::qc_report"},
], "r")
ROUTES = {**FETCH, **MEGAHIT, **FLYE, **VIRAL_ID, **VOTU}
# The study that started this work: an accession yields short reads, or short
# and long reads. The hybrid assembler pairs them through their accession.
HYBRID_ACC = mt.step_transform(
    "hybrid", {"acc": ("mock::accession", []), "short": ("mock::short_reads", ["acc"]), "long": ("mock::long_reads", ["acc"])},
    "mock::hybrid_assembly", "acc",
)
STUDY = {**FETCH_HYBRID, **MEGAHIT, **HYBRID_ACC, **VIRAL_ID, **VOTU}


def _study(tmp_path) -> Study:
    return Study(tmp_path, {"studyX.json": ("study", []), "s1.acc": ("accession", ["studyX.json"])})


@pytest.fixture
def solves(monkeypatch):
    seen: list[tuple[dict, object]] = []
    real = case_merge.solve_by_mcts

    def record(**kw):
        r = real(**kw)
        seen.append(({t: g for t, g in kw["fork_groups"]}, r))
        return r
    monkeypatch.setattr(case_merge, "solve_by_mcts", record)
    return seen


def test_each_outcome_of_a_fork_gets_its_own_route(tmp_path, solves):
    st = _study(tmp_path)
    plan = st.plan(ROUTES, [case("S", [st.sample("studyX.json", "s1.acc")], ("votu_table", []))])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert sorted(tuple(f.values()) for f, _ in solves) == [(), (1,)]
    (fetch,) = steps_of(plan, "fetch")
    assert [sorted(i.dtype_name for i in g) for g in fetch.produces] == [
        ["mock::sample_metadata", "mock::short_reads"], ["mock::long_reads", "mock::sample_metadata"],
    ]
    assert fetch.produces[0][0].dtype != fetch.produces[1][0].dtype
    (viral_id,) = steps_of(plan, "viral_id")
    assert producers(plan, viral_id, "assembly") == {"megahit", "flye"}
    assert len(steps_of(plan, "votu")) == 1
    assert plan.cases == ["S"]
    assert {s for st_ in plan.steps for s in st_.cases} == {"S"}
    packed = plan.Pack()
    assert packed["streams"] and "cases" not in packed


def _reads(step, slot_type: str):
    dep = next(d for d in step.transform.model.requires if d.properties == TYPES[slot_type])
    return step.dependency_map[dep]


def _group_of(fork, inst) -> int:
    (g,) = [g for g, insts in enumerate(fork.produces) if inst in insts]
    return g


def test_a_step_that_reads_a_shared_product_is_placed_once_per_group(tmp_path):
    st = _study(tmp_path)
    plan = st.plan({**FETCH_HYBRID, **MEGAHIT, **SHORT_STATS, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", "s1.acc")], ("short_stats", []), ("votu_table", [])),
    ])
    assert plan.dropped_targets == []
    (fetch,) = steps_of(plan, "fetch")
    short = fetch.transform.model.produces[0][0]
    assert [len(fetch.ProductsOf(g, short)) for g in (0, 1)] == [1, 1]
    for name in ("megahit", "short_stats"):
        reads = [_reads(s, "short_reads") for s in steps_of(plan, name)]
        assert sorted(_group_of(fetch, x) for (x,) in reads) == [0, 1], name
    (viral_id,) = steps_of(plan, "viral_id")
    assert len(_reads(viral_id, "assembly")) == 2
    assert len(steps_of(plan, "votu")) == 1


def test_an_outcome_with_no_route_fails_the_plan_and_names_its_group(tmp_path):
    st = _study(tmp_path)
    plan = st.plan({**FETCH, **MEGAHIT, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", "s1.acc")], ("votu_table", [])),
    ])
    assert plan.steps == []
    assert "s1.acc" in " ".join(plan.dropped_samples)
    (hint,) = [h for h in plan.hints if h.kind == "fork_branch"]
    assert hint.candidate_transforms == ["fetch"]
    assert "mock::long_reads" in hint.message and "mock::short_reads" not in hint.message


def test_a_sibling_keeps_the_steps_up_to_the_fork_and_is_guided_by_none_below_it(tmp_path, monkeypatch):
    seen: list[dict] = []
    real = case_merge.solve_by_mcts

    def record(**kw):
        seen.append(kw)
        return real(**kw)
    monkeypatch.setattr(case_merge, "solve_by_mcts", record)
    st = _study(tmp_path)
    plan = st.plan({**FETCH_HYBRID, **MEGAHIT, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", "s1.acc")], ("votu_table", [])),
    ])
    assert plan.steps
    parent, sibling = seen
    assert list(parent["partial"]) == [] and list(parent["guide"]) == []
    (fetch,) = steps_of(plan, "fetch")
    assert [t.key for t in sibling["partial"]] == [fetch.transform.model.key]
    assert list(sibling["guide"]) == []


def test_nested_forks_solve_every_combination_once(tmp_path, solves):
    st = _study(tmp_path)
    plan = st.plan({**FETCH_HYBRID, **SPLIT, **MEGAHIT, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", "s1.acc")], ("short_stats", []), ("votu_table", [])),
    ])
    assert plan.dropped_targets == []
    names = {t: t_.name for s in plan.steps for t, t_ in [(s.transform.model, s.transform)]}
    combos = sorted(tuple(sorted((names[t], g) for t, g in f.items())) for f, _ in solves)
    assert combos == [(), (("fetch", 1),), (("fetch", 1), ("split", 1)), (("split", 1),)]
    splits = steps_of(plan, "split")
    assert len(splits) == 2
    assert all(len([i for g in s.produces for i in g]) == 3 for s in splits)


def test_a_fork_stages_one_output_per_group_and_each_group_its_own_stream(tmp_path):
    st = _study(tmp_path)
    plan = st.plan({**FETCH_HYBRID, **MEGAHIT, **SHORT_STATS, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", "s1.acc")], ("short_stats", []), ("votu_table", [])),
    ])
    body, _ = st.stage(plan, "ws")
    (fetch,) = steps_of(plan, "fetch")
    short = fetch.transform.model.produces[0][0]
    by_group = [fetch.ProductsOf(g, short)[0].dtype for g in (0, 1)]
    streams = [plan.streams.get(e.key, e.key) for e in by_group]
    assert streams[0] != streams[1]
    for g, e in enumerate(by_group):
        assert f'path("*-{g+1}.*-{e.key}{e.GetPreferredFileExtension()}"), optional: true' in body
    for s in steps_of(plan, "megahit"):
        (x,) = _reads(s, "short_reads")
        assert f"o.group('{plan.streams.get(x.dtype.key, x.dtype.key)}'," in body
    meta = json.loads(next(
        ln[4:] for ln in (st.tmp_path / "ws" / f"workflow.step_{fetch.order}.meta").read_text().splitlines()
        if ln.startswith("dot ")
    ))
    assert [sorted(len(v) for v in g.values()) for g in meta] == [[1], [1, 1]]


def _accessions(tmp_path, *extra) -> Study:
    items = {"studyX.json": ("study", []), "s1.acc": ("accession", ["studyX.json"]), "s2.acc": ("accession", ["studyX.json"])}
    return Study(tmp_path, {**items, **dict(extra)})


def test_the_short_read_and_hybrid_study_plans_a_route_for_each_outcome(tmp_path):
    st = _accessions(tmp_path)
    plan = st.plan(STUDY, [case("S", [st.sample("studyX.json", a) for a in ("s1.acc", "s2.acc")], ("votu_table", []))])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert plan.cases == ["S"]
    (fetch,) = steps_of(plan, "fetch")
    assemblers = [s for s in plan.steps if s.transform.name in {"megahit", "hybrid"}]
    reads = {(s.transform.name, _group_of(fetch, x)) for s in assemblers for x in _reads(s, "short_reads")}
    assert reads == {("megahit", 0), ("hybrid", 1)}
    assert all(len(_reads(s, "short_reads")) == 1 for s in assemblers)
    (viral_id,) = steps_of(plan, "viral_id")
    assert len(_reads(viral_id, "assembly")) == len(assemblers)
    (votu,) = steps_of(plan, "votu")
    assert producers(plan, votu, "viral_contigs") == {"viral_id"}


def test_samples_with_a_given_assembly_skip_assembly_while_accessions_fork(tmp_path):
    st = _accessions(tmp_path, ("g/asm.fa", ("short_assembly", ["studyX.json"])))
    plan = st.plan(STUDY, [
        case("G", [st.sample("studyX.json", "g/asm.fa")], ("votu_table", [])),
        case("F", [st.sample("studyX.json", a) for a in ("s1.acc", "s2.acc")], ("votu_table", [])),
    ])
    assert plan.dropped_targets == [] and plan.dropped_samples == []
    assert served(plan, "fetch") == [{"F"}]
    assert all(set(s.cases) == {"F"} for s in plan.steps if s.transform.name in {"megahit", "hybrid"})
    (viral_id,) = steps_of(plan, "viral_id")
    assert "<given>" in producers(plan, viral_id, "assembly")
    assert served(plan, "votu") == [{"F", "G"}]


def test_a_study_whose_short_read_outcome_has_no_assembler_fails_and_names_it(tmp_path):
    st = _accessions(tmp_path)
    plan = st.plan({**FETCH_HYBRID, **HYBRID_ACC, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", a) for a in ("s1.acc", "s2.acc")], ("votu_table", [])),
    ])
    assert plan.steps == []
    (hint,) = [h for h in plan.hints if h.kind == "fork_branch"]
    assert hint.candidate_transforms == ["fetch"]
    assert "mock::short_reads" in hint.message and "mock::long_reads" not in hint.message
