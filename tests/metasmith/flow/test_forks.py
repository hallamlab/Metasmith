# A transform with several product groups is a fork: each group is one outcome.
# Every outcome becomes a case of its own, solved and merged into one plan,
# and the plan fails if any outcome cannot reach the target.

from __future__ import annotations

import pytest

from metasmith.models.workflow import case_merge
from metasmith.testing import mock_transforms as mt

from .test_cases import FLYE, MEGAHIT, VIRAL_ID, VOTU, Study, case, producers, steps_of

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
    assert fetch.produces[0][0] is fetch.produces[1][0]
    (viral_id,) = steps_of(plan, "viral_id")
    assert producers(plan, viral_id, "assembly") == {"megahit", "flye"}
    assert len(steps_of(plan, "votu")) == 1
    assert plan.cases == ["S"]
    assert {s for st_ in plan.steps for s in st_.cases} == {"S"}
    packed = plan.Pack()
    assert packed["streams"] and "cases" not in packed


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


def test_a_guide_that_reaches_the_target_solves_a_sibling_at_once(tmp_path, solves):
    st = _study(tmp_path)
    plan = st.plan({**FETCH_HYBRID, **MEGAHIT, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", "s1.acc")], ("votu_table", [])),
    ])
    assert plan.steps
    ((parent, first), (sibling, second)) = solves
    assert parent == {} and list(sibling.values()) == [1]
    assert second._iterations == 1
    assert [a.transform for a in second.dependency_plan[1:-1]] == [a.transform for a in first.dependency_plan[1:-1]]


def test_nested_forks_solve_every_combination_once(tmp_path, solves):
    st = _study(tmp_path)
    plan = st.plan({**FETCH_HYBRID, **SPLIT, **MEGAHIT, **VIRAL_ID, **VOTU}, [
        case("S", [st.sample("studyX.json", "s1.acc")], ("short_stats", []), ("votu_table", [])),
    ])
    assert plan.dropped_targets == []
    names = {t: t_.name for s in plan.steps for t, t_ in [(s.transform.model, s.transform)]}
    combos = sorted(tuple(sorted((names[t], g) for t, g in f.items())) for f, _ in solves)
    assert combos == [(), (("fetch", 1),), (("fetch", 1), ("split", 1)), (("split", 1),)]
    (split,) = steps_of(plan, "split")
    assert len([i for g in split.produces for i in g]) == 3
