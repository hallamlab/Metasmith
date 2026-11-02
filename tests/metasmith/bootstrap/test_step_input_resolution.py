"""The seam between the ids the compiler writes and the ids the agent reloads.

No other test in this suite crosses it. The virtual runtime writes its own `din` rather
than the compiler's, the container end-to-end test runs the stub lane which never reaches
this code, and the round-trip unit test compares one object against itself. So a step
whose input was produced by another step in the same plan -- the only shape that triggers
the mismatch -- has never been exercised.
"""
from __future__ import annotations

import pytest

from metasmith.bootstrap import resolve_step_inputs
from metasmith.models.lineage import ArityMismatchError, MissingInstanceError


class _Dep:
    def __init__(self, key):
        self.key = key


class _Model:
    def __init__(self, requires):
        self.requires = requires


class _Transform:
    def __init__(self, requires):
        self.model = _Model(requires)


class _Step:
    def __init__(self, requires, dependency_map):
        self.transform = _Transform(requires)
        self.dependency_map = dependency_map


def _step(dep_key, instances):
    dep = _Dep(dep_key)
    return _Step([dep], {dep: instances}), dep


def test_an_id_that_resolves_is_used_as_written():
    step, dep = _step("A", ["from-the-map"])
    got = resolve_step_inputs(step, {"A": ["slot-1"]}, {"slot-1": "from-lookup"}, {})
    assert got[dep] == ["from-lookup"]


def test_a_slot_id_the_reloaded_plan_never_saw_falls_back_to_the_step_s_own_map():
    # The compile pass stamps the producer's slot id onto the consumer's instance and
    # writes `din` from it. The plan on disk still names the archetype, so the lookup
    # this agent builds cannot contain that id -- and the dependency map is the very
    # list `din` was written from.
    step, dep = _step("A", ["produced-upstream"])
    got = resolve_step_inputs(step, {"A": ["slot-nobody-has"]}, {"archetype": "x"}, {})
    assert got[dep] == ["produced-upstream"]


def test_it_still_raises_when_there_is_nothing_to_fall_back_to():
    step, dep = _step("A", [])
    with pytest.raises(MissingInstanceError):
        resolve_step_inputs(step, {"A": ["slot-nobody-has"]}, {}, {})


def test_a_dependency_with_no_ids_written_stays_empty_rather_than_raising():
    step, dep = _step("A", ["something"])
    assert resolve_step_inputs(step, {}, {}, {})[dep] == []


def test_the_arity_check_still_applies_to_a_fallback():
    # The fallback must not become a way past the preflight: it is the same list the
    # arity was measured on, so a disagreement is still a disagreement.
    step, dep = _step("A", ["one", "two"])
    with pytest.raises(ArityMismatchError):
        resolve_step_inputs(step, {"A": ["slot-nobody-has"]}, {}, {"A": 1})
    assert resolve_step_inputs(step, {"A": ["slot-nobody-has"]}, {}, {"A": 2})[dep] == \
        ["one", "two"]
