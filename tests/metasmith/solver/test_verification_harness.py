from __future__ import annotations

import inspect
import io
import token
import tokenize

import pytest

from metasmith.models.solver import Application, Endpoint, Transform
from metasmith.testing import solver_verification as sv
from metasmith.testing.solver_verification import (
    GeneratorDials,
    SolverProblem,
    check_plan,
    exhaustive_solvable,
    forward_closure_solvable,
    generate_problem,
    plan_fingerprint,
    problem_of_plan,
)


def _micro_plan(*, anchor_props: set[str]):
    given_tr = Transform()
    d_root = given_tr.AddProduct(properties={"root"})
    d_side = given_tr.AddProduct(properties={"side"})
    e_root = Endpoint(properties={"root"})
    e_side = Endpoint(properties={"side"})
    given = Application(
        initial_timeline=0,
        transform=given_tr,
        used={},
        produced=[{d_root: e_root, d_side: e_side}],
    )

    maker = Transform()
    m_in = maker.AddRequirement(properties={"root"})
    m_out = maker.AddProduct(properties={"leaf"})
    e_leaf = Endpoint(properties={"leaf"})
    make = Application(
        initial_timeline=0,
        transform=maker,
        used={m_in: e_root},
        produced=[{m_out: e_leaf}],
    )

    target = Transform()
    anchor = target.AddRequirement(properties=set(anchor_props))
    leaf_dep = target.AddRequirement(properties={"leaf"}, parents={anchor})
    anchor_ep = e_root if "root" in anchor_props else e_side
    tail = Application(
        initial_timeline=0,
        transform=target,
        used={anchor: anchor_ep, leaf_dep: e_leaf},
        produced=[{}],
    )

    problem = SolverProblem(
        given=[{e_root, e_side}], transforms=[maker], target=target, name="micro"
    )
    return problem, [given, make, tail]


class _FakeSolution:
    def __init__(self, steps):
        self.dependency_plan = list(steps)


class TestFingerprint:
    def test_the_harness_never_reads_instance_id(self):
        source = inspect.getsource(sv)
        names = {
            tok.string
            for tok in tokenize.generate_tokens(io.StringIO(source).readline)
            if tok.type == token.NAME
        }
        assert "instance_id" not in names

    def test_is_stable_across_repeated_solves(self):
        a = generate_problem(11, GeneratorDials(n_types=6, n_extra_transforms=3))
        b = generate_problem(11, GeneratorDials(n_types=6, n_extra_transforms=3))
        assert plan_fingerprint(a.solve()) == plan_fingerprint(b.solve())

    def test_ignores_step_order(self):
        solution = generate_problem(
            12, GeneratorDials(n_types=6, n_extra_transforms=4)
        ).solve()
        forward = plan_fingerprint(solution)
        backward = plan_fingerprint(list(reversed(solution.dependency_plan)))
        assert forward == backward

    def test_separates_a_swapped_binding(self):
        tr = Transform()
        slot_a = tr.AddRequirement(properties={"a"})
        slot_b = tr.AddRequirement(properties={"b"})
        tr.AddProduct(properties={"out"})
        left = Endpoint(properties={"a", "b", "left"})
        right = Endpoint(properties={"a", "b", "right"})

        straight = Application(0, tr, {slot_a: left, slot_b: right}, [{}])
        swapped = Application(0, tr, {slot_a: right, slot_b: left}, [{}])
        assert plan_fingerprint([straight]) != plan_fingerprint([swapped])

    def test_separates_two_structurally_different_plans(self):
        short = generate_problem(1, GeneratorDials(n_types=4, n_extra_transforms=1))
        long = generate_problem(1, GeneratorDials(n_types=9, n_extra_transforms=1))
        assert plan_fingerprint(short.solve()) != plan_fingerprint(long.solve())

    def test_an_empty_plan_fingerprints_without_raising(self):
        assert plan_fingerprint([])


class TestChecker:
    def test_accepts_a_sound_plan(self):
        problem, steps = _micro_plan(anchor_props={"root"})
        verdict = check_plan(problem, _FakeSolution(steps))
        assert verdict.ok, verdict.violations

    def test_accepts_every_shipped_generator_shape(self):
        for name, seed, dials in _DIAL_MATRIX:
            problem = generate_problem(seed, dials, name=name)
            verdict = check_plan(problem, problem.solve())
            assert verdict.ok, f"[{name}] {verdict.violations}"

    def test_refuses_a_lineage_constraint_that_is_not_an_ancestor(self):
        problem, steps = _micro_plan(anchor_props={"side"})
        verdict = check_plan(problem, _FakeSolution(steps))
        assert not verdict.ok
        assert any("descended from" in v for v in verdict.violations), verdict.violations

    def test_refuses_an_input_no_step_produces(self):
        problem, steps = _micro_plan(anchor_props={"root"})
        tail = steps[-1]
        dep = next(d for d in tail.used if "leaf" in d.properties)
        tail.used[dep] = Endpoint(properties={"leaf", "smuggled"})
        verdict = check_plan(problem, _FakeSolution(steps))
        assert not verdict.ok
        assert any("no step produces" in v for v in verdict.violations), verdict.violations

    def test_refuses_a_binding_that_does_not_satisfy_its_slot(self):
        problem, steps = _micro_plan(anchor_props={"root"})
        make = steps[1]
        dep = next(iter(make.used))
        make.used[dep] = Endpoint(properties={"unrelated"})
        verdict = check_plan(problem, _FakeSolution(steps))
        assert not verdict.ok
        assert any("slot requiring" in v for v in verdict.violations), verdict.violations

    def test_refuses_a_cycle_between_two_steps(self):
        up = Transform()
        up_in = up.AddRequirement(properties={"b"})
        up_out = up.AddProduct(properties={"a"})
        down = Transform()
        down_in = down.AddRequirement(properties={"a"})
        down_out = down.AddProduct(properties={"b"})
        ea = Endpoint(properties={"a"})
        eb = Endpoint(properties={"b"})
        target = Transform()
        t_in = target.AddRequirement(properties={"a"})

        steps = [
            Application(0, up, {up_in: eb}, [{up_out: ea}]),
            Application(0, down, {down_in: ea}, [{down_out: eb}]),
            Application(0, target, {t_in: ea}, [{}]),
        ]
        problem = SolverProblem(given=[set()], transforms=[up, down], target=target)
        verdict = check_plan(problem, _FakeSolution(steps))
        assert not verdict.ok
        assert any("cycle" in v for v in verdict.violations), verdict.violations

    def test_refuses_a_plan_that_never_reaches_the_target(self):
        problem, steps = _micro_plan(anchor_props={"root"})
        verdict = check_plan(problem, _FakeSolution(steps[:-1]))
        assert not verdict.ok
        assert any("target transform" in v for v in verdict.violations), verdict.violations

    def test_refuses_a_step_that_consumes_a_later_step_s_output(self):
        problem, steps = _micro_plan(anchor_props={"root"})
        shuffled = [steps[0], steps[2], steps[1]]
        verdict = check_plan(problem, _FakeSolution(shuffled))
        assert not verdict.ok
        assert any("later step" in v for v in verdict.violations), verdict.violations

    def test_refuses_an_empty_plan(self):
        problem, _ = _micro_plan(anchor_props={"root"})
        assert not check_plan(problem, _FakeSolution([])).ok


class TestProblemOfPlan:
    def test_a_plan_without_its_problem_declines_rather_than_guesses(self):
        class _Bare:
            pass

        assert problem_of_plan(_Bare()) is None

    def test_the_reconstructed_problem_is_the_one_that_was_solved(self):
        source = generate_problem(3, GeneratorDials(n_types=6))

        class _Plan:
            _solver_inputs = (source.given, source.transforms, source.target)

        rebuilt = problem_of_plan(_Plan(), name="t")
        assert rebuilt is not None
        assert rebuilt.target is source.target
        assert [set(g) for g in rebuilt.given] == [set(g) for g in source.given]
        assert list(rebuilt.transforms) == list(source.transforms)
        assert check_plan(rebuilt, rebuilt.solve()).ok

    def test_generate_attaches_the_triple(self):
        from dataclasses import fields

        from metasmith.models.workflow.plan import WorkflowPlan

        assert "_solver_inputs" in {f.name for f in fields(WorkflowPlan)}


class TestOracles:
    def test_forward_closure_confirms_the_generated_spine_is_reachable(self):
        for seed in range(12):
            problem = generate_problem(
                seed, GeneratorDials(n_types=7, n_extra_transforms=4)
            )
            assert forward_closure_solvable(problem), f"seed {seed}"

    def test_forward_closure_refuses_an_unreachable_target(self):
        tr = Transform()
        tr.AddRequirement(properties={"a"})
        tr.AddProduct(properties={"b"})
        target = Transform()
        target.AddRequirement(properties={"unreachable"})
        problem = SolverProblem(
            given=[{Endpoint(properties={"a"})}], transforms=[tr], target=target
        )
        assert not forward_closure_solvable(problem)

    def test_exhaustive_agrees_with_forward_closure_where_lineage_is_absent(self):
        for seed in range(8):
            problem = generate_problem(
                seed, GeneratorDials(n_types=5, n_extra_transforms=2)
            )
            verdict = exhaustive_solvable(problem, max_applications=6)
            if verdict is None:
                continue
            assert verdict == forward_closure_solvable(problem), f"seed {seed}"

    def test_exhaustive_handles_a_lineage_constraint_the_closure_cannot(self):
        transforms = []
        for w in ("w0", "w1"):
            tr = Transform()
            tr.AddRequirement(properties={"root"})
            tr.AddProduct(properties={"mid", w})
            transforms.append(tr)
        target = Transform()
        anchor = target.AddRequirement(properties={"mid", "w0"})
        target.AddRequirement(properties={"mid", "w1"}, parents={anchor})
        problem = SolverProblem(
            given=[{Endpoint(properties={"root"})}],
            transforms=transforms,
            target=target,
        )
        assert forward_closure_solvable(problem)
        assert exhaustive_solvable(problem, max_applications=4) is False

    def test_exhaustive_says_unknown_rather_than_guessing(self):
        problem = generate_problem(
            3, GeneratorDials(n_types=14, n_extra_transforms=14)
        )
        assert exhaustive_solvable(problem, max_applications=3) is None


_DIAL_MATRIX: list[tuple[str, int, GeneratorDials]] = [
    ("plain", 21, GeneratorDials(n_types=6, n_extra_transforms=3)),
    ("cyclic", 22, GeneratorDials(n_types=7, n_extra_transforms=5, cycle_density=0.8)),
    (
        "lineage",
        23,
        GeneratorDials(
            n_types=7, n_extra_transforms=5, lineage_density=0.9, target_lineage=1.0
        ),
    ),
    (
        "duplicates",
        24,
        GeneratorDials(n_types=7, n_extra_transforms=3, n_duplicate_transforms=5),
    ),
    (
        "product-groups",
        25,
        GeneratorDials(n_types=7, n_extra_transforms=4, product_group_density=0.9),
    ),
    (
        "multi-given",
        26,
        GeneratorDials(n_types=7, n_given=2, n_given_groups=3, n_extra_transforms=4),
    ),
]


class TestGenerator:
    def test_one_seed_gives_one_problem(self):
        dials = GeneratorDials(n_types=7, n_extra_transforms=5, lineage_density=0.7)
        a = generate_problem(99, dials)
        b = generate_problem(99, dials)
        assert [str(t) for t in a.transforms] == [str(t) for t in b.transforms]
        assert str(a.target) == str(b.target)
        assert plan_fingerprint(a.solve()) == plan_fingerprint(b.solve())

    def test_different_seeds_give_different_problems(self):
        dials = GeneratorDials(n_types=8, n_extra_transforms=6, lineage_density=0.7)
        shapes = {
            tuple(sorted(str(t) for t in generate_problem(s, dials).transforms))
            for s in range(6)
        }
        assert len(shapes) > 1

    def test_duplicate_transforms_really_share_a_key(self):
        problem = generate_problem(
            31, GeneratorDials(n_types=6, n_extra_transforms=2, n_duplicate_transforms=3)
        )
        keys = [t.key for t in problem.transforms]
        assert len(keys) != len(set(keys))

    def test_cycle_density_puts_a_cycle_in_the_transform_graph(self):
        problem = generate_problem(
            32, GeneratorDials(n_types=6, n_extra_transforms=6, cycle_density=1.0)
        )
        produced = {p for t in problem.transforms for g in t.produces for p in g}
        feedback = [
            t
            for t in problem.transforms
            for g in t.produces
            for p in g
            if any(p.properties == r.properties for r in t.requires)
        ]
        assert produced and feedback, "no transform feeds its own input type"

    @pytest.mark.parametrize("seed", range(24))
    def test_a_generated_corpus_solves_soundly(self, seed):
        name, _, dials = _DIAL_MATRIX[seed % len(_DIAL_MATRIX)]
        problem = generate_problem(seed, dials, name=f"{name}-{seed}")
        verdict = check_plan(problem, problem.solve())
        assert verdict.ok, f"[{problem.name}] {verdict.violations}"
