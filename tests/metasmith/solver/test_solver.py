import pytest
from metasmith.models.solver import solve_by_mcts, Transform, Endpoint, Application
from metasmith.testing.solver_verification import (
    SolverProblem, check_plan, exhaustive_solvable,
)


def _solve(given, target, transforms, **kwargs):
    problem = SolverProblem(given=given, transforms=transforms, target=target)
    sol = problem.solve(**kwargs)
    verdict = check_plan(problem, sol)
    assert verdict.ok, f"solver returned an unrunnable plan: {verdict.violations}"
    return sol


class TestBasicSolver:
    def test_trivial_given_match(self):
        transforms = []
        t = Transform()
        t.AddRequirement(properties={"x"})
        t.AddProduct(properties={"y"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"a"})
        t.AddProduct(properties={"b"})
        transforms.append(t)

        have = {
            Endpoint(properties={"given"})
        }

        target = Transform()
        target.AddRequirement(properties={"given"})
        sol = _solve(given=[have], target=target, transforms=transforms)
        assert sol.complete

    def test_simple_chain(self):
        transforms = []
        t = Transform()
        t.AddRequirement(properties={"assembly"})
        t.AddProduct(properties={"bins"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"bins"})
        t.AddProduct(properties={"tax"})
        transforms.append(t)

        have = {
            Endpoint(properties={"assembly"}),
        }

        target = Transform()
        a = target.AddRequirement(properties={"bins"})
        target.AddRequirement(properties={"tax"}, parents={a})
        sol = _solve(given=[have], target=target, transforms=transforms)
        assert sol.complete

    def test_multiple_given_groups(self):
        transforms = []
        t = Transform()
        t.AddRequirement(properties={"a"})
        t.AddProduct(properties={"x"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"b"})
        t.AddProduct(properties={"x"})
        transforms.append(t)

        target = Transform()
        target.AddRequirement(properties={"x"})
        sol = _solve(given=[
            {
                Endpoint(properties={"a"}),
            },
            {
                Endpoint(properties={"b"}),
            },
        ], target=target, transforms=transforms)
        assert sol.complete

    def test_application_signature_is_dependency_ordered(self):
        tr = Transform()
        dep_a = tr.AddRequirement(properties={"a"})
        dep_b = tr.AddRequirement(properties={"b"})
        tr.AddProduct(properties={"out"})

        ep_a = Endpoint(properties={"input_a"})
        ep_b = Endpoint(properties={"input_b"})
        app1 = Application(
            initial_timeline=0,
            transform=tr,
            used={dep_a: ep_a, dep_b: ep_b},
            produced=[{}],
        )
        app2 = Application(
            initial_timeline=0,
            transform=tr,
            used={dep_a: ep_b, dep_b: ep_a},
            produced=[{}],
        )
        assert app1.Signature() != app2.Signature()


def _loop_problem() -> SolverProblem:
    transforms = []
    for req, prod in [
        ("start", "a"), ("a", "b"), ("b", "a"),
        ("b", "c"), ("c", "b"), ("c", "target"),
    ]:
        t = Transform()
        t.AddRequirement(properties={req})
        t.AddProduct(properties={prod})
        transforms.append(t)
    target = Transform()
    a = target.AddRequirement(properties={"a"})
    target.AddRequirement(properties={"target"}, parents={a})
    return SolverProblem(
        given=[{Endpoint(properties={"c"})}], transforms=transforms, target=target,
    )


class TestCircularDependencies:
    def test_a_cyclic_search_terminates_and_says_so(self):
        problem = _loop_problem()
        sol = problem.solve()
        assert not sol.complete, (
            "the search exhausted its budget; saying otherwise is what let a "
            "256-step non-plan reach WorkflowPlan.Generate"
        )
        assert not any(s.transform is problem.target for s in sol.dependency_plan)

    def test_the_search_budget_is_what_bounds_a_cyclic_walk(self):
        for budget in (32, 64):
            sol = _loop_problem().solve(max_iter=budget)
            assert len(sol.dependency_plan) == budget

    @pytest.mark.xfail(
        strict=True,
        reason="the search cannot solve a solvable 6-transform cyclic problem",
    )
    def test_a_cyclic_graph_still_gets_the_plan_the_oracle_proves_exists(self):
        problem = _loop_problem()
        assert exhaustive_solvable(problem, max_applications=5) is True
        sol = problem.solve()
        assert check_plan(problem, sol).ok


class TestBranching:
    def test_branching_basic(self):
        transforms = []

        tr = Transform()
        tr.AddRequirement(properties={"start"})
        tr.AddProduct(properties={"x"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"x"})
        tr.AddProduct(properties={"a"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"b"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"a"})
        tr.AddProduct(properties={"a2"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"a2"})
        tr.AddProduct(properties={"y", "a"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"b"})
        tr.AddProduct(properties={"b2"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"b2"})
        tr.AddProduct(properties={"y", "b"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"y"})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        given = {Endpoint(properties={"start"})}
        target = Transform()
        target.AddRequirement(properties={"target"})
        sol = _solve(
            given=[given],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_nested(self):
        transforms = []

        tr = Transform()
        tr.AddRequirement(properties={"start"})
        tr.AddProduct(properties={"x"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"x"})
        tr.AddProduct(properties={"a"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"b"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"a"})
        tr.AddProduct(properties={"a2"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"a2"})
        tr.AddProduct(properties={"y", "a"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"b"})
        tr.AddProduct(properties={"b2"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"b"})
        tr.AddProduct(properties={"f"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"g"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"f"})
        tr.AddProduct(properties={"y", "yf"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"g"})
        tr.AddProduct(properties={"g2"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"g2"})
        tr.AddProduct(properties={"y", "g"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"y"})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        given = {Endpoint(properties={"start"})}
        target = Transform()
        target.AddRequirement(properties={"target"})
        sol = _solve(
            given=[given],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_with_lineage(self):
        transforms = []

        tr = Transform()
        tr.AddRequirement(properties={"start"})
        tr.AddProduct(properties={"x"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"x"})
        tr.AddProduct(properties={"a", "v"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"b", "v"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"v"})
        tr.AddProduct(properties={"z"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"z"})
        tr.AddProduct(properties={"z2"})
        transforms.append(tr)

        tr = Transform()
        dep = tr.AddRequirement(properties={"z"})
        tr.AddRequirement(properties={"z2"}, parents={dep})
        tr.AddProduct(properties={"z3"})
        transforms.append(tr)

        tr = Transform()
        dep = tr.AddRequirement(properties={"b"})
        tr.AddRequirement(properties={"z3"}, parents={dep})
        tr.AddProduct(properties={"y"})
        transforms.append(tr)

        tr = Transform()
        dep = tr.AddRequirement(properties={"a"})
        tr.AddRequirement(properties={"z3"}, parents={dep})
        tr.AddProduct(properties={"y"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"y"})
        tr.AddProduct(properties={"y2"})
        transforms.append(tr)

        tr = Transform()
        dep = tr.AddRequirement(properties={"y"})
        tr.AddRequirement(properties={"y2"}, parents={dep})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        given = {Endpoint(properties={"start"})}
        target = Transform()
        target.AddRequirement(properties={"target"})
        sol = _solve(
            given=[given],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_multiple_given(self):
        transforms = []

        tr = Transform()
        tr.AddRequirement(properties={"start"})
        tr.AddProduct(properties={"x", "a"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"x", "b"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"a"})
        tr.AddProduct(properties={"b"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"b"})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        target = Transform()
        target.AddRequirement(properties={"b"})
        sol = _solve(
            given=[
                {Endpoint(properties={"a"})},
                {Endpoint(properties={"b"})},
            ],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_overlapping_groups(self):
        transforms = []

        tr = Transform()
        tr.AddRequirement(properties={"start"})
        tr.AddProduct(properties={"x", "a"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"x", "a", "b"})
        transforms.append(tr)
        tr.NewProductGroup()
        tr.AddProduct(properties={"x", "b"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"a"})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"b"})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        target = Transform()
        target.AddRequirement(properties={"target"})
        sol = _solve(
            given=[
                {Endpoint(properties={"start"})},
            ],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_complex_workflow(self):
        transforms = []

        tr = Transform()
        tr.AddRequirement(properties={"start"})
        tr.AddProduct(properties={"read_meta"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"read_meta"})
        tr.AddProduct(properties={"sra"})
        transforms.append(tr)

        tr = Transform()
        x = tr.AddRequirement(properties={"read_meta"})
        tr.AddRequirement(properties={"sra"}, parents={x})
        tr.AddProduct(properties={"reads", "long", "single"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"reads", "short", "single"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"reads", "short", "paired"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"reads", "short"})
        tr.AddProduct(properties={"read_qc"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"reads", "long"})
        tr.AddProduct(properties={"read_qc"})
        transforms.append(tr)

        tr = Transform()
        tr.AddRequirement(properties={"reads", "long"})
        tr.AddProduct(properties={"clean_reads", "long"})
        transforms.append(tr)

        tr = Transform()
        meta = tr.AddRequirement(properties={"read_meta"})
        reads = tr.AddRequirement(properties={"reads", "short"}, parents={meta})
        tr.AddRequirement(properties={"read_qc"}, parents={reads})
        tr.AddProduct(properties={"clean_reads", "short"})
        transforms.append(tr)

        tr = Transform()
        x = tr.AddRequirement(properties={"reads", "long"})
        tr.AddRequirement(properties={"clean_reads", "long"}, parents={x})
        tr.AddRequirement(properties={"read_qc"}, parents={x})
        tr.AddProduct(properties={"assembly"})
        transforms.append(tr)

        tr = Transform()
        meta = tr.AddRequirement(properties={"read_meta"})
        tr.AddRequirement(properties={"clean_reads", "short"}, parents={meta})
        tr.AddProduct(properties={"assembly"})
        transforms.append(tr)

        tr = Transform()
        meta = tr.AddRequirement(properties={"read_meta"})
        tr.AddRequirement(properties={"assembly"}, parents={meta})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        tr = Transform()
        meta = tr.AddRequirement(properties={"read_meta"})
        tr.AddRequirement(properties={"clean_reads"}, parents={meta})
        tr.AddRequirement(properties={"read_qc"}, parents={meta})
        tr.AddRequirement(properties={"assembly"}, parents={meta})
        tr.AddProduct(properties={"assembly_stats"})
        transforms.append(tr)

        target = Transform()
        target.AddRequirement(properties={"assembly_stats"})
        sol = _solve(
            given=[
                {Endpoint(properties={"start"})},
            ],
            target=target,
            transforms=transforms,
        )
        assert sol.complete


def _get_all_produced(sol) -> set[frozenset[str]]:
    produced = set()
    for app in sol.dependency_plan:
        for pgroup in app.produced:
            for endpoint in pgroup.values():
                produced.add(frozenset(endpoint.properties))
    return produced


class TestGivenLineage:
    def test_given_with_immediate_parent(self):
        transforms = []

        t = Transform()
        parent_dep = t.AddRequirement(properties={"parent"})
        t.AddRequirement(properties={"child"}, parents={parent_dep})
        t.AddProduct(properties={"output"})
        transforms.append(t)

        parent_ep = Endpoint(properties={"parent"})
        child_ep = Endpoint(properties={"child"}, parents={parent_ep})

        target = Transform()
        target.AddRequirement(properties={"output"})
        sol = _solve(
            given=[{parent_ep, child_ep}],
            target=target,
            transforms=transforms
        )
        assert sol.complete

        produced = _get_all_produced(sol)
        assert frozenset({"output"}) in produced

    def test_given_with_grandparent_lineage(self):
        transforms = []

        t = Transform()
        gp_dep = t.AddRequirement(properties={"grandparent"})
        p_dep = t.AddRequirement(properties={"parent"}, parents={gp_dep})
        t.AddRequirement(properties={"child"}, parents={p_dep})
        t.AddProduct(properties={"output"})
        transforms.append(t)

        gp_ep = Endpoint(properties={"grandparent"})
        p_ep = Endpoint(properties={"parent"}, parents={gp_ep})
        child_ep = Endpoint(properties={"child"}, parents={p_ep})

        target = Transform()
        target.AddRequirement(properties={"output"})
        sol = _solve(
            given=[{gp_ep, p_ep, child_ep}],
            target=target,
            transforms=transforms
        )
        assert sol.complete

        produced = _get_all_produced(sol)
        assert frozenset({"output"}) in produced

    def test_given_fails_lineage_falls_back_to_produced(self):
        interleave = Transform()
        interleave.AddRequirement(properties={"meta"})
        pair_in = interleave.AddRequirement(properties={"pair"})
        interleave.AddRequirement(properties={"r1"}, parents={pair_in})
        interleave.AddRequirement(properties={"r2"}, parents={pair_in})
        interleave.AddProduct(properties={"reads", "short_reads"})

        consumer = Transform()
        meta_in = consumer.AddRequirement(properties={"meta"})
        consumer.AddRequirement(properties={"reads"}, parents={meta_in})
        consumer.AddProduct(properties={"bam"})

        meta_ep = Endpoint(properties={"meta"})
        pair_ep = Endpoint(properties={"pair"})
        r1_ep = Endpoint(properties={"r1", "reads"}, parents={pair_ep})
        r2_ep = Endpoint(properties={"r2", "reads"}, parents={pair_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = _solve(
            given=[{meta_ep, pair_ep, r1_ep, r2_ep}],
            target=target,
            transforms=[interleave, consumer],
        )
        assert sol.complete
        plan_transforms = {a.transform for a in sol.dependency_plan}
        assert interleave in plan_transforms, \
            "interleave dropped — produced short_reads alternative not considered"
        assert consumer in plan_transforms, \
            "consumer dropped — bam target not in plan"

        produced = _get_all_produced(sol)
        assert frozenset({"bam"}) in produced

    def test_binning_workflow_all_three_binners(self):
        transforms = []

        t = Transform()
        meta = t.AddRequirement(properties={"read_metadata"})
        reads = t.AddRequirement(properties={"reads"}, parents={meta})
        t.AddRequirement(properties={"assembly"}, parents={reads})
        t.AddProduct(properties={"bam"})
        transforms.append(t)

        t = Transform()
        asm = t.AddRequirement(properties={"assembly"})
        t.AddRequirement(properties={"bam"}, parents={asm})
        t.AddProduct(properties={"bins", "method:metabat2"})
        transforms.append(t)

        t = Transform()
        asm = t.AddRequirement(properties={"assembly"})
        t.AddRequirement(properties={"bam"}, parents={asm})
        t.AddProduct(properties={"bins", "method:maxbin2"})
        transforms.append(t)

        t = Transform()
        asm = t.AddRequirement(properties={"assembly"})
        t.AddRequirement(properties={"bam"}, parents={asm})
        t.AddProduct(properties={"bins", "method:concoct"})
        transforms.append(t)

        meta_ep = Endpoint(properties={"read_metadata"})
        reads_ep = Endpoint(properties={"reads"}, parents={meta_ep})
        asm_ep = Endpoint(properties={"assembly"}, parents={reads_ep})

        target = Transform()
        target.AddRequirement(properties={"bins", "method:metabat2"})
        target.AddRequirement(properties={"bins", "method:maxbin2"})
        target.AddRequirement(properties={"bins", "method:concoct"})

        sol = _solve(
            given=[{meta_ep, reads_ep, asm_ep}],
            target=target,
            transforms=transforms
        )
        assert sol.complete

        produced = _get_all_produced(sol)
        assert frozenset({"bins", "method:metabat2"}) in produced
        assert frozenset({"bins", "method:maxbin2"}) in produced
        assert frozenset({"bins", "method:concoct"}) in produced

        assert frozenset({"bam"}) in produced

        assert len(sol.dependency_plan) == 6

    def test_input_rooted_multi_hop_lineage(self):
        transforms = []

        t = Transform()
        meta = t.AddRequirement(properties={"meta"})
        reads = t.AddRequirement(properties={"reads"}, parents={meta})
        t.AddRequirement(properties={"assembly"}, parents={reads})
        t.AddProduct(properties={"bam"})
        transforms.append(t)

        meta_ep = Endpoint(properties={"meta"})
        pair_ep = Endpoint(properties={"pair"}, parents={meta_ep})
        r1_ep = Endpoint(properties={"r1", "reads"}, parents={pair_ep})
        asm_ep = Endpoint(properties={"assembly"}, parents={r1_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = _solve(
            given=[{meta_ep, pair_ep, r1_ep, asm_ep}],
            target=target,
            transforms=transforms,
        )
        assert sol.complete
        produced = _get_all_produced(sol)
        assert frozenset({"bam"}) in produced


class TestMultiSampleBinningWorkflow:
    def test_given_stats_with_lineage_prevents_seqkit(self):
        seqkit = Transform()
        seqkit.AddRequirement(properties={"reads"})
        seqkit.AddProduct(properties={"read_qc_stats"})

        asm_stats = Transform()
        meta = asm_stats.AddRequirement(properties={"read_metadata"})
        reads = asm_stats.AddRequirement(properties={"reads"}, parents={meta})
        asm_stats.AddRequirement(properties={"read_qc_stats"}, parents={reads})
        asm_stats.AddRequirement(properties={"assembly"}, parents={reads})
        asm_stats.AddProduct(properties={"bam"})

        meta_ep = Endpoint(properties={"read_metadata"})
        reads_ep = Endpoint(properties={"reads"}, parents={meta_ep})
        stats_ep = Endpoint(properties={"read_qc_stats"}, parents={reads_ep})
        asm_ep = Endpoint(properties={"assembly"}, parents={reads_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = _solve(
            given=[{meta_ep, reads_ep, stats_ep, asm_ep}],
            target=target,
            transforms=[seqkit, asm_stats]
        )
        assert sol.complete

        plan_transforms = {app.transform for app in sol.dependency_plan}
        assert seqkit not in plan_transforms, \
            "seqkit included despite read_qc_stats with lineage being given"

    def test_mixed_samples_all_have_stats_with_lineage(self):
        seqkit = Transform()
        seqkit.AddRequirement(properties={"reads"})
        seqkit.AddProduct(properties={"read_qc_stats"})

        asm_stats = Transform()
        meta = asm_stats.AddRequirement(properties={"read_metadata"})
        reads = asm_stats.AddRequirement(properties={"reads"}, parents={meta})
        asm_stats.AddRequirement(properties={"read_qc_stats"}, parents={reads})
        asm_stats.AddRequirement(properties={"assembly"}, parents={reads})
        asm_stats.AddProduct(properties={"bam"})

        meta_a = Endpoint(properties={"read_metadata"})
        reads_a = Endpoint(properties={"reads", "read_length:long"}, parents={meta_a})
        stats_a = Endpoint(properties={"read_qc_stats"}, parents={reads_a})
        asm_a = Endpoint(properties={"assembly"}, parents={reads_a})

        meta_b = Endpoint(properties={"read_metadata"})
        reads_b = Endpoint(properties={"reads", "read_length:short"}, parents={meta_b})
        stats_b = Endpoint(properties={"read_qc_stats"}, parents={reads_b})
        asm_b = Endpoint(properties={"assembly"}, parents={reads_b})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = _solve(
            given=[
                {meta_a, reads_a, stats_a, asm_a},
                {meta_b, reads_b, stats_b, asm_b},
            ],
            target=target,
            transforms=[seqkit, asm_stats]
        )
        assert sol.complete

        plan_transforms = {app.transform for app in sol.dependency_plan}
        assert seqkit not in plan_transforms, \
            "seqkit included for mixed samples despite all having read_qc_stats with lineage"

    def test_stats_without_lineage_triggers_seqkit(self):
        seqkit = Transform()
        seqkit.AddRequirement(properties={"reads"})
        seqkit.AddProduct(properties={"read_qc_stats"})

        asm_stats = Transform()
        meta = asm_stats.AddRequirement(properties={"read_metadata"})
        reads = asm_stats.AddRequirement(properties={"reads"}, parents={meta})
        asm_stats.AddRequirement(properties={"read_qc_stats"}, parents={reads})
        asm_stats.AddRequirement(properties={"assembly"}, parents={reads})
        asm_stats.AddProduct(properties={"bam"})

        meta_ep = Endpoint(properties={"read_metadata"})
        reads_ep = Endpoint(properties={"reads"}, parents={meta_ep})
        stats_ep = Endpoint(properties={"read_qc_stats"})
        asm_ep = Endpoint(properties={"assembly"}, parents={reads_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = _solve(
            given=[{meta_ep, reads_ep, stats_ep, asm_ep}],
            target=target,
            transforms=[seqkit, asm_stats]
        )
        assert sol.complete

        plan_transforms = {app.transform for app in sol.dependency_plan}
        assert seqkit in plan_transforms, \
            "seqkit not included even though read_qc_stats lacks lineage"
