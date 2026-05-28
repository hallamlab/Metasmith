"""Tests for the MCTS workflow solver.

Converted from main/workflow_solver/branching_test.py
"""

import pytest
from metasmith.models.solver import solve_by_mcts, Transform, Endpoint, Application


class TestBasicSolver:
    """Tests for basic solver functionality."""

    def test_trivial_given_match(self):
        """Given data matches target directly - simplest case."""
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
        sol = solve_by_mcts(given=[have], target=target, transforms=transforms)
        assert sol.complete

    def test_simple_chain(self):
        """Linear chain: assembly -> bins -> tax."""
        transforms = []
        t = Transform()  # assembly -> bins
        t.AddRequirement(properties={"assembly"})
        t.AddProduct(properties={"bins"})
        transforms.append(t)

        t = Transform()  # bins -> tax
        t.AddRequirement(properties={"bins"})
        t.AddProduct(properties={"tax"})
        transforms.append(t)

        have = {
            Endpoint(properties={"assembly"}),
        }

        target = Transform()
        a = target.AddRequirement(properties={"bins"})
        target.AddRequirement(properties={"tax"}, parents={a})
        sol = solve_by_mcts(given=[have], target=target, transforms=transforms)
        assert sol.complete

    def test_multiple_given_groups(self):
        """Multiple given groups can independently satisfy requirements."""
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
        sol = solve_by_mcts(given=[
            {
                Endpoint(properties={"a"}),
            },
            {
                Endpoint(properties={"b"}),
            },
        ], target=target, transforms=transforms)
        assert sol.complete

    def test_application_signature_is_dependency_ordered(self):
        """Application signatures should differ when dependency bindings swap."""
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


class TestCircularDependencies:
    """Tests for handling circular dependencies in transforms."""

    def test_loop_handling(self):
        """Solver handles circular dependencies without infinite loops."""
        transforms = []
        t = Transform()
        t.AddRequirement(properties={"start"})
        t.AddProduct(properties={"a"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"a"})
        t.AddProduct(properties={"b"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"b"})
        t.AddProduct(properties={"a"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"b"})
        t.AddProduct(properties={"c"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"c"})
        t.AddProduct(properties={"b"})
        transforms.append(t)

        t = Transform()
        t.AddRequirement(properties={"c"})
        t.AddProduct(properties={"target"})
        transforms.append(t)

        target = Transform()
        a = target.AddRequirement(properties={"a"})
        target.AddRequirement(properties={"target"}, parents={a})
        sol = solve_by_mcts(given=[
            {
                Endpoint(properties={"c"}),
            },
        ], target=target, transforms=transforms)
        assert sol.complete


class TestBranching:
    """Tests for branching workflow scenarios."""

    def test_branching_basic(self):
        """Basic branching: x -> a/b with separate paths to target."""
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
        sol = solve_by_mcts(
            given=[given],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_nested(self):
        """Nested branching: x -> a/b, then b -> f/g."""
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
        sol = solve_by_mcts(
            given=[given],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_with_lineage(self):
        """Branching with lineage constraints within branches."""
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

        # Used by both branches, identical signatures,
        # but can't merge due to lineage
        tr = Transform()
        tr.AddRequirement(properties={"z"})
        tr.AddProduct(properties={"z2"})
        transforms.append(tr)

        # Lineage constraint contained within branch
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

        # Used by both branches, identical signatures,
        # but CAN merge despite lineage
        tr = Transform()
        dep = tr.AddRequirement(properties={"y"})
        tr.AddRequirement(properties={"y2"}, parents={dep})
        tr.AddProduct(properties={"target"})
        transforms.append(tr)

        given = {Endpoint(properties={"start"})}
        target = Transform()
        target.AddRequirement(properties={"target"})
        sol = solve_by_mcts(
            given=[given],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_multiple_given(self):
        """Branching with multiple given inputs."""
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
        sol = solve_by_mcts(
            given=[
                {Endpoint(properties={"a"})},
                {Endpoint(properties={"b"})},
            ],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_overlapping_groups(self):
        """Product groups with overlapping properties."""
        transforms = []

        tr = Transform()
        tr.AddRequirement(properties={"start"})
        tr.AddProduct(properties={"x", "a"})
        tr.NewProductGroup()
        tr.AddProduct(properties={"x", "a", "b"})  # Overlapping groupings
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
        sol = solve_by_mcts(
            given=[
                {Endpoint(properties={"start"})},
            ],
            target=target,
            transforms=transforms,
        )
        assert sol.complete

    def test_branching_complex_workflow(self):
        """Full reads -> assembly pipeline with multiple read types."""
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
        sol = solve_by_mcts(
            given=[
                {Endpoint(properties={"start"})},
            ],
            target=target,
            transforms=transforms,
        )
        assert sol.complete


def _get_all_produced(sol) -> set[frozenset[str]]:
    """Extract all produced endpoint properties from a solution."""
    produced = set()
    for app in sol.dependency_plan:
        for pgroup in app.produced:
            for endpoint in pgroup.values():
                produced.add(frozenset(endpoint.properties))
    return produced


class TestGivenLineage:
    """Tests for proper handling of given endpoint lineage."""

    def test_given_with_immediate_parent(self):
        """Given endpoint with immediate parent satisfies lineage constraint."""
        transforms = []

        # Transform: requires parent, produces child with lineage
        t = Transform()
        parent_dep = t.AddRequirement(properties={"parent"})
        t.AddRequirement(properties={"child"}, parents={parent_dep})
        t.AddProduct(properties={"output"})
        transforms.append(t)

        # Given: child with parent in lineage
        parent_ep = Endpoint(properties={"parent"})
        child_ep = Endpoint(properties={"child"}, parents={parent_ep})

        target = Transform()
        target.AddRequirement(properties={"output"})
        sol = solve_by_mcts(
            given=[{parent_ep, child_ep}],
            target=target,
            transforms=transforms
        )
        assert sol.complete

        # Verify output was produced
        produced = _get_all_produced(sol)
        assert frozenset({"output"}) in produced

    def test_given_with_grandparent_lineage(self):
        """Given endpoint with grandparent lineage is properly tracked."""
        transforms = []

        # Transform: grandparent -> parent -> child, requires full lineage
        t = Transform()
        gp_dep = t.AddRequirement(properties={"grandparent"})
        p_dep = t.AddRequirement(properties={"parent"}, parents={gp_dep})
        t.AddRequirement(properties={"child"}, parents={p_dep})
        t.AddProduct(properties={"output"})
        transforms.append(t)

        # Given: child -> parent -> grandparent lineage chain
        gp_ep = Endpoint(properties={"grandparent"})
        p_ep = Endpoint(properties={"parent"}, parents={gp_ep})
        child_ep = Endpoint(properties={"child"}, parents={p_ep})

        target = Transform()
        target.AddRequirement(properties={"output"})
        sol = solve_by_mcts(
            given=[{gp_ep, p_ep, child_ep}],
            target=target,
            transforms=transforms
        )
        assert sol.complete

        # Verify output was produced
        produced = _get_all_produced(sol)
        assert frozenset({"output"}) in produced

    def test_given_fails_lineage_falls_back_to_produced(self):
        """When given candidates fail _satisfies_lineage even after walking
        ancestors transitively, a produced alternative that pulls the needed
        ancestor into its flattened lineage must still be considered.
        Regression for the prefer-given short-circuit in _find_endpoints."""
        # interleave: meta + pair + r1 (parent=pair) + r2 (parent=pair) -> short_reads
        # interleave consumes meta directly, so its produced short_reads will
        # carry meta in its flattened lineage.
        interleave = Transform()
        interleave.AddRequirement(properties={"meta"})
        pair_in = interleave.AddRequirement(properties={"pair"})
        interleave.AddRequirement(properties={"r1"}, parents={pair_in})
        interleave.AddRequirement(properties={"r2"}, parents={pair_in})
        interleave.AddProduct(properties={"reads", "short_reads"})

        # consumer: meta -> reads(parent=meta) -> bam
        consumer = Transform()
        meta_in = consumer.AddRequirement(properties={"meta"})
        consumer.AddRequirement(properties={"reads"}, parents={meta_in})
        consumer.AddProduct(properties={"bam"})

        # Given: meta (orphan), pair (orphan), r1/r2 (parent=pair).
        # r1/r2 are IsA(reads), but their full ancestor walk does NOT reach
        # meta — pair has no parent. So consumer cannot bind reads to a
        # given endpoint; it must use interleave's produced short_reads,
        # which DOES include meta in its lineage.
        meta_ep = Endpoint(properties={"meta"})
        pair_ep = Endpoint(properties={"pair"})
        r1_ep = Endpoint(properties={"r1", "reads"}, parents={pair_ep})
        r2_ep = Endpoint(properties={"r2", "reads"}, parents={pair_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = solve_by_mcts(
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
        """Simulates binning workflow with 3 binners: metabat2, maxbin2, concoct."""
        transforms = []

        # assembly_stats: meta -> reads -> assembly, produces bam
        t = Transform()
        meta = t.AddRequirement(properties={"read_metadata"})
        reads = t.AddRequirement(properties={"reads"}, parents={meta})
        t.AddRequirement(properties={"assembly"}, parents={reads})
        t.AddProduct(properties={"bam"})
        transforms.append(t)

        # metabat2: assembly + bam (with asm lineage) -> bins
        t = Transform()
        asm = t.AddRequirement(properties={"assembly"})
        t.AddRequirement(properties={"bam"}, parents={asm})
        t.AddProduct(properties={"bins", "method:metabat2"})
        transforms.append(t)

        # maxbin2: assembly + bam (with asm lineage) -> bins
        t = Transform()
        asm = t.AddRequirement(properties={"assembly"})
        t.AddRequirement(properties={"bam"}, parents={asm})
        t.AddProduct(properties={"bins", "method:maxbin2"})
        transforms.append(t)

        # concoct: assembly + bam (with asm lineage) -> bins
        t = Transform()
        asm = t.AddRequirement(properties={"assembly"})
        t.AddRequirement(properties={"bam"}, parents={asm})
        t.AddProduct(properties={"bins", "method:concoct"})
        transforms.append(t)

        # Given: full lineage chain
        meta_ep = Endpoint(properties={"read_metadata"})
        reads_ep = Endpoint(properties={"reads"}, parents={meta_ep})
        asm_ep = Endpoint(properties={"assembly"}, parents={reads_ep})

        # Target: all three binners
        target = Transform()
        target.AddRequirement(properties={"bins", "method:metabat2"})
        target.AddRequirement(properties={"bins", "method:maxbin2"})
        target.AddRequirement(properties={"bins", "method:concoct"})

        sol = solve_by_mcts(
            given=[{meta_ep, reads_ep, asm_ep}],
            target=target,
            transforms=transforms
        )
        assert sol.complete

        # Verify all three binners were produced
        produced = _get_all_produced(sol)
        assert frozenset({"bins", "method:metabat2"}) in produced
        assert frozenset({"bins", "method:maxbin2"}) in produced
        assert frozenset({"bins", "method:concoct"}) in produced

        # Verify bam was produced (needed by all binners)
        assert frozenset({"bam"}) in produced

        # Verify correct number of steps: 1 given + 1 bam + 3 binners + 1 target = 6
        assert len(sol.dependency_plan) == 6

    def test_input_rooted_multi_hop_lineage(self):
        """Input chain meta <- pair <- R1 should satisfy a transform that
        requires reads(parents=meta), even though R1's direct parent is pair.

        Reproduces inbox msg #133: assembly_stats unreachable from input-rooted
        DAGs because _satisfies_lineage uses direct `in e.parents` check, but
        input endpoint .parents is stored as a nested tree (one level of direct
        parents per endpoint), not a flat ancestor closure.
        """
        transforms = []

        # assembly_stats-like: meta, reads(parents=meta), asm(parents=reads) -> bam
        t = Transform()
        meta = t.AddRequirement(properties={"meta"})
        reads = t.AddRequirement(properties={"reads"}, parents={meta})
        t.AddRequirement(properties={"assembly"}, parents={reads})
        t.AddProduct(properties={"bam"})
        transforms.append(t)

        # Input chain (nested): meta <- pair <- R1 (IsA reads) <- asm
        # Note: pair is an intermediate that the transform does NOT name,
        # so meta is a grandparent (not direct parent) of R1.
        meta_ep = Endpoint(properties={"meta"})
        pair_ep = Endpoint(properties={"pair"}, parents={meta_ep})
        r1_ep = Endpoint(properties={"r1", "reads"}, parents={pair_ep})
        asm_ep = Endpoint(properties={"assembly"}, parents={r1_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = solve_by_mcts(
            given=[{meta_ep, pair_ep, r1_ep, asm_ep}],
            target=target,
            transforms=transforms,
        )
        assert sol.complete
        produced = _get_all_produced(sol)
        assert frozenset({"bam"}) in produced


class TestMultiSampleBinningWorkflow:
    """Tests for solver handling binning workflow with multiple sample types."""

    def test_given_stats_with_lineage_prevents_seqkit(self):
        """read_qc_stats with proper lineage satisfies assembly_stats requirement."""
        # seqkit_reads: reads -> read_qc_stats
        seqkit = Transform()
        seqkit.AddRequirement(properties={"reads"})
        seqkit.AddProduct(properties={"read_qc_stats"})

        # assembly_stats: meta -> reads -> read_qc_stats + assembly -> bam
        asm_stats = Transform()
        meta = asm_stats.AddRequirement(properties={"read_metadata"})
        reads = asm_stats.AddRequirement(properties={"reads"}, parents={meta})
        asm_stats.AddRequirement(properties={"read_qc_stats"}, parents={reads})
        asm_stats.AddRequirement(properties={"assembly"}, parents={reads})
        asm_stats.AddProduct(properties={"bam"})

        # Given: full lineage chain including read_qc_stats
        meta_ep = Endpoint(properties={"read_metadata"})
        reads_ep = Endpoint(properties={"reads"}, parents={meta_ep})
        stats_ep = Endpoint(properties={"read_qc_stats"}, parents={reads_ep})
        asm_ep = Endpoint(properties={"assembly"}, parents={reads_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = solve_by_mcts(
            given=[{meta_ep, reads_ep, stats_ep, asm_ep}],
            target=target,
            transforms=[seqkit, asm_stats]
        )
        assert sol.complete

        # seqkit should NOT be in the plan - given stats has proper lineage
        plan_transforms = {app.transform for app in sol.dependency_plan}
        assert seqkit not in plan_transforms, \
            "seqkit included despite read_qc_stats with lineage being given"

    def test_mixed_samples_all_have_stats_with_lineage(self):
        """Multiple sample types, all with read_qc_stats, should not need seqkit."""
        seqkit = Transform()
        seqkit.AddRequirement(properties={"reads"})
        seqkit.AddProduct(properties={"read_qc_stats"})

        asm_stats = Transform()
        meta = asm_stats.AddRequirement(properties={"read_metadata"})
        reads = asm_stats.AddRequirement(properties={"reads"}, parents={meta})
        asm_stats.AddRequirement(properties={"read_qc_stats"}, parents={reads})
        asm_stats.AddRequirement(properties={"assembly"}, parents={reads})
        asm_stats.AddProduct(properties={"bam"})

        # Sample A: long_reads with full lineage
        meta_a = Endpoint(properties={"read_metadata"})
        reads_a = Endpoint(properties={"reads", "read_length:long"}, parents={meta_a})
        stats_a = Endpoint(properties={"read_qc_stats"}, parents={reads_a})
        asm_a = Endpoint(properties={"assembly"}, parents={reads_a})

        # Sample B: short_reads with full lineage
        meta_b = Endpoint(properties={"read_metadata"})
        reads_b = Endpoint(properties={"reads", "read_length:short"}, parents={meta_b})
        stats_b = Endpoint(properties={"read_qc_stats"}, parents={reads_b})
        asm_b = Endpoint(properties={"assembly"}, parents={reads_b})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = solve_by_mcts(
            given=[
                {meta_a, reads_a, stats_a, asm_a},
                {meta_b, reads_b, stats_b, asm_b},
            ],
            target=target,
            transforms=[seqkit, asm_stats]
        )
        assert sol.complete

        # seqkit should NOT be in the plan
        plan_transforms = {app.transform for app in sol.dependency_plan}
        assert seqkit not in plan_transforms, \
            "seqkit included for mixed samples despite all having read_qc_stats with lineage"

    def test_stats_without_lineage_triggers_seqkit(self):
        """read_qc_stats WITHOUT proper lineage should trigger seqkit."""
        seqkit = Transform()
        seqkit.AddRequirement(properties={"reads"})
        seqkit.AddProduct(properties={"read_qc_stats"})

        asm_stats = Transform()
        meta = asm_stats.AddRequirement(properties={"read_metadata"})
        reads = asm_stats.AddRequirement(properties={"reads"}, parents={meta})
        asm_stats.AddRequirement(properties={"read_qc_stats"}, parents={reads})
        asm_stats.AddRequirement(properties={"assembly"}, parents={reads})
        asm_stats.AddProduct(properties={"bam"})

        # Given: read_qc_stats WITHOUT parent lineage (simulating save/load bug)
        meta_ep = Endpoint(properties={"read_metadata"})
        reads_ep = Endpoint(properties={"reads"}, parents={meta_ep})
        stats_ep = Endpoint(properties={"read_qc_stats"})  # NO PARENTS - bug!
        asm_ep = Endpoint(properties={"assembly"}, parents={reads_ep})

        target = Transform()
        target.AddRequirement(properties={"bam"})

        sol = solve_by_mcts(
            given=[{meta_ep, reads_ep, stats_ep, asm_ep}],
            target=target,
            transforms=[seqkit, asm_stats]
        )
        assert sol.complete

        # seqkit SHOULD be in the plan because given stats lacks lineage
        plan_transforms = {app.transform for app in sol.dependency_plan}
        assert seqkit in plan_transforms, \
            "seqkit not included even though read_qc_stats lacks lineage"
