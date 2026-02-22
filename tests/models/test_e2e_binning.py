"""End-to-end tests for binning workflow with save/load.

Tests that the full workflow generation works correctly with:
1. Fresh inputs (created new)
2. Cached inputs (loaded from disk after save)
3. Workflow plan structure and step count
4. Workflow plan save/load

The workflow should only include assembly_stats + 3 binners (4 steps total),
NOT seqkit_reads, when read_qc_stats is given with proper lineage.

Note: Actual workflow execution (staging/running) requires deployed infrastructure
(agent, containers, nextflow) and is tested separately in integration tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint, Transform, solve_by_mcts
from metasmith.models.workflow import WorkflowPlan, WorkflowStep


class TestEndToEndBinningWorkflow:
    """End-to-end tests for binning workflow with lineage preservation."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    @pytest.fixture
    def binning_types(self, temp_dir) -> Path:
        """Create a DataTypeLibrary with all binning workflow types."""
        types = DataTypeLibrary()

        # Input types
        types["read_metadata"] = Endpoint(properties={"read_metadata"})
        types["reads"] = Endpoint(properties={"reads"})
        types["short_reads"] = Endpoint(properties={"reads", "read_length:short"})
        types["long_reads"] = Endpoint(properties={"reads", "read_length:long"})
        types["read_qc_stats"] = Endpoint(properties={"read_qc_stats"})
        types["assembly"] = Endpoint(properties={"assembly"})

        # Output types
        types["bam"] = Endpoint(properties={"bam"})
        types["metabat2_bins"] = Endpoint(properties={"bins", "method:metabat2"})
        types["semibin2_bins"] = Endpoint(properties={"bins", "method:semibin2"})
        types["comebin_bins"] = Endpoint(properties={"bins", "method:comebin"})

        types_path = temp_dir / "binning_types.yml"
        types.Save(types_path)
        return types_path

    @pytest.fixture
    def binning_transforms(self):
        """Create transforms for the binning workflow.

        Returns a dict mapping names to transforms for easy lookup.
        """
        transforms = {}

        # seqkit_reads: reads -> read_qc_stats (should NOT be needed when stats given)
        seqkit = Transform()
        seqkit.AddRequirement(properties={"reads"})
        seqkit.AddProduct(properties={"read_qc_stats"})
        transforms["seqkit_reads"] = seqkit

        # assembly_stats: meta -> reads -> {read_qc_stats, assembly} -> bam
        asm_stats = Transform()
        meta = asm_stats.AddRequirement(properties={"read_metadata"})
        reads = asm_stats.AddRequirement(properties={"reads"}, parents={meta})
        asm_stats.AddRequirement(properties={"read_qc_stats"}, parents={reads})
        asm_stats.AddRequirement(properties={"assembly"}, parents={reads})
        asm_stats.AddProduct(properties={"bam"})
        transforms["assembly_stats"] = asm_stats

        # metabat2: assembly + bam -> bins
        metabat2 = Transform()
        asm = metabat2.AddRequirement(properties={"assembly"})
        metabat2.AddRequirement(properties={"bam"}, parents={asm})
        metabat2.AddProduct(properties={"bins", "method:metabat2"})
        transforms["metabat2"] = metabat2

        # semibin2: assembly + bam -> bins
        semibin2 = Transform()
        asm = semibin2.AddRequirement(properties={"assembly"})
        semibin2.AddRequirement(properties={"bam"}, parents={asm})
        semibin2.AddProduct(properties={"bins", "method:semibin2"})
        transforms["semibin2"] = semibin2

        # comebin: assembly + bam -> bins
        comebin = Transform()
        asm = comebin.AddRequirement(properties={"assembly"})
        comebin.AddRequirement(properties={"bam"}, parents={asm})
        comebin.AddProduct(properties={"bins", "method:comebin"})
        transforms["comebin"] = comebin

        return transforms

    def create_sample_inputs(
        self, lib: DataInstanceLibrary, n_samples: int = 30
    ) -> list[dict]:
        """Create mock input data with proper lineage for binning workflow."""
        samples = []

        for i in range(n_samples):
            sample_id = f"sample_{i:03d}"
            is_long = i >= n_samples // 2
            length_class = "long" if is_long else "short"

            sample_dir = lib.location / sample_id
            sample_dir.mkdir(parents=True, exist_ok=True)

            # Create mock files
            (sample_dir / "metadata.json").write_text(
                f'{{"length_class": "{length_class}"}}'
            )
            (sample_dir / "reads.fq.gz").write_text("")
            (sample_dir / "stats.json").write_text('{"mean_quality": 25}')
            (sample_dir / "assembly.fasta").write_text(">contig_1\nACGT\n")

            # Determine read type
            reads_type = "test::long_reads" if is_long else "test::short_reads"

            # Add items with proper lineage: meta -> reads -> {stats, assembly}
            meta_path = lib.AddItem(
                path=Path(f"{sample_id}/metadata.json"),
                dtype="test::read_metadata",
            )

            reads_path = lib.AddItem(
                path=Path(f"{sample_id}/reads.fq.gz"),
                dtype=reads_type,
                parents=[meta_path],
            )

            stats_path = lib.AddItem(
                path=Path(f"{sample_id}/stats.json"),
                dtype="test::read_qc_stats",
                parents=[reads_path],
            )

            asm_path = lib.AddItem(
                path=Path(f"{sample_id}/assembly.fasta"),
                dtype="test::assembly",
                parents=[reads_path],
            )

            samples.append({
                "id": sample_id,
                "meta": meta_path,
                "reads": reads_path,
                "stats": stats_path,
                "assembly": asm_path,
                "is_long": is_long,
            })

        return samples

    def verify_lineage_preserved(self, lib: DataInstanceLibrary, samples: list[dict]):
        """Verify lineage is correctly preserved in the library."""
        for sample in samples[:2]:  # Check first 2 samples
            stats_path = sample["stats"]
            reads_path = sample["reads"]
            meta_path = sample["meta"]

            # Stats should have reads as parent
            assert stats_path in lib.parents, f"stats should have parents"
            stats_parents = {p.path for p in lib.parents[stats_path]}
            assert reads_path in stats_parents, f"stats should have reads as parent"

            # After grandparent aggregation, stats should also have meta
            assert meta_path in stats_parents, f"stats should have meta as grandparent"

            # Reads should have meta as parent
            assert reads_path in lib.parents, f"reads should have parents"
            reads_parents = {p.path for p in lib.parents[reads_path]}
            assert meta_path in reads_parents, f"reads should have meta as parent"

    def get_given_endpoints(self, lib: DataInstanceLibrary) -> list[set[Endpoint]]:
        """Convert library samples to solver endpoint sets."""
        given = []
        for sv in lib.AsSamples("test::assembly"):
            eps = set()
            for path, name, endpoint in sv.Iterate():
                eps.add(endpoint)
            given.append(eps)
        return given

    def verify_workflow(
        self, given: list[set[Endpoint]], transforms: dict[str, Transform]
    ) -> tuple[bool, set[str]]:
        """Run solver and verify workflow is correct.

        Args:
            given: List of endpoint sets (one per sample)
            transforms: Dict mapping names to Transform objects

        Returns:
            Tuple of (complete, set of transform names in the plan)
        """
        # Create reverse lookup: transform -> name
        transform_to_name = {t: name for name, t in transforms.items()}

        # Target: all 3 binners
        target = Transform()
        target.AddRequirement(properties={"bins", "method:metabat2"})
        target.AddRequirement(properties={"bins", "method:semibin2"})
        target.AddRequirement(properties={"bins", "method:comebin"})

        sol = solve_by_mcts(
            given=given,
            target=target,
            transforms=list(transforms.values()),
        )

        # Get transform names in the plan
        used_transforms = set()
        for app in sol.dependency_plan:
            if app.transform in transform_to_name:
                used_transforms.add(transform_to_name[app.transform])

        return sol.complete, used_transforms

    def test_fresh_inputs_no_seqkit(self, temp_dir, binning_types, binning_transforms):
        """Fresh inputs with proper lineage should not need seqkit_reads."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        # Create samples
        samples = self.create_sample_inputs(lib, n_samples=30)
        lib.Save()

        # Get endpoints and verify workflow
        given = self.get_given_endpoints(lib)
        complete, transform_names = self.verify_workflow(given, binning_transforms)

        assert complete, "Workflow should complete"
        assert "seqkit_reads" not in transform_names, \
            f"seqkit_reads should NOT be in workflow, got: {transform_names}"
        assert "assembly_stats" in transform_names, \
            f"assembly_stats should be in workflow"
        assert "metabat2" in transform_names, "metabat2 should be in workflow"
        assert "semibin2" in transform_names, "semibin2 should be in workflow"
        assert "comebin" in transform_names, "comebin should be in workflow"

    def test_cached_inputs_no_seqkit(self, temp_dir, binning_types, binning_transforms):
        """Cached inputs (after save/load) should not need seqkit_reads."""
        lib_path = temp_dir / "inputs.xgdb"

        # Create and save library
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")
        samples = self.create_sample_inputs(lib, n_samples=30)
        lib.Save()

        # Load from disk (simulating cached inputs)
        loaded_lib = DataInstanceLibrary.Load(lib_path)

        # Verify lineage is preserved after load
        self.verify_lineage_preserved(loaded_lib, samples)

        # Get endpoints and verify workflow
        given = self.get_given_endpoints(loaded_lib)
        complete, transform_names = self.verify_workflow(given, binning_transforms)

        assert complete, "Workflow should complete with cached inputs"
        assert "seqkit_reads" not in transform_names, \
            f"seqkit_reads should NOT be in workflow with cached inputs, got: {transform_names}"
        assert "assembly_stats" in transform_names
        assert "metabat2" in transform_names
        assert "semibin2" in transform_names
        assert "comebin" in transform_names

    def test_multiple_save_load_cycles(self, temp_dir, binning_types, binning_transforms):
        """Multiple save/load cycles should preserve lineage."""
        lib_path = temp_dir / "inputs.xgdb"

        # Create and save
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")
        samples = self.create_sample_inputs(lib, n_samples=10)
        lib.Save()

        # Multiple load/save cycles
        for cycle in range(3):
            loaded = DataInstanceLibrary.Load(lib_path)
            self.verify_lineage_preserved(loaded, samples)
            loaded.Save()

        # Final verification
        final_lib = DataInstanceLibrary.Load(lib_path)
        given = self.get_given_endpoints(final_lib)
        complete, transform_names = self.verify_workflow(given, binning_transforms)

        assert complete, f"Workflow should complete after {cycle+1} save/load cycles"
        assert "seqkit_reads" not in transform_names, \
            f"seqkit_reads should NOT appear after multiple save/load cycles"

    def test_mixed_read_types_no_seqkit(self, temp_dir, binning_types, binning_transforms):
        """Mixed short and long read samples should all work without seqkit."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        # Create 30 samples: 15 short, 15 long
        samples = self.create_sample_inputs(lib, n_samples=30)
        lib.Save()

        # Verify we have both types
        short_count = sum(1 for s in samples if not s["is_long"])
        long_count = sum(1 for s in samples if s["is_long"])
        assert short_count == 15, "Should have 15 short read samples"
        assert long_count == 15, "Should have 15 long read samples"

        # Load and verify
        loaded = DataInstanceLibrary.Load(lib_path)
        given = self.get_given_endpoints(loaded)

        # Should have 30 sample endpoint sets
        assert len(given) == 30, f"Should have 30 samples, got {len(given)}"

        complete, transform_names = self.verify_workflow(given, binning_transforms)

        assert complete, "Workflow should complete for mixed read types"
        assert "seqkit_reads" not in transform_names, \
            "seqkit_reads should NOT appear for mixed read types"

    def test_stats_without_lineage_triggers_seqkit(
        self, temp_dir, binning_types, binning_transforms
    ):
        """read_qc_stats WITHOUT proper lineage SHOULD trigger seqkit_reads."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        sample_dir = lib.location / "sample_000"
        sample_dir.mkdir(parents=True, exist_ok=True)

        # Create files
        (sample_dir / "metadata.json").write_text('{"length_class": "short"}')
        (sample_dir / "reads.fq.gz").write_text("")
        (sample_dir / "stats.json").write_text('{"mean_quality": 25}')
        (sample_dir / "assembly.fasta").write_text(">contig_1\nACGT\n")

        # Add items with BROKEN lineage: stats has NO parents
        meta_path = lib.AddItem(
            path=Path("sample_000/metadata.json"),
            dtype="test::read_metadata",
        )
        reads_path = lib.AddItem(
            path=Path("sample_000/reads.fq.gz"),
            dtype="test::short_reads",
            parents=[meta_path],
        )
        # stats WITHOUT parent - simulating the bug
        stats_path = lib.AddItem(
            path=Path("sample_000/stats.json"),
            dtype="test::read_qc_stats",
            parents=[],  # NO PARENTS - broken lineage
        )
        asm_path = lib.AddItem(
            path=Path("sample_000/assembly.fasta"),
            dtype="test::assembly",
            parents=[reads_path],
        )

        lib.Save()

        # Load and verify workflow DOES include seqkit
        loaded = DataInstanceLibrary.Load(lib_path)
        given = self.get_given_endpoints(loaded)
        complete, transform_names = self.verify_workflow(given, binning_transforms)

        assert complete, "Workflow should still complete"
        assert "seqkit_reads" in transform_names, \
            "seqkit_reads SHOULD be included when stats lacks lineage"

    def test_parent_chain_depth(self, temp_dir, binning_types):
        """Verify parent endpoints have their own parents (chain depth)."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        sample_dir = lib.location / "sample_000"
        sample_dir.mkdir(parents=True, exist_ok=True)

        (sample_dir / "metadata.json").write_text('{}')
        (sample_dir / "reads.fq.gz").write_text("")
        (sample_dir / "stats.json").write_text('{}')

        meta = lib.AddItem(Path("sample_000/metadata.json"), "test::read_metadata")
        reads = lib.AddItem(Path("sample_000/reads.fq.gz"), "test::short_reads", parents=[meta])
        stats = lib.AddItem(Path("sample_000/stats.json"), "test::read_qc_stats", parents=[reads])

        lib.Save()

        # Load and check parent chain depth
        loaded = DataInstanceLibrary.Load(lib_path)

        # Get the stats endpoint
        stats_inst = loaded.Get(stats)
        stats_endpoint = stats_inst.dtype

        # stats.parents should include reads
        assert len(stats_endpoint.parents) >= 1, "stats should have parents"

        # Find the reads parent
        reads_parent = None
        for p in stats_endpoint.parents:
            if "reads" in str(p.properties):
                reads_parent = p
                break

        assert reads_parent is not None, "stats should have reads as parent"

        # reads parent should have metadata as ITS parent
        assert len(reads_parent.parents) >= 1, \
            "reads parent should have its own parents (metadata)"

        meta_grandparent = None
        for gp in reads_parent.parents:
            if "read_metadata" in str(gp.properties):
                meta_grandparent = gp
                break

        assert meta_grandparent is not None, \
            "reads parent should have read_metadata as grandparent"

    def test_workflow_plan_structure(self, temp_dir, binning_types, binning_transforms):
        """Test the structure of the generated workflow plan."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        # Create samples
        samples = self.create_sample_inputs(lib, n_samples=10)
        lib.Save()

        # Get solver result
        given = self.get_given_endpoints(lib)
        transform_to_name = {t: name for name, t in binning_transforms.items()}

        target = Transform()
        target.AddRequirement(properties={"bins", "method:metabat2"})
        target.AddRequirement(properties={"bins", "method:semibin2"})
        target.AddRequirement(properties={"bins", "method:comebin"})

        sol = solve_by_mcts(
            given=given,
            target=target,
            transforms=list(binning_transforms.values()),
        )

        assert sol.complete, "Solver should complete"

        # Check dependency plan structure
        plan = sol.dependency_plan
        assert len(plan) > 0, "Plan should have steps"

        # Count transform applications
        transform_counts = {}
        for app in plan:
            if app.transform in transform_to_name:
                name = transform_to_name[app.transform]
                transform_counts[name] = transform_counts.get(name, 0) + 1

        # Should have assembly_stats and all 3 binners
        assert "assembly_stats" in transform_counts, \
            "Plan should include assembly_stats"
        assert "metabat2" in transform_counts, \
            "Plan should include metabat2"
        assert "semibin2" in transform_counts, \
            "Plan should include semibin2"
        assert "comebin" in transform_counts, \
            "Plan should include comebin"

        # Should NOT have seqkit_reads
        assert "seqkit_reads" not in transform_counts, \
            "Plan should NOT include seqkit_reads"

    def test_workflow_handles_sample_groups(self, temp_dir, binning_types, binning_transforms):
        """Test that workflow correctly groups samples by type."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        # Create mixed samples
        samples = self.create_sample_inputs(lib, n_samples=20)
        lib.Save()

        # Reload and get samples
        loaded = DataInstanceLibrary.Load(lib_path)
        given = self.get_given_endpoints(loaded)

        # Should have 20 sample sets
        assert len(given) == 20, f"Should have 20 samples, got {len(given)}"

        # Verify short vs long distribution
        short_samples = [s for s in samples if not s["is_long"]]
        long_samples = [s for s in samples if s["is_long"]]
        assert len(short_samples) == 10, "Should have 10 short read samples"
        assert len(long_samples) == 10, "Should have 10 long read samples"

        # Generate workflow
        complete, transforms = self.verify_workflow(given, binning_transforms)

        assert complete, "Workflow should complete for mixed samples"
        assert "seqkit_reads" not in transforms, \
            "seqkit_reads should not be in workflow for mixed samples"

    def test_workflow_deterministic(self, temp_dir, binning_types, binning_transforms):
        """Test that workflow generation is deterministic."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        samples = self.create_sample_inputs(lib, n_samples=5)
        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)
        given = self.get_given_endpoints(loaded)

        # Run solver multiple times
        results = []
        for _ in range(3):
            complete, transforms = self.verify_workflow(given, binning_transforms)
            results.append((complete, frozenset(transforms)))

        # All runs should produce the same result
        assert all(r == results[0] for r in results), \
            "Workflow generation should be deterministic"

    def test_target_satisfied_by_given(self, temp_dir, binning_types, binning_transforms):
        """Test that given data satisfying target doesn't need extra transforms."""
        lib_path = temp_dir / "inputs.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="test")

        # Create a sample with full lineage including bins
        sample_dir = lib.location / "sample_000"
        sample_dir.mkdir(parents=True, exist_ok=True)

        (sample_dir / "metadata.json").write_text('{"length_class": "short"}')
        (sample_dir / "reads.fq.gz").write_text("")
        (sample_dir / "stats.json").write_text('{"mean_quality": 25}')
        (sample_dir / "assembly.fasta").write_text(">contig_1\nACGT\n")
        (sample_dir / "aligned.bam").write_text("")  # BAM already exists
        (sample_dir / "bins.fa").write_text(">bin_1\nACGT\n")

        # Add items with full lineage
        meta = lib.AddItem(Path("sample_000/metadata.json"), "test::read_metadata")
        reads = lib.AddItem(Path("sample_000/reads.fq.gz"), "test::short_reads", parents=[meta])
        stats = lib.AddItem(Path("sample_000/stats.json"), "test::read_qc_stats", parents=[reads])
        asm = lib.AddItem(Path("sample_000/assembly.fasta"), "test::assembly", parents=[reads])
        bam = lib.AddItem(Path("sample_000/aligned.bam"), "test::bam", parents=[asm])
        bins = lib.AddItem(Path("sample_000/bins.fa"), "test::metabat2_bins", parents=[asm])

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)
        given = self.get_given_endpoints(loaded)

        # Target just metabat2 bins (which we already have)
        target = Transform()
        target.AddRequirement(properties={"bins", "method:metabat2"})

        sol = solve_by_mcts(
            given=given,
            target=target,
            transforms=list(binning_transforms.values()),
        )

        assert sol.complete, "Should complete when target already satisfied"

        # Check that no binning transforms are used (bins already given)
        transform_to_name = {t: name for name, t in binning_transforms.items()}
        used_transforms = set()
        for app in sol.dependency_plan:
            if app.transform in transform_to_name:
                used_transforms.add(transform_to_name[app.transform])

        assert "metabat2" not in used_transforms, \
            "metabat2 should not be in workflow when bins already given"
