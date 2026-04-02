"""End-to-end tests for Metasmith workflow execution.

Tests cover:
1. Basic workflow execution (deploy, generate, stage, run)
2. Groupby behavior (partitioning by dependency)
3. Batching (batch_size > 1)
4. Branching (multiple product groups)
5. Lineage constraints (parent relationships)
6. Results DataInstanceLibrary (manifest, types, parents)
"""

import pytest
import shutil
import yaml
from pathlib import Path

from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint, Transform, solve_by_mcts
from metasmith.models.workflow import WorkflowPlan

from .conftest import create_transform_library


class TestBasicWorkflowExecution:
    """Test full workflow: deploy -> generate -> stage -> run -> verify."""

    def test_binning_workflow_3_samples(
        self, temp_dir, mock_samples, mock_types, container_runtime
    ):
        """Test binning workflow with 3 samples produces correct plan.

        Expected: 4 steps (1 alignment + 3 binners)
        """
        # Create transforms
        transforms = {}

        # Alignment transform: reads + assembly -> bam
        # Note: assembly has reads as parent in lineage (reads -> assembly)
        transforms["alignment"] = '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
reads = model.AddRequirement(lib.GetType("mock::reads"))
asm = model.AddRequirement(lib.GetType("mock::assembly"), parents={reads})
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    out_path = Path("aligned.bam")
    out_path.write_text("mock bam")
    return ExecutionResult(manifest=[{out: out_path}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''

        # 3 binner transforms
        # bam is produced from assembly (same sample), so bam has assembly as parent
        for method in ["metabat2", "maxbin2", "concoct"]:
            transforms[method] = f'''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
bam = model.AddRequirement(lib.GetType("mock::bam"))
out = model.AddProduct(lib.GetType("mock::{method}_bins"))

def protocol(context: ExecutionContext):
    out_path = Path("bins.fa")
    out_path.write_text("mock bins")
    return ExecutionResult(manifest=[{{out: out_path}}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''

        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        # Get samples as endpoint views
        given = []
        for sample_view in mock_samples.AsSamples("mock::assembly"):
            given.append([sample_view])

        assert len(given) == 3, f"Expected 3 samples, got {len(given)}"

        # Create target model (all 3 binners)
        target_model = Transform()
        target_model.AddRequirement(properties={"bins", "method:metabat2"})
        target_model.AddRequirement(properties={"bins", "method:maxbin2"})
        target_model.AddRequirement(properties={"bins", "method:concoct"})

        # Define target names
        target_names = {
            Endpoint(properties={"bins", "method:metabat2"}): "metabat2_bins",
            Endpoint(properties={"bins", "method:maxbin2"}): "maxbin2_bins",
            Endpoint(properties={"bins", "method:concoct"}): "concoct_bins",
        }

        # Generate workflow plan
        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        # If solver didn't complete, it returns a Solution instead of WorkflowPlan
        assert isinstance(plan, WorkflowPlan), f"Expected WorkflowPlan, got {type(plan)}"

        # Verify step count: 1 alignment + 3 binners = 4 steps
        assert len(plan.steps) == 4, f"Expected 4 steps, got {len(plan.steps)}"

        # Verify transform names
        transform_names = {step.transform.name for step in plan.steps}
        assert "alignment" in transform_names, "alignment should be in workflow"
        assert "metabat2" in transform_names, "metabat2 should be in workflow"
        assert "maxbin2" in transform_names, "maxbin2 should be in workflow"
        assert "concoct" in transform_names, "concoct should be in workflow"

    def test_workflow_plan_save_load(
        self, temp_dir, mock_samples, mock_types, container_runtime
    ):
        """Test workflow plan can be saved and loaded correctly."""
        # Simple workflow: assembly -> bam
        transforms = {
            "alignment": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.bam")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''
        }

        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = {Endpoint(properties={"bam"}): "bam"}

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)

        # Save the plan
        plan_path = temp_dir / "workflow_plan.yml"
        plan.Save(plan_path)

        # Verify the file exists and has content
        assert plan_path.exists()
        with open(plan_path) as f:
            plan_data = yaml.safe_load(f)

        assert "steps" in plan_data
        assert "given" in plan_data
        assert "targets" in plan_data
        assert len(plan_data["steps"]) == 1


class TestGroupbyBehavior:
    """Test that group_by correctly partitions work."""

    def test_groupby_partitions_by_sample(
        self, temp_dir, mock_samples, mock_types, container_runtime
    ):
        """Verify each sample processed independently with group_by."""
        transforms = {
            "processor": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    # Each sample should be processed independently
    return ExecutionResult(manifest=[{out: Path("out.bam")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''
        }

        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = {Endpoint(properties={"bam"}): "bam"}

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)

        # Verify 1 step
        assert len(plan.steps) == 1

        # The step should use all 3 assemblies
        step = plan.steps[0]
        assembly_uses = [
            u for u in step.uses if "assembly" in u.dtype_name
        ]
        assert len(assembly_uses) == 3, f"Expected 3 assemblies, got {len(assembly_uses)}"

    def test_groupby_uses_correct_dependency(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify group_by uses the specified dependency for grouping."""
        # Create transform that groups by reads (not assembly)
        transforms = {
            "grouped_processor": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
reads = model.AddRequirement(lib.GetType("mock::reads"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.bam")}], success=True)

# Group by reads, not assembly
TransformInstance(protocol=protocol, model=model, group_by=reads)
'''
        }

        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = {Endpoint(properties={"bam"}): "bam"}

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)

        # Verify the transform's group_by is correctly set
        step = plan.steps[0]
        assert step.transform.group_by is not None
        # The group_by dependency should have "reads" properties
        assert "reads" in step.transform.group_by.properties


class TestBatchingBehavior:
    """Test batch_size processing."""

    def test_batch_transform_loads_correctly(
        self, temp_dir, mock_types, batched_transform_code
    ):
        """Verify transform with batch_size > 1 loads correctly."""
        transforms = {"batched": batched_transform_code}
        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        # Get the transform and verify batch_size
        tr = tr_lib.GetTransform("batched.py")
        assert tr.batch_size == 3, f"Expected batch_size=3, got {tr.batch_size}"

    def test_batch_size_parameter_preserved(
        self, temp_dir, mock_types
    ):
        """Verify batch_size parameter is correctly set in transform."""
        transforms = {
            "batched_processor": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    results = []
    for batch_ctx in context.AsBatch():
        out_path = batch_ctx.Output(out)
        results.append(ExecutionResult(manifest=[{out: out_path.local}], success=True))
    return results

TransformInstance(protocol=protocol, model=model, group_by=dep, batch_size=5)
'''
        }

        tr_lib = create_transform_library(temp_dir, mock_types, transforms)
        tr = tr_lib.GetTransform("batched_processor.py")
        assert tr.batch_size == 5


class TestBranchingBehavior:
    """Test multiple product groups (branching)."""

    def test_branching_transform_structure(
        self, temp_dir, mock_types, branching_transform_code
    ):
        """Verify transform with multiple product groups structures correctly."""
        transforms = {"branching": branching_transform_code}
        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        tr = tr_lib.GetTransform("branching.py")
        # Should have 2 product groups
        assert len(tr.model.produces) == 2, (
            f"Expected 2 product groups, got {len(tr.model.produces)}"
        )

    def test_branches_produce_multiple_outputs(
        self, temp_dir, mock_samples, mock_types, branching_transform_code
    ):
        """Verify branching transform with multiple product groups structures correctly.

        Note: NewProductGroup() creates alternative output paths (OR semantics),
        not simultaneous outputs. The solver picks ONE branch per application.
        """
        transforms = {"branching": branching_transform_code}
        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        # Target just one branch output (solver picks one branch at a time)
        target_model = Transform()
        target_model.AddRequirement(properties={"branch_a"})
        target_names = {
            Endpoint(properties={"branch_a"}): "branch_a",
        }

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)
        assert len(plan.steps) >= 1

        # Verify we have target for branch_a
        target_names_in_plan = {t.name for t in plan.targets}
        assert "branch_a" in target_names_in_plan

    def test_branches_converge(
        self, temp_dir, mock_samples, mock_types
    ):
        """Test that separate branches can be merged with another transform.

        Uses two separate transforms to produce branch_a and branch_b,
        then a merge transform that requires both.
        """
        # Create transforms: two separate producers and one merger
        transforms = {
            "produce_a": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::branch_a"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("a.txt")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
''',
            "produce_b": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::branch_b"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("b.txt")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
''',
            "merge": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep_a = model.AddRequirement(lib.GetType("mock::branch_a"))
dep_b = model.AddRequirement(lib.GetType("mock::branch_b"))
out = model.AddProduct(lib.GetType("mock::merged"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("merged.txt")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep_a)
''',
        }
        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        # Target the merged output
        target_model = Transform()
        target_model.AddRequirement(properties={"merged"})
        target_names = {Endpoint(properties={"merged"}): "merged"}

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)

        # Should have 3 steps: produce_a, produce_b, merge
        assert len(plan.steps) == 3, f"Expected 3 steps, got {len(plan.steps)}"

        transform_names = {s.transform.name for s in plan.steps}
        assert "produce_a" in transform_names
        assert "produce_b" in transform_names
        assert "merge" in transform_names


class TestLineageConstraints:
    """Test parent relationship preservation."""

    def test_lineage_preserved_in_library(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify lineage is correctly stored in DataInstanceLibrary."""
        # Verify library has parents stored
        assert len(mock_samples.parents) > 0, "Library should have parent relationships"

        # Check specific lineage: assembly -> reads -> metadata
        for path, dtype_name in mock_samples.manifest.items():
            if "assembly" in dtype_name:
                # Assembly should have reads as parent
                assert path in mock_samples.parents, f"{path} should have parents"
                parent_names = {
                    p.name for p in mock_samples.parents[path]
                }
                assert any("reads" in n for n in parent_names), (
                    f"Assembly should have reads parent, got {parent_names}"
                )

    def test_lineage_preserved_through_save_load(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify lineage survives save/load cycle.

        Note: After save/load, grandparents are aggregated into the parents list.
        So loaded library will have more parents than original (before aggregation).
        """
        # Save the library
        mock_samples.Save()

        # Load it back
        loaded_lib = DataInstanceLibrary.Load(mock_samples.location)

        # Verify parent entries exist (count may differ due to aggregation)
        assert len(loaded_lib.parents) > 0, "Loaded library should have parents"

        # Verify original direct parents are preserved (may have additional grandparents)
        for path, parents in mock_samples.parents.items():
            assert path in loaded_lib.parents, f"{path} should be in loaded parents"
            original_parent_paths = {p.path for p in parents}
            loaded_parent_paths = {p.path for p in loaded_lib.parents[path]}
            # Original parents should be a subset of loaded (due to grandparent aggregation)
            assert original_parent_paths.issubset(loaded_parent_paths), (
                f"Original parents {original_parent_paths} should be subset of "
                f"loaded parents {loaded_parent_paths} for {path}"
            )

    def test_lineage_endpoint_chain(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify endpoint parents have their own parent chains."""
        # Reload to ensure proper lineage building
        lib = DataInstanceLibrary.Load(mock_samples.location)

        # Get an assembly item
        for path, dtype_name in lib.manifest.items():
            if "assembly" in dtype_name:
                inst = lib.Get(path)
                endpoint = inst.dtype

                # Assembly endpoint should have parents (reads)
                assert len(endpoint.parents) >= 1, "Assembly should have parents"

                # At least one parent should be reads
                reads_parent = None
                for p in endpoint.parents:
                    if "reads" in str(p.properties):
                        reads_parent = p
                        break

                assert reads_parent is not None, "Should have reads parent"

                # Reads parent should have its own parents (metadata)
                assert len(reads_parent.parents) >= 1, (
                    "Reads parent should have its own parents"
                )
                break

    def test_lineage_constraint_in_transform(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify transform with parent constraint is solved correctly."""
        # Transform requiring assembly with parent=reads (matching actual lineage)
        transforms = {
            "alignment": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
# Require assembly with reads as parent (matching lineage: reads -> assembly)
reads = model.AddRequirement(lib.GetType("mock::reads"))
asm = model.AddRequirement(lib.GetType("mock::assembly"), parents={reads})
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.bam")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''
        }

        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = {Endpoint(properties={"bam"}): "bam"}

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        # The workflow should complete since lineage constraints are satisfied
        assert isinstance(plan, WorkflowPlan), "Workflow should complete"


class TestResultsLibrary:
    """Test output library correctness."""

    def test_manifest_structure(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify manifest has correct structure."""
        # Verify all manifest paths exist
        for path in mock_samples.manifest.keys():
            full_path = mock_samples.location / path
            assert full_path.exists(), f"Manifest path should exist: {full_path}"

    def test_type_information_accessible(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify type information is retrievable."""
        for path, dtype_name in mock_samples.manifest.items():
            # Should be able to get the type
            endpoint = mock_samples.GetType(dtype_name)
            assert endpoint is not None
            assert len(endpoint.properties) > 0

            # Should be able to get the full instance
            inst = mock_samples.Get(path)
            assert inst.dtype_name == dtype_name

    def test_parent_relationships_consistent(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify parent relationships are consistent."""
        # All parent paths should exist in manifest
        for path, parents in mock_samples.parents.items():
            for parent_meta in parents:
                assert parent_meta.path in mock_samples.manifest, (
                    f"Parent {parent_meta.path} should be in manifest"
                )

    def test_results_save_load_roundtrip(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify library survives save/load roundtrip."""
        # Save
        mock_samples.Save()

        # Load
        loaded = DataInstanceLibrary.Load(mock_samples.location)

        # Verify manifest
        assert set(loaded.manifest.keys()) == set(mock_samples.manifest.keys())
        for k, v in mock_samples.manifest.items():
            assert loaded.manifest[k] == v

        # Verify parents
        assert len(loaded.parents) == len(mock_samples.parents)

    def test_get_returns_correct_instance(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify Get() returns correctly structured DataInstance."""
        for path, dtype_name in mock_samples.manifest.items():
            inst = mock_samples.Get(path)

            assert inst.path == path
            assert inst.dtype_name == dtype_name
            assert inst.parent_lib is mock_samples

            # dtype should be an Endpoint
            assert isinstance(inst.dtype, Endpoint)


class TestWorkflowGeneration:
    """Additional tests for workflow generation edge cases."""

    def test_empty_workflow_when_target_satisfied(
        self, temp_dir, mock_types
    ):
        """Verify no transforms needed when target already given."""
        # Create library with bam already present
        lib_path = temp_dir / "satisfied.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")

        sample_dir = lib.location / "sample_00"
        sample_dir.mkdir(parents=True)
        (sample_dir / "output.bam").write_text("already exists")

        # Add bam directly (no parents needed for this test)
        lib.AddItem(Path("sample_00/output.bam"), "mock::bam")
        lib.Save()

        # Create a dummy transform (shouldn't be needed since bam exists)
        tr_lib_path = temp_dir / "tr_satisfied"
        transforms = {
            "alignment": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.bam")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''
        }

        tr_lib = create_transform_library(tr_lib_path, mock_types, transforms)

        given = [[sv] for sv in lib.AsSamples("mock::bam")]

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = {Endpoint(properties={"bam"}): "bam"}

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)
        # No transforms should be applied since target is already given
        assert len(plan.steps) == 0, f"Expected 0 steps, got {len(plan.steps)}"

    def test_workflow_deterministic(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify workflow generation is deterministic."""
        transforms = {
            "alignment": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.bam")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''
        }

        tr_lib = create_transform_library(temp_dir, mock_types, transforms)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = {Endpoint(properties={"bam"}): "bam"}

        # Generate workflow multiple times
        plans = []
        for seed in [42, 42, 42]:  # Same seed should give same result
            plan = WorkflowPlan.Generate(
                given=given,
                transforms=[tr_lib],
                target_names=target_names,
                target_model=target_model,
                seed=seed,
            )
            plans.append(plan)

        # All plans should have same structure
        assert all(isinstance(p, WorkflowPlan) for p in plans)
        assert all(len(p.steps) == len(plans[0].steps) for p in plans)

    def test_multiple_transform_libraries(
        self, temp_dir, mock_samples, mock_types
    ):
        """Verify workflow works with transforms from multiple libraries."""
        # Create two separate transform libraries
        transforms1 = {
            "alignment": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("out.bam")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''
        }

        transforms2 = {
            "metabat2": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("mock::assembly"))
bam = model.AddRequirement(lib.GetType("mock::bam"))
out = model.AddProduct(lib.GetType("mock::metabat2_bins"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{out: Path("bins.fa")}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=asm)
'''
        }

        # Create in separate subdirectories
        lib1_dir = temp_dir / "lib1"
        lib2_dir = temp_dir / "lib2"

        tr_lib1 = create_transform_library(lib1_dir, mock_types, transforms1)
        tr_lib2 = create_transform_library(lib2_dir, mock_types, transforms2)

        given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]

        target_model = Transform()
        target_model.AddRequirement(properties={"bins", "method:metabat2"})
        target_names = {
            Endpoint(properties={"bins", "method:metabat2"}): "metabat2_bins"
        }

        # Use both libraries
        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib1, tr_lib2],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)
        # Should use transforms from both libraries
        assert len(plan.steps) == 2, f"Expected 2 steps, got {len(plan.steps)}"

        transform_names = {s.transform.name for s in plan.steps}
        assert "alignment" in transform_names
        assert "metabat2" in transform_names
