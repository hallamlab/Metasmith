"""Regression test: solver scaling with cyanoverse ANI topology.

Mirrors the exact setup from cyanoverse/tasks/binning/pairwise_ani.py:
- 1 pangenome entity via AddValue
- 21,081 assemblies parented to it (medium-quality MAG count)
- Container resource library (skani.oci)
- Target: taxonomy::ani_table
- Transform: skani_triangle (pangenome + assembly -> ani_table)

Before commit e099799 + the AsSamples/Generate dedup fixes, this hung
for 45+ minutes due to O(n^2) scaling in mask computation, view
iteration, and list concatenation.
"""

import time
import shutil
import tempfile
from pathlib import Path

import pytest

from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataInstanceLibraryView,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan

# Cyanoverse type libraries and transforms
MLIB = Path("/home/tony/workspace/msm/lib-metabolomics")
CYANO_TRANSFORMS = Path(
    "/home/tony/agentic_workspace/main/cyanoverse/tasks/binning/repo/transforms"
)

TYPES_AVAILABLE = MLIB.exists() and CYANO_TRANSFORMS.exists()


@pytest.mark.skipif(not TYPES_AVAILABLE, reason="lib-metabolomics or cyanoverse transforms not found")
class TestSolverScalingCyanoverse:
    """Reproduce the cyanoverse ANI scaling scenario locally."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    def _build_pangenome_lib(self, temp_dir, n):
        """Replicate _build_input_library from pairwise_ani.py."""
        lib_path = temp_dir / "ani_inputs.xgdb"
        inputs = DataInstanceLibrary(lib_path)
        inputs.AddTypeLibrary(MLIB / "data_types" / "sequences.yml")
        inputs.AddTypeLibrary(MLIB / "data_types" / "taxonomy.yml")
        inputs.AddTypeLibrary(MLIB / "data_types" / "pangenome.yml")

        # Single pangenome entity grouping all bins — exactly like pairwise_ani.py
        group = inputs.AddValue(
            "quality_bins_project", "all_quality_mags", "pangenome::pangenome"
        )

        # Add n assemblies parented to the pangenome
        for i in range(n):
            bin_file = lib_path / f"bin_{i:05d}.fna"
            bin_file.parent.mkdir(parents=True, exist_ok=True)
            bin_file.write_text("")
            inputs.AddItem(
                Path(f"bin_{i:05d}.fna"), "sequences::assembly", parents={group}
            )

        inputs.Save()
        return inputs, lib_path

    def test_generate_21k_ani_workflow(self, temp_dir):
        """WorkflowPlan.Generate with 21,081 bins under 1 pangenome completes in <30s.

        Uses the exact cyanoverse topology: pangenome grouping + assemblies +
        container resources + skani_triangle transform → taxonomy::ani_table.
        """
        n = 21081
        inputs, lib_path = self._build_pangenome_lib(temp_dir, n=n)

        # Container resource library — same as pairwise_ani.py
        containers = DataInstanceLibrary.Load(MLIB / "resources/containers")
        res_views = [DataInstanceLibraryView(containers)]

        # Load the real cyanoverse transforms (skani_triangle)
        transforms = [TransformInstanceLibrary.Load(CYANO_TRANSFORMS)]

        # AsSamples on assembly — same as pairwise_ani.py line 197
        t0 = time.time()
        samples = list(inputs.AsSamples("sequences::assembly"))
        as_time = time.time() - t0
        print(f"\nAsSamples({n}): {as_time:.1f}s, {len(samples)} views")

        # Dedup check: all assemblies share the same parent, so AsSamples
        # should yield 1 deduplicated view, not 21K identical views
        assert len(samples) == 1, (
            f"Expected 1 deduplicated view, got {len(samples)}. "
            f"AsSamples should deduplicate views with identical masks."
        )

        # Target: ani_table — same as pairwise_ani.py TargetBuilder().Add("taxonomy::ani_table")
        target_model = Transform()
        ani_ep = transforms[0].GetType("taxonomy::ani_table")
        target_model.AddRequirement(ani_ep)
        target_names = {ani_ep: "taxonomy::ani_table"}

        start = time.time()
        plan = WorkflowPlan.Generate(
            given=[[sv] + res_views for sv in samples],
            transforms=transforms,
            target_names=target_names,
            target_model=target_model,
        )
        elapsed = time.time() - start

        assert isinstance(plan, WorkflowPlan)
        assert len(plan.steps) == 1, f"Expected 1 step (skani_triangle), got {len(plan.steps)}"
        assert elapsed < 30, f"Generate took {elapsed:.1f}s (limit 30s)"
        print(f"Generate({n}): {elapsed:.1f}s, {len(plan.steps)} steps")
