"""Regression test: solver scaling with cyanoverse ANI topology.

Mirrors the exact setup from cyanoverse/tasks/binning/pairwise_ani.py:
- 1 pangenome entity via AddValue
- N assemblies parented to it
- Container resource library (skani.oci)
- Target: taxonomy::ani_table
- Transform: skani_triangle (pangenome + assembly -> ani_table)

Before commit e099799, WorkflowPlan.Generate() hung for 45+ minutes
with 21K items due to O(n^2) list concatenation in given_map construction.

NOTE: AsSamples("sequences::assembly") with this topology produces N views
where each view contains all N siblings — making the full Generate loop
O(n^2) in view iteration. At 2K items this takes ~60s; at 21K it would
take hours. The test uses 2K items as a practical regression check.
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

    def test_generate_2k_ani_workflow(self, temp_dir):
        """WorkflowPlan.Generate with 2K bins under 1 pangenome completes in <120s.

        Uses the exact cyanoverse topology: pangenome grouping + assemblies +
        container resources + skani_triangle transform → taxonomy::ani_table.

        Before the fix (e099799), even 1K items caused Generate to take minutes
        due to O(n^2) list concatenation. With the fix, 2K completes in ~70s.
        """
        n = 2000
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
        assert len(samples) == n, f"Expected {n} samples, got {len(samples)}"
        print(f"\nAsSamples({n}): {as_time:.1f}s")

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
        assert elapsed < 120, f"Generate took {elapsed:.1f}s (limit 120s)"
        print(f"Generate({n}): {elapsed:.1f}s, {len(plan.steps)} steps")
