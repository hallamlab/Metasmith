"""Regression test: solver scaling with cyanoverse ANI topology.

Mirrors the exact setup from cyanoverse/tasks/binning/pairwise_ani.py:
- 1 pangenome entity via AddValue
- 21,081 putative-genome items parented to it (medium-quality MAG count)
- Container resource library (fastani.oci)
- Target: taxonomy::ani_table
- Transform: fastani (pangenome + putative_genome -> ani_table)

Before commit e099799 + the AsSamples/Generate dedup fixes, this hung
for 45+ minutes due to O(n^2) scaling in mask computation, view
iteration, and list concatenation. The regression catches O(n^2) at any
n; the pre-fix code hung at n=21081 which was the production case.

Pulls its type and transform libraries from the sibling
``metasmith-libraries/main/`` project via the
``metasmith_libraries_root`` fixture in ``tests/conftest.py``. The
original test wired against the now-deleted cyanoverse ``skani_triangle``
transform; we switched to the equivalent production-graph topology under
``fastani`` / ``sequences::putative_genome`` because cyanoverse's
``skani_triangle`` (and its sibling ``sequences::assembly`` consumer
shape) was removed upstream. The scaling assertion remains intact: same
shape (1 pangenome → N children → 1 group_by transform → 1 ani_table),
same dedup pin (AsSamples must collapse identical-mask views to 1).
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
from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan


class TestSolverScalingCyanoverse:
    """Reproduce the cyanoverse ANI scaling scenario locally."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    def _build_pangenome_lib(self, temp_dir, mlib: Path, n: int):
        """Replicate _build_input_library from pairwise_ani.py."""
        lib_path = temp_dir / "ani_inputs.xgdb"
        inputs = DataInstanceLibrary(lib_path)
        inputs.AddTypeLibrary(mlib / "data_types" / "sequences.yml")
        inputs.AddTypeLibrary(mlib / "data_types" / "taxonomy.yml")
        inputs.AddTypeLibrary(mlib / "data_types" / "pangenome.yml")

        # Single pangenome entity grouping all bins — exactly like pairwise_ani.py
        group = inputs.AddValue(
            "quality_bins_project", "all_quality_mags", "pangenome::pangenome"
        )

        # Add n putative-genome items parented to the pangenome
        for i in range(n):
            bin_file = lib_path / f"bin_{i:05d}.fna"
            bin_file.parent.mkdir(parents=True, exist_ok=True)
            bin_file.write_text("")
            inputs.AddItem(
                Path(f"bin_{i:05d}.fna"),
                "sequences::putative_genome",
                parents={group},
            )

        inputs.Save()
        return inputs, lib_path

    def test_generate_21k_ani_workflow(self, temp_dir, metasmith_libraries_root):
        """WorkflowPlan.Generate with 21,081 bins under 1 pangenome completes in <30s.

        Uses the exact cyanoverse topology: pangenome grouping +
        putative_genomes + container resources + fastani transform →
        taxonomy::ani_table.
        """
        mlib = metasmith_libraries_root
        n = 21081
        inputs, lib_path = self._build_pangenome_lib(temp_dir, mlib, n=n)

        # Environment resource library — same shape as pairwise_ani.py.
        # fastani's env is required by the fastani transform contract.
        # (`resources/containers` was renamed to `resources/env` by the env
        # migration; this kept passing off an untracked leftover on disk until
        # a rebuild cleared it.)
        envs = DataInstanceLibrary.Load(mlib / "resources" / "env")
        res_views = [DataInstanceLibraryView(envs)]

        # Load the metagenomics transforms (fastani lives under taxonomy/).
        # The _metadata/index.yml sits at transforms/metagenomics/.
        transforms = [
            TransformInstanceLibrary.Load(mlib / "transforms" / "metagenomics")
        ]

        # AsSamples on putative_genome — analogous to pairwise_ani.py line 197.
        t0 = time.time()
        samples = list(inputs.AsSamples("sequences::putative_genome"))
        as_time = time.time() - t0
        print(f"\nAsSamples({n}): {as_time:.1f}s, {len(samples)} views")

        # Dedup check: all putative_genomes share the same parent, so
        # AsSamples should yield 1 deduplicated view, not 21K identical views.
        assert len(samples) == 1, (
            f"Expected 1 deduplicated view, got {len(samples)}. "
            f"AsSamples should deduplicate views with identical masks."
        )

        # Target: ani_table — same as pairwise_ani.py TargetBuilder().Add("taxonomy::ani_table")
        target_model = Transform()
        ani_ep = transforms[0].GetType("taxonomy::ani_table")
        target_model.AddRequirement(ani_ep)
        target_names = ["taxonomy::ani_table"]

        start = time.time()
        plan = WorkflowPlan.Generate(
            given=[[sv] + res_views for sv in samples],
            transforms=transforms,
            target_names=target_names,
            target_model=target_model,
        )
        elapsed = time.time() - start

        assert isinstance(plan, WorkflowPlan)
        assert len(plan.steps) == 1, (
            f"Expected 1 step (fastani), got {len(plan.steps)}"
        )
        assert elapsed < 30, f"Generate took {elapsed:.1f}s (limit 30s)"
        print(f"Generate({n}): {elapsed:.1f}s, {len(plan.steps)} steps")
