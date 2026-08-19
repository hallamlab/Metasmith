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
    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    def _build_pangenome_lib(self, temp_dir, mlib: Path, n: int):
        lib_path = temp_dir / "ani_inputs.xgdb"
        inputs = DataInstanceLibrary(lib_path)
        inputs.AddTypeLibrary(mlib / "data_types" / "sequences.yml")
        inputs.AddTypeLibrary(mlib / "data_types" / "taxonomy.yml")
        inputs.AddTypeLibrary(mlib / "data_types" / "pangenome.yml")

        group = inputs.AddValue(
            "quality_bins_project", "all_quality_mags", "pangenome::pangenome"
        )

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
        mlib = metasmith_libraries_root
        n = 21081
        inputs, lib_path = self._build_pangenome_lib(temp_dir, mlib, n=n)

        envs = DataInstanceLibrary.Load(mlib / "resources" / "env")
        res_views = [DataInstanceLibraryView(envs)]

        transforms = [
            TransformInstanceLibrary.Load(mlib / "transforms" / "metagenomics")
        ]

        t0 = time.time()
        samples = list(inputs.AsSamples("sequences::putative_genome"))
        as_time = time.time() - t0
        print(f"\nAsSamples({n}): {as_time:.1f}s, {len(samples)} views")

        assert len(samples) == 1, (
            f"Expected 1 deduplicated view, got {len(samples)}. "
            f"AsSamples should deduplicate views with identical masks."
        )

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
