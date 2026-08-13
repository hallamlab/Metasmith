"""Does DataInstanceLibrary still hold up at 10k-21k items?

Split out of tests/bootstrap/test_libraries.py, which is otherwise entirely
sub-millisecond. These six are 22-52s each, and while they sat in the same
file the whole bootstrap axis had to be marked `slow` to contain them -- so
~100 genuinely fast workspace-prep tests were absent from the daily loop.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint


@pytest.mark.slow
@pytest.mark.timeout(180)
class TestDataInstanceLibraryPerformance:
    """Performance tests for DataInstanceLibrary with 10k samples."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    @pytest.fixture
    def mock_types(self, temp_dir) -> Path:
        types = DataTypeLibrary()
        types["metadata"] = Endpoint(properties={"metadata"})
        types["reads"] = Endpoint(properties={"reads"})
        types["assembly"] = Endpoint(properties={"assembly"})
        types["bam"] = Endpoint(properties={"bam"})
        types_path = temp_dir / "mock_types.yml"
        types.Save(types_path)
        return types_path

    def _build_10k_lib(self, temp_dir, mock_types, n=10000):
        """Create a library with n samples, each with lineage: metadata -> reads -> assembly -> bam."""
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")

        for i in range(n):
            d = f"s{i:05d}"
            for f in ["meta.json", "reads.fq", "asm.fa", "out.bam"]:
                p = lib_path / d / f
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text("")
            m = lib.AddItem(Path(f"{d}/meta.json"), "mock::metadata")
            r = lib.AddItem(Path(f"{d}/reads.fq"), "mock::reads", parents=[m])
            a = lib.AddItem(Path(f"{d}/asm.fa"), "mock::assembly", parents=[r])
            lib.AddItem(Path(f"{d}/out.bam"), "mock::bam", parents=[a])

        lib.Save()
        return lib_path

    def test_save_load_10k(self, temp_dir, mock_types):
        """Load 10k-sample library under 30s."""
        import time
        lib_path = self._build_10k_lib(temp_dir, mock_types)

        start = time.time()
        loaded = DataInstanceLibrary.Load(lib_path)
        elapsed = time.time() - start

        assert len(loaded.manifest) == 40000
        assert elapsed < 30, f"Load took {elapsed:.1f}s (limit 30s)"

    def test_as_samples_10k(self, temp_dir, mock_types):
        """AsSamples iteration over 10k samples under 30s."""
        import time
        lib_path = self._build_10k_lib(temp_dir, mock_types)
        loaded = DataInstanceLibrary.Load(lib_path)

        start = time.time()
        count = sum(1 for _ in loaded.AsSamples("mock::metadata"))
        elapsed = time.time() - start

        assert count == 10000
        assert elapsed < 30, f"AsSamples took {elapsed:.1f}s (limit 30s)"

    def test_trace_10k(self, temp_dir, mock_types):
        """Trace across 10k samples under 30s."""
        import time
        lib_path = self._build_10k_lib(temp_dir, mock_types)
        loaded = DataInstanceLibrary.Load(lib_path)

        start = time.time()
        results = list(loaded.Trace("mock::bam", "mock::metadata"))
        elapsed = time.time() - start

        assert len(results) == 10000
        assert elapsed < 30, f"Trace took {elapsed:.1f}s (limit 30s)"

    def test_rename_by_parent_10k(self, temp_dir, mock_types):
        """RenameByParent on 10k samples under 60s."""
        import time
        lib_path = self._build_10k_lib(temp_dir, mock_types)
        loaded = DataInstanceLibrary.Load(lib_path)

        start = time.time()
        loaded.RenameByParent("mock::metadata")
        elapsed = time.time() - start

        assert elapsed < 60, f"RenameByParent took {elapsed:.1f}s (limit 60s)"
        # Verify reads were renamed to use metadata stem
        renamed_reads = [p for p, t in loaded.manifest.items() if t == "mock::reads"]
        assert len(renamed_reads) == 10000
        assert all(p.stem == "meta" for p in renamed_reads)

    def test_save_load_roundtrip_10k(self, temp_dir, mock_types):
        """Pack equivalence after round-trip on 10k samples under 60s."""
        import time
        lib_path = self._build_10k_lib(temp_dir, mock_types)

        start = time.time()
        loaded = DataInstanceLibrary.Load(lib_path)
        packed1 = loaded.Pack()
        loaded.Save()
        reloaded = DataInstanceLibrary.Load(lib_path)
        packed2 = reloaded.Pack()
        elapsed = time.time() - start

        assert packed1["manifest"] == packed2["manifest"]
        assert elapsed < 60, f"Round-trip took {elapsed:.1f}s (limit 60s)"

    def _build_single_parent_lib(self, temp_dir, mock_types, n=21000):
        """Create a library with n items all under 1 parent (single-group pattern)."""
        lib_path = temp_dir / "single_parent_lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")

        # Create parent item
        parent_dir = lib.location / "group"
        parent_dir.mkdir(parents=True, exist_ok=True)
        (parent_dir / "meta.json").write_text("{}")
        parent = lib.AddItem(Path("group/meta.json"), "mock::metadata")

        # Create n children under that single parent
        for i in range(n):
            d = f"item_{i:05d}"
            p = lib_path / d
            p.mkdir(parents=True, exist_ok=True)
            (p / "asm.fa").write_text("")
            lib.AddItem(Path(f"{d}/asm.fa"), "mock::assembly", parents=[parent])

        lib.Save()
        return lib_path

    def test_generate_workflow_21k_single_parent(self, temp_dir, mock_types):
        """WorkflowPlan.Generate with 21K items under 1 parent completes in <10s."""
        import time
        from metasmith.models.workflow import WorkflowPlan
        from metasmith.models.solver import Transform
        from metasmith.models.libraries import TransformInstanceLibrary
        from metasmith.testing.mock_transforms import identity_transform
        from tests.metasmith.e2e.docker.conftest import create_transform_library

        lib_path = self._build_single_parent_lib(temp_dir, mock_types)
        loaded = DataInstanceLibrary.Load(lib_path)

        # AsSamples on the parent type — yields 1 view with all 21K children
        samples = list(loaded.AsSamples("mock::metadata"))
        assert len(samples) == 1

        # Trivial transform: assembly -> bam
        transforms = identity_transform("mock::assembly", "mock::bam")
        tr_lib = create_transform_library(temp_dir / "tr_21k", mock_types, transforms)

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = ["bam"]

        start = time.time()
        plan = WorkflowPlan.Generate(
            given=[[sv] for sv in samples],
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )
        elapsed = time.time() - start

        assert isinstance(plan, WorkflowPlan)
        assert elapsed < 10, f"Generate took {elapsed:.1f}s (limit 10s)"

    def test_as_samples_dedup_child_type(self, temp_dir, mock_types):
        """AsSamples on child type deduplicates views when all share same parent."""
        lib_path = self._build_single_parent_lib(temp_dir, mock_types, n=1000)
        loaded = DataInstanceLibrary.Load(lib_path)

        # AsSamples on the child type — all 1000 assemblies share 1 parent,
        # so all views have the same mask. Should yield 1 view, not 1000.
        samples = list(loaded.AsSamples("mock::assembly"))
        assert len(samples) == 1, (
            f"Expected 1 deduplicated view, got {len(samples)}"
        )

        # The single view should contain all items (parent + 1000 children)
        assert len(samples[0]._mask) == 1001
