from metasmith.models.workflow import Case
import pytest
import shutil
from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint

from ._cpu import cpu_seconds


def _mock_types(root: Path) -> Path:
    types = DataTypeLibrary()
    for name in ["metadata", "reads", "assembly", "bam"]:
        types[name] = Endpoint(properties={name})
    types_path = root / "mock_types.yml"
    types.Save(types_path)
    return types_path


def _build_10k_lib(root: Path, mock_types: Path, n=10000) -> Path:
    lib_path = root / "lib"
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


@pytest.mark.slow
@pytest.mark.timeout(300)
class TestDataInstanceLibraryPerformance:
    @pytest.fixture
    def temp_dir(self, tmp_path):
        return tmp_path

    @pytest.fixture
    def mock_types(self, tmp_path):
        return _mock_types(tmp_path)

    @pytest.fixture(scope="class")
    def lib_10k(self, tmp_path_factory) -> Path:
        root = tmp_path_factory.mktemp("lib_10k")
        return _build_10k_lib(root, _mock_types(root))

    @pytest.fixture(scope="class")
    def loaded_10k(self, lib_10k) -> DataInstanceLibrary:
        return DataInstanceLibrary.Load(lib_10k)

    @pytest.fixture
    def lib_10k_copy(self, lib_10k, tmp_path) -> Path:
        shutil.copytree(lib_10k.parent, tmp_path / "copy", symlinks=True)
        return tmp_path / "copy" / lib_10k.name

    def test_save_load_10k(self, lib_10k):
        start = cpu_seconds()
        loaded = DataInstanceLibrary.Load(lib_10k)
        elapsed = cpu_seconds() - start

        assert len(loaded.manifest) == 40000
        assert elapsed < 30, f"Load took {elapsed:.1f}s CPU (limit 30s)"

    def test_as_samples_10k(self, loaded_10k):
        start = cpu_seconds()
        count = sum(1 for _ in loaded_10k.AsSamples("mock::metadata"))
        elapsed = cpu_seconds() - start

        assert count == 10000
        assert elapsed < 30, f"AsSamples took {elapsed:.1f}s CPU (limit 30s)"

    def test_trace_10k(self, loaded_10k):
        start = cpu_seconds()
        results = list(loaded_10k.Trace("mock::bam", "mock::metadata"))
        elapsed = cpu_seconds() - start

        assert len(results) == 10000
        assert elapsed < 30, f"Trace took {elapsed:.1f}s CPU (limit 30s)"

    def test_rename_by_parent_10k(self, lib_10k_copy):
        loaded = DataInstanceLibrary.Load(lib_10k_copy)

        start = cpu_seconds()
        loaded.RenameByParent("mock::metadata")
        elapsed = cpu_seconds() - start

        assert elapsed < 60, f"RenameByParent took {elapsed:.1f}s CPU (limit 60s)"
        renamed_reads = [p for p, t in loaded.manifest.items() if t == "mock::reads"]
        assert len(renamed_reads) == 10000
        assert all(p.stem == "meta" for p in renamed_reads)

    def test_save_load_roundtrip_10k(self, lib_10k_copy):
        start = cpu_seconds()
        loaded = DataInstanceLibrary.Load(lib_10k_copy)
        packed1 = loaded.Pack()
        loaded.Save()
        reloaded = DataInstanceLibrary.Load(lib_10k_copy)
        packed2 = reloaded.Pack()
        elapsed = cpu_seconds() - start

        assert packed1["manifest"] == packed2["manifest"]
        assert elapsed < 60, f"Round-trip took {elapsed:.1f}s CPU (limit 60s)"

    def _build_single_parent_lib(self, temp_dir, mock_types, n=21000):
        lib_path = temp_dir / "single_parent_lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")

        parent_dir = lib.location / "group"
        parent_dir.mkdir(parents=True, exist_ok=True)
        (parent_dir / "meta.json").write_text("{}")
        parent = lib.AddItem(Path("group/meta.json"), "mock::metadata")

        for i in range(n):
            d = f"item_{i:05d}"
            p = lib_path / d
            p.mkdir(parents=True, exist_ok=True)
            (p / "asm.fa").write_text("")
            lib.AddItem(Path(f"{d}/asm.fa"), "mock::assembly", parents=[parent])

        lib.Save()
        return lib_path

    def test_generate_workflow_21k_single_parent(self, temp_dir, mock_types):
        from metasmith.models.workflow import WorkflowPlan
        from metasmith.models.solver import Transform
        from metasmith.models.libraries import TransformInstanceLibrary
        from metasmith.testing.mock_transforms import identity_transform
        from tests.metasmith.support import create_transform_library

        lib_path = self._build_single_parent_lib(temp_dir, mock_types)
        loaded = DataInstanceLibrary.Load(lib_path)

        samples = list(loaded.AsSamples("mock::metadata"))
        assert len(samples) == 1

        transforms = identity_transform("mock::assembly", "mock::bam")
        tr_lib = create_transform_library(temp_dir / "tr_21k", mock_types, transforms)

        target_model = Transform()
        target_model.AddRequirement(properties={"bam"})
        target_names = ["bam"]

        start = cpu_seconds()
        plan = WorkflowPlan.Generate(
            cases=Case.ByShape([[sv] for sv in samples], target=target_model, target_names=target_names),
            transforms=[tr_lib],
        )
        elapsed = cpu_seconds() - start

        assert isinstance(plan, WorkflowPlan)
        assert elapsed < 10, f"Generate took {elapsed:.1f}s CPU (limit 10s)"

    def test_as_samples_dedup_child_type(self, temp_dir, mock_types):
        lib_path = self._build_single_parent_lib(temp_dir, mock_types, n=1000)
        loaded = DataInstanceLibrary.Load(lib_path)

        samples = list(loaded.AsSamples("mock::assembly"))
        assert len(samples) == 1, (
            f"Expected 1 deduplicated view, got {len(samples)}"
        )

        assert len(samples[0]._mask) == 1001
