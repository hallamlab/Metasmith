import pytest
import tempfile
import shutil
from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint


class TestDataInstanceLibrarySaveLoad:
    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    @pytest.fixture
    def binning_types(self, temp_dir) -> DataTypeLibrary:
        types = DataTypeLibrary()
        types["read_metadata"] = Endpoint(properties={"read_metadata"})
        types["reads"] = Endpoint(properties={"reads"})
        types["read_qc_stats"] = Endpoint(properties={"read_qc_stats"})
        types["assembly"] = Endpoint(properties={"assembly"})
        types["bam"] = Endpoint(properties={"bam"})

        types_path = temp_dir / "binning_types.yml"
        types.Save(types_path)
        return types_path

    def test_save_load_preserves_manifest(self, temp_dir, binning_types):
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])

        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)

        assert meta_path in loaded.manifest
        assert reads_path in loaded.manifest
        assert loaded.manifest[meta_path] == "binning::read_metadata"
        assert loaded.manifest[reads_path] == "binning::reads"

    def test_save_load_preserves_immediate_parent(self, temp_dir, binning_types):
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])

        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)

        assert reads_path in loaded.parents
        parent_paths = {p.path for p in loaded.parents[reads_path]}
        assert meta_path in parent_paths

    def test_save_load_preserves_grandparent_lineage(self, temp_dir, binning_types):
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        stats_path = lib.AddItem(Path("sample1/stats.json"), "binning::read_qc_stats", parents=[reads_path])

        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/stats.json").write_text("{}")

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)

        assert stats_path in loaded.parents
        stats_parent_paths = {p.path for p in loaded.parents[stats_path]}
        assert reads_path in stats_parent_paths

        assert reads_path in loaded.parents
        reads_parent_paths = {p.path for p in loaded.parents[reads_path]}
        assert meta_path in reads_parent_paths

    def test_save_load_binning_workflow_full_lineage(self, temp_dir, binning_types):
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        stats_path = lib.AddItem(Path("sample1/stats.json"), "binning::read_qc_stats", parents=[reads_path])
        asm_path = lib.AddItem(Path("sample1/assembly.fasta"), "binning::assembly", parents=[reads_path])

        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/stats.json").write_text("{}")
        (lib_path / "sample1/assembly.fasta").write_text("")

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)

        assert stats_path in loaded.parents
        stats_parents = loaded.parents[stats_path]
        stats_parent_paths = {p.path for p in stats_parents}
        assert reads_path in stats_parent_paths, \
            f"stats should have reads as parent, got: {stats_parent_paths}"

        assert asm_path in loaded.parents
        asm_parent_paths = {p.path for p in loaded.parents[asm_path]}
        assert reads_path in asm_parent_paths, \
            f"assembly should have reads as parent, got: {asm_parent_paths}"

        assert reads_path in loaded.parents
        reads_parent_paths = {p.path for p in loaded.parents[reads_path]}
        assert meta_path in reads_parent_paths, \
            f"reads should have metadata as parent, got: {reads_parent_paths}"

    def test_save_load_multiple_samples_preserve_lineage(self, temp_dir, binning_types):
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        meta_a = lib.AddItem(Path("sampleA/metadata.json"), "binning::read_metadata")
        reads_a = lib.AddItem(Path("sampleA/reads.fastq"), "binning::reads", parents=[meta_a])
        stats_a = lib.AddItem(Path("sampleA/stats.json"), "binning::read_qc_stats", parents=[reads_a])
        asm_a = lib.AddItem(Path("sampleA/assembly.fasta"), "binning::assembly", parents=[reads_a])

        meta_b = lib.AddItem(Path("sampleB/metadata.json"), "binning::read_metadata")
        reads_b = lib.AddItem(Path("sampleB/reads.fastq"), "binning::reads", parents=[meta_b])
        stats_b = lib.AddItem(Path("sampleB/stats.json"), "binning::read_qc_stats", parents=[reads_b])
        asm_b = lib.AddItem(Path("sampleB/assembly.fasta"), "binning::assembly", parents=[reads_b])

        for sample in ["sampleA", "sampleB"]:
            (lib_path / sample).mkdir(parents=True)
            (lib_path / sample / "metadata.json").write_text("{}")
            (lib_path / sample / "reads.fastq").write_text("")
            (lib_path / sample / "stats.json").write_text("{}")
            (lib_path / sample / "assembly.fasta").write_text("")

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)

        assert stats_a in loaded.parents
        stats_a_ancestors = {p.path for p in loaded.parents[stats_a]}
        assert reads_a in stats_a_ancestors, "stats_a should have reads_a as ancestor"
        assert meta_a in stats_a_ancestors, "stats_a should have meta_a as grandparent ancestor"

        assert reads_a in loaded.parents
        assert {p.path for p in loaded.parents[reads_a]} == {meta_a}

        assert stats_b in loaded.parents
        stats_b_ancestors = {p.path for p in loaded.parents[stats_b]}
        assert reads_b in stats_b_ancestors, "stats_b should have reads_b as ancestor"
        assert meta_b in stats_b_ancestors, "stats_b should have meta_b as grandparent ancestor"

        assert reads_b in loaded.parents
        assert {p.path for p in loaded.parents[reads_b]} == {meta_b}

        assert reads_b not in stats_a_ancestors, "stats_a should not have reads_b"
        assert meta_b not in stats_a_ancestors, "stats_a should not have meta_b"

        assert reads_a not in stats_b_ancestors, "stats_b should not have reads_a"
        assert meta_a not in stats_b_ancestors, "stats_b should not have meta_a"

    def test_save_load_pack_unpack_equivalence(self, temp_dir, binning_types):
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        stats_path = lib.AddItem(Path("sample1/stats.json"), "binning::read_qc_stats", parents=[reads_path])
        asm_path = lib.AddItem(Path("sample1/assembly.fasta"), "binning::assembly", parents=[reads_path])

        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/stats.json").write_text("{}")
        (lib_path / "sample1/assembly.fasta").write_text("")

        packed_before = lib.Pack()

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)
        packed_after = loaded.Pack()

        assert packed_before["manifest"] == packed_after["manifest"], \
            f"Manifest mismatch:\nBefore: {packed_before['manifest']}\nAfter: {packed_after['manifest']}"

    def test_save_load_grandparent_aggregation_order_independent(self, temp_dir, binning_types):
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        asm_path = lib.AddItem(Path("sample1/assembly.fasta"), "binning::assembly", parents=[reads_path])

        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/assembly.fasta").write_text("")

        lib.Save()

        loaded = DataInstanceLibrary.Load(lib_path)

        assert asm_path in loaded.parents
        asm_ancestors = {p.path for p in loaded.parents[asm_path]}

        assert reads_path in asm_ancestors, \
            f"assembly should have reads as parent, got: {asm_ancestors}"
        assert meta_path in asm_ancestors, \
            f"assembly should have metadata as grandparent, got: {asm_ancestors}"


class TestDataInstanceLibraryTrace:
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
        types["qc_stats"] = Endpoint(properties={"qc_stats"})
        types["bam"] = Endpoint(properties={"bam"})
        types_path = temp_dir / "mock_types.yml"
        types.Save(types_path)
        return types_path

    def _make_lib(self, temp_dir, mock_types, name="lib"):
        lib_path = temp_dir / name
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")
        return lib, lib_path

    def _make_file(self, lib_path, rel_path):
        p = lib_path / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("")


    def test_trace_child_to_parent(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        paths = {}
        for i in range(3):
            self._make_file(lib_path, f"s{i}/meta.json")
            self._make_file(lib_path, f"s{i}/reads.fq")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
            r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
            paths[i] = (m, r)

        results = list(lib.Trace("mock::reads", "mock::metadata"))
        assert len(results) == 3
        for reads_inst, meta_inst in results:
            assert reads_inst.dtype_name == "mock::reads"
            assert meta_inst.dtype_name == "mock::metadata"

    def test_trace_parent_to_child(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for i in range(3):
            self._make_file(lib_path, f"s{i}/meta.json")
            self._make_file(lib_path, f"s{i}/reads.fq")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
            lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])

        results = list(lib.Trace("mock::metadata", "mock::reads"))
        assert len(results) == 3
        for meta_inst, reads_inst in results:
            assert meta_inst.dtype_name == "mock::metadata"
            assert reads_inst.dtype_name == "mock::reads"

    def test_trace_grandchild_to_grandparent(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for i in range(2):
            self._make_file(lib_path, f"s{i}/meta.json")
            self._make_file(lib_path, f"s{i}/reads.fq")
            self._make_file(lib_path, f"s{i}/asm.fa")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
            r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
            lib.AddItem(Path(f"s{i}/asm.fa"), "mock::assembly", parents=[r])

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 2
        for asm_inst, meta_inst in results:
            assert asm_inst.dtype_name == "mock::assembly"
            assert meta_inst.dtype_name == "mock::metadata"

    def test_trace_no_relationship(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "meta.json")
        self._make_file(lib_path, "asm.fa")
        lib.AddItem(Path("meta.json"), "mock::metadata")
        lib.AddItem(Path("asm.fa"), "mock::assembly")

        results = list(lib.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 0

    def test_trace_same_type(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "meta1.json")
        self._make_file(lib_path, "meta2.json")
        lib.AddItem(Path("meta1.json"), "mock::metadata")
        lib.AddItem(Path("meta2.json"), "mock::metadata")

        results = list(lib.Trace("mock::metadata", "mock::metadata"))
        assert len(results) == 0


    def test_trace_many_samples_linear(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        sample_map = {}
        for i in range(10):
            for f in ["meta.json", "reads.fq", "asm.fa"]:
                self._make_file(lib_path, f"s{i}/{f}")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
            r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
            a = lib.AddItem(Path(f"s{i}/asm.fa"), "mock::assembly", parents=[r])
            sample_map[i] = (m, r, a)

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 10

    def test_trace_many_samples_no_cross_contamination(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        sample_map = {}
        for i in range(10):
            for f in ["meta.json", "reads.fq", "asm.fa"]:
                self._make_file(lib_path, f"s{i}/{f}")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
            r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
            a = lib.AddItem(Path(f"s{i}/asm.fa"), "mock::assembly", parents=[r])
            sample_map[str(a)] = str(m)

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 10
        for asm_inst, meta_inst in results:
            assert sample_map[str(asm_inst.path)] == str(meta_inst.path), \
                f"assembly {asm_inst.path} should map to {sample_map[str(asm_inst.path)]}, got {meta_inst.path}"


    def test_trace_diamond_dependency(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for f in ["meta.json", "reads.fq", "asm.fa", "qc.json", "out.bam"]:
            self._make_file(lib_path, f"s1/{f}")
        m = lib.AddItem(Path("s1/meta.json"), "mock::metadata")
        r = lib.AddItem(Path("s1/reads.fq"), "mock::reads", parents=[m])
        a = lib.AddItem(Path("s1/asm.fa"), "mock::assembly", parents=[r])
        q = lib.AddItem(Path("s1/qc.json"), "mock::qc_stats", parents=[r])
        b = lib.AddItem(Path("s1/out.bam"), "mock::bam", parents=[a, r])

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("mock::bam", "mock::metadata"))
        assert len(results) == 1

        results = list(loaded.Trace("mock::bam", "mock::reads"))
        assert len(results) == 1

        results = list(loaded.Trace("mock::bam", "mock::assembly"))
        assert len(results) == 1

        results = list(loaded.Trace("mock::bam", "mock::qc_stats"))
        assert len(results) == 0

    def test_trace_fan_out(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for f in ["reads.fq", "asm.fa", "qc.json"]:
            self._make_file(lib_path, f)
        r = lib.AddItem(Path("reads.fq"), "mock::reads")
        lib.AddItem(Path("asm.fa"), "mock::assembly", parents=[r])
        lib.AddItem(Path("qc.json"), "mock::qc_stats", parents=[r])

        results = list(lib.Trace("mock::reads", "mock::assembly"))
        assert len(results) == 1

        results = list(lib.Trace("mock::reads", "mock::qc_stats"))
        assert len(results) == 1

    def test_trace_fan_in_merge(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for f in ["reads.fq", "asm.fa", "out.bam"]:
            self._make_file(lib_path, f)
        r = lib.AddItem(Path("reads.fq"), "mock::reads")
        a = lib.AddItem(Path("asm.fa"), "mock::assembly")
        lib.AddItem(Path("out.bam"), "mock::bam", parents=[r, a])

        results = list(lib.Trace("mock::bam", "mock::reads"))
        assert len(results) == 1

        results = list(lib.Trace("mock::bam", "mock::assembly"))
        assert len(results) == 1

    def test_trace_many_samples_with_batching_pattern(self, temp_dir, mock_types):
        binning_types = DataTypeLibrary()
        binning_types["read_metadata"] = Endpoint(properties={"read_metadata"})
        binning_types["reads"] = Endpoint(properties={"reads"})
        binning_types["assembly"] = Endpoint(properties={"assembly"})
        binning_types["bam"] = Endpoint(properties={"bam"})
        types_path = temp_dir / "binning_types.yml"
        binning_types.Save(types_path)

        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(types_path, namespace="binning")

        sample_map = {}
        for i in range(12):
            for f in ["meta.json", "reads.fq", "asm.fa", "out.bam"]:
                self._make_file(lib_path, f"s{i}/{f}")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "binning::read_metadata")
            r = lib.AddItem(Path(f"s{i}/reads.fq"), "binning::reads", parents=[m])
            a = lib.AddItem(Path(f"s{i}/asm.fa"), "binning::assembly", parents=[r])
            b = lib.AddItem(Path(f"s{i}/out.bam"), "binning::bam", parents=[a])
            sample_map[i] = {"meta": m, "reads": r, "asm": a, "bam": b}

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("binning::bam", "binning::reads"))
        assert len(results) == 12

        results = list(loaded.Trace("binning::bam", "binning::read_metadata"))
        assert len(results) == 12

        results = list(loaded.Trace("binning::reads", "binning::bam"))
        assert len(results) == 12

        bam_to_meta = {str(b.path): str(m.path) for b, m in loaded.Trace("binning::bam", "binning::read_metadata")}
        for i in range(12):
            bam_path = str(sample_map[i]["bam"])
            meta_path = str(sample_map[i]["meta"])
            assert bam_to_meta[bam_path] == meta_path, \
                f"sample {i}: bam {bam_path} should map to {meta_path}, got {bam_to_meta[bam_path]}"


    def test_trace_after_save_load(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for f in ["meta.json", "reads.fq", "asm.fa"]:
            self._make_file(lib_path, f"s1/{f}")
        m = lib.AddItem(Path("s1/meta.json"), "mock::metadata")
        r = lib.AddItem(Path("s1/reads.fq"), "mock::reads", parents=[m])
        lib.AddItem(Path("s1/asm.fa"), "mock::assembly", parents=[r])

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 1
        assert results[0][0].dtype_name == "mock::assembly"
        assert results[0][1].dtype_name == "mock::metadata"

        results = list(loaded.Trace("mock::metadata", "mock::assembly"))
        assert len(results) == 1

    def test_trace_many_samples_after_save_load(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        sample_map = {}
        for i in range(8):
            for f in ["meta.json", "reads.fq", "asm.fa", "out.bam"]:
                self._make_file(lib_path, f"s{i}/{f}")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
            r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
            a = lib.AddItem(Path(f"s{i}/asm.fa"), "mock::assembly", parents=[r])
            b = lib.AddItem(Path(f"s{i}/out.bam"), "mock::bam", parents=[a])
            sample_map[i] = {"meta": m, "reads": r, "asm": a, "bam": b}

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("mock::bam", "mock::metadata"))
        assert len(results) == 8

        results = list(loaded.Trace("mock::bam", "mock::reads"))
        assert len(results) == 8

        results = list(loaded.Trace("mock::metadata", "mock::bam"))
        assert len(results) == 8

        bam_to_meta = {str(b.path): str(m.path) for b, m in loaded.Trace("mock::bam", "mock::metadata")}
        for i in range(8):
            bam_path = str(sample_map[i]["bam"])
            meta_path = str(sample_map[i]["meta"])
            assert bam_to_meta[bam_path] == meta_path


class TestDataInstanceLibraryRenameByParent:
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
        types["qc_stats"] = Endpoint(properties={"qc_stats"})
        types["bam"] = Endpoint(properties={"bam"})
        types_path = temp_dir / "mock_types.yml"
        types.Save(types_path)
        return types_path

    def _make_lib(self, temp_dir, mock_types, name="lib"):
        lib_path = temp_dir / name
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")
        return lib, lib_path

    def _make_file(self, lib_path, rel_path):
        p = lib_path / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("content")


    def test_basic_rename(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "s1/sample_A.json")
        self._make_file(lib_path, "s1/abc123.fq")
        self._make_file(lib_path, "s1/def456.fa")
        m = lib.AddItem(Path("s1/sample_A.json"), "mock::metadata")
        r = lib.AddItem(Path("s1/abc123.fq"), "mock::reads", parents=[m])
        a = lib.AddItem(Path("s1/def456.fa"), "mock::assembly", parents=[r])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert Path("s1/sample_A.fq") in loaded.manifest
        assert Path("s1/sample_A.fa") in loaded.manifest
        assert Path("s1/sample_A.json") in loaded.manifest
        assert loaded.manifest[Path("s1/sample_A.fq")] == "mock::reads"
        assert loaded.manifest[Path("s1/sample_A.fa")] == "mock::assembly"

    def test_preserves_extensions(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "my_sample.json")
        self._make_file(lib_path, "hash1.fq")
        self._make_file(lib_path, "hash2.fa")
        self._make_file(lib_path, "hash3.bam")
        m = lib.AddItem(Path("my_sample.json"), "mock::metadata")
        r = lib.AddItem(Path("hash1.fq"), "mock::reads", parents=[m])
        a = lib.AddItem(Path("hash2.fa"), "mock::assembly", parents=[m])
        b = lib.AddItem(Path("hash3.bam"), "mock::bam", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert Path("my_sample.fq") in loaded.manifest
        assert Path("my_sample.fa") in loaded.manifest
        assert Path("my_sample.bam") in loaded.manifest

    def test_skips_parent_type_items(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "hash.fq")
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        r = lib.AddItem(Path("hash.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert Path("sample.json") in loaded.manifest
        assert loaded.manifest[Path("sample.json")] == "mock::metadata"

    def test_skips_items_without_matching_parent(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "orphan.fa")
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "linked.fq")
        lib.AddItem(Path("orphan.fa"), "mock::assembly")
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        lib.AddItem(Path("linked.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert Path("orphan.fa") in loaded.manifest

    def test_multiple_samples_no_collision(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for i, name in enumerate(["alpha", "beta", "gamma"]):
            self._make_file(lib_path, f"s{i}/{name}.json")
            self._make_file(lib_path, f"s{i}/hash{i}.fq")
            m = lib.AddItem(Path(f"s{i}/{name}.json"), "mock::metadata")
            lib.AddItem(Path(f"s{i}/hash{i}.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert Path("s0/alpha.fq") in loaded.manifest
        assert Path("s1/beta.fq") in loaded.manifest
        assert Path("s2/gamma.fq") in loaded.manifest

    def test_collision_adds_hash(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "hash1.fa")
        self._make_file(lib_path, "hash2.fq")
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        a1 = lib.AddItem(Path("hash1.fa"), "mock::assembly", parents=[m])
        a2 = lib.AddItem(Path("hash2.fq"), "mock::reads", parents=[m])

        lib2, lib_path2 = self._make_lib(temp_dir, mock_types, name="lib2")
        self._make_file(lib_path2, "sample.json")
        self._make_file(lib_path2, "hash1.json")
        m = lib2.AddItem(Path("sample.json"), "mock::metadata")
        r = lib2.AddItem(Path("hash1.json"), "mock::reads", parents=[m])
        lib2.Save()
        loaded = DataInstanceLibrary.Load(lib_path2)

        loaded.RenameByParent("mock::metadata")

        renamed_reads = [p for p in loaded.manifest if loaded.manifest[p] == "mock::reads"]
        assert len(renamed_reads) == 1
        name = renamed_reads[0].name
        assert name.startswith("sample_") and name.endswith(".json")
        assert name != "sample.json"

    def test_no_collision_across_directories(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for d in ["dir1", "dir2"]:
            self._make_file(lib_path, f"{d}/sample.json")
            self._make_file(lib_path, f"{d}/hash.fq")
            m = lib.AddItem(Path(f"{d}/sample.json"), "mock::metadata")
            lib.AddItem(Path(f"{d}/hash.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert Path("dir1/sample.fq") in loaded.manifest
        assert Path("dir2/sample.fq") in loaded.manifest

    def test_filesystem_reflects_rename(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "sample_A.json")
        self._make_file(lib_path, "abc123.fq")
        m = lib.AddItem(Path("sample_A.json"), "mock::metadata")
        lib.AddItem(Path("abc123.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert not (lib_path / "abc123.fq").exists()
        assert (lib_path / "sample_A.fq").exists()
        assert (lib_path / "sample_A.json").exists()

    def test_save_load_roundtrip(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "s1/sample_A.json")
        self._make_file(lib_path, "s1/hash1.fq")
        self._make_file(lib_path, "s1/hash2.fa")
        m = lib.AddItem(Path("s1/sample_A.json"), "mock::metadata")
        r = lib.AddItem(Path("s1/hash1.fq"), "mock::reads", parents=[m])
        a = lib.AddItem(Path("s1/hash2.fa"), "mock::assembly", parents=[r])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        reloaded = DataInstanceLibrary.Load(lib_path)
        assert Path("s1/sample_A.fq") in reloaded.manifest
        assert Path("s1/sample_A.fa") in reloaded.manifest
        assert reloaded.manifest[Path("s1/sample_A.fq")] == "mock::reads"
        assert reloaded.manifest[Path("s1/sample_A.fa")] == "mock::assembly"

        assert Path("s1/sample_A.fq") in reloaded.parents
        parent_names = {p.name for p in reloaded.parents[Path("s1/sample_A.fq")]}
        assert "mock::metadata" in parent_names

    def test_manifest_unchanged_on_error(self, temp_dir, mock_types):
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "hash1.fq")
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        lib.AddItem(Path("hash1.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        (lib_path / "hash1.fq").unlink()

        original_manifest = dict(loaded.manifest)
        with pytest.raises(Exception):
            loaded.RenameByParent("mock::metadata")

        assert loaded.manifest == original_manifest
