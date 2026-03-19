"""Tests for DataInstanceLibrary save/load functionality.

Verifies that lineage (parent relationships) is correctly preserved
after save/load round-trips.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint


class TestDataInstanceLibrarySaveLoad:
    """Tests for DataInstanceLibrary save/load round-trip."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    @pytest.fixture
    def binning_types(self, temp_dir) -> DataTypeLibrary:
        """Create a DataTypeLibrary with binning workflow types."""
        types = DataTypeLibrary()
        types["read_metadata"] = Endpoint(properties={"read_metadata"})
        types["reads"] = Endpoint(properties={"reads"})
        types["read_qc_stats"] = Endpoint(properties={"read_qc_stats"})
        types["assembly"] = Endpoint(properties={"assembly"})
        types["bam"] = Endpoint(properties={"bam"})

        # Save types to a file
        types_path = temp_dir / "binning_types.yml"
        types.Save(types_path)
        return types_path

    def test_save_load_preserves_manifest(self, temp_dir, binning_types):
        """Basic test: manifest items are preserved after save/load."""
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        # Add items
        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])

        # Create dummy files
        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")

        lib.Save()

        # Load and verify
        loaded = DataInstanceLibrary.Load(lib_path)

        assert meta_path in loaded.manifest
        assert reads_path in loaded.manifest
        assert loaded.manifest[meta_path] == "binning::read_metadata"
        assert loaded.manifest[reads_path] == "binning::reads"

    def test_save_load_preserves_immediate_parent(self, temp_dir, binning_types):
        """Immediate parent relationship is preserved after save/load."""
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        # Create lineage: metadata -> reads
        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])

        # Create dummy files
        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")

        lib.Save()

        # Load and verify parents
        loaded = DataInstanceLibrary.Load(lib_path)

        assert reads_path in loaded.parents
        parent_paths = {p.path for p in loaded.parents[reads_path]}
        assert meta_path in parent_paths

    def test_save_load_preserves_grandparent_lineage(self, temp_dir, binning_types):
        """Grandparent lineage chain is preserved after save/load."""
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        # Create lineage: metadata -> reads -> read_qc_stats
        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        stats_path = lib.AddItem(Path("sample1/stats.json"), "binning::read_qc_stats", parents=[reads_path])

        # Create dummy files
        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/stats.json").write_text("{}")

        lib.Save()

        # Load and verify
        loaded = DataInstanceLibrary.Load(lib_path)

        # stats should have reads as parent
        assert stats_path in loaded.parents
        stats_parent_paths = {p.path for p in loaded.parents[stats_path]}
        assert reads_path in stats_parent_paths

        # reads should have metadata as parent
        assert reads_path in loaded.parents
        reads_parent_paths = {p.path for p in loaded.parents[reads_path]}
        assert meta_path in reads_parent_paths

    def test_save_load_binning_workflow_full_lineage(self, temp_dir, binning_types):
        """Full binning workflow lineage: meta -> reads -> {stats, assembly}."""
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        # Create binning workflow lineage:
        # read_metadata -> reads -> read_qc_stats
        #                       -> assembly
        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        stats_path = lib.AddItem(Path("sample1/stats.json"), "binning::read_qc_stats", parents=[reads_path])
        asm_path = lib.AddItem(Path("sample1/assembly.fasta"), "binning::assembly", parents=[reads_path])

        # Create dummy files
        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/stats.json").write_text("{}")
        (lib_path / "sample1/assembly.fasta").write_text("")

        lib.Save()

        # Load and verify full lineage
        loaded = DataInstanceLibrary.Load(lib_path)

        # Verify stats has reads as parent (and grandparent metadata via lineage)
        assert stats_path in loaded.parents
        stats_parents = loaded.parents[stats_path]
        stats_parent_paths = {p.path for p in stats_parents}
        assert reads_path in stats_parent_paths, \
            f"stats should have reads as parent, got: {stats_parent_paths}"

        # Verify assembly has reads as parent
        assert asm_path in loaded.parents
        asm_parent_paths = {p.path for p in loaded.parents[asm_path]}
        assert reads_path in asm_parent_paths, \
            f"assembly should have reads as parent, got: {asm_parent_paths}"

        # Verify reads has metadata as parent
        assert reads_path in loaded.parents
        reads_parent_paths = {p.path for p in loaded.parents[reads_path]}
        assert meta_path in reads_parent_paths, \
            f"reads should have metadata as parent, got: {reads_parent_paths}"

    def test_save_load_multiple_samples_preserve_lineage(self, temp_dir, binning_types):
        """Multiple samples with distinct lineage chains are preserved."""
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        # Sample A: long reads workflow
        meta_a = lib.AddItem(Path("sampleA/metadata.json"), "binning::read_metadata")
        reads_a = lib.AddItem(Path("sampleA/reads.fastq"), "binning::reads", parents=[meta_a])
        stats_a = lib.AddItem(Path("sampleA/stats.json"), "binning::read_qc_stats", parents=[reads_a])
        asm_a = lib.AddItem(Path("sampleA/assembly.fasta"), "binning::assembly", parents=[reads_a])

        # Sample B: short reads workflow
        meta_b = lib.AddItem(Path("sampleB/metadata.json"), "binning::read_metadata")
        reads_b = lib.AddItem(Path("sampleB/reads.fastq"), "binning::reads", parents=[meta_b])
        stats_b = lib.AddItem(Path("sampleB/stats.json"), "binning::read_qc_stats", parents=[reads_b])
        asm_b = lib.AddItem(Path("sampleB/assembly.fasta"), "binning::assembly", parents=[reads_b])

        # Create dummy files
        for sample in ["sampleA", "sampleB"]:
            (lib_path / sample).mkdir(parents=True)
            (lib_path / sample / "metadata.json").write_text("{}")
            (lib_path / sample / "reads.fastq").write_text("")
            (lib_path / sample / "stats.json").write_text("{}")
            (lib_path / sample / "assembly.fasta").write_text("")

        lib.Save()

        # Load and verify both samples maintain distinct lineage
        loaded = DataInstanceLibrary.Load(lib_path)

        # After load, parents include full ancestor chain (grandparents aggregated)
        # Sample A: stats_a has both reads_a and meta_a (grandparent) in ancestors
        assert stats_a in loaded.parents
        stats_a_ancestors = {p.path for p in loaded.parents[stats_a]}
        assert reads_a in stats_a_ancestors, "stats_a should have reads_a as ancestor"
        assert meta_a in stats_a_ancestors, "stats_a should have meta_a as grandparent ancestor"

        # reads_a has meta_a as parent
        assert reads_a in loaded.parents
        assert {p.path for p in loaded.parents[reads_a]} == {meta_a}

        # Sample B: stats_b has both reads_b and meta_b in ancestors
        assert stats_b in loaded.parents
        stats_b_ancestors = {p.path for p in loaded.parents[stats_b]}
        assert reads_b in stats_b_ancestors, "stats_b should have reads_b as ancestor"
        assert meta_b in stats_b_ancestors, "stats_b should have meta_b as grandparent ancestor"

        # reads_b has meta_b as parent
        assert reads_b in loaded.parents
        assert {p.path for p in loaded.parents[reads_b]} == {meta_b}

        # Verify no cross-contamination: sample A ancestors should not include sample B
        assert reads_b not in stats_a_ancestors, "stats_a should not have reads_b"
        assert meta_b not in stats_a_ancestors, "stats_a should not have meta_b"

        # Verify no cross-contamination: sample B ancestors should not include sample A
        assert reads_a not in stats_b_ancestors, "stats_b should not have reads_a"
        assert meta_a not in stats_b_ancestors, "stats_b should not have meta_a"

    def test_save_load_pack_unpack_equivalence(self, temp_dir, binning_types):
        """Packed representation matches after save/load round-trip."""
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        # Create binning workflow
        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        stats_path = lib.AddItem(Path("sample1/stats.json"), "binning::read_qc_stats", parents=[reads_path])
        asm_path = lib.AddItem(Path("sample1/assembly.fasta"), "binning::assembly", parents=[reads_path])

        # Create dummy files
        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/stats.json").write_text("{}")
        (lib_path / "sample1/assembly.fasta").write_text("")

        # Pack before save
        packed_before = lib.Pack()

        lib.Save()

        # Load and pack again
        loaded = DataInstanceLibrary.Load(lib_path)
        packed_after = loaded.Pack()

        # Manifest should be identical
        assert packed_before["manifest"] == packed_after["manifest"], \
            f"Manifest mismatch:\nBefore: {packed_before['manifest']}\nAfter: {packed_after['manifest']}"

    def test_save_load_grandparent_aggregation_order_independent(self, temp_dir, binning_types):
        """Grandparent aggregation works regardless of alphabetical order.

        This tests the bug where items processed before their parents
        (due to alphabetical sorting) wouldn't get grandparents aggregated.
        """
        lib_path = temp_dir / "lib"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(binning_types, namespace="binning")

        # Use names where child comes BEFORE parent alphabetically
        # "assembly" < "reads" alphabetically, so assembly is processed first
        meta_path = lib.AddItem(Path("sample1/metadata.json"), "binning::read_metadata")
        reads_path = lib.AddItem(Path("sample1/reads.fastq"), "binning::reads", parents=[meta_path])
        # "assembly" comes before "reads" alphabetically
        asm_path = lib.AddItem(Path("sample1/assembly.fasta"), "binning::assembly", parents=[reads_path])

        # Create dummy files
        (lib_path / "sample1").mkdir(parents=True)
        (lib_path / "sample1/metadata.json").write_text("{}")
        (lib_path / "sample1/reads.fastq").write_text("")
        (lib_path / "sample1/assembly.fasta").write_text("")

        lib.Save()

        # Load and verify grandparent is aggregated despite alphabetical order
        loaded = DataInstanceLibrary.Load(lib_path)

        # Assembly should have BOTH reads (parent) and metadata (grandparent)
        assert asm_path in loaded.parents
        asm_ancestors = {p.path for p in loaded.parents[asm_path]}

        assert reads_path in asm_ancestors, \
            f"assembly should have reads as parent, got: {asm_ancestors}"
        assert meta_path in asm_ancestors, \
            f"assembly should have metadata as grandparent, got: {asm_ancestors}"


class TestDataInstanceLibraryTrace:
    """Tests for DataInstanceLibrary.Trace method."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield Path(d)
        shutil.rmtree(d)

    @pytest.fixture
    def mock_types(self, temp_dir) -> Path:
        """Create a DataTypeLibrary with mock workflow types."""
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

    # --- Basic tracing ---

    def test_trace_child_to_parent(self, temp_dir, mock_types):
        """Trace from reads to metadata (direct parent)."""
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
        """Trace from metadata to reads (descendant direction)."""
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
        """Trace from assembly to metadata (skipping reads). Works because parents stores transitive closure."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for i in range(2):
            self._make_file(lib_path, f"s{i}/meta.json")
            self._make_file(lib_path, f"s{i}/reads.fq")
            self._make_file(lib_path, f"s{i}/asm.fa")
            m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
            r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
            lib.AddItem(Path(f"s{i}/asm.fa"), "mock::assembly", parents=[r])

        # Save/load to get transitive closure in parents
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        results = list(loaded.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 2
        for asm_inst, meta_inst in results:
            assert asm_inst.dtype_name == "mock::assembly"
            assert meta_inst.dtype_name == "mock::metadata"

    def test_trace_no_relationship(self, temp_dir, mock_types):
        """Trace between unrelated types yields empty results."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        # metadata and assembly with no lineage connection
        self._make_file(lib_path, "meta.json")
        self._make_file(lib_path, "asm.fa")
        lib.AddItem(Path("meta.json"), "mock::metadata")
        lib.AddItem(Path("asm.fa"), "mock::assembly")

        results = list(lib.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 0

    def test_trace_same_type(self, temp_dir, mock_types):
        """Trace from a type to itself yields nothing (items aren't their own parent)."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "meta1.json")
        self._make_file(lib_path, "meta2.json")
        lib.AddItem(Path("meta1.json"), "mock::metadata")
        lib.AddItem(Path("meta2.json"), "mock::metadata")

        results = list(lib.Trace("mock::metadata", "mock::metadata"))
        assert len(results) == 0

    # --- Many samples ---

    def test_trace_many_samples_linear(self, temp_dir, mock_types):
        """10 samples, each with lineage metadata -> reads -> assembly."""
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
        """10 samples, verify each assembly traces back to its OWN metadata."""
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

    # --- Complex lineage ---

    def test_trace_diamond_dependency(self, temp_dir, mock_types):
        """Diamond: metadata -> reads -> {assembly, qc_stats}, assembly + reads -> bam."""
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

        # bam -> metadata (transitive through reads or assembly)
        results = list(loaded.Trace("mock::bam", "mock::metadata"))
        assert len(results) == 1

        # bam -> reads
        results = list(loaded.Trace("mock::bam", "mock::reads"))
        assert len(results) == 1

        # bam -> assembly
        results = list(loaded.Trace("mock::bam", "mock::assembly"))
        assert len(results) == 1

        # bam -> qc_stats: no direct lineage (different branch)
        results = list(loaded.Trace("mock::bam", "mock::qc_stats"))
        assert len(results) == 0

    def test_trace_fan_out(self, temp_dir, mock_types):
        """One parent produces multiple different output types."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for f in ["reads.fq", "asm.fa", "qc.json"]:
            self._make_file(lib_path, f)
        r = lib.AddItem(Path("reads.fq"), "mock::reads")
        lib.AddItem(Path("asm.fa"), "mock::assembly", parents=[r])
        lib.AddItem(Path("qc.json"), "mock::qc_stats", parents=[r])

        # reads -> assembly
        results = list(lib.Trace("mock::reads", "mock::assembly"))
        assert len(results) == 1

        # reads -> qc_stats
        results = list(lib.Trace("mock::reads", "mock::qc_stats"))
        assert len(results) == 1

    def test_trace_fan_in_merge(self, temp_dir, mock_types):
        """Multiple parent types feed into one output."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for f in ["reads.fq", "asm.fa", "out.bam"]:
            self._make_file(lib_path, f)
        r = lib.AddItem(Path("reads.fq"), "mock::reads")
        a = lib.AddItem(Path("asm.fa"), "mock::assembly")
        lib.AddItem(Path("out.bam"), "mock::bam", parents=[r, a])

        # bam -> reads
        results = list(lib.Trace("mock::bam", "mock::reads"))
        assert len(results) == 1

        # bam -> assembly
        results = list(lib.Trace("mock::bam", "mock::assembly"))
        assert len(results) == 1

    def test_trace_many_samples_with_batching_pattern(self, temp_dir, mock_types):
        """12 samples with full lineage, verify all directions and no cross-contamination."""
        # Use binning namespace types
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

        # bam -> reads (12 pairs)
        results = list(loaded.Trace("binning::bam", "binning::reads"))
        assert len(results) == 12

        # bam -> read_metadata (12 pairs)
        results = list(loaded.Trace("binning::bam", "binning::read_metadata"))
        assert len(results) == 12

        # reads -> bam (reverse, 12 pairs)
        results = list(loaded.Trace("binning::reads", "binning::bam"))
        assert len(results) == 12

        # Verify no cross-contamination for bam -> read_metadata
        bam_to_meta = {str(b.path): str(m.path) for b, m in loaded.Trace("binning::bam", "binning::read_metadata")}
        for i in range(12):
            bam_path = str(sample_map[i]["bam"])
            meta_path = str(sample_map[i]["meta"])
            assert bam_to_meta[bam_path] == meta_path, \
                f"sample {i}: bam {bam_path} should map to {meta_path}, got {bam_to_meta[bam_path]}"

    # --- After save/load round-trip ---

    def test_trace_after_save_load(self, temp_dir, mock_types):
        """Trace works correctly after save/load round-trip."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        for f in ["meta.json", "reads.fq", "asm.fa"]:
            self._make_file(lib_path, f"s1/{f}")
        m = lib.AddItem(Path("s1/meta.json"), "mock::metadata")
        r = lib.AddItem(Path("s1/reads.fq"), "mock::reads", parents=[m])
        lib.AddItem(Path("s1/asm.fa"), "mock::assembly", parents=[r])

        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        # assembly -> metadata (transitive)
        results = list(loaded.Trace("mock::assembly", "mock::metadata"))
        assert len(results) == 1
        assert results[0][0].dtype_name == "mock::assembly"
        assert results[0][1].dtype_name == "mock::metadata"

        # metadata -> assembly (reverse)
        results = list(loaded.Trace("mock::metadata", "mock::assembly"))
        assert len(results) == 1

    def test_trace_many_samples_after_save_load(self, temp_dir, mock_types):
        """8 samples with full lineage chain, save/load, verify all trace directions."""
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

        # Forward: bam -> metadata
        results = list(loaded.Trace("mock::bam", "mock::metadata"))
        assert len(results) == 8

        # Forward: bam -> reads
        results = list(loaded.Trace("mock::bam", "mock::reads"))
        assert len(results) == 8

        # Reverse: metadata -> bam
        results = list(loaded.Trace("mock::metadata", "mock::bam"))
        assert len(results) == 8

        # Verify correct pairing (no cross-contamination)
        bam_to_meta = {str(b.path): str(m.path) for b, m in loaded.Trace("mock::bam", "mock::metadata")}
        for i in range(8):
            bam_path = str(sample_map[i]["bam"])
            meta_path = str(sample_map[i]["meta"])
            assert bam_to_meta[bam_path] == meta_path


class TestDataInstanceLibraryRenameByParent:
    """Tests for DataInstanceLibrary.RenameByParent method."""

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

    # --- Basic rename ---

    def test_basic_rename(self, temp_dir, mock_types):
        """1 sample: metadata -> reads -> assembly. Rename by mock::metadata gives metadata's stem."""
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
        assert Path("s1/sample_A.json") in loaded.manifest  # parent type item unchanged
        assert loaded.manifest[Path("s1/sample_A.fq")] == "mock::reads"
        assert loaded.manifest[Path("s1/sample_A.fa")] == "mock::assembly"

    def test_preserves_extensions(self, temp_dir, mock_types):
        """Files with different extensions all keep their suffixes."""
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
        """Items of the parent type itself are not renamed."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "hash.fq")
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        r = lib.AddItem(Path("hash.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        # metadata item path unchanged
        assert Path("sample.json") in loaded.manifest
        assert loaded.manifest[Path("sample.json")] == "mock::metadata"

    def test_skips_items_without_matching_parent(self, temp_dir, mock_types):
        """Orphan items (no parent of given type) are unchanged."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "orphan.fa")
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "linked.fq")
        lib.AddItem(Path("orphan.fa"), "mock::assembly")  # no parent
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        lib.AddItem(Path("linked.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        assert Path("orphan.fa") in loaded.manifest  # unchanged

    def test_multiple_samples_no_collision(self, temp_dir, mock_types):
        """3 samples with unique metadata stems in separate dirs → clean rename."""
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
        """2 items with same parent stem in same directory → both get _<hash> suffix."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "hash1.fa")
        self._make_file(lib_path, "hash2.fq")
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        # Both children would want to be named "sample.*" but .fa and .fq have different suffixes
        # so no collision. Let's create a real collision with same suffix.
        a1 = lib.AddItem(Path("hash1.fa"), "mock::assembly", parents=[m])
        a2 = lib.AddItem(Path("hash2.fq"), "mock::reads", parents=[m])

        # Actually for a real collision we need same target filename. Use same extension items.
        # Let me use a different setup: two assemblies with same parent in same dir.
        # But manifest keys must be unique. Let's use qc_stats too.
        lib2, lib_path2 = self._make_lib(temp_dir, mock_types, name="lib2")
        self._make_file(lib_path2, "sample.json")
        self._make_file(lib_path2, "hash1.json")  # reads with .json extension - will collide with sample.json
        m = lib2.AddItem(Path("sample.json"), "mock::metadata")
        r = lib2.AddItem(Path("hash1.json"), "mock::reads", parents=[m])
        lib2.Save()
        loaded = DataInstanceLibrary.Load(lib_path2)

        loaded.RenameByParent("mock::metadata")

        # hash1.json wants to become sample.json, but that's already occupied by metadata
        # So it should get a hash suffix
        renamed_reads = [p for p in loaded.manifest if loaded.manifest[p] == "mock::reads"]
        assert len(renamed_reads) == 1
        name = renamed_reads[0].name
        assert name.startswith("sample_") and name.endswith(".json")
        assert name != "sample.json"  # has hash appended

    def test_no_collision_across_directories(self, temp_dir, mock_types):
        """Same parent stems in different directories → no hash needed."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        # Two samples with same metadata stem but in different dirs
        for d in ["dir1", "dir2"]:
            self._make_file(lib_path, f"{d}/sample.json")
            self._make_file(lib_path, f"{d}/hash.fq")
            m = lib.AddItem(Path(f"{d}/sample.json"), "mock::metadata")
            lib.AddItem(Path(f"{d}/hash.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        loaded.RenameByParent("mock::metadata")

        # Both should be cleanly renamed without hash
        assert Path("dir1/sample.fq") in loaded.manifest
        assert Path("dir2/sample.fq") in loaded.manifest

    def test_filesystem_reflects_rename(self, temp_dir, mock_types):
        """Old files gone, new files present on disk."""
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
        assert (lib_path / "sample_A.json").exists()  # parent unchanged

    def test_save_load_roundtrip(self, temp_dir, mock_types):
        """After rename, save/load preserves manifest and lineage."""
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

        # Load again and verify
        reloaded = DataInstanceLibrary.Load(lib_path)
        assert Path("s1/sample_A.fq") in reloaded.manifest
        assert Path("s1/sample_A.fa") in reloaded.manifest
        assert reloaded.manifest[Path("s1/sample_A.fq")] == "mock::reads"
        assert reloaded.manifest[Path("s1/sample_A.fa")] == "mock::assembly"

        # Verify lineage is preserved
        assert Path("s1/sample_A.fq") in reloaded.parents
        parent_names = {p.name for p in reloaded.parents[Path("s1/sample_A.fq")]}
        assert "mock::metadata" in parent_names

    def test_manifest_unchanged_on_error(self, temp_dir, mock_types):
        """If a filesystem rename fails, manifest is unchanged (transactional safety)."""
        lib, lib_path = self._make_lib(temp_dir, mock_types)
        self._make_file(lib_path, "sample.json")
        self._make_file(lib_path, "hash1.fq")
        m = lib.AddItem(Path("sample.json"), "mock::metadata")
        lib.AddItem(Path("hash1.fq"), "mock::reads", parents=[m])
        lib.Save()
        loaded = DataInstanceLibrary.Load(lib_path)

        # Remove the file to cause a rename error
        (lib_path / "hash1.fq").unlink()

        original_manifest = dict(loaded.manifest)
        with pytest.raises(Exception):
            loaded.RenameByParent("mock::metadata")

        # Manifest should be unchanged
        assert loaded.manifest == original_manifest


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
        from tests.integration.conftest import create_transform_library

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
        target_names = {Endpoint(properties={"bam"}): "bam"}

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
