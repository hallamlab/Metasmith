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
