"""
Unit tests on the data-type inheritance graph.

These pin the contracts that the reads/assembly rework introduced or
deliberately preserved. They load yml libraries directly and use
the `Endpoint.IsA(other)` predicate.

Matrix rows covered: 41-49, 51 (see plan file).
"""
from pathlib import Path

import pytest

from metasmith.python_api import DataTypeLibrary


MLIB = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def sequences():
    return dict(DataTypeLibrary.Load(MLIB / "data_types/sequences.yml"))


@pytest.fixture(scope="module")
def binning():
    return dict(DataTypeLibrary.Load(MLIB / "data_types/binning.yml"))


# --- read-half hygiene (rows 41-43) -----------------------------------

def test_forward_short_reads_not_isa_reads(sequences):
    # Row 41: directional half must NOT match generic `reads`.
    assert not sequences["forward_short_reads"].IsA(sequences["reads"])


def test_reverse_short_reads_not_isa_reads(sequences):
    assert not sequences["reverse_short_reads"].IsA(sequences["reads"])


def test_zipped_forward_not_isa_reads(sequences):
    assert not sequences["zipped_forward_short_reads"].IsA(sequences["reads"])


def test_zipped_reverse_not_isa_reads(sequences):
    assert not sequences["zipped_reverse_short_reads"].IsA(sequences["reads"])


def test_forward_ora_not_isa_reads(sequences):
    assert not sequences["forward_ora_reads"].IsA(sequences["reads"])


def test_reverse_ora_not_isa_reads(sequences):
    assert not sequences["reverse_ora_reads"].IsA(sequences["reads"])


def test_zipped_directional_halves_isa_unpaired_read_half(sequences):
    # Row 42 (zipped variants — no property overrides, so strict IsA holds).
    parent = sequences["unpaired_read_half"]
    for name in ("zipped_forward_short_reads", "zipped_reverse_short_reads"):
        assert sequences[name].IsA(parent), f"{name} should be unpaired_read_half"


def test_overriding_directional_halves_declare_unpaired_read_half_parent():
    # Row 42 (variants that override ext/compression — strict IsA breaks
    # because of overrides, same way `clean_short_reads NOT IsA reads`. Pin
    # the lineage at the yaml declaration level instead.)
    import yaml
    doc = yaml.safe_load((MLIB / "data_types/sequences.yml").read_text())
    for name in (
        "forward_short_reads", "reverse_short_reads",
        "forward_ora_reads", "reverse_ora_reads",
    ):
        body = doc["types"][name]
        ext = body.get("extends")
        if isinstance(ext, str):
            ext = [ext]
        assert "unpaired_read_half" in ext, f"{name} should extend unpaired_read_half"
        assert "reads" not in ext, f"{name} must not extend `reads` directly"


def test_short_reads_pe_isa_reads(sequences):
    # Row 43: interleaved reads still IsA `reads` (no regression).
    assert sequences["short_reads_pe"].IsA(sequences["reads"])


def test_short_reads_se_isa_reads(sequences):
    assert sequences["short_reads_se"].IsA(sequences["reads"])


def test_long_reads_isa_reads(sequences):
    assert sequences["long_reads"].IsA(sequences["reads"])


# --- putative_genome unification (rows 44-49) --------------------------

def test_bin_fasta_isa_putative_genome(sequences):
    # Row 44.
    assert sequences["bin_fasta"].IsA(sequences["putative_genome"])


def test_bin_fasta_not_isa_assembly(sequences):
    # Row 45 (restored): with `origin: assembler` on assembly, bin_fasta
    # (no origin property) no longer property-subsets assembly. This is the
    # invariant that breaks the bin↔assembly_stats feedback loop (msg #136).
    assert not sequences["bin_fasta"].IsA(sequences["assembly"])


def test_metabat2_bin_fasta_not_isa_assembly(sequences):
    assert not sequences["metabat2_bin_fasta"].IsA(sequences["assembly"])


def test_chromosomal_contig_not_isa_assembly(sequences):
    # `origin: long_read_pseudo_bin` value-conflicts with `origin: assembler`.
    assert not sequences["chromosomal_contig"].IsA(sequences["assembly"])


def test_isolate_assembly_isa_assembly_and_putative_genome(sequences):
    # Row 46.
    iso = sequences["isolate_assembly"]
    assert iso.IsA(sequences["assembly"])
    assert iso.IsA(sequences["putative_genome"])


def test_megahit_assembly_not_isa_putative_genome(sequences):
    # Row 47: mixed-contig assemblies are deliberately excluded.
    assert not sequences["megahit_assembly"].IsA(sequences["putative_genome"])


def test_flye_assembly_not_isa_putative_genome(sequences):
    assert not sequences["flye_assembly"].IsA(sequences["putative_genome"])


def test_hifiasm_meta_assembly_not_isa_putative_genome(sequences):
    assert not sequences["hifiasm_meta_assembly"].IsA(sequences["putative_genome"])


def test_chromosomal_contig_isa_putative_genome(sequences):
    # Row 48.
    assert sequences["chromosomal_contig"].IsA(sequences["putative_genome"])


def test_metabat2_bin_fasta_isa_bin_fasta_and_putative_genome(sequences):
    # Row 49.
    t = sequences["metabat2_bin_fasta"]
    assert t.IsA(sequences["bin_fasta"])
    assert t.IsA(sequences["putative_genome"])


def test_semibin2_bin_fasta_isa_bin_fasta(sequences):
    assert sequences["semibin2_bin_fasta"].IsA(sequences["bin_fasta"])
    assert sequences["semibin2_bin_fasta"].IsA(sequences["putative_genome"])


def test_comebin_bin_fasta_isa_bin_fasta(sequences):
    assert sequences["comebin_bin_fasta"].IsA(sequences["bin_fasta"])
    assert sequences["comebin_bin_fasta"].IsA(sequences["putative_genome"])


# --- structural / regression (row 51) ---------------------------------

def test_no_cross_file_extends():
    # Row 51: every `extends:` reference must resolve inside its own file.
    import yaml
    for yml in (MLIB / "data_types").glob("*.yml"):
        doc = yaml.safe_load(yml.read_text())
        if not doc or "types" not in doc:
            continue
        local_names = set(doc["types"].keys())
        for type_name, body in doc["types"].items():
            if not isinstance(body, dict):
                continue
            ext = body.get("extends")
            if not ext:
                continue
            if isinstance(ext, str):
                ext = [ext]
            for parent in ext:
                if "::" in parent:
                    pytest.fail(
                        f"{yml.name}::{type_name} uses cross-file extends [{parent}]"
                    )
                assert parent in local_names, (
                    f"{yml.name}::{type_name} extends [{parent}] which is not defined in {yml.name}"
                )


def test_binning_yml_has_no_bin_fasta_types(binning):
    # Sanity: confirm the relocation took.
    for name in ("bin_fasta", "metabat2_bin_fasta", "semibin2_bin_fasta", "comebin_bin_fasta"):
        assert name not in binning, f"{name} should have been moved to sequences.yml"


def test_sequences_yml_has_putative_genome_types(sequences):
    for name in ("bin_fasta", "metabat2_bin_fasta", "semibin2_bin_fasta",
                 "comebin_bin_fasta", "putative_genome", "chromosomal_contig"):
        assert name in sequences, f"{name} missing from sequences.yml"


def test_sequences_yml_has_unpaired_read_half(sequences):
    assert "unpaired_read_half" in sequences
