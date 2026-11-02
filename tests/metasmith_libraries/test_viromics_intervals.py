"""The interval arithmetic in merge_candidate_calls, which nothing else checks.

That transform cannot be exercised by `metasmith run`: it recovers which sample
and which contig batch a call came from with `context.SourceOf`, which reads the
`slk` channel map the compiler writes and matches on inherited lineage ids. A
direct run supplies files with no parent relationships between them, so the
lineage genuinely is not there to recover -- passing a slot-channel map would not
fix it. What is testable in isolation is the part most likely to be silently
wrong, and this is it: merging overlapping calls, and the 1-based-inclusive to
BED conversion that sits between a caller's coordinates and seqkit's cut.
"""
import importlib.util
import subprocess
from pathlib import Path

import pytest

from metasmith.python_api import TransformInstanceLibrary

from conftest import MLIB

SEQKIT_IMAGE = "docker.io/staphb/seqkit:2.13.0"


@pytest.fixture(scope="module")
def merge_module():
    # The module calls lib.GetType at import, so the library has to be loaded
    # first -- the same order `metasmith run` uses.
    TransformInstanceLibrary.Load(MLIB / "transforms/viromics")
    path = MLIB / "transforms/viromics/merge_candidate_calls.py"
    spec = importlib.util.spec_from_file_location("merge_candidate_calls", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_overlapping_calls_from_two_callers_become_one_interval(merge_module):
    merged = merge_module._union([
        (1000, 1800, "genomad"),
        (1500, 2000, "virsorter2"),
    ])
    assert merged == [(1000, 2000, {"genomad", "virsorter2"})]


def test_disjoint_calls_stay_separate(merge_module):
    merged = merge_module._union([
        (1000, 1800, "genomad"),
        (3000, 3500, "vibrant"),
    ])
    assert [(s, e) for s, e, _ in merged] == [(1000, 1800), (3000, 3500)]


def test_abutting_calls_merge(merge_module):
    # end + 1 == start is one provirus described by two callers who disagreed
    # about where it stops, not two adjacent regions to emit twice.
    merged = merge_module._union([
        (100, 200, "genomad"),
        (201, 300, "vibrant"),
    ])
    assert merged == [(100, 300, {"genomad", "vibrant"})]


def test_one_caller_seen_twice_on_the_same_interval_is_not_duplicated(merge_module):
    merged = merge_module._union([
        (100, 200, "genomad"),
        (100, 200, "genomad"),
    ])
    assert merged == [(100, 200, {"genomad"})]


@pytest.mark.skipif(
    subprocess.run(["docker", "image", "inspect", SEQKIT_IMAGE],
                   capture_output=True).returncode != 0,
    reason="seqkit image not cached",
)
def test_the_bed_conversion_cuts_exactly_the_called_bases(merge_module, tmp_path):
    """A 1-based inclusive call must come back as exactly those bases.

    This is the off-by-one the type's comment exists to prevent. BED is 0-based
    half-open, so the start converts and the end does not; getting it wrong
    shortens every frozen contig by one base at the 5' end, which no downstream
    tool would ever complain about.
    """
    contig = "".join("ACGT"[i % 4] for i in range(500))
    (tmp_path/"contigs.fna").write_text(f">ctg1\n{contig}\n")

    start, end = 100, 200                       # 1-based inclusive
    expected = contig[start - 1:end]            # 101 bases
    assert len(expected) == 101

    # the transform's own conversion, not a restatement of it here
    bed_start, bed_end = merge_module._to_bed(start, end)
    (tmp_path/"r.bed").write_text(f"ctg1\t{bed_start}\t{bed_end}\n")
    subprocess.run(
        ["docker", "run", "--rm", "--network=none", "--entrypoint=",
         "-v", f"{tmp_path}:/w", "-w", "/w", SEQKIT_IMAGE,
         "sh", "-c", "seqkit subseq --bed r.bed contigs.fna > out.fna 2>/dev/null"],
        check=True,
    )

    lines = (tmp_path/"out.fna").read_text().splitlines()
    header, seq = lines[0], "".join(lines[1:])
    assert seq == expected, "the BED conversion is off by one"

    # seqkit reports the interval back in 1-based inclusive form, which is what
    # _extract keys on rather than trusting positional order.
    assert header.startswith(f">ctg1_{start}-{end}")


@pytest.mark.skipif(
    subprocess.run(["docker", "image", "inspect", SEQKIT_IMAGE],
                   capture_output=True).returncode != 0,
    reason="seqkit image not cached",
)
def test_a_contig_id_containing_underscores_round_trips(tmp_path):
    """The pipeline mints `SAMPLE|assembler|k141_37`, and _extract rsplits on the
    last underscore to recover it from seqkit's `<contig>_<start>-<end>` name."""
    name = "VSG1W5|megahit|k141_37"
    contig = "".join("ACGT"[i % 4] for i in range(300))
    (tmp_path/"contigs.fna").write_text(f">{name}\n{contig}\n")
    (tmp_path/"r.bed").write_text(f"{name}\t49\t150\n")
    subprocess.run(
        ["docker", "run", "--rm", "--network=none", "--entrypoint=",
         "-v", f"{tmp_path}:/w", "-w", "/w", SEQKIT_IMAGE,
         "sh", "-c", "seqkit subseq --bed r.bed contigs.fna > out.fna 2>/dev/null"],
        check=True,
    )
    header = (tmp_path/"out.fna").read_text().splitlines()[0][1:]
    recovered, _, span = header.split(":")[0].rpartition("_")
    assert recovered == name
    assert span == "50-150"
