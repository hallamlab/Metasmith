"""The key/description split, tested at the level it is written: the parsers and
the merge, not the tools they wrap.
"""
import csv
import importlib.util
import sys
from pathlib import Path

import pytest

from conftest import MLIB

TRANSFORMS = MLIB / "transforms" / "functionalAnnotation"


def _load(name):
    path = TRANSFORMS / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"_msmlib_{name}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def uniref():
    return _load("diamond_uniref50")


@pytest.fixture(scope="module")
def interproscan():
    return _load("interproscan")


@pytest.fixture(scope="module")
def kofamscan():
    return _load("kofamscan")


@pytest.fixture(scope="module")
def merge_uniref():
    return _load("merge_diamond_uniref50")


class _Staged:
    def __init__(self, path):
        self.local = path


class TestUniref50:
    def test_the_hit_table_is_headered_and_carries_no_title(self, uniref):
        assert uniref._HITS_HEADER.split("\t")[0] == "qseqid"
        assert "stitle" not in uniref._HITS_HEADER
        assert uniref._HITS_HEADER.rstrip("\n").split("\t")[-1] == "bsr"

    def test_a_title_with_a_tab_stays_one_field(self, uniref):
        # diamond puts stitle last, so it is split off by position; a tab inside
        # it would otherwise become two more columns in the hit table.
        row = "\t".join(["q1", "UniRef50_A", "90"] + ["1"] * 9) + "\tChaperone\tprotein\n"
        fields = row.rstrip("\n").split("\t", uniref._STITLE_AT)
        assert len(fields) == uniref._STITLE_AT + 1
        assert uniref._one_line(fields[uniref._STITLE_AT]) == "Chaperone protein"


class TestInterProScan:
    GFF = (
        "##gff-version 3\n"
        "orf1\tPfam\tprotein_match\t1\t100\t1e-20\t+\t.\t"
        "Name=PF00001;signature_desc=7 transmembrane receptor;Dbxref=\"InterPro:IPR000276\"\n"
        "orf2\tPfam\tprotein_match\t5\t60\t.\t+\t.\t"
        "Name=PF00001;signature_desc=7 transmembrane receptor\n"
        "orf2\tPfam\tpolypeptide\t1\t60\t.\t+\t.\tID=ignored\n"
    )

    def test_the_hit_table_keeps_its_seven_columns(self, interproscan, tmp_path):
        gff, hits, desc = (tmp_path / n for n in ("in.gff3", "hits.csv", "desc.csv"))
        gff.write_text(self.GFF)
        interproscan.parse_interpro_gff(str(gff), str(hits), str(desc))
        rows = list(csv.DictReader(hits.open()))
        assert list(rows[0]) == [
            "orf", "start", "stop", "score", "database", "Name", "Dbxref"]
        assert [r["orf"] for r in rows] == ["orf1", "orf2"]
        assert rows[0]["Dbxref"] == "IPR000276"

    def test_the_description_table_is_one_row_per_signature(self, interproscan, tmp_path):
        gff, hits, desc = (tmp_path / n for n in ("in.gff3", "hits.csv", "desc.csv"))
        gff.write_text(self.GFF)
        interproscan.parse_interpro_gff(str(gff), str(hits), str(desc))
        assert list(csv.reader(desc.open())) == [
            ["Name", "description"],
            ["PF00001", "7 transmembrane receptor"],
        ]


class TestKofamScan:
    KO_LIST = (
        "knum\tthreshold\tscore_type\tprofile_type\tF-measure\tnseq\tnseq_used"
        "\talen\tmlen\teff_nseq\tre/pos\tdefinition\n"
        "K00001\t320.40\tdomain\tall\t0.88\t97\t92\t560\t288\t1.26\t0.589"
        "\talcohol dehydrogenase [EC:1.1.1.1]\n"
        "K00002\t451.07\tfull\tall\t0.38\t3423\t3272\t7497\t439\t7.19\t0.590"
        "\talcohol dehydrogenase (NADP+) [EC:1.1.1.2]\n"
    )
    DETAIL = (
        "#\n"
        "* gene1\tK00001\t320.40\t400.0\t1e-30\talcohol dehydrogenase\n"
        "  gene2\tK00002\t451.07\t100.0\t1e-05\talcohol dehydrogenase (NADP+)\n"
    )

    def test_only_above_threshold_kos_reach_the_description_table(
            self, kofamscan, tmp_path):
        raw, hits, desc, kl = (
            tmp_path / n for n in ("raw.txt", "hits.csv", "desc.csv", "ko_list"))
        raw.write_text(self.DETAIL)
        kl.write_text(self.KO_LIST)
        kos = kofamscan.parse_kofamscan(str(raw), str(hits))
        assert kos == {"K00001"}
        kofamscan.write_ko_definitions(kl, kos, str(desc))
        assert list(csv.reader(desc.open())) == [
            ["KO", "definition"],
            ["K00001", "alcohol dehydrogenase [EC:1.1.1.1]"],
        ]

    def test_a_multi_word_definition_survives(self, kofamscan, tmp_path):
        # The detail format's own definition field is whitespace-split by the hit
        # parser; ko_list is read instead precisely so this stays one string.
        desc, kl = tmp_path / "desc.csv", tmp_path / "ko_list"
        kl.write_text(self.KO_LIST)
        kofamscan.write_ko_definitions(kl, {"K00002"}, str(desc))
        rows = list(csv.reader(desc.open()))
        assert rows[1] == ["K00002", "alcohol dehydrogenase (NADP+) [EC:1.1.1.2]"]


class TestMerge:
    def test_the_description_merge_keeps_one_row_per_key(self, merge_uniref, tmp_path):
        a, b, out = (tmp_path / n for n in ("a.tsv", "b.tsv", "merged.tsv"))
        a.write_text("sseqid\tdescription\nUniRef50_A\tone\nUniRef50_B\ttwo\n")
        b.write_text("sseqid\tdescription\nUniRef50_B\ttwo\nUniRef50_C\tthree\n")
        merge_uniref._concat_unique([_Staged(a), _Staged(b)], out, "\t")
        assert out.read_text().splitlines() == [
            "sseqid\tdescription", "UniRef50_A\tone", "UniRef50_B\ttwo",
            "UniRef50_C\tthree",
        ]

    def test_the_hit_merge_writes_the_header_once(self, merge_uniref, tmp_path):
        a, b, out = (tmp_path / n for n in ("a.tsv", "b.tsv", "merged.tsv"))
        a.write_text("qseqid\tbsr\nq1\t0.5\n")
        b.write_text("qseqid\tbsr\nq2\t0.7\n")
        merge_uniref._concat_with_header([_Staged(a), _Staged(b)], out)
        assert out.read_text().splitlines() == ["qseqid\tbsr", "q1\t0.5", "q2\t0.7"]
