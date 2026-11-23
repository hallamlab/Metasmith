import sys

import pandas as pd

from conftest import MLIB

sys.path.insert(0, str(MLIB / "resources" / "lib"))
sys.path.insert(0, str(MLIB / "resources" / "lib" / "pathologic"))
import build_pf  # noqa: E402
import fabfos_evidence as fe  # noqa: E402
import gather  # noqa: E402
import pathway_table  # noqa: E402


def _hit(orf, lane, term, description, bsr, start=1, end=100, evalue=1e-30):
    return dict(orf=orf, lane=lane, term=term, description=description, score=bsr,
                start=start, end=end, evalue=evalue)


def test_gather_follows_metapathways_4(tmp_path):
    long, short = "M" * 80, "M" * 59
    lengths = gather.orf_lengths(_fasta(tmp_path, {"c1_1": long, "c1_2": long, "c1_3": long,
                                                   "c1_4": short, "c1_5": long}))
    table = pd.DataFrame([
        # A MetaCyc name weighs four times a UniRef90 one, and its reactions go in as METACYC.
        _hit("c1_1", "uniref90", "UniRef90_Q1", "glucokinase", 0.9),
        _hit("c1_1", "metacyc", "gnl|META|MONOMER-1", "glucokinase subunit of the thing (Escherichia coli)", 0.5),
        _hit("c1_1", "swissprot", "sp|P1|GLK_ECOLI", "Glucokinase OS=Escherichia coli", 0.8),
        # Below MP4's BSR floor, so UniRef90's name stands; its EC map adds an EC.
        _hit("c1_2", "metacyc", "gnl|META|MONOMER-1", "glucokinase (E. coli)", 0.39),
        _hit("c1_2", "uniref90", "UniRef90_Q2", "hexokinase", 0.6),
        # MP4 writes no ORF whose chosen name is exactly "hypothetical protein".
        _hit("c1_3", "uniref50", "UniRef50_Q3", "hypothetical protein", 0.9),
        # Under 60 aa, and an alignment under 45 aa, are both out.
        _hit("c1_4", "uniref90", "UniRef90_Q2", "hexokinase", 0.9),
        _hit("c1_5", "uniref90", "UniRef90_Q2", "hexokinase", 0.9, start=1, end=44),
    ])
    ec_map = {"P1": ["2.7.1.2"], "UniRef90_Q2": ["2.7.1.1"]}
    df = gather.gather(table, lengths, ec_map, {"MONOMER-1": ["GLUCOKIN-RXN"]})
    rows = sorted(zip(df["orf_id"], df["kind"], df["value"]))
    assert rows == [
        ("c1_1", "EC", "2.7.1.2"),
        ("c1_1", "FUNCTION", "glucokinase subunit thing"),
        ("c1_1", "METACYC", "GLUCOKIN-RXN"),
        ("c1_2", "EC", "2.7.1.1"),
        ("c1_2", "FUNCTION", "hexokinase"),
    ]


def _fasta(tmp_path, seqs):
    path = tmp_path / "orfs.faa"
    path.write_text("".join(f">{k} # x\n{v}\n" for k, v in seqs.items()))
    return path


def _ann(tmp_path, rows):
    ann = tmp_path / "ann.parquet"
    fe.write_parquet(pd.DataFrame(rows, columns=["orf_id", "kind", "value"]).assign(score=1.0, confidence="x"), ann)
    return ann


def test_the_community_pf_keeps_one_orf_per_name_and_a_mag_borrows_its_lines(tmp_path):
    ann = _ann(tmp_path, [("c1_1", "FUNCTION", "glucokinase"), ("c1_1", "EC", "2.7.1.2"),
                          ("c1_1", "METACYC", "GLUCOKIN-RXN"), ("c2_1", "FUNCTION", "glucokinase"),
                          ("c2_1", "EC", "9.9.9.9")])
    sys.argv = ["build_pf", "--annotations", str(ann), "--name", "s", "--out-dir", str(tmp_path / "all")]
    build_pf.main()
    assert (tmp_path / "all" / "0.pf").read_text() == (
        "ID\tc1_1\nNAME\tc1_1\nFUNCTION\tglucokinase\nMETACYC\tGLUCOKIN-RXN\nEC\t2.7.1.2\nPRODUCT-TYPE\tP\n//\n")
    mag = tmp_path / "bin.1.fna"
    mag.write_text(">c2\nACGT\n")
    sys.argv = ["build_pf", "--annotations", str(ann), "--name", "1bin.1", "--bin-fasta", str(mag),
                "--out-dir", str(tmp_path / "mag")]
    build_pf.main()
    assert (tmp_path / "mag" / "0.pf").read_text() == (
        "ID\tc2_1\nNAME\tc2_1\nFUNCTION\tglucokinase\nMETACYC\tGLUCOKIN-RXN\nEC\t2.7.1.2\nPRODUCT-TYPE\tP\n//\n")
    params = (tmp_path / "mag" / "organism-params.dat").read_text()
    assert "ID\tP1bin1\n" in params and "NCBI-TAXON-ID\t131567\n" in params


REPORT_HEADER = " | ".join(pathway_table.REPORT_COLUMNS) + "\n"


def test_the_reports_give_score_and_the_extra_columns(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "pwy-inference-report_2026-11-04.txt").write_text(
        "::: Pathway Inference Report\n"
        " (PWY-1 T 70\n  (PASSING-SCORE (REACTIONS-PRESENT (R1 R2)))\n  1.0 0.29)\n"
        " (PWY-2 NIL 85\n  (X)\n  1.0 0.0)\n"
        "List of pathways pruned (1):\n (PWY-9 T 1 (X) 1.0 0.5)\n")
    (reports / "pathways-report_2026-11-04.txt").write_text(
        "# fields\n" + REPORT_HEADER + "glycolysis|PWY-1|Glycolysis|C|0.29|1.17|1.0|PASSING-SCORE|http://x\n")
    assert pathway_table.inference_scores(reports) == {"PWY-1": "0.29"}
    assert pathway_table.report_rows(reports)["PWY-1"]["Reason to Keep"] == "PASSING-SCORE"
    code = "(COMPLETE-PATHWAY INSIDE-TAXONOMIC-RANGE (REACTIONS-PRESENT (R1 R2 R3)))"
    assert pathway_table.reactions_present(code) == ["R1", "R2", "R3"]
    assert pathway_table.reactions_present("PWY-HAS-NON-COMPUTATIONAL-EVIDENCE") == []


def test_a_pgdb_with_no_pathways_is_an_empty_table_only_when_the_reports_agree(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "pathways.dat").write_text(
        "Error: The flat-file generation program didn't specify what kind of data to put in this file.\n")
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "pathways-report_2026-11-04.txt").write_text("# fields\n" + REPORT_HEADER)
    (reports / "pwy-evidence-list.dat").write_text(";;; This file contains all inferred pathways\n")
    assert list(pathway_table.rows(tmp_path, "s", tmp_path / "w")) == []
    (reports / "pwy-evidence-list.dat").write_text(";;; header\n(PWY-1 R1)\n")
    try:
        list(pathway_table.rows(tmp_path, "s", tmp_path / "w"))
    except SystemExit:
        return
    raise AssertionError("an evidence list naming pathways passed as empty")
