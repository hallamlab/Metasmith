import gzip
import math
import sys

import pytest

from conftest import MLIB

sys.path.insert(0, str(MLIB / "resources" / "lib"))
sys.path.insert(0, str(MLIB / "resources" / "lib" / "lcastar"))
import contig_taxonomy  # noqa: E402
import ec_consensus  # noqa: E402
import lcastar as ls  # noqa: E402
import ncbi_to_gtdb  # noqa: E402
import pandas as pd  # noqa: E402

ECOLI = "d__Bacteria;p__Pseudomonadota;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Escherichia;s__Escherichia coli"
SALMO = "d__Bacteria;p__Pseudomonadota;c__Gammaproteobacteria;o__Enterobacterales;f__Enterobacteriaceae;g__Salmonella;s__Salmonella enterica"
SYNE = "d__Bacteria;p__Cyanobacteriota;c__Cyanobacteriia;o__Cyanobacteriales;f__Microcystaceae;g__Synechocystis;s__Synechocystis sp000284455"


class TestParsers:
    def test_standard_and_metabuli_forms_agree(self):
        doubled = ";".join(f"{p[0]}_{p}" for p in ECOLI.split(";"))
        assert ls.parse_gtdb(ECOLI) == ls.parse_gtdb(doubled)
        assert ls.parse_gtdb(ECOLI)[-1] == "s__Escherichia coli"

    def test_empty_ranks_and_unknown_prefixes_are_dropped(self):
        assert ls.parse_gtdb("d__Bacteria;p__;-_root;x__junk") == ("d__Bacteria",)
        assert ls.gtdb_rank(ls.parse_gtdb("d__Bacteria;p__Bacillota")) == "phylum"

    def test_ec_path_stops_at_a_dash(self):
        assert ls.parse_ec("EC:1.2.3.4") == ("1", "1.2", "1.2.3", "1.2.3.4")
        assert ls.parse_ec("2.7.-.-") == ("2", "2.7")
        assert ls.ec_rank(ls.parse_ec("2.7.-.-")) == "subclass"

    def test_lca_is_the_common_prefix(self):
        e, s = ls.parse_gtdb(ECOLI), ls.parse_gtdb(SALMO)
        assert ls.lca([e, s])[-1] == "f__Enterobacteriaceae"
        assert ls.lca([e, ls.parse_gtdb(SYNE)]) == ("d__Bacteria",)


class TestLcaStar:
    def test_the_majority_branch_wins_not_the_minority(self):
        # The greedy port this replaces sorted children by sum(r log r) ascending,
        # which descends into the one-vote branch here.
        e, s = ls.parse_gtdb(ECOLI), ls.parse_gtdb(SYNE)
        r = ls.lcastar([(e, 1.0)] * 10 + [(s, 1.0)])
        assert r.lineage == e
        assert r.majority
        assert r.fraction == pytest.approx(10 / 11)

    def test_a_split_genus_resolves_to_the_family(self):
        e, s = ls.parse_gtdb(ECOLI), ls.parse_gtdb(SALMO)
        r = ls.lcastar([(e, 1.0)] * 4 + [(s, 1.0)] * 4 + [(ls.parse_gtdb(SYNE), 1.0)])
        assert r.lineage[-1] == "f__Enterobacteriaceae"
        assert not r.majority
        assert r.votes == 8

    def test_fractional_weights_vote_like_counts(self):
        e, s = ls.parse_gtdb(ECOLI), ls.parse_gtdb(SYNE)
        whole = ls.lcastar([(e, 3.0), (s, 1.0)])
        scaled = ls.lcastar([(e, 0.3), (s, 0.1)])
        assert whole.lineage == scaled.lineage
        assert whole.fraction == pytest.approx(scaled.fraction)

    def test_no_branch_over_alpha_is_the_root_at_p_one(self):
        r = ls.lcastar([(ls.parse_ec("1.1.1.1"), 5.0), (ls.parse_ec("2.7.1.1"), 3.0),
                        (ls.parse_ec("3.1.1.1"), 2.0)])
        assert r.lineage == ()
        assert r.p_value == 1.0

    def test_no_votes_is_no_result(self):
        assert ls.lcastar([]) is None
        assert ls.lcastar([(ls.parse_gtdb(ECOLI), 0.0)]) is None

    def test_the_ec_hierarchy_votes_the_same_way(self):
        r = ls.lcastar([(ls.parse_ec("1.1.1.1"), 1.0), (ls.parse_ec("1.1.1.2"), 1.0),
                        (ls.parse_ec("2.7.1.1"), 1.0)], alpha=0.51)
        assert r.lineage == ("1", "1.1", "1.1.1")


class TestPValue:
    def test_natural_log_statistic(self):
        x, m = 10.0, 2.0
        t = 2 * (m * math.log(2 * m / (m + x)) + x * math.log(2 * x / (m + x)))
        assert ls.supremacy_pvalue(x, m) == pytest.approx(math.erfc(math.sqrt(t / 2)))
        assert ls.supremacy_pvalue(x, m) < 0.05

    def test_no_supremacy_is_p_one(self):
        assert ls.supremacy_pvalue(3, 3) == 1.0
        assert ls.supremacy_pvalue(2, 5) == 1.0


class TestNcbiToGtdb:
    # 1 root > 2 Bacteria > 10 family > (11, 12, 13 species); 1 > 4 Archaea > 14;
    # 1 > 3 Eukaryota > 30. 13, 3 and 30 have no GTDB genome.
    PARENT = {1: 1, 2: 1, 10: 2, 11: 10, 12: 10, 13: 10, 4: 1, 14: 4, 3: 1, 30: 3}
    ARCH = "d__Archaea;p__Methanobacteriota"

    def test_species_family_and_unsequenced_taxa(self):
        e, s = ls.parse_gtdb(ECOLI), ls.parse_gtdb(SALMO)
        pairs = {(11, e): 9, (11, s): 1, (12, s): 5, (14, ls.parse_gtdb(self.ARCH)): 2}
        rows = ncbi_to_gtdb.build(self.PARENT, {99: 11}, pairs, 0.5)
        assert rows[11][0] == e and rows[11][1] == pytest.approx(0.9)
        # 9 of 15 genomes under 10 are E. coli: a majority, not unanimity.
        assert rows[10][0] == e and rows[10][1] == pytest.approx(0.6)
        assert rows[13][0][-1] == "f__Enterobacteriaceae" and rows[13][2] == "ancestor:10"
        assert rows[99] == rows[11]
        assert 30 not in rows and 3 not in rows

    def test_the_map_round_trips_through_the_cli(self, tmp_path):
        dump = tmp_path / "dump"
        dump.mkdir()
        (dump / "nodes.dmp").write_text(
            "".join(f"{c}\t|\t{p}\t|\tno rank\t|\n" for c, p in self.PARENT.items()))
        (dump / "merged.dmp").write_text("")
        meta = tmp_path / "bac120_metadata_r232.tsv.gz"
        with gzip.open(meta, "wt") as fh:
            fh.write("accession\tgtdb_taxonomy\tncbi_taxid\n")
            fh.write(f"RS_1\t{ECOLI}\t11\nRS_2\t{SALMO}\t12\n")
        out = tmp_path / "map.tsv.gz"
        sys.argv = ["ncbi_to_gtdb", "--taxdump", str(dump), "--metadata", str(meta), "--out", str(out)]
        ncbi_to_gtdb.main()
        with gzip.open(out, "rt") as fh:
            lines = fh.read().splitlines()
        assert lines[0] == "taxid\tlineage\tsupport\tvia"
        assert any(line.startswith(f"11\t{ECOLI}\t1.0000\tgenomes") for line in lines)


HIT_COLS = ["qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
            "qstart", "qend", "sstart", "send", "evalue", "bitscore", "bsr"]


class TestContigTaxonomy:
    def _hit(self, orf, sseqid, bits=300.0, bsr=0.9, length=200):
        return [orf, sseqid, "90", str(length), "1", "0", "1", "200", "1", "200", "1e-50", str(bits), str(bsr)]

    def test_three_sources_vote_per_contig(self, tmp_path):
        hits = tmp_path / "hits.tsv"
        rows = [self._hit(f"c1_{i}", "UniRef50_A") for i in (1, 2, 3)]
        rows += [self._hit("c2_1", "UniRef50_B"), self._hit("c2_2", "UniRef50_A", bsr=0.1)]
        hits.write_text("\t".join(HIT_COLS) + "\n" + "".join("\t".join(r) + "\n" for r in rows))
        descs = tmp_path / "descs.tsv"
        descs.write_text("sseqid\tdescription\n"
                         "UniRef50_A\tUniRef50_A Enzyme n=3 Tax=Escherichia coli TaxID=11 RepID=X\n"
                         "UniRef50_B\tUniRef50_B Enzyme n=2 Tax=Salmonella TaxID=12 RepID=Y\n")
        tax_map = tmp_path / "map.tsv.gz"
        with gzip.open(tax_map, "wt") as fh:
            fh.write(f"taxid\tlineage\tsupport\tvia\n11\t{ECOLI}\t1\tgenomes\n12\t{SALMO}\t1\tgenomes\n")
        doubled = ";".join(f"{p[0]}_{p}" for p in ECOLI.split(";"))
        metabuli = tmp_path / "metabuli.tsv"
        metabuli.write_text(f"1\tc1\t5\t5000\t0.9\t1e-10\tspecies\t{doubled}\t5:10 6:2\n"
                            f"1\tc2\t5\t4000\t0.9\t1e-10\tspecies\t{doubled}\t\n"
                            f"0\tc3\t0\t900\t0\t1\t-\t-\t\n")
        mb_ref = tmp_path / "mb_ref"
        mb_ref.mkdir()
        rrna = tmp_path / "rrna.tsv"
        rrna.write_text("gene\tcontig\tproduct\tlineage\trank\tbest_identity\tbest_bitscore\tn_hits\n"
                        f"rrna_1\tc1\t16S\t{ECOLI}\tspecies\t99.9\t2700\t1\n")
        out = tmp_path / "out.tsv"
        sys.argv = ["contig_taxonomy", "--hits", str(hits), "--descriptions", str(descs),
                    "--map", str(tax_map), "--metabuli", str(metabuli), "--metabuli-ref", str(mb_ref),
                    "--rrna", str(rrna), "--out", str(out)]
        contig_taxonomy.main()

        table = pd.read_csv(out, sep="\t").set_index("contig")
        assert table.loc["c1", "lineage"] == ECOLI
        assert (table.loc["c1", "orf_votes"], table.loc["c1", "metabuli_votes"],
                table.loc["c1", "rrna_votes"]) == (3, 10, 1)
        assert table.loc["c1", "metabuli_call"] == ECOLI
        # One Salmonella ORF in the assembly is below the support of 2, so it
        # votes for the family it shares with the E. coli ORFs.
        assert table.loc["c2", "lineage"].endswith("f__Enterobacteriaceae")
        # An empty match column falls back to one vote for the contig's own call.
        assert table.loc["c2", "metabuli_votes"] == 1
        assert "c3" not in table.index


class TestEcConsensus:
    def test_lanes_vote_once_each(self):
        gpr = pd.DataFrame({
            "orf": ["o1"] * 4,
            "channel": ["clean", "kofam", "uniref50", "pbert"],
            "mnxr": ["M1", "M1", "M2", "M3"],
            "intermediate_id": ["1.1.1.1", "K00001", "P00001", "M3"],
        })
        bridge = pd.DataFrame({"id": ["1.1.1.1", "1.1.1.2", "K00001"],
                               "id_source": ["ec", "ec", "ko"], "mnxr": ["M1", "M2", "M1"]})
        rows = ec_consensus.consensus(ec_consensus.lane_ecs(gpr, ec_consensus.mnxr_to_ecs(bridge)), 0.51)
        orf, ec, rank, _, fraction, votes, lanes, agreeing = rows[0]
        assert (orf, ec, rank, lanes, agreeing) == ("o1", "1.1.1.1", "enzyme", 3, "clean,kofam")
        assert fraction == pytest.approx(2 / 3)
