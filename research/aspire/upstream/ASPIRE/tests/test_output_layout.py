from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "processes" / "output_layout" / "organize_outputs.py"
SPEC = importlib.util.spec_from_file_location("organize_outputs", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class OutputLayoutTest(unittest.TestCase):
    def write(self, root: Path, relative: str, content: str = "fixture\n") -> None:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def test_atomic_publication_and_routing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            staging = root / "run_tmp" / "publication_staging"
            output = root / "run_output"
            config = root / "run.yml"
            config.write_text("paths:\n  output_dir: run_output\n")

            self.write(staging, "metadata/run_manifest.tsv")
            self.write(staging, "metadata/data_loss_sankey.svg", "<svg/>\n")
            self.write(staging, "metadata/data_loss_sankey.label.svg", "<svg/>\n")
            self.write(staging, "metadata/data_loss_sankey.label.html", "<html></html>\n")
            self.write(staging, "metadata/type_group_swarmplot_micro.svg", "<svg/>\n")
            self.write(staging, "metadata/collectors_curve_overlay.svg", "<svg/>\n")
            self.write(staging, "metadata/final_upset.tsv")
            self.write(staging, "metadata/final_upset.svg", "<svg/>\n")
            self.write(
                staging,
                "metadata/metadata_updated_micro.tsv",
                "sample_id\tParticipant_ID\tType_Group\tCase\n"
                "S1\tP1\tBAL\tControl\nS2\tP2\tBrush\tCancer\n",
            )
            self.write(staging, "ASVs/ASV_target.tsv")
            self.write(staging, "ASVs/ASV_counts.tsv")
            self.write(staging, "ASVs/ASV_final.micro.tsv", "ASV\tS1\tS2\nASV1\t2\t0\nASV2\t0\t3\n")
            self.write(staging, "stats/fastq_stats.tsv", "file\tnum_seqs\na_R1\t100\na_R2\t100\n")
            self.write(staging, "stats/fastp_fastqs.tsv", "file\tnum_seqs\na_R1\t90\na_R2\t90\n")
            self.write(staging, "stats/filtered_fastas.tsv", "file\tnum_seqs\na\t85\n")
            self.write(staging, "mito/mitomap/mito_ncbi.blast6.tsv")
            self.write(staging, "mito/ASVs/ASV_target.mito.tsv")
            self.write(staging, "taxonomy/ASV_taxonomy.tsv")
            self.write(staging, "taxonomy_patient_aware/cancer_control_taxa.tsv")
            self.write(staging, "indicspecies/Type_Group_indicator_species.tsv")
            self.write(staging, "indicspecies/plots/indicator_summary.svg", "<svg/>\n")
            self.write(staging, "diversity/shannon.tsv")
            self.write(staging, "diversity/alpha.svg", "<svg/>\n")
            self.write(staging, "spieceasi/spieceasi_network_pos_all.graphml")
            self.write(staging, "spieceasi/spieceasi_modules_all.tsv")
            self.write(staging, "spieceasi/network_degree_POS_ALL.svg", "<svg/>\n")
            self.write(staging, "reference/db/source.fasta", ">x\nACGT\n")
            self.write(staging, "summary/tables/ASV_master_long.tsv")
            self.write(staging, "logs/nextflow_report.html", "<html>report</html>\n")
            self.write(staging, "logs/nextflow_trace.tsv", "task_id\tstatus\n")

            MODULE.publish(staging, output, config)

            self.assertTrue((staging / "diversity/shannon.tsv").exists())
            self.assertTrue((output / "modules/diversity/tables/shannon.tsv").exists())
            self.assertTrue((output / "modules/diversity/plots/alpha.svg").exists())
            self.assertTrue((output / "modules/diversity/tables").is_dir())
            self.assertTrue((output / "modules/diversity/plots").is_dir())
            self.assertTrue((output / "modules/non_target_filtering/tables/ASV_target.tsv").exists())
            self.assertTrue((output / "modules/non_target_filtering/tables/ASV_target.mito.tsv").exists())
            self.assertTrue((output / "modules/non_target_filtering/tables/mitomap/mito_ncbi.blast6.tsv").exists())
            self.assertTrue((output / "modules/taxonomy/tables/ASV_taxonomy.tsv").exists())
            self.assertTrue((output / "modules/taxonomy/tables/cancer_control_taxa.tsv").exists())
            self.assertTrue((output / "modules/indicator_analysis/tables/Type_Group_indicator_species.tsv").exists())
            self.assertTrue((output / "modules/indicator_analysis/plots/indicator_summary.svg").exists())
            self.assertTrue((output / "modules/sankey/plots/data_loss_sankey.svg").exists())
            self.assertTrue((output / "modules/network_analysis/tables/spieceasi_network_pos_all.graphml").exists())
            self.assertTrue((output / "modules/network_analysis/tables/spieceasi_modules_all.tsv").exists())
            self.assertTrue((output / "modules/network_analysis/plots/network_degree_POS_ALL.svg").exists())
            self.assertTrue((output / "modules/upset/tables/final_upset.tsv").exists())
            self.assertTrue((output / "intermediates/ASVs/ASV_counts.tsv").exists())
            self.assertTrue((output / "references/reference/db/source.fasta").exists())
            self.assertTrue((output / "summary/tables/run_manifest.tsv").exists())
            self.assertTrue((output / "summary/tables/run_config.yml").exists())
            self.assertTrue((output / "summary/tables/module_output_manifest.tsv").exists())
            self.assertTrue((output / "summary/tables/module_summary.tsv").exists())
            self.assertTrue((output / "summary/tables/reference_checksums.tsv").exists())
            self.assertTrue((output / "summary/tables/intermediate_manifest.tsv").exists())
            self.assertTrue((output / "summary/tables/nextflow_artifact_manifest.tsv").exists())
            self.assertTrue((output / "summary/plots").is_dir())
            self.assertTrue((output / "summary/report").is_dir())
            self.assertTrue((output / "summary/plots/module_output_summary.svg").exists())
            self.assertTrue((output / "summary/report/ASPIRE_run_report.html").exists())
            report = (output / "summary/report/ASPIRE_run_report.html").read_text()
            self.assertIn("Non-interpretive inventory", report)
            self.assertIn("Data Accounting Summary", report)
            self.assertIn("Samples analyzed", report)
            self.assertIn("Participants", report)
            self.assertIn("ASVs retained", report)
            self.assertIn("Input read records", report)
            self.assertIn("Type Group", report)
            self.assertIn('src="../../modules/sankey/plots/data_loss_sankey.label.html"', report)
            self.assertIn("<iframe", report)
            self.assertIn("../../modules/metadata_plots/plots/type_group_swarmplot_micro.svg", report)
            self.assertIn("../../modules/upset/plots/final_upset.svg", report)
            self.assertIn("../../modules/collectors_curve/plots/collectors_curve_overlay.svg", report)
            self.assertLess(report.index("Data Accounting Summary"), report.index("Output Inventory"))
            self.assertIn("Nextflow run details", report)
            self.assertIn("../../logs/nextflow_report.html", report)
            self.assertIn("../../logs/nextflow_trace.tsv", report)
            self.assertIn("nextflow_artifact_manifest.tsv", report)

            MODULE.publish(staging, output, config)
            self.assertTrue((output / "modules/diversity/tables/shannon.tsv").exists())

    def test_nested_runtime_survives_republication(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "run_output"
            runtime = output / ".aspire"
            staging = runtime / "publication_staging"
            cached_task = runtime / "nf_work/ab/cached-task/.command.out"
            cached_task.parent.mkdir(parents=True)
            cached_task.write_text("cached\n")
            self.write(staging, "diversity/shannon.tsv", "sample\tshannon\nS1\t1.0\n")

            MODULE.publish(staging, output)

            self.assertEqual(cached_task.read_text(), "cached\n")
            self.assertTrue((staging / "diversity/shannon.tsv").exists())
            published = output / "modules/diversity/tables/shannon.tsv"
            self.assertIn("S1\t1.0", published.read_text())

            self.write(staging, "diversity/shannon.tsv", "sample\tshannon\nS1\t2.0\n")
            MODULE.publish(staging, output)

            self.assertEqual(cached_task.read_text(), "cached\n")
            self.assertIn("S1\t2.0", published.read_text())

            MODULE.organize(output)
            self.assertEqual(cached_task.read_text(), "cached\n")
            self.assertFalse((output / "modules/.aspire").exists())


if __name__ == "__main__":
    unittest.main()
