from __future__ import annotations

import hashlib
import importlib.util
import gzip
import tempfile
import unittest
from pathlib import Path

import yaml


SCRIPT = Path(__file__).parents[1] / "examples/mock_test/configure_mock_run.py"
SPEC = importlib.util.spec_from_file_location("configure_mock_run", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

VALIDATOR_SCRIPT = Path(__file__).parents[1] / "examples/mock_test/validate_results.py"
VALIDATOR_SPEC = importlib.util.spec_from_file_location("validate_results", VALIDATOR_SCRIPT)
VALIDATOR = importlib.util.module_from_spec(VALIDATOR_SPEC)
assert VALIDATOR_SPEC.loader is not None
VALIDATOR_SPEC.loader.exec_module(VALIDATOR)


class MockTestConfigTest(unittest.TestCase):
    def test_build_config_replaces_all_fixture_paths(self) -> None:
        project = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "fixture"
            output = root / "output"
            runtime = output / ".aspire"
            config = MODULE.build_config(
                project / "examples/mock.local.yml", dataset, output, runtime, project
            )
            self.assertEqual(config["paths"]["input_dir"], str(dataset / "fastq"))
            self.assertEqual(config["paths"]["manifest"], str(dataset / "fastq_manifest.tsv"))
            self.assertEqual(config["metadata_plots"]["metadata"], str(dataset / "sample_metadata.tsv"))
            self.assertEqual(config["voc_correlation"]["voc_table"], str(dataset / "chemistry.tsv"))
            self.assertEqual(config["paths"]["output_dir"], str(output))
            self.assertEqual(config["paths"]["runtime_dir"], str(runtime))
            self.assertTrue(config["paths"]["keep_runtime_dir"])
            self.assertTrue(Path(config["table_filter"]["script"]).is_absolute())
            self.assertTrue(all(Path(path).is_absolute() for path in config["environments"].values()))

    def test_checksum_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp)
            payload = dataset / "payload.tsv"
            payload.write_text("fixture\n")
            digest = hashlib.sha256(payload.read_bytes()).hexdigest()
            (dataset / "checksums.tsv").write_text(
                f"relative_path\tsha256\npayload.tsv\t{digest}\n"
            )
            MODULE.validate_checksums(dataset)
            payload.write_text("changed\n")
            with self.assertRaises(SystemExit):
                MODULE.validate_checksums(dataset)

    def test_generated_yaml_is_serializable(self) -> None:
        project = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = MODULE.build_config(
                project / "examples/mock.local.yml",
                root / "fixture",
                root / "output",
                root / "runtime",
                project,
            )
            reparsed = yaml.safe_load(yaml.safe_dump(config, sort_keys=False))
            self.assertEqual(reparsed["indicspecies"]["perms"], 999)
            self.assertEqual(reparsed["power_analysis"]["sample_sizes_cancer"], "4,6,8,10")

    def test_truth_mapping_accepts_trimmed_inferred_sequences(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "dataset"
            results = root / "results"
            dataset.mkdir()
            fasta = results / "intermediates/ASVs/ASVs.fasta.gz"
            fasta.parent.mkdir(parents=True)
            (dataset / "ground_truth_feature_registry.tsv").write_text(
                "ASV_ID\tv4_sequence\ntruth_1\tAAAACCCCGGGGTTTT\n"
            )
            with gzip.open(fasta, "wt") as handle:
                handle.write(">ASV1;size=20\nCCCCGGGG\n")
            self.assertEqual(
                VALIDATOR.map_inferred_to_truth(dataset, results), {"ASV1": "truth_1"}
            )

    def test_portable_manifest_recovers_stale_fastq_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "mock_dataset"
            fastq = dataset / "fastq"
            fastq.mkdir(parents=True)
            (fastq / "S1_R1.fastq.gz").write_bytes(b"fixture")
            (fastq / "S1_R2.fastq.gz").write_bytes(b"fixture")
            (dataset / "fastq_manifest.tsv").write_text(
                "sample_id\tfastq_r1\tfastq_r2\n"
                "S1\t/old/machine/S1_R1.fastq.gz\t/old/machine/S1_R2.fastq.gz\n"
            )
            destination = Path(tmp) / "mock_run.manifest.tsv"
            path, ids = MODULE.write_portable_manifest(dataset, destination)
            content = path.read_text()
            self.assertEqual(ids, {"S1"})
            self.assertIn(str((fastq / "S1_R1.fastq.gz").resolve()), content)
            self.assertIn(str((fastq / "S1_R2.fastq.gz").resolve()), content)


if __name__ == "__main__":
    unittest.main()
