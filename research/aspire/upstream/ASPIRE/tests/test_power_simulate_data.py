import importlib.util
from pathlib import Path
import unittest

import numpy as np


SCRIPT = Path(__file__).parents[1] / "processes" / "power_analysis_pipeline" / "simulate_data.py"
SPEC = importlib.util.spec_from_file_location("simulate_data", SCRIPT)
SIMULATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SIMULATE)


class PowerSimulationHelperTests(unittest.TestCase):
    def setUp(self):
        self.counts = np.array([[10, 90], [20, 80], [30, 70], [40, 60]])
        self.patients = np.array(["case_1", "case_1", "ctrl_1", "ctrl_1"])
        self.status = np.array(["Cancer", "Cancer", "Control", "Control"])

    def test_patient_bootstrap_preserves_repeated_samples_and_groups(self):
        counts, patients, status = SIMULATE.bootstrap_patients(
            self.counts, self.patients, self.status, 2, 2, seed=4
        )
        self.assertEqual(counts.shape, (8, 2))
        self.assertEqual(len(np.unique(patients[status == "Cancer"])), 2)
        self.assertEqual(len(np.unique(patients[status == "Control"])), 2)

    def test_spike_preserves_depth_and_only_changes_target_group(self):
        spiked = SIMULATE.spike_in_fold_change(self.counts, self.status, [0], 2.0)
        np.testing.assert_array_equal(spiked.sum(axis=1), self.counts.sum(axis=1))
        np.testing.assert_array_equal(spiked[self.status == "Control"], self.counts[self.status == "Control"])
        self.assertTrue((spiked[self.status == "Cancer", 0] > self.counts[self.status == "Cancer", 0]).all())

    def test_sample_type_filter_keeps_arrays_aligned(self):
        types = np.array(["BAL", "Brush", "BAL", "Brush"])
        counts, patients, status = SIMULATE.filter_by_sample_type(
            self.counts, self.patients, self.status, types, "BAL"
        )
        np.testing.assert_array_equal(counts, self.counts[[0, 2]])
        np.testing.assert_array_equal(patients, self.patients[[0, 2]])
        np.testing.assert_array_equal(status, self.status[[0, 2]])

    def test_patient_aggregation_sums_repeated_samples(self):
        counts, patients = SIMULATE.aggregate_to_patient_level(self.counts, self.patients)
        np.testing.assert_array_equal(patients, ["case_1", "ctrl_1"])
        np.testing.assert_array_equal(counts, [[30, 170], [70, 130]])


if __name__ == "__main__":
    unittest.main()
