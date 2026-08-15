from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT = Path(__file__).parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


try:
    DIAGNOSTICS = load_module(
        "grouping_diagnostics",
        PROJECT / "processes" / "grouping_diagnostics" / "grouping_diagnostics.py",
    )
    AUGMENTATION = load_module(
        "group_label_augmentation",
        PROJECT / "processes" / "group_label_augmentation" / "group_label_augmentation.py",
    )
    MODULE_IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    DIAGNOSTICS = None
    AUGMENTATION = None
    MODULE_IMPORT_ERROR = exc


@unittest.skipIf(MODULE_IMPORT_ERROR is not None, "grouping module dependencies are supplied by its environment")
class GroupLabelAugmentationTest(unittest.TestCase):
    def test_outlier_is_not_used_as_soft_label_training_data(self) -> None:
        metadata = pd.DataFrame(
            {"group": ["A", "A", "outlier", np.nan]},
            index=["a1", "a2", "outlier_sample", "missing"],
        )
        distance = np.array(
            [
                [0.0, 0.2, 0.8, 0.5],
                [0.2, 0.0, 0.8, 0.6],
                [0.8, 0.8, 0.0, 0.01],
                [0.5, 0.6, 0.01, 0.0],
            ]
        )

        assignments = DIAGNOSTICS.soft_label_missing(
            distance,
            metadata,
            "group",
            k=2,
            excluded_labels={"outlier"},
            min_class_samples=2,
        )

        self.assertEqual(assignments.loc[0, "assigned_label"], "A")
        self.assertNotIn("outlier", set(assignments["assigned_label"]))

    def test_only_validated_non_outlier_assignments_are_applied(self) -> None:
        metadata = pd.DataFrame(
            {
                "sampleID": ["observed", "outlier_sample", "accepted", "rejected"],
                "group": ["A", "outlier", np.nan, np.nan],
            }
        )
        assignments = pd.DataFrame(
            [
                {
                    "sample": "accepted",
                    "group_col": "group",
                    "assigned_label": "A",
                    "confidence": 0.9,
                    "nearest_distance": 0.1,
                    "neighbor_agreement": 0.8,
                },
                {
                    "sample": "rejected",
                    "group_col": "group",
                    "assigned_label": "outlier",
                    "confidence": 0.99,
                    "nearest_distance": 0.01,
                    "neighbor_agreement": 1.0,
                },
            ]
        )
        validation = pd.DataFrame(
            [
                {
                    "group_col": "group",
                    "status": "ok",
                    "balanced_accuracy": 0.8,
                    "nearest_distance_threshold": 0.5,
                }
            ]
        )

        selected = AUGMENTATION.select_assignments(
            assignments,
            validation,
            "group",
            {"outlier"},
            min_confidence=0.7,
            min_neighbor_agreement=0.6,
            min_cv_balanced_accuracy=0.6,
        )
        augmented, audit = AUGMENTATION.augment_metadata(
            metadata,
            selected,
            "sampleID",
            "group",
        )

        values = augmented.set_index("sampleID")["group"].to_dict()
        sources = augmented.set_index("sampleID")["group_assignment_source"].to_dict()
        self.assertEqual(values["outlier_sample"], "outlier")
        self.assertEqual(values["accepted"], "A")
        self.assertTrue(pd.isna(values["rejected"]))
        self.assertEqual(sources["accepted"], "soft_assigned")
        self.assertEqual(sources["rejected"], "unassigned")
        self.assertEqual(len(audit), 4)


if __name__ == "__main__":
    unittest.main()
