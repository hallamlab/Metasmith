import importlib.util
from pathlib import Path
import sys
import types
import unittest

import numpy as np


# The helper under test has no SciPy dependency; stub heavy analysis imports so
# this regression test also runs in lightweight controller environments.
scipy = types.ModuleType("scipy")
scipy.stats = types.ModuleType("scipy.stats")
scipy.stats.mannwhitneyu = lambda *args, **kwargs: None
sys.modules.setdefault("scipy", scipy)
sys.modules.setdefault("scipy.stats", scipy.stats)
statsmodels = types.ModuleType("statsmodels")
statsmodels.stats = types.ModuleType("statsmodels.stats")
statsmodels.stats.multitest = types.ModuleType("statsmodels.stats.multitest")
statsmodels.stats.multitest.multipletests = lambda *args, **kwargs: None
sys.modules.setdefault("statsmodels", statsmodels)
sys.modules.setdefault("statsmodels.stats", statsmodels.stats)
sys.modules.setdefault("statsmodels.stats.multitest", statsmodels.stats.multitest)


SCRIPT = Path(__file__).parents[1] / "processes" / "power_analysis_pipeline" / "power_taxonomic_abundance.py"
SPEC = importlib.util.spec_from_file_location("power_taxonomic_abundance", SCRIPT)
POWER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POWER)


class TaxonomicPowerTests(unittest.TestCase):
    def test_case_group_counts_are_patient_level(self):
        patients = np.array(["case_1", "case_1", "case_2", "ctrl_1", "ctrl_1"])
        status = np.array(["Cancer", "Cancer", "Cancer", "Control", "Control"])
        self.assertEqual(POWER.case_group_patient_counts(patients, status), (2, 1))

    def test_case_group_counts_detect_missing_group(self):
        patients = np.array(["ctrl_1", "ctrl_2"])
        status = np.array(["Control", "Non-Cancer"])
        self.assertEqual(POWER.case_group_patient_counts(patients, status), (0, 2))


if __name__ == "__main__":
    unittest.main()
