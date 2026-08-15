from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SHIM = Path(__file__).parents[1] / "processes/controller/bin/mamba"


class MambaShimTest(unittest.TestCase):
    def run_shim(self, environment_file: str) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_mamba = root / "mamba"
            fake_mamba.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$@\"\n")
            fake_mamba.chmod(0o755)
            env = os.environ.copy()
            env.update(
                {
                    "ASPIRE_REAL_MAMBA": str(fake_mamba),
                    "ASPIRE_MAMBA_LOCK_FILE": str(root / "mamba.lock"),
                    "ASPIRE_MAMBA_BUILD_TIMEOUT": "10",
                }
            )
            result = subprocess.run(
                [str(SHIM), "env", "create", "--file", environment_file],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
            return result.stdout

    def test_qiime2_environment_uses_flexible_priority(self) -> None:
        output = self.run_shim("/project/processes/shared_envs/qiime2.yml")
        self.assertIn("--override-channels\n", output)
        self.assertIn("--channel-priority\nflexible\n", output)
        self.assertNotIn("--strict-channel-priority", output)

    def test_taxonomy_environment_uses_flexible_priority(self) -> None:
        output = self.run_shim("/project/processes/taxonomy/env.yml")
        self.assertIn("--channel-priority\nflexible\n", output)

    def test_other_environments_use_strict_priority(self) -> None:
        output = self.run_shim("/project/processes/diversity_analysis/env.yml")
        self.assertIn("--override-channels\n", output)
        self.assertIn("--strict-channel-priority\n", output)
        self.assertNotIn("--channel-priority\nflexible", output)


if __name__ == "__main__":
    unittest.main()
