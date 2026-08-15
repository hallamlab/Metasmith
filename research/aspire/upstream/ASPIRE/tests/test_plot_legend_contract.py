from __future__ import annotations

import ast
import unittest
from pathlib import Path


PROJECT = Path(__file__).parents[1]


class PlotLegendContractTest(unittest.TestCase):
    def test_legends_are_outside_and_right_aligned(self) -> None:
        failures: list[str] = []
        for path in (PROJECT / "processes").rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Attribute) or node.func.attr != "legend":
                    continue
                owner = ast.dump(node.func.value)
                keywords = {keyword.arg: keyword.value for keyword in node.keywords if keyword.arg}

                # This temporary, invisible legend is used only to measure layout size.
                if "axtmp" in owner:
                    continue

                loc = keywords.get("loc")
                if not isinstance(loc, ast.Constant) or loc.value != "upper left":
                    failures.append(f"{path.relative_to(PROJECT)}:{node.lineno}: loc must be upper left")
                if "bbox_to_anchor" not in keywords:
                    failures.append(
                        f"{path.relative_to(PROJECT)}:{node.lineno}: bbox_to_anchor is required"
                    )

        self.assertEqual(failures, [], "\n".join(failures))


if __name__ == "__main__":
    unittest.main()
