from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..models.workflow import WorkflowTask


@dataclass
class PlanExecutionOracle:
    """Validate virtual runtime trace against WorkflowPlan semantics."""

    task: WorkflowTask

    def expected_bootstrap_sequence(self) -> list[int]:
        expected: list[int] = []
        for step in self.task.plan.steps:
            total = max(1, len(step.group_by_instances))
            batch = max(1, int(step.transform.batch_size))
            calls = (total + batch - 1) // batch
            expected.extend([step.order] * calls)
        return expected

    def expected_arity(self) -> dict[int, dict[str, int]]:
        out: dict[int, dict[str, int]] = {}
        for step in self.task.plan.steps:
            out[step.order] = {
                dep.key: len(step.dependency_map.get(dep, []))
                for dep in list(step.transform.model.requires)
                + [d for group in step.transform.model.produces for d in group]
            }
        return out

    def validate_trace(self, events: list[dict[str, Any]]) -> None:
        bootstrap = [e for e in events if e.get("type") == "bootstrap_call"]
        actual = [int(e["step"]) for e in bootstrap]
        expected = self.expected_bootstrap_sequence()
        assert actual == expected, f"bootstrap sequence mismatch: expected {expected}, got {actual}"

        expected_arity = self.expected_arity()
        for e in bootstrap:
            step = int(e["step"])
            got = {str(k): int(v) for k, v in dict(e.get("dep_arity", {})).items()}
            assert step in expected_arity, f"unknown step [{step}] in trace"
            assert got == expected_arity[step], (
                f"dependency arity mismatch for step {step}: "
                f"expected {expected_arity[step]}, got {got}"
            )

        manifests = [e for e in events if e.get("type") == "manifest_written"]
        target_keys = {t.instance.dtype.key for t in self.task.plan.targets}
        written_keys = {str(e.get("dep_key")) for e in manifests if int(e.get("count", 0)) > 0}
        missing = sorted(target_keys - written_keys)
        assert not missing, f"missing manifests for target dependency keys: {missing}"
