from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..models.workflow import WorkflowPlan, WorkflowTask

if TYPE_CHECKING:
    from .contract_runtime import CompiledTask, ContractReport


@dataclass
class PlanExecutionOracle:
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

        results = [e for e in events if e.get("type") == "bootstrap_result"]
        if results:
            assert any(int(e.get("code", 1)) == 0 for e in results), (
                "no bootstrap_result reported success"
            )

    def validate_contract_only(
        self, plan: WorkflowPlan, compiled: "CompiledTask"
    ) -> "ContractReport":
        from .contract_runtime import validate_contract

        assert plan is compiled.task.plan, (
            "validate_contract_only: compiled.task.plan must be the plan argument"
        )
        return validate_contract(compiled)
