"""Tutorial 1: build a pangenome heatmap from three NCBI accessions."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from .base import PromptContext, VerifyContext, standard_verify


@dataclass
class MyFirstAgentScenario:
    name: str = "my_first_agent"
    tutorial_path: str = "agentic/tutorials/my_first_agent.rst"
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/results/**/heatmap*.svg",
        "workspace/results/**/*.svg",
    ])
    expected_trace: tuple[str, str] | None = (
        "ncbi::assembly_accession",
        "pangenome::heatmap",
    )
    timeout_s: float = 1800.0
    pre_install_metasmith: bool = True

    def build_prompt(self, ctx: PromptContext) -> str:
        template = (Path(__file__).resolve().parents[1]
                    / "prompts" / "ralph_system.md").read_text()
        return template.format(
            PRELUDE=ctx.prelude_text,
            TUTORIAL_REL=self.tutorial_path,
            RUNTIME=ctx.runtime,
        )

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        return standard_verify(
            vctx, result,
            artifact_globs=self.expected_artifact_globs,
            expected_trace=self.expected_trace,
        )
