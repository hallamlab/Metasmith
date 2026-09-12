from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from ..harness.sandbox import SandboxLayout
from ..install_mock.verify_local_artifacts import InstallContext
from .base import PromptContext, VerifyContext, standard_verify
from .my_first_agent import _stage_pangenome_fixtures


@dataclass
class CustomTransformsScenario:
    name: str = "custom_transforms"
    tutorial_path: str = "tutorials/custom_transforms.rst"
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/**/runs/*/results/**/*.tsv",
    ])
    expected_trace: tuple[str, str] | None = (
        "ncbi::assembly_accession",
        "ani::table",
    )
    timeout_s: float = 1800.0
    pre_install_metasmith: bool = True

    def setup_fixtures(self, layout: SandboxLayout, ctx: InstallContext) -> None:
        _stage_pangenome_fixtures(layout)

    def build_prompt(self, ctx: PromptContext) -> str:
        template = (Path(__file__).resolve().parents[1]
                    / "prompts" / "ralph_system.md").read_text()
        return template.format(
            SANDBOX=str(ctx.sandbox),
            TUTORIAL_REL=self.tutorial_path,
            RUNTIME=ctx.runtime,
            TASK_KEY=self.name,
        )

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        return standard_verify(
            vctx, result,
            artifact_globs=self.expected_artifact_globs,
            expected_trace=self.expected_trace,
        )
