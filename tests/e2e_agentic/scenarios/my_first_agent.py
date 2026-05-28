"""Tutorial 1: build a pangenome heatmap from three NCBI accessions."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from ..harness.sandbox import SandboxLayout
from ..install_mock.verify_local_artifacts import InstallContext
from .base import PromptContext, VerifyContext, standard_verify
from ._fixture_utils import stage_real_libraries


def _stage_pangenome_fixtures(layout: SandboxLayout) -> None:
    """Clone the real MetasmithLibraries checkout into the sandbox.

    The tutorials load ``MLIB/data_types/*.yml``, ``MLIB/resources/{containers,lib}``,
    and ``MLIB/transforms/{logistics,pangenome}`` from the canonical
    ``MetasmithLibraries`` layout. Rather than synthesising fixtures that
    drift from real type and transform names (``getNcbiAssembly``,
    ``ppanggolin``, ``heatmap``, etc.), the harness clones the real lib
    in via :func:`stage_real_libraries`. The agent then sees the same
    library a tutorial reader would clone with
    ``git clone https://github.com/hallamlab/MetasmithLibraries.git``.
    """
    stage_real_libraries(layout)


@dataclass
class MyFirstAgentScenario:
    name: str = "my_first_agent"
    tutorial_path: str = "tutorials/my_first_agent.rst"
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        # Metasmith places step outputs under
        # <workspace>/<msm_home|agent_home>/runs/<task_key>/results/<step>/
        # — anywhere a `runs/<key>/results/pangenome-heatmap/*.svg` shows
        # up counts as success.
        "workspace/**/runs/*/results/pangenome-heatmap/*.svg",
    ])
    expected_trace: tuple[str, str] | None = (
        "ncbi::assembly_accession",
        "pangenome::heatmap",
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
