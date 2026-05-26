"""Tutorial 1: build a pangenome heatmap from three NCBI accessions."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from ..harness.sandbox import SandboxLayout
from ..install_mock.verify_local_artifacts import InstallContext
from .base import PromptContext, VerifyContext, standard_verify
from ._fixture_utils import (
    TypeSpec, TransformSpec,
    build_type_lib_dir, build_transform_lib,
)


def _stage_pangenome_fixtures(layout: SandboxLayout) -> None:
    """Stage the type + transform libraries the tutorials reference.

    The tutorial uses relative paths (``data_types/ncbi.yml``,
    ``transforms/logistics``, ``transforms/pangenome``) from the sandbox
    root, so we stage there directly. The pre-existing sandbox copies
    of these dirs are overwritten with tutorial-shaped contents.
    """
    types_dir = layout.root / "data_types"
    build_type_lib_dir(layout, types_dir, "ncbi", [
        TypeSpec("assembly_accession", {"_": "NCBI assembly accession id"}),
    ])
    build_type_lib_dir(layout, types_dir, "sequences", [
        TypeSpec("gbk", {"_": "GenBank flat file", "ext": "gbk"}),
    ])
    build_type_lib_dir(layout, types_dir, "pangenome", [
        TypeSpec("pangenome", {"_": "pangenome group container"}),
        TypeSpec("heatmap",   {"_": "pangenome similarity heatmap", "ext": "svg"}),
    ])
    build_transform_lib(layout, layout.root / "transforms" / "logistics", types_dir, [
        TransformSpec("fetch_genome",
                      inputs=["ncbi::assembly_accession"],
                      outputs=["sequences::gbk"]),
    ])
    build_transform_lib(layout, layout.root / "transforms" / "pangenome", types_dir, [
        TransformSpec("build_heatmap",
                      inputs=["sequences::gbk", "pangenome::pangenome"],
                      outputs=["pangenome::heatmap"],
                      group_by="pangenome::pangenome"),
    ])


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
