from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from ..install_mock.verify_local_artifacts import InstallContext
from ..harness.sandbox import SandboxLayout
from .base import PromptContext, VerifyContext, _self_report_failures, _artifact_failures
from ._fixture_utils import (
    TypeSpec, TransformSpec,
    build_type_lib_dir, build_transform_lib,
)


_EXPECTED_TYPES = {"myproj::raw_data", "myproj::processed_data"}
_EXPECTED_TRANSFORMS = {"process", "finalize"}


_PROMPT = """\
You are a synthetic user evaluating metasmith {VERSION}.

A type library and a transform library have been pre-staged for you:

  {SANDBOX}/workspace/types/myproj.yml
  {SANDBOX}/workspace/transforms/

Your task is to use the `metasmith` CLI to:
  1. List the data types defined in the type library.
  2. List the transforms in the transform library.
  3. Pick ONE type and ONE transform you saw, then write their names to
     {SANDBOX}/workspace/ANSWER.txt — exactly two lines, no extra text:
       line 1: <namespace::typename>
       line 2: <transform_name>

Discover the right subcommands yourself from `metasmith --help`.

If any metasmith command fails or produces unexpected output, stop and run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When `ANSWER.txt` is written and contains exactly two non-empty lines, run:

```bash
metasmith e2e checkpoint done --key story-browse-libraries
```
"""


@dataclass
class StoryBrowseLibrariesScenario:
    name: str = "story_browse_libraries"
    tutorial_path: str = ""
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/ANSWER.txt",
    ])
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 600.0
    pre_install_metasmith: bool = True

    def setup_fixtures(self, layout: SandboxLayout, ctx: InstallContext) -> None:
        ws = layout.workspace
        types_dir = ws / "types"
        build_type_lib_dir(layout, types_dir, "myproj", [
            TypeSpec("raw_data",       {"stage": "raw"}),
            TypeSpec("processed_data", {"stage": "processed"}),
        ])
        build_transform_lib(layout, ws / "transforms", types_dir, [
            TransformSpec("process",
                          inputs=["myproj::raw_data"],
                          outputs=["myproj::processed_data"]),
            TransformSpec("finalize",
                          inputs=["myproj::processed_data"],
                          outputs=["myproj::processed_data"]),
        ])

    def build_prompt(self, ctx: PromptContext) -> str:
        return _PROMPT.format(SANDBOX=str(ctx.sandbox), VERSION=ctx.version)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        fails.extend(_artifact_failures(vctx.sandbox, self.expected_artifact_globs))
        ans = vctx.sandbox / "workspace" / "ANSWER.txt"
        if not ans.exists():
            return fails
        lines = [ln.strip() for ln in ans.read_text().splitlines() if ln.strip()]
        if len(lines) < 2:
            fails.append(f"ANSWER.txt has {len(lines)} non-empty lines, expected 2")
            return fails
        type_line, transform_line = lines[0], lines[1]
        if type_line not in _EXPECTED_TYPES:
            fails.append(
                f"ANSWER.txt line 1 = {type_line!r}, expected one of "
                f"{sorted(_EXPECTED_TYPES)}"
            )
        if transform_line not in _EXPECTED_TRANSFORMS:
            fails.append(
                f"ANSWER.txt line 2 = {transform_line!r}, expected one of "
                f"{sorted(_EXPECTED_TRANSFORMS)}"
            )
        return fails
