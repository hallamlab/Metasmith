from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from ..install_mock.verify_local_artifacts import InstallContext
from ..harness.sandbox import SandboxLayout
from .base import PromptContext, VerifyContext, _self_report_failures, _artifact_failures
from ._fixture_utils import (
    TypeSpec, TransformSpec,
    build_type_lib_dir, build_transform_lib, write_text_file,
)


_PROMPT = """\
You are a synthetic user evaluating metasmith {VERSION}.

A transform library and a concrete input file have been pre-staged for
you:

  type lib:      {SANDBOX}/workspace/types/myproj.yml
  transform lib: {SANDBOX}/workspace/transforms/   (contains transform `copy`)
  input file:    {SANDBOX}/workspace/input.txt     (type `myproj::input_text`)

Your task: use the `metasmith run` command (NOT `metasmith plan`) to
execute the `copy` transform directly against the input file, writing
outputs under {SANDBOX}/workspace/run_output/.

`metasmith run --help` documents the exact flag layout (transform-lib,
transform, input bindings of the form TYPE=PATH, work-dir).

When the run completes and `workspace/run_output/` contains at least one
file produced by the transform, write {SANDBOX}/workspace/ANSWER.txt —
exactly two non-empty lines, no extra text:
  line 1: <work_dir path you used>
  line 2: <one filename you see inside run_output/ (basename only)>

If any metasmith command fails or produces unexpected output, stop and
run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When `ANSWER.txt` is written and `workspace/run_output/` is populated,
run:

```bash
metasmith e2e checkpoint done --key story-run-direct
```
"""


@dataclass
class StoryRunDirectScenario:
    name: str = "story_run_direct"
    tutorial_path: str = ""
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/ANSWER.txt",
        "workspace/run_output/**/*",
    ])
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 600.0
    pre_install_metasmith: bool = True

    def setup_fixtures(self, layout: SandboxLayout, ctx: InstallContext) -> None:
        ws = layout.workspace
        types_dir = ws / "types"
        build_type_lib_dir(layout, types_dir, "myproj", [
            TypeSpec("input_text",  {"role": "input"}),
            TypeSpec("output_text", {"role": "output"}),
        ])
        build_transform_lib(layout, ws / "transforms", types_dir, [
            TransformSpec("copy",
                          inputs=["myproj::input_text"],
                          outputs=["myproj::output_text"]),
        ])
        write_text_file(ws / "input.txt", "hello\n")

    def build_prompt(self, ctx: PromptContext) -> str:
        return _PROMPT.format(SANDBOX=str(ctx.sandbox), VERSION=ctx.version)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        fails.extend(_artifact_failures(vctx.sandbox, self.expected_artifact_globs))
        ans = vctx.sandbox / "workspace" / "ANSWER.txt"
        if ans.exists():
            lines = [ln.strip() for ln in ans.read_text().splitlines() if ln.strip()]
            if len(lines) < 2:
                fails.append(
                    f"ANSWER.txt has {len(lines)} non-empty lines, expected 2"
                )
        return fails
