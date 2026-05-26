"""User-story smoke: plan a workflow, then inspect the cached plan.

The whole ``metasmith task`` subcommand tree (``list / show / dag /
hints``) is currently untouched by other scenarios. This scenario stages
a solvable data lib + transform lib and asks the agent to plan + inspect
end-to-end.

Pre-staged:
  workspace/types/myproj.yml         3 types: raw -> mid -> final
  workspace/transforms/              2 transforms forming a complete chain
  workspace/data.xgdb                one item of type raw_data

Pass criteria:
  - workspace/ANSWER.txt has two lines: <task_key> and <step_count>
  - the task_key resolves under the agent's workspace dir
  - step_count parses to a positive int
  - the agent produced a DAG file under workspace/
"""
from __future__ import annotations

import glob
from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from ..install_mock.verify_local_artifacts import InstallContext
from ..harness.sandbox import SandboxLayout
from .base import PromptContext, VerifyContext, _self_report_failures, _artifact_failures
from ._fixture_utils import (
    TypeSpec, TransformSpec, DataItemSpec,
    build_type_lib_dir, build_transform_lib, build_data_lib,
)


_PROMPT = """\
You are a synthetic user evaluating metasmith {VERSION}.

A type library, a transform library, and a data instance library have
been pre-staged for you:

  type lib:     {SANDBOX}/workspace/types/myproj.yml
  transform lib: {SANDBOX}/workspace/transforms/
  data lib:      {SANDBOX}/workspace/data.xgdb

The transform library defines a complete chain:
    myproj::raw_data -> myproj::mid_data -> myproj::final_data

The data library contains one item of type ``myproj::raw_data``.

Your task — use the `metasmith` CLI to:
  1. Plan a workflow that produces ``myproj::final_data`` from samples of
     type ``myproj::raw_data``, with this transform library.
  2. List your cached tasks, then show the plan details for the one you
     just created.
  3. Render the plan's DAG to {SANDBOX}/workspace/dag.dot
     (pass `--format dot` so no graphviz is required).
  4. Write the result to {SANDBOX}/workspace/ANSWER.txt — exactly two
     non-empty lines, no extra text:
       line 1: <task_key>      (the cached plan's key)
       line 2: <step_count>    (positive integer; how many steps the plan has)

Discover the right subcommands yourself from `metasmith --help`,
`metasmith plan --help`, and `metasmith task --help`.

If any metasmith command fails or produces unexpected output, stop and run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When `ANSWER.txt` is written and `dag.dot` exists, run:

```bash
metasmith e2e checkpoint done --key story-plan-and-inspect
```
"""


@dataclass
class StoryPlanAndInspectScenario:
    name: str = "story_plan_and_inspect"
    tutorial_path: str = ""
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/ANSWER.txt",
        "workspace/dag.dot",
    ])
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 900.0
    pre_install_metasmith: bool = True

    def setup_fixtures(self, layout: SandboxLayout, ctx: InstallContext) -> None:
        ws = layout.workspace
        types_dir = ws / "types"
        types_yml = build_type_lib_dir(layout, types_dir, "myproj", [
            TypeSpec("raw_data",   {"stage": "raw"}),
            TypeSpec("mid_data",   {"stage": "mid"}),
            TypeSpec("final_data", {"stage": "final"}),
        ])
        build_transform_lib(layout, ws / "transforms", types_dir, [
            TransformSpec("step1",
                          inputs=["myproj::raw_data"],
                          outputs=["myproj::mid_data"]),
            TransformSpec("step2",
                          inputs=["myproj::mid_data"],
                          outputs=["myproj::final_data"]),
        ])
        build_data_lib(layout, ws / "data.xgdb", types_yml, [
            DataItemSpec(
                name="example_raw",
                dtype="myproj::raw_data",
                value="hello",
            ),
        ])

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
            else:
                task_key, count_str = lines[0], lines[1]
                # task_key should be findable in the agent's workspace dir;
                # the agent's HOME is <sandbox>/home, so default workspace
                # is <sandbox>/home/.metasmith/workspace/<task_key>
                ws_candidates = glob.glob(
                    str(vctx.sandbox / "home" / ".metasmith" / "workspace" / task_key)
                )
                if not ws_candidates:
                    fails.append(
                        f"ANSWER.txt task_key {task_key!r} not found under "
                        f"<sandbox>/home/.metasmith/workspace/"
                    )
                try:
                    n = int(count_str)
                    if n <= 0:
                        fails.append(
                            f"ANSWER.txt step_count = {count_str!r}, "
                            f"expected positive integer"
                        )
                except ValueError:
                    fails.append(
                        f"ANSWER.txt step_count = {count_str!r}, "
                        f"expected integer"
                    )
        return fails
