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

  type lib:      {SANDBOX}/workspace/types/myproj.yml
  transform lib: {SANDBOX}/workspace/transforms/
  data lib:      {SANDBOX}/workspace/data.xgdb

The transform library defines a complete chain:
    myproj::raw_data -> myproj::mid_data -> myproj::final_data

If you try to plan a workflow:

```bash
metasmith --json plan \\
    --data-library {SANDBOX}/workspace/data.xgdb \\
    --sample-type myproj::raw_data \\
    --target-type myproj::final_data \\
    --transform-library {SANDBOX}/workspace/transforms
```

it will fail (``success: false``) because the data library currently has
no items of type ``myproj::raw_data``. The response will contain a
``hints`` array describing the gap.

Your task — diagnose and recover:

  1. Run the plan command above. Read the ``hints`` array.
  2. Use the appropriate ``metasmith data ...`` command (per the hint's
     guidance) to add a ``myproj::raw_data`` item to
     ``workspace/data.xgdb``. The value can be anything — e.g. the
     string ``"sample1"``.
  3. Re-run the plan command. It should now succeed with a ``task_key``
     in the response.
  4. Write the resulting ``task_key`` (one line, no extra text) to
     {SANDBOX}/workspace/ANSWER.txt.

If after diagnosis you cannot make the plan succeed, or any command
fails in an unexpected way, run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When `ANSWER.txt` is written and the second plan succeeded, run:

```bash
metasmith e2e checkpoint done --key recover-unreachable-target
```
"""


@dataclass
class RecoverUnreachableTargetScenario:
    name: str = "recover_unreachable_target"
    tutorial_path: str = ""
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/ANSWER.txt",
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
            TypeSpec("unrelated",  {"stage": "unrelated"}),
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
                name="placeholder",
                dtype="myproj::unrelated",
                value="placeholder",
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
            task_key = ans.read_text().strip().splitlines()[0].strip() if ans.read_text().strip() else ""
            if not task_key:
                fails.append("ANSWER.txt is empty (no task_key)")
            else:
                ws_path = vctx.sandbox / "home" / ".metasmith" / "workspace" / task_key
                if not ws_path.exists():
                    fails.append(
                        f"ANSWER.txt task_key {task_key!r} not found under "
                        f"<sandbox>/home/.metasmith/workspace/"
                    )
        return fails
