from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult
from ..install_mock.verify_local_artifacts import InstallContext
from ..harness.sandbox import SandboxLayout
from .base import PromptContext, VerifyContext, _self_report_failures, _artifact_failures
from ._fixture_utils import (
    TypeSpec, DataItemSpec,
    build_type_lib_dir, build_data_lib, write_raw_transform_lib,
)


_ATTACH_TRANSFORM = '''\
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
group  = model.AddRequirement(lib.GetType("myns::group"))
child  = model.AddRequirement(lib.GetType("myns::child"), parents={group})
result = model.AddProduct(lib.GetType("myns::child_with_group"))


def protocol(context: ExecutionContext):
    result_path = context.Output(result)
    context.external_shell.Exec(f"touch {result_path.external}")
    return ExecutionResult(
        manifest=[
            {result: result_path.local},
        ],
        success=True,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=child,
)
'''


_PROMPT = """\
You are a synthetic user evaluating metasmith {VERSION}.

A type library, a transform library, and a data instance library have
been pre-staged for you:

  type lib:      {SANDBOX}/workspace/types/myns.yml
  transform lib: {SANDBOX}/workspace/transforms/
  data lib:      {SANDBOX}/workspace/data.xgdb

The data library contains two items:
  - one of type ``myns::group``
  - one of type ``myns::child``

If you try to plan a workflow:

```bash
metasmith --json plan \\
    --data-library {SANDBOX}/workspace/data.xgdb \\
    --sample-type myns::child \\
    --target-type myns::child_with_group \\
    --transform-library {SANDBOX}/workspace/transforms
```

it will fail (``success: false``). The ``hints`` array will contain at
least one hint of kind ``lineage_mismatch`` — meaning an item matches
by properties but is not registered as a descendant of the required
parent.

Your task — diagnose and recover:

  1. Run the plan command above. Read the ``hints`` array.
  2. Look at the data library (``metasmith data inspect`` and
     ``metasmith data list``) to discover the item paths.
  3. Use ``metasmith data set-parents`` to attach the ``group`` item as
     a parent of the ``child`` item.
  4. Re-run the plan command. It should now succeed with a ``task_key``.
  5. Write the resulting ``task_key`` (one line, no extra text) to
     {SANDBOX}/workspace/ANSWER.txt.

If after diagnosis you cannot make the plan succeed, or any command
fails in an unexpected way, run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When `ANSWER.txt` is written and the second plan succeeded, run:

```bash
metasmith e2e checkpoint done --key recover-lineage-mismatch
```
"""


@dataclass
class RecoverLineageMismatchScenario:
    name: str = "recover_lineage_mismatch"
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
        types_yml = build_type_lib_dir(layout, types_dir, "myns", [
            TypeSpec("group",              {"role": "group"}),
            TypeSpec("child",              {"role": "child"}),
            TypeSpec("child_with_group",   {"role": "child_with_group"}),
        ])
        write_raw_transform_lib(
            layout,
            ws / "transforms",
            types_dir,
            {"attach": _ATTACH_TRANSFORM},
        )
        build_data_lib(layout, ws / "data.xgdb", types_yml, [
            DataItemSpec(name="g1", dtype="myns::group", value="group_one"),
            DataItemSpec(name="c1", dtype="myns::child", value="child_one"),
        ])

    def build_prompt(self, ctx: PromptContext) -> str:
        return _PROMPT.format(SANDBOX=str(ctx.sandbox), VERSION=ctx.version)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        fails.extend(_artifact_failures(vctx.sandbox, self.expected_artifact_globs))
        ans = vctx.sandbox / "workspace" / "ANSWER.txt"
        if ans.exists():
            text = ans.read_text().strip()
            task_key = text.splitlines()[0].strip() if text else ""
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
