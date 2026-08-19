from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult, LoopOutcome
from .base import PromptContext, VerifyContext, _self_report_failures


_SMOKE_PROMPT = """\
Run the following commands in order:

```bash
source $(conda info --base)/etc/profile.d/conda.sh && conda activate msm_env
msm --help > {SANDBOX}/workspace/help.txt
```

If any command above produces an error or unexpected output, stop immediately and run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When all commands above succeed, run:

```bash
metasmith e2e checkpoint done --key harness-smoke
```
"""


@dataclass
class HarnessSmokeScenario:
    name: str = "harness_smoke"
    tutorial_path: str = "(internal harness-smoke prompt)"
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/help.txt",
    ])
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 300.0
    pre_install_metasmith: bool = True

    def build_prompt(self, ctx: PromptContext) -> str:
        return _SMOKE_PROMPT.format(SANDBOX=str(ctx.sandbox))

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        help_file = vctx.sandbox / "workspace" / "help.txt"
        if not help_file.exists():
            fails.append(f"missing {help_file}")
        elif help_file.stat().st_size == 0:
            fails.append(f"{help_file} is empty")
        return fails
