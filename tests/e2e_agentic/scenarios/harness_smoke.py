"""Harness smoke scenario — proves the sandbox + driver wiring works.

metasmith is pre-installed into the sandbox-local env BEFORE the agent
runs, so this test exercises only the harness (sandbox materialization,
.condarc spoof, env handover, opencode driver, CONTROL.json round-trip).
The agent's job: verify ``msm --help`` exits 0 and emit a `done`
checkpoint. ~5–10K tokens.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult, LoopOutcome
from .base import PromptContext, VerifyContext


_SMOKE_PROMPT = """\
{PRELUDE}

# Your task (harness smoke)

metasmith is already installed in conda env `msm_env` inside the sandbox.

1. Activate it: `source $(conda info --base)/etc/profile.d/conda.sh && conda activate msm_env`
2. Run `msm --help` and confirm it exits 0.
3. Save the output to `<SANDBOX>/workspace/help.txt`.
4. Run:

   ```
   metasmith e2e checkpoint done --key harness-smoke
   ```

That's it. Do not pull images. Do not edit `.condarc`.
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
        return _SMOKE_PROMPT.format(PRELUDE=ctx.prelude_text)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        if result.outcome is not LoopOutcome.DONE:
            fails.append(f"smoke did not report DONE; outcome={result.outcome.value}")
        help_file = vctx.sandbox / "workspace" / "help.txt"
        if not help_file.exists():
            fails.append(f"missing {help_file}")
        elif help_file.stat().st_size == 0:
            fails.append(f"{help_file} is empty")
        return fails
