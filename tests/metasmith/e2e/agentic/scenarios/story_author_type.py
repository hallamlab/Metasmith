from __future__ import annotations

import subprocess
from dataclasses import dataclass, field

from ..harness.loop import LoopResult
from ..install_mock.verify_local_artifacts import InstallContext
from ..harness.sandbox import SandboxLayout
from .base import PromptContext, VerifyContext, _self_report_failures, _artifact_failures


_EXPECTED_ANSWER = "myproj::sample_id"


_PROMPT = """\
You are a synthetic user evaluating metasmith {VERSION}.

Your task: author a new metasmith data type from scratch using the CLI.

Requirements:
  - Library YAML must be created at: {SANDBOX}/workspace/myproj.yml
  - The library must define exactly one type:
      namespace: myproj
      name:      sample_id
      properties: {{"format": "tsv", "has_header": true}}
  - After creation, verify the type loaded by running `metasmith type show`
    against the new library.

Discover the right subcommands yourself from `metasmith --help` and
`metasmith type --help`.

When the type is created and `metasmith type show` confirms it, write the
type's fully-qualified name (``namespace::name``) to
{SANDBOX}/workspace/ANSWER.txt — exactly one line, no extra text.

If any metasmith command fails or produces unexpected output, stop and run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When `ANSWER.txt` is written, run:

```bash
metasmith e2e checkpoint done --key story-author-type
```
"""


@dataclass
class StoryAuthorTypeScenario:
    name: str = "story_author_type"
    tutorial_path: str = ""
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/ANSWER.txt",
        "workspace/myproj.yml",
    ])
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 600.0
    pre_install_metasmith: bool = True

    def setup_fixtures(self, layout: SandboxLayout, ctx: InstallContext) -> None:
        return

    def build_prompt(self, ctx: PromptContext) -> str:
        return _PROMPT.format(SANDBOX=str(ctx.sandbox), VERSION=ctx.version)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        fails.extend(_artifact_failures(vctx.sandbox, self.expected_artifact_globs))
        ans = vctx.sandbox / "workspace" / "ANSWER.txt"
        if ans.exists():
            text = ans.read_text().strip()
            if text != _EXPECTED_ANSWER:
                fails.append(
                    f"ANSWER.txt = {text!r}, expected {_EXPECTED_ANSWER!r}"
                )
        yml = vctx.sandbox / "workspace" / "myproj.yml"
        if yml.exists():
            msm_bin = vctx.installed_env_path / "bin" / "metasmith"
            if msm_bin.exists():
                r = subprocess.run(
                    [str(msm_bin), "type", "show", _EXPECTED_ANSWER,
                     "-t", str(yml)],
                    env=vctx.agent_env, capture_output=True, text=True,
                )
                if r.returncode != 0:
                    fails.append(
                        f"independent `metasmith type show {_EXPECTED_ANSWER}` "
                        f"failed (exit {r.returncode}): "
                        f"{r.stderr.strip()[-300:]}"
                    )
        return fails
