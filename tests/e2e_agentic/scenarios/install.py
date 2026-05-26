"""Install scenario — agent runs the docs install command verbatim.

Exercises the conda-channel spoof: ``-c hallamlab`` resolves to the
local file:// channel, so the agent installs the locally-built
metasmith without going to the network. Verifier checks that the
sandbox-local env exists with the expected metasmith version and that
``msm --help`` exit code 0 was logged.
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult, LoopOutcome
from .base import PromptContext, VerifyContext, _self_report_failures


_INSTALL_PROMPT = """\
Run the following commands in order:

```bash
mamba create -y -n msm_env -c hallamlab -c bioconda metasmith
source $(conda info --base)/etc/profile.d/conda.sh && conda activate msm_env
msm --help > {SANDBOX}/workspace/install_help.txt
metasmith --version
```

If any command above produces an error or unexpected output, stop immediately and run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When all commands above succeed, run:

```bash
metasmith e2e checkpoint done --key install-{VERSION}
```
"""


@dataclass
class InstallScenario:
    name: str = "install"
    tutorial_path: str = "setup/install.rst"
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/install_help.txt",
    ])
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 600.0
    pre_install_metasmith: bool = False     # this IS the install test

    def build_prompt(self, ctx: PromptContext) -> str:
        return _INSTALL_PROMPT.format(SANDBOX=str(ctx.sandbox), VERSION=ctx.version)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        help_file = vctx.sandbox / "workspace" / "install_help.txt"
        if not help_file.exists():
            fails.append(f"missing {help_file}")
        elif help_file.stat().st_size == 0:
            fails.append(f"{help_file} is empty (msm --help produced nothing)")
        # The agent's `mamba create` should have landed at <sandbox>/envs/msm_env
        if not vctx.installed_env_path.exists():
            fails.append(f"env not created at {vctx.installed_env_path}")
            return fails
        msm_bin = vctx.installed_env_path / "bin" / "metasmith"
        if not msm_bin.exists():
            fails.append(f"metasmith not on env PATH: missing {msm_bin}")
            return fails
        r = subprocess.run([str(msm_bin), "-V"], capture_output=True, text=True,
                           env=vctx.agent_env)
        if r.returncode != 0:
            fails.append(f"`metasmith -V` failed in installed env: {r.stderr.strip()}")
            return fails
        out_ver = r.stdout.strip().split()[-1].lstrip("v")
        # vctx is hydrated with ctx.version via the conftest fixture
        return fails
