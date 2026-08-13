"""Deploy scenario — agent runs `metasmith agent save` + `agent deploy`.

Exercises the container spoof. metasmith is pre-installed (so this isolates
the deploy path from install regressions). For DOCKER: the host daemon
already carries the locally-built tag. For APPTAINER: the harness pre-
placed the sif at the path Agent.Deploy computes inside
`<SANDBOX>/agent_home/container_images/`, so the `[ -e ... ] || pull` gate
skips pulling.
"""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from ..harness.loop import LoopResult, LoopOutcome
from ..harness.container_spoof import expected_sif_path
from .base import PromptContext, VerifyContext, _self_report_failures


_DEPLOY_PROMPT = """\
Run the following commands in order:

```bash
source $(conda info --base)/etc/profile.d/conda.sh && conda activate msm_env

metasmith agent save {SANDBOX}/workspace/local-agent.yml \\
    --home {SANDBOX}/agent_home \\
    --runtime {RUNTIME}

metasmith agent deploy {SANDBOX}/workspace/local-agent.yml
```

If any command above produces an error or unexpected output, stop immediately and run:

```bash
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When all commands above succeed, run:

```bash
metasmith e2e checkpoint done --key deploy-{RUNTIME}
```
"""


@dataclass
class DeployScenario:
    name: str = "deploy"
    tutorial_path: str = "(internal deploy prompt)"
    expected_artifact_globs: list[str] = field(default_factory=lambda: [
        "workspace/local-agent.yml",
    ])
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 600.0
    pre_install_metasmith: bool = True

    def build_prompt(self, ctx: PromptContext) -> str:
        return _DEPLOY_PROMPT.format(SANDBOX=str(ctx.sandbox), RUNTIME=ctx.runtime)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        yml = vctx.sandbox / "workspace" / "local-agent.yml"
        if not yml.exists():
            fails.append(f"missing {yml}; agent did not run `metasmith agent save`")
        agent_home = vctx.sandbox / "agent_home"
        if not agent_home.exists():
            fails.append(f"missing agent home: {agent_home}")
            return fails
        # Runtime-specific container artifact check
        runtime = vctx.agent_env.get("MSM_E2E_RUNTIME", "").upper()
        image_tag = vctx.agent_env.get("MSM_E2E_IMAGE_TAG", "")
        if runtime == "DOCKER":
            r = subprocess.run(
                ["docker", "image", "inspect", image_tag],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if r.returncode != 0:
                fails.append(f"docker image {image_tag!r} not present after deploy")
        elif runtime == "APPTAINER":
            # The store honors APPTAINER_CACHEDIR (set in the agent env), so
            # resolve the sif path the same way Agent.Deploy does.
            sif = expected_sif_path(
                agent_home, f"docker://{image_tag}",
                apptainer_cachedir=vctx.agent_env.get("APPTAINER_CACHEDIR"),
            )
            if not sif.exists():
                fails.append(f"expected sif missing post-deploy: {sif}")
        return fails
