from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from pathlib import Path

from ...harness.loop import LoopResult
from ...harness.sandbox import SandboxLayout
from ...install_mock.verify_local_artifacts import InstallContext
from ..arms import Arm, DEFAULT_ARM
from ..base import (
    PromptContext,
    VerifyContext,
    _artifact_failures,
    _self_report_failures,
    compose_prompt,
)
from ._pipeline import (
    INSTALL_ENV_CHANNELS,
    TOOLS,
    ToolSpec,
    arm_with_preamble,
    build_env_spec,
    build_intermediate_contigs,
    provision_metasmith,
    stage_reads,
    _reads_present,
)

_PROBE_OUT_REL = "workspace/probe_out"

_READS_TOOLS = {"fastp", "spades"}
_CONTIG_TOOLS = {"bakta", "abricate"}


def _t1_shared_block(tool: ToolSpec, env_channel: str, sandbox: Path) -> str:
    sb = str(sandbox)
    if env_channel == "metasmith":
        install_line = (
            f"Author and register a metasmith transform for `{tool.binary}` "
            f"(the {tool.role} tool), then run it via the metasmith CLI so that it"
        )
    else:
        install_line = (
            f"Install `{tool.binary}` (the {tool.role} tool) into this "
            f"environment, then run it so that it"
        )
    return textwrap.dedent(f"""\
        # Install probe — {tool.key}

        ## Goal
        {install_line}
        {tool.probe}, writing the output under:

          {sb}/{_PROBE_OUT_REL}/

        ## Data
        Probe inputs are staged under `{sb}/workspace/` (paired reads and/or a
        small contigs FASTA, depending on the tool). Reference DBs (bakta-light,
        eggNOG) are pre-provisioned host fixtures.

        ## Done protocol
        If installation or the probe run fails or produces unexpected output, stop
        immediately and run:

        ```bash
        metasmith e2e report_issue --cwd "{sb}" --reason "<one line describing what you saw>"
        ```

        When `{tool.binary}` has run and produced output under
        `{sb}/{_PROBE_OUT_REL}/`, run:

        ```bash
        metasmith e2e checkpoint done --cwd "{sb}" --key t1_install_{tool.key}
        ```
        """)


@dataclass
class InstallToolScenario:
    env_channel: str = "container"
    tool: str = "fastp"
    tutorial_path: str = ""
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 900.0
    pre_install_metasmith: bool = True
    expected_artifact_globs: list[str] = field(
        default_factory=lambda: [f"{_PROBE_OUT_REL}/**/*"]
    )
    max_tokens: int | None = None

    def __post_init__(self) -> None:
        if self.env_channel not in INSTALL_ENV_CHANNELS:
            raise ValueError(
                f"env_channel {self.env_channel!r} not in {INSTALL_ENV_CHANNELS}"
            )
        if self.tool not in TOOLS:
            raise ValueError(f"tool {self.tool!r} not in {sorted(TOOLS)}")

    @property
    def name(self) -> str:
        return f"t1_install_{self.env_channel}_{self.tool}"

    def _toolspec(self) -> ToolSpec:
        return TOOLS[self.tool]

    def build_prompt(self, ctx: PromptContext) -> str:
        shared = _t1_shared_block(self._toolspec(), self.env_channel, ctx.sandbox)
        return compose_prompt(shared, arm_with_preamble(ctx.arm))

    def setup_fixtures(self, layout: SandboxLayout, ctx: InstallContext,
                       arm: Arm = DEFAULT_ARM) -> None:
        (layout.workspace / "probe_out").mkdir(parents=True, exist_ok=True)
        if self.tool in _READS_TOOLS and _reads_present():
            stage_reads(layout)
        if self.tool in _CONTIG_TOOLS or self.tool not in _READS_TOOLS:
            build_intermediate_contigs(layout)
        if self.env_channel == "metasmith":
            provision_metasmith(layout, ctx)
        elif self.env_channel in ("mamba", "container"):
            build_env_spec(layout, self.env_channel)

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        fails.extend(_artifact_failures(vctx.sandbox, self.expected_artifact_globs))
        return fails
