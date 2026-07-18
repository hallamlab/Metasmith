"""t1 — install (one-shot): a single tool runs on a probe input.

Standalone (does NOT use the pipeline goal/oracle): the install test collapses
orchestration — a lone tool has none — so its "arms" are the 4 ENV CHANNELS
{ad-hoc, mamba, container, metasmith} (table C of the condition matrix), applied
to each of the 6 tools {fastp, spades, bakta, eggnog-mapper, clusterprofiler,
abricate}. Instantiate one scenario per ``(env_channel, tool)`` cell.

``Done`` = the tool runs on a probe input and emits its expected output under
``<sandbox>/workspace/probe_out/``. For env_channel == 'metasmith', "install" =
author + register a metasmith transform for the tool and run it via the CLI.

Modeled on :mod:`..install` — verbatim-command shape, self-report + artifact
oracle.
"""
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

# Which probe input each tool consumes.
_READS_TOOLS = {"fastp", "spades"}          # consume the raw paired reads
_CONTIG_TOOLS = {"bakta", "abricate"}       # consume assembled contigs
# eggnog-mapper / clusterprofiler consume derived inputs the agent produces from
# the probe; the probe still bottoms out at the staged contigs.


def _t1_shared_block(tool: ToolSpec, env_channel: str, sandbox: Path) -> str:
    """The GOAL/DATA/DONE block for one install cell — byte-identical across arms.

    Depends only on (tool, env_channel, sandbox), never on the runtime ``Arm``'s
    orchestrator — the arm's native tooling/reference material is the additive
    preamble prepended by ``compose_prompt``.
    """
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
    env_channel: str = "container"   # one of INSTALL_ENV_CHANNELS
    tool: str = "fastp"              # one of TOOLS
    tutorial_path: str = ""
    expected_trace: tuple[str, str] | None = None
    timeout_s: float = 1800.0
    pre_install_metasmith: bool = True   # metasmith is the harness control plane
    expected_artifact_globs: list[str] = field(
        default_factory=lambda: [f"{_PROBE_OUT_REL}/**/*"]
    )

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
        # Stage the probe input(s) the tool consumes.
        if self.tool in _READS_TOOLS and _reads_present():
            stage_reads(layout)
        if self.tool in _CONTIG_TOOLS or self.tool not in _READS_TOOLS:
            build_intermediate_contigs(layout)
        # Env-channel start-state.
        if self.env_channel == "metasmith":
            provision_metasmith(layout, ctx)
        elif self.env_channel in ("mamba", "container"):
            build_env_spec(layout, self.env_channel)
        # 'ad-hoc' needs no env spec.

    def verify(self, vctx: VerifyContext, result: LoopResult) -> list[str]:
        fails: list[str] = []
        fails.extend(_self_report_failures(result))
        fails.extend(_artifact_failures(vctx.sandbox, self.expected_artifact_globs))
        return fails
