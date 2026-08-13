"""Arm abstraction — the ``env × orchestrator`` cell a scenario runs in.

The token benchmark runs one scenario in any of 10 "arms" (see
``.awm/data/token-benchmark/condition_matrix.md`` § B). An arm is the pair
``(env, orchestrator)``:

    env          ∈ {ad-hoc, mamba, container, metasmith}
    orchestrator ∈ {ad-hoc, snakemake, nextflow, metasmith}

The 9 non-metasmith arms are the full 3×3 factorial of
``{ad-hoc, mamba, container} × {ad-hoc, snakemake, nextflow}``; the 10th
(A10) is full metasmith (``metasmith / metasmith``). Metasmith is tested
ONLY as full metasmith — there are no half-metasmith cells.

A prompt is composed as a SHARED goal/data/done block that is byte-identical
across arms, plus the arm's additive ``preamble`` ("your environment /
available tools / reference material"). The metasmith arm (A10) carries an
empty preamble, so metasmith scenarios render exactly as they do today.

This module intentionally ships only the abstraction + defaults. Per-arm
prompt text (``preamble``), reference-doc bundles (``reference_docs``), and
the provisioning hook (``provision``) are stubs for a later workstream (P4)
to fill in — see the field docs below.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class Arm:
    """One ``env × orchestrator`` benchmark cell.

    Fields:
      id            arm identifier, "A1".."A10".
      env           environment channel: ad-hoc | mamba | container | metasmith.
      orchestrator  orchestrator: ad-hoc | snakemake | nextflow | metasmith.
      preamble      additive prompt prefix describing the arm's environment,
                    available tools, and reference material. Empty for A10
                    (metasmith) so its prompt is the shared block verbatim.
                    P4 fills this in per non-metasmith arm.
      reference_docs
                    identifiers/paths of the native reference docs handed to
                    this arm (Nextflow docs, Snakemake docs, tool man-pages,
                    metasmith agentic docs, ...). Advisory bundle for the
                    provisioning hook / prompt builder; P4 populates it.
      provision     optional per-arm provisioning hook. When set, the runner
                    (P4) calls it to stage the arm's environment into the
                    sandbox (install a conda env, pull containers, drop
                    reference docs, ...). None = nothing arm-specific to do.
                    Signature is left open (``Callable``) until P4 pins the
                    exact ``(layout, ctx)`` contract it needs.
    """
    id: str
    env: str
    orchestrator: str
    preamble: str = ""
    reference_docs: list[str] = field(default_factory=list)
    provision: Callable | None = None

    @property
    def is_metasmith(self) -> bool:
        """True for the full-metasmith arm (A10)."""
        return self.env == "metasmith"


# The 3×3 non-metasmith factorial, in the A1..A9 order of the condition matrix
# (env-major: env outer, orchestrator inner).
_ENVS = ("ad-hoc", "mamba", "container")
_ORCHS = ("ad-hoc", "snakemake", "nextflow")

ARMS: list[Arm] = [
    Arm(id=f"A{i + 1}", env=env, orchestrator=orch)
    for i, (env, orch) in enumerate(
        (env, orch) for env in _ENVS for orch in _ORCHS
    )
] + [
    Arm(id="A10", env="metasmith", orchestrator="metasmith"),
]

ARM_BY_ID: dict[str, Arm] = {a.id: a for a in ARMS}

# The default arm keeps existing (metasmith) scenarios unchanged when no arm
# is supplied: full metasmith, empty preamble, metasmith-only verification.
DEFAULT_ARM: Arm = ARM_BY_ID["A10"]
