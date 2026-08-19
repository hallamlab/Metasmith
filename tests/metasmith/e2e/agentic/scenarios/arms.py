from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class Arm:
    id: str
    env: str
    orchestrator: str
    preamble: str = ""
    reference_docs: list[str] = field(default_factory=list)
    provision: Callable | None = None

    @property
    def is_metasmith(self) -> bool:
        return self.env == "metasmith"


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

DEFAULT_ARM: Arm = ARM_BY_ID["A10"]
