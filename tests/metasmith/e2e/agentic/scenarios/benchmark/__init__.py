from __future__ import annotations

from .t1_install import InstallToolScenario
from .t2_pipeline import PipelineScenario
from .t3_run import RunScenario
from .t4_adapt_new_host import AdaptNewHostScenario
from .t5_adapt_hpc import AdaptHpcScenario
from .t6_adapt_add_tool import AddToolScenario
from .t7_adapt_from_middle import FromMiddleScenario

BENCHMARK_SCENARIOS = {
    "t1_install": InstallToolScenario,
    "t2_pipeline": PipelineScenario,
    "t3_run": RunScenario,
    "t4_adapt_new_host": AdaptNewHostScenario,
    "t5_adapt_hpc": AdaptHpcScenario,
    "t6_adapt_add_tool": AddToolScenario,
    "t7_adapt_from_middle": FromMiddleScenario,
}

__all__ = [
    "InstallToolScenario",
    "PipelineScenario",
    "RunScenario",
    "AdaptNewHostScenario",
    "AdaptHpcScenario",
    "AddToolScenario",
    "FromMiddleScenario",
    "BENCHMARK_SCENARIOS",
]
