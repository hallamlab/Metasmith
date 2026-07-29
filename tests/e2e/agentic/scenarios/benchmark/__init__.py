"""Token-benchmark scenarios (the 7 tests × 10 arms study).

The seven benchmark tests (t1..t7) drive one *E. coli* functional-genomics
pipeline (``fastp -> SPAdes -> bakta -> eggNOG-mapper -> clusterProfiler``)
across the 10 arms defined in :mod:`..arms`. Pipeline-specific constants and
all start-state builders live in :mod:`._pipeline`; each ``tN_*`` module is a
thin :class:`..base.Scenario` binding a goal + start-state + oracle.
"""
from __future__ import annotations

from .t1_install import InstallToolScenario
from .t2_pipeline import PipelineScenario
from .t3_run import RunScenario
from .t4_adapt_new_host import AdaptNewHostScenario
from .t5_adapt_hpc import AdaptHpcScenario
from .t6_adapt_add_tool import AddToolScenario
from .t7_adapt_from_middle import FromMiddleScenario

#: All benchmark scenario classes, keyed by their canonical test name.
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
