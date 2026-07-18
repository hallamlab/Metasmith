"""t3 — run: full pipeline from raw reads to the clusterProfiler artifact.

The reference cell of the whole study. ``Done`` = the final clusterProfiler
KEGG/GO enrichment artifact (a PNG dotplot + TSV table) is produced from the
raw paired-end reads and lands under ``<sandbox>/workspace/results/``.

Fully real for the reference arms A10 (metasmith) and A7 (container/ad-hoc):
  * A10 — ``provision_metasmith`` stages the std transform library + a typed
    ``inputs.xgdb``; the agent plans + runs the metasmith workflow and collects
    the enrichment artifact. The metasmith-only lineage trace
    (``paired_reads_forward -> enrichment_plot``) is checked by ``standard_verify``.
  * A7 — ``build_env_spec(container)`` + ``build_scripts_repo`` stage the sif
    image list and a shell-glue repo; the agent wires the containerized tools by
    hand and writes the final PNG to ``workspace/results/``.

Other arms get their real start-state files too (see ``provision_for``); the
deep host-materialization steps are the team-fill hooks in ``_pipeline``.
"""
from __future__ import annotations

from dataclasses import dataclass

from ._base import BenchmarkScenario


@dataclass
class RunScenario(BenchmarkScenario):
    name: str = "t3_run"
    timeout_s: float = 3600.0
