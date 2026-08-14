"""Unit test for the ``fabfos.pipelines.assembly`` driver.

Calls ``assembly.generate_workflow`` directly -- the same function the CLI
entry point uses -- so the plan under test is exactly what a real invocation
would produce, not a hand-rolled duplicate. Planning only: reads/host/pCC1 are
empty files, and ``algorithm::fabfos_recovery.py`` (no producer yet -- see the
module docstring in ``assembly.py``) stages as a stub, which the driver
reports rather than failing on.

Renders the resolved DAG to ``tests/artifacts/assembly_dag.svg``.

Requirements: an env with ``metasmith`` + graphviz ``dot`` (e.g. ``msm``):

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/test_assembly_driver.py -v

Runs standalone too: ``python tests/test_assembly_driver.py``.
"""
from __future__ import annotations

from pathlib import Path

from metasmith.python_api import Runtime

from fabfos.pipelines import assembly, common

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

# transform-file stems the recovery chain must route through: read QC/clean,
# host filtering (coerced in by resolve_inserts' lineage pin), both
# assemblers, the recovery step itself, and coverage of the recovered inserts.
EXPECTED_TRANSFORMS = {
    "seqkit_reads", "bbduk", "background_filter",
    "megahit", "spades", "resolve_inserts", "assembly_stats",
}


def _plan(work: Path):
    reads = work / "pool_0.fq.gz"
    reads.touch()
    host = work / "host.fna"
    host.touch()
    pcc1 = work / "pcc1.fna"
    pcc1.touch()
    return assembly.generate_workflow(
        work, experiment="test_assembly", reads=[reads], parity="paired",
        host=host, pcc1=pcc1, recovery_lib=None, runtime=Runtime.APPTAINER,
    )


def test_assembly_driver_plans(tmp_path):
    """``assembly.generate_workflow`` resolves and wires in every expected stage."""
    agent, task, stubs = _plan(tmp_path)

    assert task.ok, f"assembly driver failed to plan: {task.plan}"

    names = common.step_transform_names(task)
    missing = EXPECTED_TRANSFORMS - names
    assert not missing, (
        f"assembly plan is missing expected transform(s): {sorted(missing)}; "
        f"plan used: {sorted(names)}"
    )

    # the recovery script has no producer yet (see assembly.py's module
    # docstring), so it must be the one and only stub -- anything else staging
    # as a stub is an unexpected reference gap.
    assert set(stubs) == {"algorithm::fabfos_recovery.py"}, stubs

    svg = common.render_dag(task, ARTIFACTS / "assembly_dag")
    assert svg.exists() and svg.stat().st_size > 0, f"DAG SVG not written: {svg}"


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        agent, task, stubs = _plan(Path(td))
        if not task.ok:
            raise SystemExit(f"FAILED to plan: {task.plan}")
        common.print_plan(task)
        common.report_stubs("assembly", stubs)
        svg = common.render_dag(task, ARTIFACTS / "assembly_dag")
        print(f"DAG written to: {svg}")
