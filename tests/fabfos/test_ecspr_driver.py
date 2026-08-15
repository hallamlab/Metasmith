"""Unit test for the ``fabfos.pipelines.ecspr`` driver.

Calls ``ecspr.generate_workflow`` directly, with two units so the multi-unit
staging path (``group_by=exp``) is exercised -- see the driver's module
docstring. The per-unit job fan-out is a runtime property invisible to
planning (see `metasmith-runtime-fanout`), so this only asserts the plan
resolves with both units present, not a job count.

Planning only: empty GPR table + conditions files per unit are enough.
``atom_pairs``/``direction_ratios`` are expected to resolve as REAL defaults
from this repo's ``data/processed/metabolism_bake/``, so NOTHING should stage
as a stub -- a stub here means a reference default went missing. Executing is
out of scope for a unit test (`ecspr_measure` dispatches a real solve over a
2M-row atom-pairs table); `fabfos ecspr --runtime mamba --run` is where that is
checked.

Renders the resolved DAG to ``tests/artifacts/ecspr_dag.svg``.

Requirements: an env with ``metasmith`` + graphviz ``dot`` (e.g. ``msm``):

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" python -m pytest tests/test_ecspr_driver.py -v

Runs standalone too: ``python tests/test_ecspr_driver.py``.
"""
from __future__ import annotations

from pathlib import Path

from metasmith.python_api import Runtime

from fabfos.pipelines import common, ecspr

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

EXPECTED_TRANSFORMS = {"ecspr_measure"}


def _plan(work: Path):
    units = []
    for name in ("pool_a", "pool_b"):
        gpr = work / f"gpr_{name}.parquet"
        gpr.touch()
        cond = work / f"conditions_{name}.parquet"
        cond.touch()
        units.append(ecspr.Unit(name=name, gpr_table=gpr, conditions=cond))
    return ecspr.generate_workflow(
        work, units=units, atom_pairs=None, direction_ratios=None,
        runtime=Runtime.APPTAINER,
    )


def test_ecspr_driver_plans(tmp_path):
    """``ecspr.generate_workflow`` resolves one ``ecspr_measure`` job per unit."""
    agent, task, stubs = _plan(tmp_path)

    assert task.ok, f"ecspr driver failed to plan: {task.plan}"

    names = common.step_transform_names(task)
    missing = EXPECTED_TRANSFORMS - names
    assert not missing, (
        f"ecspr plan is missing expected transform(s): {sorted(missing)}; "
        f"plan used: {sorted(names)}"
    )

    # The per-unit fan-out (one job per unit) is a RUNTIME property, not a
    # planning-time one: two units with the same plan SHAPE resolve to one
    # `ecspr_measure` step here and multiply into per-unit jobs only when
    # staged (see `metasmith-runtime-fanout`).

    # Every reference this plan stages has a real pinned default, so a stub here
    # is a missing default rather than an expected gap.
    assert stubs == {}, stubs

    svg = common.render_dag(task, ARTIFACTS / "ecspr_dag")
    assert svg.exists() and svg.stat().st_size > 0, f"DAG SVG not written: {svg}"


def test_copy_staging_names_inputs_for_the_unit(tmp_path):
    """``stage="copy"`` makes every input a RELATIVE member, named for its unit.

    Both halves matter and neither is visible in the plan. Relative, because an
    absolute local path is an external input a remote agent binds rather than
    transfers, so referencing one makes it refuse to stage at all. Named for the
    unit, because nextflow stages a process's inputs by basename and every
    composed network calls its table `gpr.parquet` -- two units in one library
    would collide on the name and hand a step the wrong network, silently.

    See `research/fabfos/examples/nostoc_ecspr.py`, which is the caller this
    exists for: one invocation per composed graph, each passing that graph's own
    atom-pair table.
    """
    pairs = tmp_path / "atom_pairs.parquet"
    pairs.touch()
    direction = tmp_path / "direction.parquet"
    direction.touch()

    units = []
    for name in ("net_a", "net_b"):
        d = tmp_path / name
        d.mkdir()
        # The colliding basenames the naming rule exists for.
        (d / "gpr.parquet").touch()
        (d / "conditions.parquet").touch()
        units.append(ecspr.Unit(name=name, gpr_table=d / "gpr.parquet",
                                conditions=d / "conditions.parquet"))

    work = tmp_path / "work"
    seen = []
    agent, task, stubs = ecspr.generate_workflow(
        work, units=units, atom_pairs=pairs, direction_ratios=direction,
        runtime=Runtime.APPTAINER, stage="copy", on_inputs=seen.append,
    )

    assert task.ok, f"copy-staged plan failed: {task.plan}"
    assert stubs == {}, stubs
    assert len(seen) == 1, "on_inputs must run once, after staging and before planning"

    staged = {p.name for p in (work / "inputs.xgdb").iterdir() if p.suffix == ".parquet"}
    assert {"net_a.gpr.parquet", "net_b.gpr.parquet",
            "net_a.conditions.parquet", "net_b.conditions.parquet",
            "atom_pairs.parquet", "direction.parquet"} <= staged, sorted(staged)


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        agent, task, stubs = _plan(Path(td))
        if not task.ok:
            raise SystemExit(f"FAILED to plan: {task.plan}")
        common.print_plan(task)
        common.report_stubs("ecspr", stubs)
        svg = common.render_dag(task, ARTIFACTS / "ecspr_dag")
        print(f"DAG written to: {svg}")
