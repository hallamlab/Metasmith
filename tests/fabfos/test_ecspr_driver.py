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
    agent, task, stubs = _plan(tmp_path)

    assert task.ok, f"ecspr driver failed to plan: {task.plan}"

    names = common.step_transform_names(task)
    missing = EXPECTED_TRANSFORMS - names
    assert not missing, (
        f"ecspr plan is missing expected transform(s): {sorted(missing)}; "
        f"plan used: {sorted(names)}"
    )


    assert stubs == {}, stubs

    svg = common.render_dag(task, ARTIFACTS / "ecspr_dag")
    assert svg.exists() and svg.stat().st_size > 0, f"DAG SVG not written: {svg}"


def test_copy_staging_names_inputs_for_the_unit(tmp_path):
    pairs = tmp_path / "atom_pairs.parquet"
    pairs.touch()
    direction = tmp_path / "direction.parquet"
    direction.touch()

    units = []
    for name in ("net_a", "net_b"):
        d = tmp_path / name
        d.mkdir()
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
