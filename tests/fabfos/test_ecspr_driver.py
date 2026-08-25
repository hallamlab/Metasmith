from __future__ import annotations

from pathlib import Path

from metasmith.python_api import Runtime

from fabfos.pipelines import common, ecspr

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

EXPECTED_TRANSFORMS = {"ecspr_measure"}


def _plan(work: Path):
    units = []
    for name in ("pool_a", "pool_b"):
        # TWO GPR TABLES PER UNIT, because that is the case the split exists for: a
        # conditions table selects the host background out of one and the clone
        # perturbation out of the other, and staging only one resolves half a condition.
        tables = []
        for which in ("host", "clones"):
            g = work / f"{which}_{name}.parquet"
            g.touch()
            tables.append(g)
        cond = work / f"conditions_{name}.parquet"
        cond.touch()
        tests = work / f"tests_{name}.parquet"
        tests.touch()
        units.append(ecspr.Unit(name=name, gpr_tables=tables, conditions=cond,
                                tests=tests))
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
        for f in (f"{name}_gpr.parquet", "conditions.parquet", "tests.parquet"):
            (d / f).touch()
        units.append(ecspr.Unit(name=name, gpr_tables=[d / f"{name}_gpr.parquet"],
                                conditions=d / "conditions.parquet",
                                tests=d / "tests.parquet"))

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
    # THE GPR TABLES KEEP THEIR OWN NAMES. Everything else is renamed for the unit, but
    # a conditions table's `table` column is a basename, so renaming a GPR table would
    # make every condition naming it select nothing.
    assert {"net_a_gpr.parquet", "net_b_gpr.parquet",
            "net_a.conditions.parquet", "net_b.conditions.parquet",
            "net_a.ecspr_tests.parquet", "net_b.ecspr_tests.parquet",
            "atom_pairs.parquet", "direction.parquet"} <= staged, sorted(staged)


def test_both_arms_plan_in_one_run(tmp_path):
    # WHY ONE PLAN. The two arms are only comparable if they measured the same
    # construction, and the thing that guarantees it is that they read one conditions
    # table inside one run rather than two invocations that agreed by hand.
    units = []
    for name in ("pool_a",):
        tables = []
        for which in ("host", "clones"):
            g = tmp_path / f"{which}_{name}.parquet"
            g.touch()
            tables.append(g)
        for f in (f"conditions_{name}.parquet", f"tests_{name}.parquet",
                  f"gem_tests_{name}.parquet", f"media_{name}.parquet",
                  "iECDH1ME8569_1439.json"):
            (tmp_path / f).touch()
        units.append(ecspr.Unit(name=name, gpr_tables=tables,
                                conditions=tmp_path / f"conditions_{name}.parquet",
                                tests=tmp_path / f"tests_{name}.parquet"))
    mnx = tmp_path / "metanetx"
    mnx.mkdir()
    ecspr.attach_fba(units, [(
        "pool_a", tmp_path / "gem_tests_pool_a.parquet",
        [tmp_path / "iECDH1ME8569_1439.json"], tmp_path / "media_pool_a.parquet")])

    agent, task, stubs = ecspr.generate_workflow(
        tmp_path / "work", units=units, atom_pairs=None, direction_ratios=None,
        metanetx=mnx, runtime=Runtime.APPTAINER,
    )
    assert task.ok, f"two-arm plan failed: {task.plan}"
    assert {"ecspr_measure", "fba_measure"} <= common.step_transform_names(task)


def test_a_unit_with_no_fba_arm_plans_exactly_as_before(tmp_path):
    agent, task, stubs = _plan(tmp_path)
    assert task.ok
    assert "fba_measure" not in common.step_transform_names(task)


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
