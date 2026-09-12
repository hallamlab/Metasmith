"""A plan that will fetch gigabytes says so before it runs.

Pins I6 of the annotation-trio investigation. The reporter's second workflow was
not given the three database files their first one was given, so the solver
quietly replaced them with three download steps. Nothing between pressing run
and ninety-five minutes later said a database was about to be fetched.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.logging import Log
from metasmith.models.workflow import NextflowGenContext

from ..fixtures.trio import build_inputs, solve_trio
from metasmith.testing.pool_fixtures import pool_backed


DOWNLOADERS = {"downloadUniRef50DB", "downloadKofamDB"}
TARGETS = [
    "annotation::kofamscan_results",
    "annotation::kofamscan_descriptions",
    "annotation::diamond_uniref50_results",
    "annotation::diamond_uniref50_descriptions",
]


def _inputs(lib_root: Path, at: Path, with_databases: bool):
    lib = build_inputs(lib_root, at)
    if with_databases:
        lib.AddTypeLibrary(lib_root / "data_types" / "ref.yml")
        for fname, dtype in [
            ("uniref50.dmnd", "ref::uniref50_diamond_db"),
            ("ko_list", "ref::kofamscan_ko_list"),
        ]:
            (lib.location / fname).write_text("stand-in for a real database\n")
            lib.AddItem(Path(fname), dtype)
        profiles = lib.location / "profiles"
        profiles.mkdir(exist_ok=True)
        (profiles / "K00001.hmm").write_text("HMMER3/f\n")
        lib.AddItem(Path("profiles"), "ref::kofamscan_profiles")
    pool_backed(lib)
    lib.Save()
    return lib


def _solve(lib_root: Path, inputs, shared: list[str] | None = None):
    return solve_trio(lib_root, inputs, targets=TARGETS, shared=shared)


def _stage(task, at: Path) -> str:
    """Run the codegen a stage runs, and return what it told the operator."""
    at.mkdir(parents=True, exist_ok=True)
    log_file = at / "stage.log"
    log_file.touch()
    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=at,
        external_work=at,
        home_dir=at / "home",
        external_home=at / "home",
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )
    Log.AddLogFile(log_file)
    try:
        task.PrepareNextflow(context)
    finally:
        Log.RemoveLogFile(log_file)
    return log_file.read_text()


@pytest.fixture(scope="module")
def plan_without_databases(metasmith_libraries_root, tmp_path_factory):
    root = tmp_path_factory.mktemp("no_dbs")
    return _solve(metasmith_libraries_root, _inputs(metasmith_libraries_root, root, False)), root


def test_a_missing_database_becomes_a_download_step(plan_without_databases):
    # The setup, and the thing the reporter never saw: an input set without the
    # databases silently grows the steps that fetch them.
    task, _ = plan_without_databases
    planned = {s.transform.name for s in task.plan.steps}
    assert DOWNLOADERS <= planned, sorted(planned)


def test_staging_names_the_download_steps_it_planned(plan_without_databases, tmp_path):
    task, _ = plan_without_databases
    log = _stage(task, tmp_path / "ws")
    unreported = sorted(n for n in DOWNLOADERS if n not in log)
    assert not unreported, (
        f"staging said nothing about [{len(unreported)}] step(s) that will fetch "
        f"a database: {unreported}"
    )


def test_a_plan_with_its_databases_given_reports_no_downloads(
    metasmith_libraries_root, tmp_path,
):
    inputs = _inputs(metasmith_libraries_root, tmp_path / "in", True)
    task = _solve(
        metasmith_libraries_root, inputs,
        shared=["uniref50.dmnd", "ko_list", "profiles"],
    )
    planned = {s.transform.name for s in task.plan.steps}
    assert not (DOWNLOADERS & planned), (
        f"the databases were given and the solver planned to fetch them anyway: "
        f"{sorted(DOWNLOADERS & planned)}"
    )
