from __future__ import annotations

from pathlib import Path

from metasmith.python_api import Runtime

from fabfos.pipelines import annotation, common

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

EXPECTED_TRANSFORMS = {
    "chunkOrfsForAnnotation",
    "kofamscan", "clean", "diamond_uniref50", "proteinbert",
    "merge_kofamscan", "merge_diamond_uniref50", "merge_proteinbert",
    "gpr_4lane",
}


def _given_orf_count(task) -> int:
    return sum(1 for i in task.plan.given if i.dtype_name == "sequences::orfs")


def _plan(work: Path):
    orfs = work / "orfs.faa"
    orfs.touch()
    return annotation.generate_workflow(
        work, orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
        uniref50_db=None, mnxr_lookup=None, label_pool=None, runtime=Runtime.APPTAINER,
    )


def test_annotation_driver_plans(tmp_path):
    agent, task, stubs = _plan(tmp_path)

    assert task.ok, f"annotation driver failed to plan: {task.plan}"

    names = common.step_transform_names(task)
    missing = EXPECTED_TRANSFORMS - names
    assert not missing, (
        f"annotation plan is missing expected transform(s): {sorted(missing)}; "
        f"plan used: {sorted(names)}"
    )

    assert not stubs, stubs

    svg = common.render_dag(task, ARTIFACTS / "annotation_dag")
    assert svg.exists() and svg.stat().st_size > 0, f"DAG SVG not written: {svg}"


def test_annotation_driver_plans_many_samples(tmp_path):
    orfs = []
    for i in range(3):
        f = tmp_path / f"shard_{i}.faa"
        f.touch()
        orfs.append(f)

    agent, task, stubs = annotation.generate_workflow(
        tmp_path, orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
        uniref50_db=None, mnxr_lookup=None, label_pool=None, runtime=Runtime.APPTAINER,
    )

    assert task.ok, f"annotation driver failed to plan {len(orfs)} samples: {task.plan}"
    assert common.step_transform_names(task) == EXPECTED_TRANSFORMS
    assert not stubs, stubs

    assert _given_orf_count(task) == len(orfs), (
        f"expected {len(orfs)} ORF samples, plan was given {_given_orf_count(task)}"
    )


def test_annotation_driver_accepts_a_bare_path(tmp_path):
    orfs = tmp_path / "orfs.faa"
    orfs.touch()
    _, task, _ = annotation.generate_workflow(
        tmp_path, orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
        uniref50_db=None, mnxr_lookup=None, label_pool=None, runtime=Runtime.APPTAINER,
    )
    assert task.ok
    assert _given_orf_count(task) == 1


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        agent, task, stubs = _plan(Path(td))
        if not task.ok:
            raise SystemExit(f"FAILED to plan: {task.plan}")
        common.print_plan(task)
        common.report_stubs("annotation", stubs)
        svg = common.render_dag(task, ARTIFACTS / "annotation_dag")
        print(f"DAG written to: {svg}")
