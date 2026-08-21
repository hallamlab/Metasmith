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
        uniref50_db=None, mnxr_lookup=None, landmarks=None, runtime=Runtime.APPTAINER,
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
        uniref50_db=None, mnxr_lookup=None, landmarks=None, runtime=Runtime.APPTAINER,
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
        uniref50_db=None, mnxr_lookup=None, landmarks=None, runtime=Runtime.APPTAINER,
    )
    assert task.ok
    assert _given_orf_count(task) == 1


def test_the_references_are_not_re_identified_on_every_plan(tmp_path):
    # Two plans in one process must agree on every reference id, and derive none.
    #
    # This is the assertion the whole pinned-library change exists for. A pinned
    # library serves its recorded ids without consulting the filesystem, and it
    # has to: an ordinary leaf id is the path and the mtime, and a `dvc checkout`
    # that restores the identical bytes moves mtime under all 24 GB. Skips where
    # the references are not materialised, since there is then nothing to pin.
    import pytest

    from fabfos import refs

    pinned = refs.load_pinned_refs(common.DATA_PROCESSED)
    if pinned is None:
        pytest.skip("no pinned reference library here; run `python -m fabfos.refs pin`")

    import metasmith.models.libraries.identity as identity

    original = identity.stat_leaf_id

    def guarded(path, *args, **kwargs):
        if str(path).startswith(str(common.DATA_PROCESSED)):
            raise AssertionError(f"a reference was re-identified during planning: {path}")
        return original(path, *args, **kwargs)

    identity.stat_leaf_id = guarded
    try:
        keys, ids = [], []
        for i in range(2):
            work = tmp_path / f"run_{i}"
            work.mkdir()
            orfs = work / "orfs.faa"
            orfs.touch()
            _, task, stubs = annotation.generate_workflow(
                work, orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
                uniref50_db=None, mnxr_lookup=None, landmarks=None,
                runtime=Runtime.APPTAINER,
            )
            assert task.ok, task.plan
            assert not stubs, stubs
            keys.append(task.GetKey())
            ids.append({i.dtype_name: i.instance_id for i in task.plan.given
                        if i.dtype_name.startswith("ref::")})
    finally:
        identity.stat_leaf_id = original

    assert len(ids[0]) == len(annotation.REF_LAYOUT), (
        f"the plan was given {sorted(ids[0])}, expected all of"
        f" {sorted(annotation.REF_LAYOUT)}"
    )
    assert ids[0] == ids[1], "a reference identity moved between two plans"
    assert keys[0] == keys[1], (
        "the task key moved between two identical plans, so the second run"
        " cannot reuse the first's cache"
    )


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
