from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))

from metasmith.python_api import Runtime                              # noqa: E402

from fabfos.pipelines import annotation, common                       # noqa: E402
import cyanoverse_gpr as cv                                           # noqa: E402

SHARDS = ["shard_0000", "shard_0001", "shard_0002"]


def _plan(work: Path, on_inputs):
    return annotation.generate_workflow(
        work, orfs=[f"{cv.SHARDS_DIR}/{s}.faa" for s in SHARDS],
        kofam_profiles=None, kofam_ko_list=None, uniref50_db=None,
        mnxr_lookup=None, label_pool=None, runtime=Runtime.APPTAINER,
        refs_root=cv.REFS_ROOT, verify_refs=False, stage_orfs="remote",
        on_inputs=on_inputs,
    )


def test_reuse_removes_the_producer(tmp_path):
    _agent, task, stubs = _plan(tmp_path, cv.make_on_inputs(SHARDS))
    assert task.ok, f"the reuse plan did not resolve: {task.plan}"
    assert not stubs, stubs

    used = common.step_transform_names(task)
    assert not (used & cv.FORBIDDEN_TRANSFORMS), (
        f"the reuse did not take -- {sorted(used & cv.FORBIDDEN_TRANSFORMS)} is "
        f"still in the plan, so the campaign would recompute lanes it already has")
    assert used == cv.EXPECTED_TRANSFORMS, (
        f"plan is {sorted(used)}, expected {sorted(cv.EXPECTED_TRANSFORMS)}")

    assert len(task.plan.steps) == len(cv.EXPECTED_TRANSFORMS)

    PER_CHUNK = {"diamond_uniref50", "proteinbert"}
    for s in task.plan.steps:
        name = Path(s.transform._path).stem
        want = 1 if name in PER_CHUNK else len(SHARDS)
        assert len(s.group_by_instances) == want, (
            f"{name} groups {len(s.group_by_instances)}, expected {want}")


def test_without_the_given_the_producer_comes_back(tmp_path):
    _agent, task, _ = _plan(tmp_path, None)
    assert task.ok
    used = common.step_transform_names(task)
    assert cv.FORBIDDEN_TRANSFORMS <= used, (
        f"expected the producer to be planned when nothing supplies it; "
        f"got {sorted(used)}")
    assert len(task.plan.steps) == len(cv.EXPECTED_TRANSFORMS | cv.FORBIDDEN_TRANSFORMS)


def test_an_unparented_given_does_not_satisfy_the_mapper(tmp_path):
    def unparented(inputs):
        shards_seen = [Path(p).stem for p, d in inputs.manifest.items()
                       if d == "sequences::orfs"]
        for shard in shards_seen:
            for dtype, pat in cv.REUSED.items():
                inputs.AddItem(f"{cv.LANES_DIR}/{pat.format(shard=shard)}", dtype)
        inputs.Save()

    _agent, task, _ = _plan(tmp_path, unparented)
    assert task.ok
    used = common.step_transform_names(task)
    assert "kofamscan" in used, (
        "an unparented kofam product satisfied the mapper's parents={orfs} pin; "
        "the pin is not doing what the campaign relies on it to do")


def test_basename_collision_is_refused(tmp_path):
    saved = dict(cv.REUSED)
    try:
        cv.REUSED["annotation::kofamscan_results"] = "{shard}.faa"
        try:
            _plan(tmp_path, cv.make_on_inputs(SHARDS))
        except SystemExit as e:
            assert "basename collision" in str(e), str(e)
        else:
            raise AssertionError("a duplicate basename was accepted")
    finally:
        cv.REUSED.clear()
        cv.REUSED.update(saved)


if __name__ == "__main__":
    import tempfile

    for fn in (test_reuse_removes_the_producer,
               test_without_the_given_the_producer_comes_back,
               test_an_unparented_given_does_not_satisfy_the_mapper,
               test_basename_collision_is_refused):
        with tempfile.TemporaryDirectory() as td:
            fn(Path(td))
        print(f"  OK  {fn.__name__}")
    print("OK")
