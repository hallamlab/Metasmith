"""Supplying the finished kofam lane must REMOVE its producer from the plan.

This is the gate the reuse rests on. An earlier pass computed kofamscan for all
2,844 assemblies; the driver hands those products to the planner as givens,
parented per shard, and the planner is supposed to prefer them over producing
(`solver.py:534-554`).

(That pass also produced ProteinBERT embeddings. They are NOT reused -- their row
order was measured not to be the fasta's and the index was lost -- so
`proteinbert` is an EXPECTED transform here, not a forbidden one.)

The failure is silent. If a given's lineage does not satisfy the mapper's
`parents={orfs}` pin, `solver.py:603-605` re-solves with `include_produced=True`
and puts the producer back -- no error, no warning, just a plan that quietly
recomputes 82 million profile hits it already has. So the test is not "does it
plan" but "is `kofamscan` ABSENT", and the negative controls prove the test could
tell the difference.

Planning is type-level, so none of the paths need to exist; that is also what
lets this run on a machine holding none of the 30 GB.

Run: python tests/test_cyanoverse_gpr_reuse.py   (or under pytest)
"""
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

    # N shards fan out INSIDE each step; three steps, not 3N.
    assert len(task.plan.steps) == len(cv.EXPECTED_TRANSFORMS)
    for s in task.plan.steps:
        assert len(s.group_by_instances) == len(SHARDS), (
            f"{Path(s.transform._path).stem} groups "
            f"{len(s.group_by_instances)}, expected {len(SHARDS)}")


def test_without_the_given_the_producer_comes_back(tmp_path):
    """The negative control: the same call, minus the supplied product.

    Without this the positive test above could pass because the plan resolved to
    four steps for some entirely unrelated reason.
    """
    _agent, task, _ = _plan(tmp_path, None)
    assert task.ok
    used = common.step_transform_names(task)
    assert cv.FORBIDDEN_TRANSFORMS <= used, (
        f"expected the producer to be planned when nothing supplies it; "
        f"got {sorted(used)}")
    assert len(task.plan.steps) == 5


def test_an_unparented_given_does_not_satisfy_the_mapper(tmp_path):
    """Supply kofam with NO parentage -- the producer must come back.

    `gpr_4lane` pins every lane to `parents={orfs}`, so a product that does not
    descend from this shard's ORFs cannot satisfy it. If the planner accepted it
    anyway, the campaign could pair each shard with somebody else's annotations
    and still plan cleanly -- so this is the check that the pin is load-bearing
    rather than decorative.
    """
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
    """Two supplied inputs sharing a basename must fail HERE, not on the cluster.

    Nextflow stages a process's inputs by basename with no `stageAs`, so a
    collision silently hands a lane the wrong file -- and it has already killed a
    run in this project after every one of its lanes had succeeded, reporting
    `run completed` with 0 FAILED.
    """
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
